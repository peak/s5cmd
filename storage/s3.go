package storage

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	urlpkg "net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

var sentinelURL = urlpkg.URL{}

const (
	// deleteObjectsMax is the max allowed objects to be deleted on single HTTP
	// request.
	deleteObjectsMax = 1000

	// Amazon Accelerated Transfer endpoint
	transferAccelEndpoint = "s3-accelerate.amazonaws.com"

	// Google Cloud Storage endpoint
	gcsEndpoint = "storage.googleapis.com"

	// Tencent Cloud COS endpoint
	cosEndpoint = ".myqcloud.com"

	// the key of the object metadata which is used to handle retry decision on NoSuchUpload error
	metadataKeyRetryID = "s5cmd-upload-retry-id"
)

// Re-used AWS sessions dramatically improve performance.
var globalSessionCache = &SessionCache{
	sessions: map[Options]*session.Session{},
}

// S3 is a storage type which interacts with S3API, DownloaderAPI and
// UploaderAPI.
type S3 struct {
	api                    s3iface.S3API
	downloader             s3manageriface.DownloaderAPI
	uploader               s3manageriface.UploaderAPI
	endpointURL            urlpkg.URL
	dryRun                 bool
	useListObjectsV1       bool
	noSuchUploadRetryCount int
	requestPayer           string
}

func (s *S3) RequestPayer() *string {
	if s.requestPayer == "" {
		return nil
	}
	return &s.requestPayer
}

func parseEndpoint(endpoint string) (urlpkg.URL, error) {
	if endpoint == "" {
		return sentinelURL, nil
	}

	u, err := urlpkg.Parse(endpoint)
	if err != nil {
		return sentinelURL, fmt.Errorf("parse endpoint %q: %v", endpoint, err)
	}

	return *u, nil
}

// NewS3Storage creates new S3 session.
func newS3Storage(ctx context.Context, opts Options) (*S3, error) {
	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	awsSession, err := globalSessionCache.newSession(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &S3{
		api:                    s3.New(awsSession),
		downloader:             s3manager.NewDownloader(awsSession),
		uploader:               s3manager.NewUploader(awsSession),
		endpointURL:            endpointURL,
		dryRun:                 opts.DryRun,
		useListObjectsV1:       opts.UseListObjectsV1,
		requestPayer:           opts.RequestPayer,
		noSuchUploadRetryCount: opts.NoSuchUploadRetryCount,
	}, nil
}

// Stat retrieves metadata from S3 object without returning the object itself.
func (s *S3) Stat(ctx context.Context, url *url.URL) (*Object, error) {
	input := &s3.HeadObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.RequestPayer(),
	}
	if url.VersionID != "" {
		input.SetVersionId(url.VersionID)
	}

	output, err := s.api.HeadObjectWithContext(ctx, input)
	if err != nil {
		if errHasCode(err, "NotFound") {
			return nil, &ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		}
		return nil, err
	}

	etag := aws.StringValue(output.ETag)
	mod := aws.TimeValue(output.LastModified)

	obj := &Object{
		URL:     url,
		Etag:    strings.Trim(etag, `"`),
		ModTime: &mod,
		Size:    aws.Int64Value(output.ContentLength),
	}

	if s.noSuchUploadRetryCount > 0 {
		if retryID, ok := output.Metadata[metadataKeyRetryID]; ok {
			obj.retryID = *retryID
		}
	}

	return obj, nil
}

// List is a non-blocking S3 list operation which paginates and filters S3
// keys. If no object found or an error is encountered during this period,
// it sends these errors to object channel.
func (s *S3) List(ctx context.Context, url *url.URL, _ bool) <-chan *Object {
	if url.VersionID != "" || url.AllVersions {
		return s.listObjectVersions(ctx, url)
	}
	if s.useListObjectsV1 {
		return s.listObjects(ctx, url)
	}

	return s.listObjectsV2(ctx, url)
}

func (s *S3) listObjectVersions(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := s3.ListObjectVersionsInput{
		Bucket: aws.String(url.Bucket),
		Prefix: aws.String(url.Prefix),
	}

	if url.Delimiter != "" {
		listInput.SetDelimiter(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		err := s.api.ListObjectVersionsPagesWithContext(ctx, &listInput,
			func(p *s3.ListObjectVersionsOutput, lastPage bool) bool {
				for _, c := range p.CommonPrefixes {
					prefix := aws.StringValue(c.Prefix)
					if !url.Match(prefix) {
						continue
					}

					newurl := url.Clone()
					newurl.Path = prefix
					objCh <- &Object{
						URL:  newurl,
						Type: ObjectType{os.ModeDir},
					}

					objectFound = true
				}
				// track the instant object iteration began,
				// so it can be used to bypass objects created after this instant
				if now.IsZero() {
					now = time.Now().UTC()
				}

				// iterate over all versions of the objects (except the delete markers)
				for _, v := range p.Versions {
					key := aws.StringValue(v.Key)
					if !url.Match(key) {
						continue
					}
					if url.VersionID != "" && url.VersionID != aws.StringValue(v.VersionId) {
						continue
					}

					mod := aws.TimeValue(v.LastModified).UTC()
					if mod.After(now) {
						objectFound = true
						continue
					}

					var objtype os.FileMode
					if strings.HasSuffix(key, "/") {
						objtype = os.ModeDir
					}

					newurl := url.Clone()
					newurl.Path = aws.StringValue(v.Key)
					newurl.VersionID = aws.StringValue(v.VersionId)
					etag := aws.StringValue(v.ETag)

					objCh <- &Object{
						URL:          newurl,
						Etag:         strings.Trim(etag, `"`),
						ModTime:      &mod,
						Type:         ObjectType{objtype},
						Size:         aws.Int64Value(v.Size),
						StorageClass: StorageClass(aws.StringValue(v.StorageClass)),
					}

					objectFound = true
				}

				// iterate over all delete marker versions of the objects
				for _, d := range p.DeleteMarkers {
					key := aws.StringValue(d.Key)
					if !url.Match(key) {
						continue
					}
					if url.VersionID != "" && url.VersionID != aws.StringValue(d.VersionId) {
						continue
					}

					mod := aws.TimeValue(d.LastModified).UTC()
					if mod.After(now) {
						objectFound = true
						continue
					}

					var objtype os.FileMode
					if strings.HasSuffix(key, "/") {
						objtype = os.ModeDir
					}

					newurl := url.Clone()
					newurl.Path = aws.StringValue(d.Key)
					newurl.VersionID = aws.StringValue(d.VersionId)

					objCh <- &Object{
						URL:     newurl,
						ModTime: &mod,
						Type:    ObjectType{objtype},
						Size:    0,
					}

					objectFound = true
				}

				return !lastPage
			})
		if err != nil {
			objCh <- &Object{Err: err}
			return
		}

		if !objectFound && !url.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

func (s *S3) listObjectsV2(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := s3.ListObjectsV2Input{
		Bucket:       aws.String(url.Bucket),
		Prefix:       aws.String(url.Prefix),
		RequestPayer: s.RequestPayer(),
	}

	if url.Delimiter != "" {
		listInput.SetDelimiter(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		err := s.api.ListObjectsV2PagesWithContext(ctx, &listInput, func(p *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, c := range p.CommonPrefixes {
				prefix := aws.StringValue(c.Prefix)
				if !url.Match(prefix) {
					continue
				}

				newurl := url.Clone()
				newurl.Path = prefix
				objCh <- &Object{
					URL:  newurl,
					Type: ObjectType{os.ModeDir},
				}

				objectFound = true
			}
			// track the instant object iteration began,
			// so it can be used to bypass objects created after this instant
			if now.IsZero() {
				now = time.Now().UTC()
			}

			for _, c := range p.Contents {
				key := aws.StringValue(c.Key)
				if !url.Match(key) {
					continue
				}

				mod := aws.TimeValue(c.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.StringValue(c.Key)
				etag := aws.StringValue(c.ETag)

				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(etag, `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         aws.Int64Value(c.Size),
					StorageClass: StorageClass(aws.StringValue(c.StorageClass)),
				}

				objectFound = true
			}

			return !lastPage
		})
		if err != nil {
			objCh <- &Object{Err: err}
			return
		}

		if !objectFound && !url.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

// listObjects is used for cloud services that does not support S3
// ListObjectsV2 API. I'm looking at you GCS.
func (s *S3) listObjects(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := s3.ListObjectsInput{
		Bucket:       aws.String(url.Bucket),
		Prefix:       aws.String(url.Prefix),
		RequestPayer: s.RequestPayer(),
	}

	if url.Delimiter != "" {
		listInput.SetDelimiter(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		err := s.api.ListObjectsPagesWithContext(ctx, &listInput, func(p *s3.ListObjectsOutput, lastPage bool) bool {
			for _, c := range p.CommonPrefixes {
				prefix := aws.StringValue(c.Prefix)
				if !url.Match(prefix) {
					continue
				}

				newurl := url.Clone()
				newurl.Path = prefix
				objCh <- &Object{
					URL:  newurl,
					Type: ObjectType{os.ModeDir},
				}

				objectFound = true
			}
			// track the instant object iteration began,
			// so it can be used to bypass objects created after this instant
			if now.IsZero() {
				now = time.Now().UTC()
			}

			for _, c := range p.Contents {
				key := aws.StringValue(c.Key)
				if !url.Match(key) {
					continue
				}

				mod := aws.TimeValue(c.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.StringValue(c.Key)
				etag := aws.StringValue(c.ETag)

				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(etag, `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         aws.Int64Value(c.Size),
					StorageClass: StorageClass(aws.StringValue(c.StorageClass)),
				}

				objectFound = true
			}

			return !lastPage
		})
		if err != nil {
			objCh <- &Object{Err: err}
			return
		}

		if !objectFound && !url.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

// Copy is a single-object copy operation which copies objects to S3
// destination from another S3 source.
func (s *S3) Copy(ctx context.Context, from, to *url.URL, metadata Metadata) error {
	if s.dryRun {
		return nil
	}

	// SDK expects CopySource like "bucket[/key]"
	copySource := from.EscapedPath()

	input := &s3.CopyObjectInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		CopySource:   aws.String(copySource),
		RequestPayer: s.RequestPayer(),
	}
	if from.VersionID != "" {
		// Unlike many other *Input and *Output types version ID is not a field,
		// but rather something that must be appended to CopySource string.
		// This is same in both v1 and v2 SDKs:
		// https://pkg.go.dev/github.com/aws/aws-sdk-go/service/s3#CopyObjectInput
		// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3#CopyObjectInput
		input.CopySource = aws.String(copySource + "?versionId=" + from.VersionID)
	}

	storageClass := metadata.StorageClass
	if storageClass != "" {
		input.StorageClass = aws.String(storageClass)
	}

	acl := metadata.ACL
	if acl != "" {
		input.ACL = aws.String(acl)
	}

	cacheControl := metadata.CacheControl
	if cacheControl != "" {
		input.CacheControl = aws.String(cacheControl)
	}

	expires := metadata.Expires
	if expires != "" {
		t, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			return err
		}
		input.Expires = aws.Time(t)
	}

	sseEncryption := metadata.EncryptionMethod
	if sseEncryption != "" {
		input.ServerSideEncryption = aws.String(sseEncryption)
		sseKmsKeyID := metadata.EncryptionKeyID
		if sseKmsKeyID != "" {
			input.SSEKMSKeyId = aws.String(sseKmsKeyID)
		}
	}

	contentEncoding := metadata.ContentEncoding
	if contentEncoding != "" {
		input.ContentEncoding = aws.String(contentEncoding)
	}

	contentDisposition := metadata.ContentDisposition
	if contentDisposition != "" {
		input.ContentDisposition = aws.String(contentDisposition)
	}

	// add retry ID to the object metadata
	if s.noSuchUploadRetryCount > 0 {
		input.Metadata[metadataKeyRetryID] = generateRetryID()
	}

	if metadata.Directive != "" {
		input.MetadataDirective = aws.String(metadata.Directive)
	}

	if metadata.ContentType != "" {
		input.ContentType = aws.String(metadata.ContentType)
	}

	if len(metadata.UserDefined) != 0 {
		m := make(map[string]*string)
		for k, v := range metadata.UserDefined {
			m[k] = aws.String(v)
		}
		input.Metadata = m
	}

	_, err := s.api.CopyObject(input)
	return err
}

// Read fetches the remote object and returns its contents as an io.ReadCloser.
func (s *S3) Read(ctx context.Context, src *url.URL) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket:       aws.String(src.Bucket),
		Key:          aws.String(src.Path),
		RequestPayer: s.RequestPayer(),
	}
	if src.VersionID != "" {
		input.SetVersionId(src.VersionID)
	}

	resp, err := s.api.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *S3) Presign(ctx context.Context, from *url.URL, expire time.Duration) (string, error) {
	input := &s3.GetObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.RequestPayer(),
	}

	req, _ := s.api.GetObjectRequest(input)

	return req.Presign(expire)
}

// Get is a multipart download operation which downloads S3 objects into any
// destination that implements io.WriterAt interface.
// Makes a single 'GetObject' call if 'concurrency' is 1 and ignores 'partSize'.
func (s *S3) Get(
	ctx context.Context,
	from *url.URL,
	to io.WriterAt,
	concurrency int,
	partSize int64,
) (int64, error) {
	if s.dryRun {
		return 0, nil
	}

	input := &s3.GetObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.RequestPayer(),
	}
	if from.VersionID != "" {
		input.VersionId = aws.String(from.VersionID)
	}

	return s.downloader.DownloadWithContext(ctx, to, input, func(u *s3manager.Downloader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	})
}

type SelectQuery struct {
	InputFormat           string
	InputContentStructure string
	FileHeaderInfo        string
	OutputFormat          string
	ExpressionType        string
	Expression            string
	CompressionType       string
}

type eventType string

const (
	jsonType    eventType = "json"
	csvType     eventType = "csv"
	parquetType eventType = "parquet"
)

func parseInputSerialization(e eventType, c string, delimiter string, headerInfo string) (*s3.InputSerialization, error) {
	var s *s3.InputSerialization

	switch e {
	case jsonType:
		s = &s3.InputSerialization{
			JSON: &s3.JSONInput{
				Type: aws.String(delimiter),
			},
		}
		if c != "" {
			s.CompressionType = aws.String(c)
		}
	case csvType:
		s = &s3.InputSerialization{
			CSV: &s3.CSVInput{
				FieldDelimiter: aws.String(delimiter),
				FileHeaderInfo: aws.String(headerInfo),
			},
		}
		if c != "" {
			s.CompressionType = aws.String(c)
		}
	case parquetType:
		s = &s3.InputSerialization{
			Parquet: &s3.ParquetInput{},
		}
	default:
		return nil, fmt.Errorf("input format is not valid")
	}

	return s, nil
}

func parseOutputSerialization(e eventType, delimiter string, reader io.Reader) (*s3.OutputSerialization, EventStreamDecoder, error) {
	var s *s3.OutputSerialization
	var decoder EventStreamDecoder

	switch e {
	case jsonType:
		s = &s3.OutputSerialization{
			JSON: &s3.JSONOutput{},
		}
		decoder = NewJSONDecoder(reader)
	case csvType:
		s = &s3.OutputSerialization{
			CSV: &s3.CSVOutput{
				FieldDelimiter: aws.String(delimiter),
			},
		}
		decoder = NewCsvDecoder(reader)
	default:
		return nil, nil, fmt.Errorf("output serialization is not valid")
	}
	return s, decoder, nil
}

func (s *S3) Select(ctx context.Context, url *url.URL, query *SelectQuery, resultCh chan<- json.RawMessage) error {
	if s.dryRun {
		return nil
	}

	var (
		inputFormat  *s3.InputSerialization
		outputFormat *s3.OutputSerialization
		decoder      EventStreamDecoder
	)
	reader, writer := io.Pipe()

	inputFormat, err := parseInputSerialization(
		eventType(query.InputFormat),
		query.CompressionType,
		query.InputContentStructure,
		query.FileHeaderInfo,
	)
	if err != nil {
		return err
	}

	// set the delimiter to ','. Otherwise, delimiter is set to "lines" or "document"
	// for json queries.
	if query.InputFormat == string(jsonType) && query.OutputFormat == string(csvType) {
		query.InputContentStructure = ","
	}

	outputFormat, decoder, err = parseOutputSerialization(
		eventType(query.OutputFormat),
		query.InputContentStructure,
		reader,
	)
	if err != nil {
		return err
	}

	input := &s3.SelectObjectContentInput{
		Bucket:              aws.String(url.Bucket),
		Key:                 aws.String(url.Path),
		ExpressionType:      aws.String(query.ExpressionType),
		Expression:          aws.String(query.Expression),
		InputSerialization:  inputFormat,
		OutputSerialization: outputFormat,
	}

	resp, err := s.api.SelectObjectContentWithContext(ctx, input)
	if err != nil {
		return err
	}

	go func() {
		defer writer.Close()

		eventch := resp.EventStream.Reader.Events()
		defer resp.EventStream.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-eventch:
				if !ok {
					return
				}

				switch e := event.(type) {
				case *s3.RecordsEvent:
					writer.Write(e.Payload)
				}
			}
		}
	}()
	for {
		val, err := decoder.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		resultCh <- val
	}

	return resp.EventStream.Reader.Err()
}

// Put is a multipart upload operation to upload resources, which implements
// io.Reader interface, into S3 destination.
func (s *S3) Put(
	ctx context.Context,
	reader io.Reader,
	to *url.URL,
	metadata Metadata,
	concurrency int,
	partSize int64,
) error {
	if s.dryRun {
		return nil
	}

	contentType := metadata.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	input := &s3manager.UploadInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		Body:         reader,
		ContentType:  aws.String(contentType),
		Metadata:     make(map[string]*string),
		RequestPayer: s.RequestPayer(),
	}

	storageClass := metadata.StorageClass
	if storageClass != "" {
		input.StorageClass = aws.String(storageClass)
	}

	acl := metadata.ACL
	if acl != "" {
		input.ACL = aws.String(acl)
	}

	cacheControl := metadata.CacheControl
	if cacheControl != "" {
		input.CacheControl = aws.String(cacheControl)
	}

	expires := metadata.Expires
	if expires != "" {
		t, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			return err
		}
		input.Expires = aws.Time(t)
	}

	sseEncryption := metadata.EncryptionMethod
	if sseEncryption != "" {
		input.ServerSideEncryption = aws.String(sseEncryption)
		sseKmsKeyID := metadata.EncryptionKeyID
		if sseKmsKeyID != "" {
			input.SSEKMSKeyId = aws.String(sseKmsKeyID)
		}
	}

	contentEncoding := metadata.ContentEncoding
	if contentEncoding != "" {
		input.ContentEncoding = aws.String(contentEncoding)
	}

	contentDisposition := metadata.ContentDisposition
	if contentDisposition != "" {
		input.ContentDisposition = aws.String(contentDisposition)
	}

	// add retry ID to the object metadata
	if s.noSuchUploadRetryCount > 0 {
		input.Metadata[metadataKeyRetryID] = generateRetryID()
	}

	if len(metadata.UserDefined) != 0 {
		m := make(map[string]*string)
		for k, v := range metadata.UserDefined {
			m[k] = aws.String(v)
		}
		input.Metadata = m
	}

	uploaderOptsFn := func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	}
	_, err := s.uploader.UploadWithContext(ctx, input, uploaderOptsFn)

	if errHasCode(err, s3.ErrCodeNoSuchUpload) && s.noSuchUploadRetryCount > 0 {
		return s.retryOnNoSuchUpload(ctx, to, input, err, uploaderOptsFn)
	}

	return err
}

func (s *S3) retryOnNoSuchUpload(ctx aws.Context, to *url.URL, input *s3manager.UploadInput,
	err error, uploaderOpts ...func(*s3manager.Uploader),
) error {
	var expectedRetryID string
	if ID, ok := input.Metadata[metadataKeyRetryID]; ok {
		expectedRetryID = *ID
	}

	attempts := 0
	for ; errHasCode(err, s3.ErrCodeNoSuchUpload) && attempts < s.noSuchUploadRetryCount; attempts++ {
		// check if object exists and has the retry ID we provided, if it does
		// then it means that one of previous uploads was succesfull despite the received error.
		obj, sErr := s.Stat(ctx, to)
		if sErr == nil && obj.retryID == expectedRetryID {
			err = nil
			break
		}

		msg := log.DebugMessage{Err: fmt.Sprintf("Retrying to upload %v upon error: %q", to, err.Error())}
		log.Debug(msg)

		_, err = s.uploader.UploadWithContext(ctx, input, uploaderOpts...)
	}

	if errHasCode(err, s3.ErrCodeNoSuchUpload) && s.noSuchUploadRetryCount > 0 {
		err = awserr.New(s3.ErrCodeNoSuchUpload, fmt.Sprintf(
			"RetryOnNoSuchUpload: %v attempts to retry resulted in %v", attempts,
			s3.ErrCodeNoSuchUpload), err)
	}
	return err
}

// chunk is an object identifier container which is used on MultiDelete
// operations. Since DeleteObjects API allows deleting objects up to 1000,
// splitting keys into multiple chunks is required.
type chunk struct {
	Bucket string
	Keys   []*s3.ObjectIdentifier
}

// calculateChunks calculates chunks for given URL channel and returns
// read-only chunk channel.
func (s *S3) calculateChunks(ch <-chan *url.URL) <-chan chunk {
	chunkch := make(chan chunk)

	chunkSize := deleteObjectsMax
	// delete each object individually if using gcs.
	if IsGoogleEndpoint(s.endpointURL) {
		chunkSize = 1
	}

	go func() {
		defer close(chunkch)

		var keys []*s3.ObjectIdentifier
		initKeys := func() {
			keys = make([]*s3.ObjectIdentifier, 0)
		}

		var bucket string
		for url := range ch {
			bucket = url.Bucket

			objid := &s3.ObjectIdentifier{Key: aws.String(url.Path)}
			if url.VersionID != "" {
				objid.VersionId = &url.VersionID
			}

			keys = append(keys, objid)
			if len(keys) == chunkSize {
				chunkch <- chunk{
					Bucket: bucket,
					Keys:   keys,
				}
				initKeys()
			}
		}

		if len(keys) > 0 {
			chunkch <- chunk{
				Bucket: bucket,
				Keys:   keys,
			}
		}
	}()

	return chunkch
}

// Delete is a single object delete operation.
func (s *S3) Delete(ctx context.Context, url *url.URL) error {
	chunk := chunk{
		Bucket: url.Bucket,
		Keys: []*s3.ObjectIdentifier{
			{Key: aws.String(url.Path)},
		},
	}

	resultch := make(chan *Object, 1)
	defer close(resultch)

	s.doDelete(ctx, chunk, resultch)
	obj := <-resultch
	return obj.Err
}

// doDelete deletes the given keys given by chunk. Results are piggybacked via
// the Object container.
func (s *S3) doDelete(ctx context.Context, chunk chunk, resultch chan *Object) {
	if s.dryRun {
		for _, k := range chunk.Keys {
			key := fmt.Sprintf("s3://%v/%v", chunk.Bucket, aws.StringValue(k.Key))
			url, _ := url.New(key)
			url.VersionID = aws.StringValue(k.VersionId)
			resultch <- &Object{URL: url}
		}
		return
	}

	// GCS does not support multi delete.
	if IsGoogleEndpoint(s.endpointURL) {
		for _, k := range chunk.Keys {
			_, err := s.api.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Bucket:       aws.String(chunk.Bucket),
				Key:          k.Key,
				RequestPayer: s.RequestPayer(),
			})
			if err != nil {
				resultch <- &Object{Err: err}
				return
			}
			key := fmt.Sprintf("s3://%v/%v", chunk.Bucket, aws.StringValue(k.Key))
			url, _ := url.New(key)
			resultch <- &Object{URL: url}
		}
		return
	}

	bucket := chunk.Bucket
	o, err := s.api.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket:       aws.String(bucket),
		Delete:       &s3.Delete{Objects: chunk.Keys},
		RequestPayer: s.RequestPayer(),
	})
	if err != nil {
		resultch <- &Object{Err: err}
		return
	}

	for _, d := range o.Deleted {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.StringValue(d.Key))
		url, _ := url.New(key)
		url.VersionID = aws.StringValue(d.VersionId)
		resultch <- &Object{URL: url}
	}

	for _, e := range o.Errors {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.StringValue(e.Key))
		url, _ := url.New(key)
		url.VersionID = aws.StringValue(e.VersionId)

		resultch <- &Object{
			URL: url,
			Err: fmt.Errorf("%v", aws.StringValue(e.Message)),
		}
	}
}

// MultiDelete is a asynchronous removal operation for multiple objects.
// It reads given url channel, creates multiple chunks and run these
// chunks in parallel. Each chunk may have at most 1000 objects since DeleteObjects
// API has a limitation.
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html.
func (s *S3) MultiDelete(ctx context.Context, urlch <-chan *url.URL) <-chan *Object {
	resultch := make(chan *Object)

	go func() {
		sem := make(chan struct{}, 10)
		defer close(sem)
		defer close(resultch)

		chunks := s.calculateChunks(urlch)

		var wg sync.WaitGroup
		for chunk := range chunks {
			chunk := chunk

			wg.Add(1)
			sem <- struct{}{}

			go func() {
				defer wg.Done()
				s.doDelete(ctx, chunk, resultch)
				<-sem
			}()
		}

		wg.Wait()
	}()

	return resultch
}

// ListBuckets is a blocking list-operation which gets bucket list and returns
// the buckets that match with given prefix.
func (s *S3) ListBuckets(ctx context.Context, prefix string) ([]Bucket, error) {
	o, err := s.api.ListBucketsWithContext(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []Bucket
	for _, b := range o.Buckets {
		bucketName := aws.StringValue(b.Name)
		if prefix == "" || strings.HasPrefix(bucketName, prefix) {
			buckets = append(buckets, Bucket{
				CreationDate: aws.TimeValue(b.CreationDate),
				Name:         bucketName,
			})
		}
	}
	return buckets, nil
}

// MakeBucket creates an S3 bucket with the given name.
func (s *S3) MakeBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// RemoveBucket removes an S3 bucket with the given name.
func (s *S3) RemoveBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// SetBucketVersioning sets the versioning property of the bucket
func (s *S3) SetBucketVersioning(ctx context.Context, versioningStatus, bucket string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.PutBucketVersioningWithContext(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String(versioningStatus),
		},
	})
	return err
}

// GetBucketVersioning returnsversioning property of the bucket
func (s *S3) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	output, err := s.api.GetBucketVersioningWithContext(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err != nil || output.Status == nil {
		return "", err
	}

	return *output.Status, nil
}

func (s *S3) HeadBucket(ctx context.Context, url *url.URL) error {
	_, err := s.api.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(url.Bucket),
	})
	return err
}

func (s *S3) HeadObject(ctx context.Context, url *url.URL) (*Object, *Metadata, error) {
	input := &s3.HeadObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.RequestPayer(),
	}

	if url.VersionID != "" {
		input.SetVersionId(url.VersionID)
	}

	output, err := s.api.HeadObjectWithContext(ctx, input)
	if err != nil {
		if errHasCode(err, "NotFound") {
			return nil, nil, &ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		}
		return nil, nil, err
	}

	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#AmazonS3-HeadObject-response-header-StorageClass
	// If the object's storage class is STANDARD, this header is not returned in the response.
	storageClassStr := "STANDARD"
	if output.StorageClass != nil {
		storageClassStr = aws.StringValue(output.StorageClass)
	}

	obj := &Object{
		URL:          url,
		ModTime:      output.LastModified,
		Etag:         strings.Trim(aws.StringValue(output.ETag), `"`),
		Size:         aws.Int64Value(output.ContentLength),
		StorageClass: StorageClass(storageClassStr),
	}

	metadata := &Metadata{
		ContentType:      aws.StringValue(output.ContentType),
		EncryptionMethod: aws.StringValue(output.ServerSideEncryption),
		UserDefined:      aws.StringValueMap(output.Metadata),
	}

	return obj, metadata, nil
}

type sdkLogger struct{}

func (l sdkLogger) Log(args ...interface{}) {
	msg := log.TraceMessage{
		Message: fmt.Sprint(args...),
	}
	log.Trace(msg)
}

// SessionCache holds session.Session according to s3Opts and it synchronizes
// access/modification.
type SessionCache struct {
	sync.Mutex
	sessions map[Options]*session.Session
}

// newSession initializes a new AWS session with region fallback and custom
// options.
func (sc *SessionCache) newSession(ctx context.Context, opts Options) (*session.Session, error) {
	sc.Lock()
	defer sc.Unlock()

	if sess, ok := sc.sessions[opts]; ok {
		return sess, nil
	}

	awsCfg := aws.NewConfig()

	if opts.NoSignRequest {
		// do not sign requests when making service API calls
		awsCfg = awsCfg.WithCredentials(credentials.AnonymousCredentials)
	} else if opts.CredentialFile != "" || opts.Profile != "" {
		awsCfg = awsCfg.WithCredentials(
			credentials.NewSharedCredentials(opts.CredentialFile, opts.Profile),
		)
	}

	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	// use virtual-host-style if the endpoint is known to support it,
	// otherwise use the path-style approach.
	isVirtualHostStyle := isVirtualHostStyle(endpointURL)

	useAccelerate := supportsTransferAcceleration(endpointURL)
	// AWS SDK handles transfer acceleration automatically. Setting the
	// Endpoint to a transfer acceleration endpoint would cause bucket
	// operations fail.
	if useAccelerate {
		endpointURL = sentinelURL
	}

	var httpClient *http.Client
	if opts.NoVerifySSL {
		httpClient = insecureHTTPClient
	}
	awsCfg = awsCfg.
		WithEndpoint(endpointURL.String()).
		WithS3ForcePathStyle(!isVirtualHostStyle).
		WithS3UseAccelerate(useAccelerate).
		WithHTTPClient(httpClient).
		// TODO WithLowerCaseHeaderMaps and WithDisableRestProtocolURICleaning options
		// are going to be unnecessary and unsupported in AWS-SDK version 2.
		// They should be removed during migration.
		WithLowerCaseHeaderMaps(true).
		// Disable URI cleaning to allow adjacent slashes to be used in S3 object keys.
		WithDisableRestProtocolURICleaning(true)

	if opts.LogLevel == log.LevelTrace {
		awsCfg = awsCfg.WithLogLevel(aws.LogDebug).
			WithLogger(sdkLogger{})
	}

	awsCfg.Retryer = newCustomRetryer(opts.MaxRetries)

	useSharedConfig := session.SharedConfigEnable
	{
		// Reverse of what the SDK does: if AWS_SDK_LOAD_CONFIG is 0 (or a
		// falsy value) disable shared configs
		loadCfg := os.Getenv("AWS_SDK_LOAD_CONFIG")
		if loadCfg != "" {
			if enable, _ := strconv.ParseBool(loadCfg); !enable {
				useSharedConfig = session.SharedConfigDisable
			}
		}
	}

	sess, err := session.NewSessionWithOptions(
		session.Options{
			Config:            *awsCfg,
			SharedConfigState: useSharedConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	// get region of the bucket and create session accordingly. if the region
	// is not provided, it means we want region-independent session
	// for operations such as listing buckets, making a new bucket etc.
	// only get bucket region when it is not specified.
	if opts.region != "" {
		sess.Config.Region = aws.String(opts.region)
	} else {
		if err := setSessionRegion(ctx, sess, opts.bucket); err != nil {
			return nil, err
		}
	}

	sc.sessions[opts] = sess

	return sess, nil
}

func (sc *SessionCache) clear() {
	sc.Lock()
	defer sc.Unlock()
	sc.sessions = map[Options]*session.Session{}
}

func setSessionRegion(ctx context.Context, sess *session.Session, bucket string) error {
	region := aws.StringValue(sess.Config.Region)

	if region != "" {
		return nil
	}

	// set default region
	sess.Config.Region = aws.String(endpoints.UsEast1RegionID)

	if bucket == "" {
		return nil
	}

	// auto-detection
	region, err := s3manager.GetBucketRegion(ctx, sess, bucket, "", func(r *request.Request) {
		// s3manager.GetBucketRegion uses Path style addressing and
		// AnonymousCredentials by default, updating Request's Config to match
		// the session config.
		r.Config.S3ForcePathStyle = sess.Config.S3ForcePathStyle
		r.Config.Credentials = sess.Config.Credentials
	})
	if err != nil {
		if errHasCode(err, "NotFound") {
			return err
		}
		// don't deny any request to the service if region auto-fetching
		// receives an error. Delegate error handling to command execution.
		err = fmt.Errorf("session: fetching region failed: %v", err)
		msg := log.ErrorMessage{Err: err.Error()}
		log.Error(msg)
	} else {
		sess.Config.Region = aws.String(region)
	}

	return nil
}

// customRetryer wraps the SDK's built in DefaultRetryer adding additional
// error codes. Such as, retry for S3 InternalError code.
type customRetryer struct {
	client.DefaultRetryer
}

func newCustomRetryer(maxRetries int) *customRetryer {
	return &customRetryer{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries: maxRetries,
		},
	}
}

// ShouldRetry overrides SDK's built in DefaultRetryer, adding custom retry
// logics that are not included in the SDK.
func (c *customRetryer) ShouldRetry(req *request.Request) bool {
	shouldRetry := errHasCode(req.Error, "InternalError") || errHasCode(req.Error, "RequestTimeTooSkewed") || errHasCode(req.Error, "SlowDown") || strings.Contains(req.Error.Error(), "connection reset") || strings.Contains(req.Error.Error(), "connection timed out")
	if !shouldRetry {
		shouldRetry = c.DefaultRetryer.ShouldRetry(req)
	}

	// Errors related to tokens
	if errHasCode(req.Error, "ExpiredToken") || errHasCode(req.Error, "ExpiredTokenException") || errHasCode(req.Error, "InvalidToken") {
		return false
	}

	if shouldRetry && req.Error != nil {
		err := fmt.Errorf("retryable error: %v", req.Error)
		msg := log.DebugMessage{Err: err.Error()}
		log.Debug(msg)
	}

	return shouldRetry
}

var insecureHTTPClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Proxy:           http.ProxyFromEnvironment,
	},
}

func supportsTransferAcceleration(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == transferAccelEndpoint
}

func IsGoogleEndpoint(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == gcsEndpoint
}

func IsTencentEndpoint(endpoint urlpkg.URL) bool {
	return strings.HasSuffix(endpoint.Hostname(), cosEndpoint)
}

// isVirtualHostStyle reports whether the given endpoint supports S3 virtual
// host style bucket name resolving. If a custom S3 API compatible endpoint is
// given, resolve the bucketname from the URL path.
func isVirtualHostStyle(endpoint urlpkg.URL) bool {
	return endpoint == sentinelURL || supportsTransferAcceleration(endpoint) || IsGoogleEndpoint(endpoint) || IsTencentEndpoint(endpoint)
}

func errHasCode(err error, code string) bool {
	if err == nil || code == "" {
		return false
	}

	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		if awsErr.Code() == code {
			return true
		}
	}

	var multiUploadErr s3manager.MultiUploadFailure
	if errors.As(err, &multiUploadErr) {
		return errHasCode(multiUploadErr.OrigErr(), code)
	}

	return false
}

// IsCancelationError reports whether given error is a storage related
// cancelation error.
func IsCancelationError(err error) bool {
	return errHasCode(err, request.CanceledErrorCode)
}

// generate a retry ID for this upload attempt
func generateRetryID() *string {
	num, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	return aws.String(num.String())
}

// EventStreamDecoder decodes a s3.Event with
// the given decoder.
type EventStreamDecoder interface {
	Decode() ([]byte, error)
}

type JSONDecoder struct {
	decoder *json.Decoder
}

func NewJSONDecoder(reader io.Reader) EventStreamDecoder {
	return &JSONDecoder{
		decoder: json.NewDecoder(reader),
	}
}

func (jd *JSONDecoder) Decode() ([]byte, error) {
	var val json.RawMessage
	err := jd.decoder.Decode(&val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

type CsvDecoder struct {
	decoder   *csv.Reader
	delimiter string
}

func NewCsvDecoder(reader io.Reader) EventStreamDecoder {
	csvDecoder := &CsvDecoder{
		decoder:   csv.NewReader(reader),
		delimiter: ",",
	}
	// returned values from AWS has double quotes in it
	// so we enable lazy quotes
	csvDecoder.decoder.LazyQuotes = true
	return csvDecoder
}

func (cd *CsvDecoder) Decode() ([]byte, error) {
	res, err := cd.decoder.Read()
	if err != nil {
		return nil, err
	}

	result := []byte{}
	for i, str := range res {
		if i != len(res)-1 {
			str = fmt.Sprintf("%s%s", str, cd.delimiter)
		}
		result = append(result, []byte(str)...)
	}
	return result, nil
}
