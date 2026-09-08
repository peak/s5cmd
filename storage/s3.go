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
	"maps"
	"math"
	"math/big"
	"net/http"
	urlpkg "net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"

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

	// the key of the object metadata which is used to handle retry decision on NoSuchUpload error
	metadataKeyRetryID = "s5cmd-upload-retry-id"

	// defaultRegion is used when no region is given and none can be detected.
	defaultRegion = "us-east-1"
)

// Re-used AWS configs dramatically improve performance.
var globalSessionCache = &SessionCache{
	sessions: map[Options]aws.Config{},
}

// s3API is the subset of the S3 client used by S3. It exists so that the
// client can be replaced by a mock in tests; the v2 SDK ships no equivalent
// of v1's s3iface.
type s3API interface {
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	DeleteBucket(context.Context, *s3.DeleteBucketInput, ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListBuckets(context.Context, *s3.ListBucketsInput, ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	ListObjects(context.Context, *s3.ListObjectsInput, ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	ListObjectVersions(context.Context, *s3.ListObjectVersionsInput, ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	PutBucketVersioning(context.Context, *s3.PutBucketVersioningInput, ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error)
	SelectObjectContent(context.Context, *s3.SelectObjectContentInput, ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error)
}

// The transfer manager is deprecated in favour of
// github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager, which is still
// pre-1.0 (v0.4.x). s5cmd stays on the stable v1.x package until its
// successor reaches v1; these aliases keep that decision, and the
// deprecation warning it implies, in one place.
type (
	//lint:ignore SA1019 successor is pre-1.0; revisit when it reaches v1
	s3Uploader = manager.Uploader
	//lint:ignore SA1019 successor is pre-1.0; revisit when it reaches v1
	s3Downloader = manager.Downloader
)

// uploaderAPI and downloaderAPI mirror the parts of the transfer manager the
// storage layer uses, so that they can be faked in tests.
type uploaderAPI interface {
	Upload(context.Context, *s3.PutObjectInput, ...func(*s3Uploader)) (*manager.UploadOutput, error)
}

type downloaderAPI interface {
	Download(context.Context, io.WriterAt, *s3.GetObjectInput, ...func(*s3Downloader)) (int64, error)
}

// S3 is a storage type which interacts with S3API, DownloaderAPI and
// UploaderAPI.
type S3 struct {
	api                    s3API
	presigner              *s3.PresignClient
	downloader             downloaderAPI
	uploader               uploaderAPI
	endpointURL            urlpkg.URL
	dryRun                 bool
	useListObjectsV1       bool
	noSuchUploadRetryCount int
	requestPayer           string
}

func (s *S3) RequestPayer() types.RequestPayer {
	if s.requestPayer == "" {
		return ""
	}
	return types.RequestPayer(s.requestPayer)
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

// s3Options returns the client options implied by opts, shared by the S3
// client and by the transfer manager.
func s3Options(opts Options) (func(*s3.Options), error) {
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

	return func(o *s3.Options) {
		if endpointURL != sentinelURL {
			o.BaseEndpoint = aws.String(endpointURL.String())
		}
		o.UsePathStyle = !isVirtualHostStyle
		o.UseAccelerate = useAccelerate

		// v2 computes a CRC32 checksum for every upload by default. Many
		// S3-compatible services reject or mishandle those, and v1 never
		// sent them, so only send a checksum where the API requires one.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}, nil
}

// NewS3Storage creates new S3 session.
func newS3Storage(ctx context.Context, opts Options) (*S3, error) {
	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	cfg, err := globalSessionCache.newSession(ctx, opts)
	if err != nil {
		return nil, err
	}

	optFn, err := s3Options(opts)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, optFn)

	return &S3{
		api:       client,
		presigner: s3.NewPresignClient(client),
		//lint:ignore SA1019 successor is pre-1.0; revisit when it reaches v1
		downloader: manager.NewDownloader(client),
		//lint:ignore SA1019 successor is pre-1.0; revisit when it reaches v1
		uploader:               manager.NewUploader(client),
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
		input.VersionId = aws.String(url.VersionID)
	}

	output, err := s.api.HeadObject(ctx, input)
	if err != nil {
		if errHasCode(err, "NotFound") {
			return nil, &ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		}
		return nil, err
	}

	etag := aws.ToString(output.ETag)
	mod := aws.ToTime(output.LastModified)

	obj := &Object{
		URL:     url,
		Etag:    strings.Trim(etag, `"`),
		ModTime: &mod,
		Size:    aws.ToInt64(output.ContentLength),
	}

	if s.noSuchUploadRetryCount > 0 {
		if retryID, ok := output.Metadata[metadataKeyRetryID]; ok {
			obj.retryID = retryID
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
		listInput.Delimiter = aws.String(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		paginator := s3.NewListObjectVersionsPaginator(s.api, &listInput)
		for paginator.HasMorePages() {
			p, err := paginator.NextPage(ctx)
			if err != nil {
				objCh <- &Object{Err: err}
				return
			}

			for _, c := range p.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
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
				key := aws.ToString(v.Key)
				if !url.Match(key) {
					continue
				}
				if url.VersionID != "" && url.VersionID != aws.ToString(v.VersionId) {
					continue
				}

				mod := aws.ToTime(v.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.ToString(v.Key)
				newurl.VersionID = aws.ToString(v.VersionId)
				etag := aws.ToString(v.ETag)

				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(etag, `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         aws.ToInt64(v.Size),
					StorageClass: StorageClass(v.StorageClass),
				}

				objectFound = true
			}

			// iterate over all delete marker versions of the objects
			for _, d := range p.DeleteMarkers {
				key := aws.ToString(d.Key)
				if !url.Match(key) {
					continue
				}
				if url.VersionID != "" && url.VersionID != aws.ToString(d.VersionId) {
					continue
				}

				mod := aws.ToTime(d.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.ToString(d.Key)
				newurl.VersionID = aws.ToString(d.VersionId)

				objCh <- &Object{
					URL:     newurl,
					ModTime: &mod,
					Type:    ObjectType{objtype},
					Size:    0,
				}

				objectFound = true
			}
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
		listInput.Delimiter = aws.String(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		paginator := s3.NewListObjectsV2Paginator(s.api, &listInput)
		for paginator.HasMorePages() {
			p, err := paginator.NextPage(ctx)
			if err != nil {
				objCh <- &Object{Err: err}
				return
			}

			for _, c := range p.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
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
				key := aws.ToString(c.Key)
				if !url.Match(key) {
					continue
				}

				mod := aws.ToTime(c.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.ToString(c.Key)
				etag := aws.ToString(c.ETag)

				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(etag, `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         aws.ToInt64(c.Size),
					StorageClass: StorageClass(c.StorageClass),
				}

				objectFound = true
			}
		}

		if !objectFound && !url.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

// listObjects is used for cloud services that does not support S3
// ListObjectsV2 API. I'm looking at you GCS.
//
// The v2 SDK ships no paginator for the legacy ListObjects operation, so the
// marker is advanced by hand.
func (s *S3) listObjects(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := s3.ListObjectsInput{
		Bucket:       aws.String(url.Bucket),
		Prefix:       aws.String(url.Prefix),
		RequestPayer: s.RequestPayer(),
	}

	if url.Delimiter != "" {
		listInput.Delimiter = aws.String(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		for {
			p, err := s.api.ListObjects(ctx, &listInput)
			if err != nil {
				objCh <- &Object{Err: err}
				return
			}

			for _, c := range p.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
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
				key := aws.ToString(c.Key)
				if !url.Match(key) {
					continue
				}

				mod := aws.ToTime(c.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.ToString(c.Key)
				etag := aws.ToString(c.ETag)

				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(etag, `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         aws.ToInt64(c.Size),
					StorageClass: StorageClass(c.StorageClass),
				}

				objectFound = true
			}

			if !aws.ToBool(p.IsTruncated) {
				break
			}

			// NextMarker is only populated when a delimiter is set;;
			// otherwise the last key of the page is the next marker.
			marker := aws.ToString(p.NextMarker)
			if marker == "" && len(p.Contents) > 0 {
				marker = aws.ToString(p.Contents[len(p.Contents)-1].Key)
			}
			if marker == "" {
				break
			}
			listInput.Marker = aws.String(marker)
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
		Metadata:     make(map[string]string),
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
		input.StorageClass = types.StorageClass(storageClass)
	}

	acl := metadata.ACL
	if acl != "" {
		input.ACL = types.ObjectCannedACL(acl)
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
		input.ServerSideEncryption = types.ServerSideEncryption(sseEncryption)
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

	if len(metadata.UserDefined) != 0 {
		m := make(map[string]string, len(metadata.UserDefined))
		maps.Copy(m, metadata.UserDefined)
		input.Metadata = m
	}

	// add retry ID to the object metadata
	if s.noSuchUploadRetryCount > 0 {
		input.Metadata[metadataKeyRetryID] = generateRetryID()
	}

	if metadata.Directive != "" {
		input.MetadataDirective = types.MetadataDirective(metadata.Directive)
	}

	if metadata.ContentType != "" {
		input.ContentType = aws.String(metadata.ContentType)
	}

	_, err := s.api.CopyObject(ctx, input)
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
		input.VersionId = aws.String(src.VersionID)
	}

	resp, err := s.api.GetObject(ctx, input)
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
	if from.VersionID != "" {
		input.VersionId = aws.String(from.VersionID)
	}

	req, err := s.presigner.PresignGetObject(ctx, input, s3.WithPresignExpires(expire))
	if err != nil {
		return "", err
	}

	return req.URL, nil
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

	return s.downloader.Download(ctx, to, input, func(u *s3Downloader) {
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

func parseInputSerialization(e eventType, c string, delimiter string, headerInfo string) (*types.InputSerialization, error) {
	var s *types.InputSerialization

	switch e {
	case jsonType:
		s = &types.InputSerialization{
			JSON: &types.JSONInput{
				Type: types.JSONType(delimiter),
			},
		}
		if c != "" {
			s.CompressionType = types.CompressionType(c)
		}
	case csvType:
		s = &types.InputSerialization{
			CSV: &types.CSVInput{
				FieldDelimiter: aws.String(delimiter),
				FileHeaderInfo: types.FileHeaderInfo(headerInfo),
			},
		}
		if c != "" {
			s.CompressionType = types.CompressionType(c)
		}
	case parquetType:
		s = &types.InputSerialization{
			Parquet: &types.ParquetInput{},
		}
	default:
		return nil, fmt.Errorf("input format is not valid")
	}

	return s, nil
}

func parseOutputSerialization(e eventType, delimiter string, reader io.Reader) (*types.OutputSerialization, EventStreamDecoder, error) {
	var s *types.OutputSerialization
	var decoder EventStreamDecoder

	switch e {
	case jsonType:
		s = &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		}
		decoder = NewJSONDecoder(reader)
	case csvType:
		s = &types.OutputSerialization{
			CSV: &types.CSVOutput{
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
		inputFormat  *types.InputSerialization
		outputFormat *types.OutputSerialization
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
		ExpressionType:      types.ExpressionType(query.ExpressionType),
		Expression:          aws.String(query.Expression),
		InputSerialization:  inputFormat,
		OutputSerialization: outputFormat,
	}

	resp, err := s.api.SelectObjectContent(ctx, input)
	if err != nil {
		return err
	}

	stream := resp.GetStream()

	go func() {
		defer writer.Close()

		eventch := stream.Events()
		defer stream.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-eventch:
				if !ok {
					return
				}

				switch e := event.(type) {
				case *types.SelectObjectContentEventStreamMemberRecords:
					writer.Write(e.Value.Payload)
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

	return stream.Err()
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

	input := &s3.PutObjectInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		Body:         reader,
		ContentType:  aws.String(contentType),
		Metadata:     make(map[string]string),
		RequestPayer: s.RequestPayer(),
	}

	storageClass := metadata.StorageClass
	if storageClass != "" {
		input.StorageClass = types.StorageClass(storageClass)
	}

	acl := metadata.ACL
	if acl != "" {
		input.ACL = types.ObjectCannedACL(acl)
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
		input.ServerSideEncryption = types.ServerSideEncryption(sseEncryption)
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

	if len(metadata.UserDefined) != 0 {
		m := make(map[string]string, len(metadata.UserDefined))
		maps.Copy(m, metadata.UserDefined)
		input.Metadata = m
	}

	// add retry ID to the object metadata
	if s.noSuchUploadRetryCount > 0 {
		input.Metadata[metadataKeyRetryID] = generateRetryID()
	}

	uploaderOptsFn := func(u *s3Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	}
	_, err := s.uploader.Upload(ctx, input, uploaderOptsFn)

	if errHasCode(err, errCodeNoSuchUpload) && s.noSuchUploadRetryCount > 0 {
		return s.retryOnNoSuchUpload(ctx, to, input, err, uploaderOptsFn)
	}

	return err
}

func (s *S3) retryOnNoSuchUpload(ctx context.Context, to *url.URL, input *s3.PutObjectInput,
	err error, uploaderOpts ...func(*s3Uploader),
) error {
	var expectedRetryID string
	if ID, ok := input.Metadata[metadataKeyRetryID]; ok {
		expectedRetryID = ID
	}

	attempts := 0
	for ; errHasCode(err, errCodeNoSuchUpload) && attempts < s.noSuchUploadRetryCount; attempts++ {
		// check if object exists and has the retry ID we provided, if it does
		// then it means that one of previous uploads was succesfull despite the received error.
		obj, sErr := s.Stat(ctx, to)
		if sErr == nil && obj.retryID == expectedRetryID {
			err = nil
			break
		}

		msg := log.DebugMessage{Err: fmt.Sprintf("Retrying to upload %v upon error: %q", to, err.Error())}
		log.Debug(msg)

		_, err = s.uploader.Upload(ctx, input, uploaderOpts...)
	}

	if errHasCode(err, errCodeNoSuchUpload) && s.noSuchUploadRetryCount > 0 {
		err = fmt.Errorf("%s: RetryOnNoSuchUpload: %v attempts to retry resulted in %s: %w",
			errCodeNoSuchUpload, attempts, errCodeNoSuchUpload, err)
	}
	return err
}

// chunk is an object identifier container which is used on MultiDelete
// operations. Since DeleteObjects API allows deleting objects up to 1000,
// splitting keys into multiple chunks is required.
type chunk struct {
	Bucket string
	Keys   []types.ObjectIdentifier
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

		var keys []types.ObjectIdentifier
		initKeys := func() {
			keys = make([]types.ObjectIdentifier, 0)
		}

		var bucket string
		for url := range ch {
			bucket = url.Bucket

			objid := types.ObjectIdentifier{Key: aws.String(url.Path)}
			if url.VersionID != "" {
				objid.VersionId = aws.String(url.VersionID)
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
		Keys: []types.ObjectIdentifier{
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
			key := fmt.Sprintf("s3://%v/%v", chunk.Bucket, aws.ToString(k.Key))
			url, _ := url.New(key)
			url.VersionID = aws.ToString(k.VersionId)
			resultch <- &Object{URL: url}
		}
		return
	}

	// GCS does not support multi delete.
	if IsGoogleEndpoint(s.endpointURL) {
		for _, k := range chunk.Keys {
			_, err := s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:       aws.String(chunk.Bucket),
				Key:          k.Key,
				RequestPayer: s.RequestPayer(),
			})
			if err != nil {
				resultch <- &Object{Err: err}
				return
			}
			key := fmt.Sprintf("s3://%v/%v", chunk.Bucket, aws.ToString(k.Key))
			url, _ := url.New(key)
			resultch <- &Object{URL: url}
		}
		return
	}

	bucket := chunk.Bucket
	o, err := s.api.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket:       aws.String(bucket),
		Delete:       &types.Delete{Objects: chunk.Keys},
		RequestPayer: s.RequestPayer(),
	})
	if err != nil {
		resultch <- &Object{Err: err}
		return
	}

	for _, d := range o.Deleted {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.ToString(d.Key))
		url, _ := url.New(key)
		url.VersionID = aws.ToString(d.VersionId)
		resultch <- &Object{URL: url}
	}

	for _, e := range o.Errors {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.ToString(e.Key))
		url, _ := url.New(key)
		url.VersionID = aws.ToString(e.VersionId)

		resultch <- &Object{
			URL: url,
			Err: fmt.Errorf("%v", aws.ToString(e.Message)),
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
	o, err := s.api.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []Bucket
	for _, b := range o.Buckets {
		bucketName := aws.ToString(b.Name)
		if prefix == "" || strings.HasPrefix(bucketName, prefix) {
			buckets = append(buckets, Bucket{
				CreationDate: aws.ToTime(b.CreationDate),
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

	_, err := s.api.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// RemoveBucket removes an S3 bucket with the given name.
func (s *S3) RemoveBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// SetBucketVersioning sets the versioning property of the bucket
func (s *S3) SetBucketVersioning(ctx context.Context, versioningStatus, bucket string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatus(versioningStatus),
		},
	})
	return err
}

// GetBucketVersioning returnsversioning property of the bucket
func (s *S3) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	output, err := s.api.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}

	return string(output.Status), nil
}

func (s *S3) HeadBucket(ctx context.Context, url *url.URL) error {
	_, err := s.api.HeadBucket(ctx, &s3.HeadBucketInput{
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
		input.VersionId = aws.String(url.VersionID)
	}

	output, err := s.api.HeadObject(ctx, input)
	if err != nil {
		if errHasCode(err, "NotFound") {
			return nil, nil, &ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		}
		return nil, nil, err
	}

	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#AmazonS3-HeadObject-response-header-StorageClass
	// If the object's storage class is STANDARD, this header is not returned in the response.
	storageClassStr := "STANDARD"
	if output.StorageClass != "" {
		storageClassStr = string(output.StorageClass)
	}

	obj := &Object{
		URL:          url,
		ModTime:      output.LastModified,
		Etag:         strings.Trim(aws.ToString(output.ETag), `"`),
		Size:         aws.ToInt64(output.ContentLength),
		StorageClass: StorageClass(storageClassStr),
	}

	// v1 always handed back a non-nil map here; keep that so an object
	// without user metadata still renders as {} rather than null.
	userDefined := output.Metadata
	if userDefined == nil {
		userDefined = map[string]string{}
	}

	metadata := &Metadata{
		ContentType:      aws.ToString(output.ContentType),
		EncryptionMethod: string(output.ServerSideEncryption),
		UserDefined:      userDefined,
	}

	return obj, metadata, nil
}

// sdkLogger adapts the SDK's logger to s5cmd's trace log.
type sdkLogger struct{}

func (l sdkLogger) Logf(classification logging.Classification, format string, v ...any) {
	msg := log.TraceMessage{
		Message: fmt.Sprintf(format, v...),
	}
	log.Trace(msg)
}

// SessionCache holds aws.Config according to s3Opts and it synchronizes
// access/modification.
type SessionCache struct {
	sync.Mutex
	sessions map[Options]aws.Config
}

// newSession initializes a new AWS config with region fallback and custom
// options.
func (sc *SessionCache) newSession(ctx context.Context, opts Options) (aws.Config, error) {
	sc.Lock()
	defer sc.Unlock()

	if cfg, ok := sc.sessions[opts]; ok {
		return cfg, nil
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRetryer(func() aws.Retryer {
			return newCustomRetryer(opts.MaxRetries)
		}),
	}

	if opts.NoSignRequest {
		// do not sign requests when making service API calls
		loadOpts = append(loadOpts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	} else {
		if opts.CredentialFile != "" {
			loadOpts = append(loadOpts, config.WithSharedCredentialsFiles([]string{opts.CredentialFile}))
		}
		if opts.Profile != "" {
			loadOpts = append(loadOpts, config.WithSharedConfigProfile(opts.Profile))
		}
	}

	{
		// Reverse of what the SDK does: if AWS_SDK_LOAD_CONFIG is 0 (or a
		// falsy value) disable shared configs. The v2 SDK always reads the
		// shared config file, so emulate the old behaviour by pointing it
		// at no config file at all.
		loadCfg := os.Getenv("AWS_SDK_LOAD_CONFIG")
		if loadCfg != "" {
			if enable, _ := strconv.ParseBool(loadCfg); !enable {
				loadOpts = append(loadOpts, config.WithSharedConfigFiles([]string{}))
			}
		}
	}

	if opts.NoVerifySSL {
		loadOpts = append(loadOpts, config.WithHTTPClient(insecureHTTPClient))
	}

	if opts.LogLevel == log.LevelTrace {
		loadOpts = append(loadOpts,
			config.WithClientLogMode(aws.LogRequestWithBody|aws.LogResponseWithBody|aws.LogRetries),
			config.WithLogger(sdkLogger{}),
		)
	}

	if opts.region != "" {
		loadOpts = append(loadOpts, config.WithRegion(opts.region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return aws.Config{}, err
	}

	// get region of the bucket and create config accordingly. if the region
	// is not provided, it means we want region-independent config
	// for operations such as listing buckets, making a new bucket etc.
	// only get bucket region when it is not specified.
	if opts.region == "" {
		if err := setSessionRegion(ctx, &cfg, opts); err != nil {
			return aws.Config{}, err
		}
	}

	sc.sessions[opts] = cfg

	return cfg, nil
}

func (sc *SessionCache) clear() {
	sc.Lock()
	defer sc.Unlock()
	sc.sessions = map[Options]aws.Config{}
}

func setSessionRegion(ctx context.Context, cfg *aws.Config, opts Options) error {
	if cfg.Region != "" {
		return nil
	}

	// set default region
	cfg.Region = defaultRegion

	if opts.bucket == "" {
		return nil
	}

	optFn, err := s3Options(opts)
	if err != nil {
		return err
	}

	// auto-detection
	client := s3.NewFromConfig(*cfg, optFn)
	region, err := manager.GetBucketRegion(ctx, client, opts.bucket, optFn, func(o *s3.Options) {
		// GetBucketRegion clears the credentials so that the lookup works
		// against public buckets. Put the configured ones back, otherwise
		// the HeadBucket it makes is unsigned and private buckets fail.
		// Anonymous credentials must stay cleared: the v2 signer rejects
		// them as a provider, and unsigned is what they ask for anyway.
		if cfg.Credentials != nil && !aws.IsCredentialsProvider(cfg.Credentials, (*aws.AnonymousCredentials)(nil)) {
			o.Credentials = cfg.Credentials
		}
	})
	if err != nil {
		// the manager reports a missing bucket with its own error type;
		// re-shape it so callers can match on the S3 error code as before.
		if _, ok := errors.AsType[manager.BucketNotFound](err); ok {
			return &smithy.GenericAPIError{
				Code:    "NotFound",
				Message: fmt.Sprintf("bucket %q not found", opts.bucket),
			}
		}
		// don't deny any request to the service if region auto-fetching
		// receives an error. Delegate error handling to command execution.
		err = fmt.Errorf("session: fetching region failed: %v", err)
		msg := log.ErrorMessage{Err: err.Error()}
		log.Error(msg)
	} else if region != "" {
		// an empty header means the service did not report a region; keep
		// the default rather than blanking it.
		cfg.Region = region
	}

	return nil
}

// customRetryer wraps the SDK's standard retryer, adding additional error
// codes. Such as, retry for S3 InternalError code.
type customRetryer struct {
	aws.Retryer
}

func newCustomRetryer(maxRetries int) aws.Retryer {
	return &customRetryer{
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			// v1 counted retries after the initial attempt; v2 counts the
			// initial attempt too.
			o.MaxAttempts = maxRetries + 1
		}),
	}
}

// IsErrorRetryable overrides the SDK's standard retryer, adding custom retry
// logics that are not included in the SDK.
func (c *customRetryer) IsErrorRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Errors related to tokens
	if errHasCode(err, "ExpiredToken") || errHasCode(err, "ExpiredTokenException") || errHasCode(err, "InvalidToken") {
		return false
	}

	shouldRetry := errHasCode(err, "InternalError") || errHasCode(err, "RequestTimeTooSkewed") || errHasCode(err, "SlowDown") ||
		strings.Contains(err.Error(), "connection reset") || strings.Contains(err.Error(), "connection timed out")
	if !shouldRetry {
		shouldRetry = c.Retryer.IsErrorRetryable(err)
	}

	if shouldRetry {
		err := fmt.Errorf("retryable error: %v", err)
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

// isVirtualHostStyle reports whether requests can be made with virtual
// host style bucket name resolving. If a custom S3 API compatible endpoint is
// given, resolve the bucketname from the URL path.
func isVirtualHostStyle(endpoint urlpkg.URL) bool {
	return endpoint == sentinelURL || supportsTransferAcceleration(endpoint) || IsGoogleEndpoint(endpoint)
}

// errCodeNoSuchUpload is the error code the S3 API returns when a multipart
// upload no longer exists. The v2 SDK models it as a type rather than a
// constant, so the string is spelled out here.
const errCodeNoSuchUpload = "NoSuchUpload"

func errHasCode(err error, code string) bool {
	if err == nil || code == "" {
		return false
	}

	// errors.As walks the chain, so an API error wrapped by the transfer
	// manager's multipart failure is found here too.
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		return apiErr.ErrorCode() == code
	}

	return false
}

// IsCancelationError reports whether given error is a storage related
// cancelation error.
func IsCancelationError(err error) bool {
	return errors.Is(err, context.Canceled)
}

// generate a retry ID for this upload attempt
func generateRetryID() string {
	num, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	return num.String()
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
