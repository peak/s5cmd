package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/peak/s5cmd/command"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	url "github.com/peak/s5cmd/storage/url"
	"io"
	urlpkg "net/url"
	"strings"
	"sync"
	"time"
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
)

type S3 struct {
	client           s3Client
	client2          s3.Client
	downloader       downloader
	uploader         manager.Uploader
	requestPayer     types.RequestPayer
	endpointURL      urlpkg.URL
	dryRun           bool
	useListObjectsV1 bool
}

type s3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	//manager.DownloadAPIClient this interface includes getObject, should we use it here?
	s3.HeadObjectAPIClient
	s3.ListObjectsV2APIClient
	manager.DeleteObjectsAPIClient
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	SelectObjectContent(ctx context.Context, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error)
}

type downloader interface {
	Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*manager.Downloader)) (n int64, err error)
}

func (s *S3) RequestPayer() types.RequestPayer {
	if s.requestPayer == "" {
		return ""
	}
	return s.requestPayer
}
func parseEndpoint(endpoint string) (urlpkg.URL, error) {
	if endpoint == "" {
		return sentinelURL, nil
	}
	// add a scheme to correctly parse the endpoint. Without a scheme,
	// url.Parse will put the host information in path"
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = "http://" + endpoint
	}
	u, err := urlpkg.Parse(endpoint)
	if err != nil {
		return sentinelURL, fmt.Errorf("parse endpoint %q: %v", endpoint, err)
	}

	return *u, nil
}

func newS3Storage(ctx context.Context, opts storage.Options) (*S3, error) {

	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}
	//TODO(bora): write further necessary config settings from globalSessionCache.newSession func

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://127.0.0.1:56229",
					Source:            aws.EndpointSourceCustom,
					HostnameImmutable: true}, nil
			})),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg)

	// from todo until here put it in a method
	return &S3{
		client:       client,
		downloader:   manager.NewDownloader(client),
		requestPayer: "",
		endpointURL:  endpointURL,
	}, nil

}

func (s *S3) Stat(ctx context.Context, url *url.URL) (*storage.Object, error) {

	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.RequestPayer(),
	})
	if err != nil {
		//TODO: errHasCode is not exported, use commented lines later
		//if storage.ErrHasCode(err, "NotFound") {
		//	return nil, &storage.ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		//}
		return nil, err
	}

	etag := aws.ToString(output.ETag)
	mod := aws.ToTime(output.LastModified)
	return &storage.Object{
		URL:     url,
		Etag:    etag,
		ModTime: &mod,
		Size:    aws.ToInt64(&output.ContentLength),
	}, nil
}

// List is a non-blocking S3 list operation which paginates and filters S3
// keys. If no object found or an error is encountered during this period,
// it sends these errors to object channel.
func (s *S3) List(ctx context.Context, url *url.URL, _ bool) <-chan *storage.Object {

	//TODO: switch to listObjectsV2 for GCS
	if isGoogleEndpoint(s.endpointURL) || s.useListObjectsV1 {
		return nil
	}

	return s.listObjectsV2(ctx, url)
}

func (s *S3) listObjectsV2(ctx context.Context, url *url.URL) <-chan *storage.Object {
	listInput := s3.ListObjectsV2Input{
		Bucket:       aws.String(url.Bucket),
		Prefix:       aws.String(url.Prefix),
		RequestPayer: s.RequestPayer(),
	}

	if url.Delimiter != "" {
		listInput.Delimiter = &url.Delimiter
	}
	objCh := make(chan *storage.Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		paginator := s3.NewListObjectsV2Paginator(s.client, &listInput)

		for paginator.HasMorePages() {
			p, err := paginator.NextPage(ctx)
			if err != nil {
				objCh <- &storage.Object{Err: err}
				return
			}
			for _, c := range p.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
				if !url.Match(prefix) {
					continue
				}
				newurl := url.Clone()
				newurl.Path = prefix
				objCh <- &storage.Object{
					URL: newurl,
					//todo : cannot set type as it is not exported, fix it later
					Type: storage.ObjectType{},
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
				//todo : cannot set type as it is not exported, fix it later
				//var objtype os.FileMode
				//if strings.HasSuffix(key, "/") {
				//	objtype = os.ModeDir
				//}

				newurl := url.Clone()
				newurl.Path = aws.ToString(c.Key)
				etag := aws.ToString(c.ETag)

				objCh <- &storage.Object{
					URL:     newurl,
					Etag:    strings.Trim(etag, `"`),
					ModTime: &mod,
					//todo : cannot set type as it is not exported, fix it later
					Type:         storage.ObjectType{},
					Size:         c.Size,
					StorageClass: storage.StorageClass(c.StorageClass),
				}

				objectFound = true
			}

		}

		if !objectFound {
			objCh <- &storage.Object{Err: storage.ErrNoObjectFound}
		}

	}()
	return objCh

}
func (s *S3) listObjects(ctx context.Context, url *url.URL) <-chan *storage.Object {
	//todo: Implement paginator for listObjectsV1
	objCh := make(chan *storage.Object)

	return objCh
}

func (s *S3) Copy(ctx context.Context, from, to *url.URL, metadata storage.Metadata) error {

	if s.dryRun {
		return nil
	}
	// SDK expects CopySource like "bucket[/key]"
	copySource := from.EscapedPath()

	input := s3.CopyObjectInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		CopySource:   aws.String(copySource),
		RequestPayer: s.RequestPayer(),
	}

	storageClass := metadata.StorageClass()
	if storageClass != "" {
		input.StorageClass = types.StorageClass(storageClass)
	}

	sseEncryption := metadata.SSE()
	if sseEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(sseEncryption)
		sseKmsKeyID := metadata.SSEKeyID()
		if sseKmsKeyID != "" {
			input.SSEKMSKeyId = aws.String(sseKmsKeyID)
		}
	}

	acl := metadata.ACL()
	if acl != "" {
		input.ACL = types.ObjectCannedACL(acl)
	}

	cacheControl := metadata.CacheControl()
	if cacheControl != "" {
		input.CacheControl = aws.String(cacheControl)
	}

	expires := metadata.Expires()
	if expires != "" {
		t, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			return err
		}
		input.Expires = aws.Time(t)
	}
	_, err := s.client.CopyObject(ctx, &input)
	return err
}

// Read fetches the remote object and returns its contents as an io.ReadCloser.
func (s *S3) Read(ctx context.Context, src *url.URL) (io.ReadCloser, error) {

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(src.Bucket),
		Key:          aws.String(src.Path),
		RequestPayer: s.RequestPayer(),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

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

	return s.downloader.Download(ctx, to, &s3.GetObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.RequestPayer(),
	}, func(u *manager.Downloader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	})
}

type SelectQuery struct {
	ExpressionType  string
	Expression      string
	CompressionType string
}

func (s *S3) Select(ctx context.Context, url *url.URL, query *SelectQuery, resultCh chan<- json.RawMessage) error {
	if s.dryRun {
		return nil
	}

	input := &s3.SelectObjectContentInput{
		Bucket:         aws.String(url.Bucket),
		Key:            aws.String(url.Path),
		ExpressionType: types.ExpressionType(*aws.String(query.ExpressionType)),
		Expression:     aws.String(query.Expression),
		InputSerialization: &types.InputSerialization{
			CompressionType: types.CompressionType(query.CompressionType),
			JSON: &types.JSONInput{
				Type: types.JSONTypeLines,
			},
		},
		OutputSerialization: &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		},
	}

	resp, err := s.client.SelectObjectContent(ctx, input)
	if err != nil {
		return err
	}

	reader, writer := io.Pipe()

	go func() {
		defer writer.Close()

		eventch := resp.GetStream().Reader.Events()
		defer resp.GetStream().Close()

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

	decoder := json.NewDecoder(reader)
	for {
		var record json.RawMessage
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		resultCh <- record
	}

	return resp.GetStream().Reader.Err()
}

// Put is a multipart upload operation to upload resources, which implements
// io.Reader interface, into S3 destination.
func (s *S3) Put(
	ctx context.Context,
	reader io.Reader,
	to *url.URL,
	metadata storage.Metadata,
	concurrency int,
	partSize int64,
) error {
	if s.dryRun {
		return nil
	}

	contentType := metadata.ContentType()
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	input := &s3.PutObjectInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		Body:         reader,
		ContentType:  aws.String(contentType),
		RequestPayer: s.RequestPayer(),
	}

	storageClass := metadata.StorageClass()
	if storageClass != "" {
		input.StorageClass = types.StorageClass(storageClass)
	}
	acl := metadata.ACL()
	if acl != "" {
		input.ACL = types.ObjectCannedACL(acl)
	}

	cacheControl := metadata.CacheControl()
	if cacheControl != "" {
		input.CacheControl = aws.String(cacheControl)
	}

	expires := metadata.Expires()
	if expires != "" {
		t, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			return err
		}
		input.Expires = aws.Time(t)
	}

	sseEncryption := metadata.SSE()
	if sseEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(sseEncryption)
		sseKmsKeyID := metadata.SSEKeyID()
		if sseKmsKeyID != "" {
			input.SSEKMSKeyId = aws.String(sseKmsKeyID)
		}
	}

	contentEncoding := metadata.ContentEncoding()
	if contentEncoding != "" {
		input.ContentEncoding = aws.String(contentEncoding)
	}

	_, err := s.uploader.Upload(ctx, input, func(u *manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	})

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
			keys = append(keys, objid)
			if len(keys) == deleteObjectsMax {
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

	resultch := make(chan *storage.Object, 1)
	defer close(resultch)

	s.doDelete(ctx, chunk, resultch)
	obj := <-resultch
	return obj.Err
}

// doDelete deletes the given keys given by chunk. Results are piggybacked via
// the Object container.
func (s *S3) doDelete(ctx context.Context, chunk chunk, resultch chan *storage.Object) {
	if s.dryRun {
		for _, k := range chunk.Keys {
			key := fmt.Sprintf("s3://%v/%v", chunk.Bucket, aws.ToString(k.Key))
			url, _ := url.New(key)
			resultch <- &storage.Object{URL: url}
		}
		return
	}

	bucket := chunk.Bucket
	o, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket:       aws.String(bucket),
		Delete:       &types.Delete{Objects: chunk.Keys},
		RequestPayer: s.RequestPayer(),
	})
	if err != nil {
		resultch <- &storage.Object{Err: err}
		return
	}

	for _, d := range o.Deleted {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.ToString(d.Key))
		url, _ := url.New(key)
		resultch <- &storage.Object{URL: url}
	}

	for _, e := range o.Errors {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.ToString(e.Key))
		url, _ := url.New(key)
		resultch <- &storage.Object{
			URL: url,
			Err: fmt.Errorf(aws.ToString(e.Message)),
		}
	}
}

// MultiDelete is an asynchronous removal operation for multiple objects.
// It reads given url channel, creates multiple chunks and run these
// chunks in parallel. Each chunk may have at most 1000 objects since DeleteObjects
// API has a limitation.
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html.
func (s *S3) MultiDelete(ctx context.Context, urlch <-chan *url.URL) <-chan *storage.Object {
	resultch := make(chan *storage.Object)

	go func() {
		sem := make(chan bool, 10)
		defer close(sem)
		defer close(resultch)

		chunks := s.calculateChunks(urlch)

		var wg sync.WaitGroup
		for chunk := range chunks {
			chunk := chunk

			wg.Add(1)
			sem <- true

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
func (s *S3) ListBuckets(ctx context.Context, prefix string) ([]storage.Bucket, error) {
	o, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []storage.Bucket
	for _, b := range o.Buckets {
		bucketName := aws.ToString(b.Name)
		if prefix == "" || strings.HasPrefix(bucketName, prefix) {
			buckets = append(buckets, storage.Bucket{
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
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// RemoveBucket removes an S3 bucket with the given name.
func (s *S3) RemoveBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

func isGoogleEndpoint(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == gcsEndpoint
}

func main() {

	nurl, err := url.New("s3://bucket/s5cmd-benchmarks-/fd11r13e/1/old/tmpaaah")
	if err != nil {
		panic(err)
	}
	var opts = storage.Options{}
	s3, err := newS3Storage(context.TODO(), opts)
	if err != nil {
		panic(err)
	}

	//obj, err := s3.Stat(context.TODO(), nurl)

	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}
	err = s3.RemoveBucket(context.TODO(), "bucket3")
	if err != nil {
		panic(err)
	}
	buckets, err := s3.ListBuckets(context.TODO(), "bucket")

	for i, bucket := range buckets {
		fmt.Println(i, bucket.Name)
	}
	for object := range s3.listObjectsV2(context.TODO(), nurl) {

		msg := command.ListMessage{
			Object: object,
		}

		log.Init("debug", false)
		fmt.Println(object.URL)
		log.Info(msg)

	}

}
