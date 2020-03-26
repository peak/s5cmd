package storage

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	urlpkg "net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"

	"github.com/peak/s5cmd/storage/url"
)

var _ Storage = (*S3)(nil)

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

// newS3Factory returns a closure that creates new S3 storage. This pattern is
// used to re-use S3 sessions which makes huge difference for batch S3
// operations.
func newS3Factory(opts S3Options) func() (*S3, error) {
	var (
		mu     sync.RWMutex
		cached *S3
	)

	return func() (*S3, error) {
		mu.RLock()
		if cached != nil {
			mu.RUnlock()
			return cached, nil
		}
		mu.RUnlock()

		s3, err := NewS3Storage(opts)
		if err != nil {
			return nil, err
		}

		mu.Lock()
		cached = s3
		mu.Unlock()
		return s3, nil
	}
}

// newCachedS3 function returns a cached S3 storage with a re-used session if
// available. Re-used AWS sessions dramatically improve performance.
var newCachedS3 func() (*S3, error)

func SetS3Options(opts S3Options) {
	newCachedS3 = newS3Factory(opts)

}

// S3 is a storage type which interacts with S3API, DownloaderAPI and
// UploaderAPI.
type S3 struct {
	api         s3iface.S3API
	downloader  s3manageriface.DownloaderAPI
	uploader    s3manageriface.UploaderAPI
	endpointURL urlpkg.URL
}

// S3Options stores configuration for S3 storage.
type S3Options struct {
	MaxRetries  int
	Endpoint    string
	Region      string
	NoVerifySSL bool
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

// NewS3Storage creates new S3 session.
func NewS3Storage(opts S3Options) (*S3, error) {
	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	awsSession, err := newSession(opts)
	if err != nil {
		return nil, err
	}

	return &S3{
		api:         s3.New(awsSession),
		downloader:  s3manager.NewDownloader(awsSession),
		uploader:    s3manager.NewUploader(awsSession),
		endpointURL: endpointURL,
	}, nil
}

// Stat retrieves metadata from S3 object without returning the object itself.
func (s *S3) Stat(ctx context.Context, url *url.URL) (*Object, error) {
	output, err := s.api.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(url.Bucket),
		Key:    aws.String(url.Path),
	})
	if err != nil {
		if errHasCode(err, "NotFound") {
			return nil, ErrGivenObjectNotFound
		}
		return nil, err
	}

	etag := aws.StringValue(output.ETag)
	mod := aws.TimeValue(output.LastModified)
	return &Object{
		URL:     url,
		Etag:    strings.Trim(etag, `"`),
		ModTime: &mod,
		Size:    aws.Int64Value(output.ContentLength),
	}, nil
}

// List is a non-blocking S3 list operation which paginates and filters S3
// keys. If no object found or an error is encountered during this period,
// it sends these errors to object channel.
func (s *S3) List(ctx context.Context, url *url.URL) <-chan *Object {
	if isGoogleEndpoint(s.endpointURL) {
		return s.listObjects(ctx, url)
	}
	return s.listObjectsV2(ctx, url)
}

func (s *S3) listObjectsV2(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := s3.ListObjectsV2Input{
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

			for _, c := range p.Contents {
				key := aws.StringValue(c.Key)
				if !url.Match(key) {
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.StringValue(c.Key)
				etag := aws.StringValue(c.ETag)
				mod := aws.TimeValue(c.LastModified)
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

		if !objectFound {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

// listObjects is used for cloud services that does not support S3
// ListObjectsV2 API. I'm looking at you GCS.
func (s *S3) listObjects(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := s3.ListObjectsInput{
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

			for _, c := range p.Contents {
				key := aws.StringValue(c.Key)
				if !url.Match(key) {
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.StringValue(c.Key)
				etag := aws.StringValue(c.ETag)
				mod := aws.TimeValue(c.LastModified)
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

		if !objectFound {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

// Copy is a single-object copy operation which copies objects to S3
// destination from another S3 source.
func (s *S3) Copy(ctx context.Context, from, to *url.URL, metadata map[string]string) error {
	// SDK expects CopySource like "bucket[/key]"
	copySource := strings.TrimPrefix(from.String(), "s3://")

	storageClass := metadata["StorageClass"]

	_, err := s.api.CopyObject(&s3.CopyObjectInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		CopySource:   aws.String(copySource),
		StorageClass: aws.String(storageClass),
	})
	return err
}

// Get is a multipart download operation which downloads S3 objects into any
// destination that implements io.WriterAt interface.
func (s *S3) Get(
	ctx context.Context,
	from *url.URL,
	to io.WriterAt,
	concurrency int,
	partSize int64,
) (int64, error) {
	n, err := s.downloader.DownloadWithContext(ctx, to, &s3.GetObjectInput{
		Bucket: aws.String(from.Bucket),
		Key:    aws.String(from.Path),
	}, func(u *s3manager.Downloader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	})

	return n, err
}

// Put is a multipart upload operation to upload resources, which implements
// io.Reader interface, into S3 destination.
func (s *S3) Put(
	ctx context.Context,
	reader io.Reader,
	to *url.URL,
	metadata map[string]string,
	concurrency int,
	partSize int64,
) error {
	storageClass := metadata["StorageClass"]
	if storageClass == "" {
		storageClass = string(StorageStandard)
	}

	contentType := metadata["ContentType"]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	_, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		Body:         reader,
		ContentType:  aws.String(contentType),
		StorageClass: aws.String(storageClass),
	}, func(u *s3manager.Uploader) {
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
	Keys   []*s3.ObjectIdentifier
}

// calculateChunks calculates chunks for given URL channel and returns
// read-only chunk channel.
func (s *S3) calculateChunks(ch <-chan *url.URL) <-chan chunk {
	chunkch := make(chan chunk)

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
	bucket := chunk.Bucket
	o, err := s.api.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{Objects: chunk.Keys},
	})
	if err != nil {
		resultch <- &Object{Err: err}
		return
	}

	for _, d := range o.Deleted {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.StringValue(d.Key))
		url, _ := url.New(key)
		resultch <- &Object{URL: url}
	}

	for _, e := range o.Errors {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.StringValue(e.Key))
		url, _ := url.New(key)
		resultch <- &Object{
			URL: url,
			Err: fmt.Errorf(aws.StringValue(e.Message)),
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
	_, err := s.api.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// NewAwsSession initializes a new AWS session with region fallback and custom
// options.
func newSession(opts S3Options) (*session.Session, error) {
	awsCfg := aws.NewConfig()

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
		WithHTTPClient(httpClient)

	if opts.Region != "" {
		awsCfg.WithRegion(opts.Region)
	}

	awsCfg.Retryer = newRequestRetryer(opts.MaxRetries)

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

	if aws.StringValue(sess.Config.Region) == "" {
		sess.Config.Region = aws.String(endpoints.UsEast1RegionID)
	}

	return sess, nil
}

type requestRetryer struct {
	*client.DefaultRetryer
}

func newRequestRetryer(maxRetries int) *requestRetryer {
	return &requestRetryer{
		DefaultRetryer: &client.DefaultRetryer{
			NumMaxRetries: maxRetries,
		},
	}
}

func (r *requestRetryer) ShouldRetry(req *request.Request) bool {
	if errHasCode(req.Error, "InternalError") {
		return true
	}
	return r.DefaultRetryer.ShouldRetry(req)

}

var insecureHTTPClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	},
}

func supportsTransferAcceleration(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == transferAccelEndpoint
}

func isGoogleEndpoint(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == gcsEndpoint
}

// isVirtualHostStyle reports whether the given endpoint supports S3 virtual
// host style bucket name resolving. If a custom S3 API compatible endpoint is
// given, resolve the bucketname from the URL path.
func isVirtualHostStyle(endpoint urlpkg.URL) bool {
	return endpoint == sentinelURL || supportsTransferAcceleration(endpoint) || isGoogleEndpoint(endpoint)
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

func IsCancelationError(err error) bool {
	return errHasCode(err, request.CanceledErrorCode)
}
