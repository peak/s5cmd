package storage

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"

	"github.com/peak/s5cmd/objurl"
)

var _ Storage = (*S3)(nil)

var (
	// ErrNoItemFound is a error type for marking empty list results.
	ErrNoItemFound = fmt.Errorf("s3: no item found")
)

const (
	// ListAllItems is a type to paginate all S3 keys.
	ListAllItems = -1

	// DeleteItemsMax is the max allowed items to be deleted on single HTTP request.
	DeleteItemsMax = 1000
)

// SequenceEndMarker is a marker that is dispatched on end of each sequence.
var SequenceEndMarker = &Object{}

// S3 is a storage type which interacts with S3API, DownloaderAPI and
// UploaderAPI.
type S3 struct {
	api        s3iface.S3API
	downloader s3manageriface.DownloaderAPI
	uploader   s3manageriface.UploaderAPI
	opts       S3Opts
	stats      *Stats
}

// S3Opts stores configuration for S3 storage.
type S3Opts struct {
	MaxRetries             int
	EndpointURL            string
	Region                 string
	NoVerifySSL            bool
	UploadChunkSizeBytes   int64
	UploadConcurrency      int
	DownloadChunkSizeBytes int64
	DownloadConcurrency    int
}

// NewS3Storage creates new S3 session.
func NewS3Storage(opts S3Opts) (*S3, error) {
	awsSession, err := newAWSSession(opts)
	if err != nil {
		return nil, err
	}

	return &S3{
		api:        s3.New(awsSession),
		downloader: s3manager.NewDownloader(awsSession),
		uploader:   s3manager.NewUploader(awsSession),
		opts:       opts,
		stats:      &Stats{},
	}, nil
}

// Stat retrieves metadata from S3 object without returning the object itself.
func (s *S3) Stat(ctx context.Context, url *objurl.ObjectURL) (*Object, error) {
	output, err := s.api.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(url.Bucket),
		Key:    aws.String(url.Path),
	})

	if err != nil {
		return nil, err
	}

	return &Object{
		URL:     url,
		Etag:    aws.StringValue(output.ETag),
		ModTime: aws.TimeValue(output.LastModified),
		Size:    aws.Int64Value(output.ContentLength),
	}, nil
}

// List is a non-blocking S3 list operation which paginates and filters S3
// keys. It sends SequenceEndMarker at the end of each pagination. If no item
// found or an error is encountered during this period, it sends these errors
// to item channel.
func (s *S3) List(ctx context.Context, url *objurl.ObjectURL, _ bool, maxKeys int64) <-chan *Object {
	listInput := s3.ListObjectsV2Input{
		Bucket: aws.String(url.Bucket),
		Prefix: aws.String(url.Prefix),
	}

	if url.Delimiter != "" {
		listInput.SetDelimiter(url.Delimiter)
	}

	shouldPaginate := maxKeys < 0
	if !shouldPaginate {
		listInput.SetMaxKeys(maxKeys)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		itemFound := false

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
					Type: os.ModeDir,
				}

				itemFound = true
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
				objCh <- &Object{
					URL:          newurl,
					Etag:         aws.StringValue(c.ETag),
					ModTime:      aws.TimeValue(c.LastModified),
					Type:         objtype,
					Size:         aws.Int64Value(c.Size),
					StorageClass: storageClass(aws.StringValue(c.StorageClass)),
				}

				itemFound = true
			}

			if itemFound && lastPage {
				objCh <- SequenceEndMarker
			}

			return shouldPaginate && !lastPage
		})

		if err != nil {
			objCh <- &Object{Err: err}
			return
		}

		if !itemFound {
			objCh <- &Object{Err: ErrNoItemFound}
		}
	}()

	return objCh
}

// Copy is a single-object copy operation which copies objects to S3
// destination from another S3 source.
func (s *S3) Copy(ctx context.Context, from, to *objurl.ObjectURL, cls string) error {
	// SDK expects CopySource like "bucket[/key]"
	copySource := strings.TrimPrefix(to.String(), "s3://")

	_, err := s.api.CopyObject(&s3.CopyObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		CopySource:   aws.String(copySource),
		StorageClass: aws.String(cls),
	})
	return err
}

// Get is a multipart download operation which downloads S3 objects into any
// destination that implements io.WriterAt interface.
func (s *S3) Get(ctx context.Context, from *objurl.ObjectURL, to io.WriterAt) error {
	_, err := s.downloader.DownloadWithContext(ctx, to, &s3.GetObjectInput{
		Bucket: aws.String(from.Bucket),
		Key:    aws.String(from.Path),
	}, func(u *s3manager.Downloader) {
		u.PartSize = s.opts.DownloadChunkSizeBytes
		u.Concurrency = s.opts.DownloadConcurrency
	})
	return err
}

// Put is a multipart upload operation to upload resources, which implements
// io.Reader interface, into S3 destination.
func (s *S3) Put(ctx context.Context, reader io.Reader, to *objurl.ObjectURL, cls string) error {
	_, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		Body:         reader,
		StorageClass: aws.String(cls),
	}, func(u *s3manager.Uploader) {
		u.PartSize = s.opts.UploadChunkSizeBytes
		u.Concurrency = s.opts.UploadConcurrency
	})

	return err
}

// Delete is a removal operation which removes multiple S3 objects from a
// bucket using single HTTP request. It allows deleting objects up to 1000.
func (s *S3) Delete(ctx context.Context, urls ...*objurl.ObjectURL) error {
	if len(urls) > DeleteItemsMax || len(urls) == 0 {
		return fmt.Errorf(
			"delete size should be between %d and %d, given: %d",
			0,
			DeleteItemsMax,
			len(urls),
		)
	}

	var objects []*s3.ObjectIdentifier
	for _, url := range urls {
		objects = append(
			objects,
			&s3.ObjectIdentifier{Key: aws.String(url.Path)},
		)
	}

	bucket := urls[0].Bucket

	o, err := s.api.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{Objects: objects},
	})
	if err != nil {
		return err
	}

	for _, d := range o.Deleted {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.StringValue(d.Key))
		s.stats.put(key, StatsResponse{Success: true})
	}

	for _, e := range o.Errors {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.StringValue(e.Key))
		s.stats.put(key, StatsResponse{
			Success: false,
			Message: aws.StringValue(e.Message),
		})
	}

	return nil
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

// UpdateRegion overrides AWS session with the region of given bucket.
func (s *S3) UpdateRegion(bucket string) error {
	o, err := s.api.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: &bucket,
	})
	if err != nil {
		return err
	}

	// don't change the session region if given bucket has no location
	// constraint set.
	if o.LocationConstraint == nil {
		return nil
	}

	ses, err := newAWSSession(S3Opts{
		MaxRetries:             s.opts.MaxRetries,
		EndpointURL:            s.opts.EndpointURL,
		Region:                 aws.StringValue(o.LocationConstraint),
		NoVerifySSL:            s.opts.NoVerifySSL,
		DownloadConcurrency:    s.opts.DownloadConcurrency,
		DownloadChunkSizeBytes: s.opts.DownloadChunkSizeBytes,
		UploadConcurrency:      s.opts.UploadConcurrency,
		UploadChunkSizeBytes:   s.opts.UploadChunkSizeBytes,
	})

	if err != nil {
		return err
	}

	s.api = s3.New(ses)
	return nil
}

// Statistics returns the stats of the storage.
func (s *S3) Statistics() *Stats {
	return s.stats
}

// NewAwsSession initializes a new AWS session with region fallback and custom
// options.
func newAWSSession(opts S3Opts) (*session.Session, error) {
	newSession := func(c *aws.Config) (*session.Session, error) {
		useSharedConfig := session.SharedConfigEnable

		// Reverse of what the SDK does: if AWS_SDK_LOAD_CONFIG is 0 (or a falsy value) disable shared configs
		loadCfg := os.Getenv("AWS_SDK_LOAD_CONFIG")
		if loadCfg != "" {
			if enable, _ := strconv.ParseBool(loadCfg); !enable {
				useSharedConfig = session.SharedConfigDisable
			}
		}
		return session.NewSessionWithOptions(session.Options{Config: *c, SharedConfigState: useSharedConfig})
	}

	awsCfg := aws.NewConfig().WithMaxRetries(opts.MaxRetries)

	if opts.EndpointURL != "" {
		awsCfg = awsCfg.WithEndpoint(opts.EndpointURL).WithS3ForcePathStyle(true)
	}

	if opts.NoVerifySSL {
		awsCfg = awsCfg.WithHTTPClient(&http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}})
	}

	if opts.Region != "" {
		awsCfg = awsCfg.WithRegion(opts.Region)
		return newSession(awsCfg)
	}

	ses, err := newSession(awsCfg)
	if err != nil {
		return nil, err
	}
	if (*ses).Config.Region == nil || *(*ses).Config.Region == "" {
		// No region specified in env or config, fallback to us-east-1
		awsCfg = awsCfg.WithRegion(endpoints.UsEast1RegionID)
		ses, err = newSession(awsCfg)
	}

	return ses, err
}

func IsCancelationError(err error) bool {
	if err == nil {
		return false
	}
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		if awsErr.Code() == request.CanceledErrorCode {
			return true
		}
	}
	return false
}
