package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/peak/s5cmd/s3url"
)

var _ Storage = (*S3)(nil)

var (
	ErrNoItemFound = fmt.Errorf("no item found")

	ErrNilResult = fmt.Errorf("nil result")
)

// ListAllItems is a type to paginate all S3 keys
const ListAllItems = -1

// SequenceEndMarker is a marker that is dispatched on end of each sequence
var SequenceEndMarker = &Item{}

type S3 struct {
	api        s3iface.S3API
	downloader s3manageriface.DownloaderAPI
	uploader   s3manageriface.UploaderAPI
	opts       S3Opts
}

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
	}, nil
}

func (s *S3) Head(ctx context.Context, url *s3url.S3Url) (*Item, error) {
	output, err := s.api.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(url.Bucket),
		Key:    aws.String(url.Key),
	})

	if err != nil {
		return nil, err
	}

	return &Item{
		Etag:         aws.StringValue(output.ETag),
		LastModified: aws.TimeValue(output.LastModified),
		Size:         aws.Int64Value(output.ContentLength),
		Key:          url.Key,
	}, nil
}

func (s *S3) List(ctx context.Context, url *s3url.S3Url, maxKeys int64) <-chan *Item {
	itemChan := make(chan *Item)
	inp := s3.ListObjectsV2Input{
		Bucket: aws.String(url.Bucket),
		Prefix: aws.String(url.Prefix),
	}

	if url.Delimiter != "" {
		inp.SetDelimiter(url.Delimiter)
	}

	shouldPaginate := maxKeys < 0
	if !shouldPaginate {
		inp.SetMaxKeys(maxKeys)
	}

	go func() {
		defer close(itemChan)
		itemFound := false

		err := s.api.ListObjectsV2PagesWithContext(ctx, &inp, func(p *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, c := range p.CommonPrefixes {
				key := url.Match(aws.StringValue(c.Prefix))
				if key == "" {
					continue
				}

				itemChan <- &Item{
					Key:         key,
					IsDirectory: true,
				}

				itemFound = true
			}

			for _, c := range p.Contents {
				key := url.Match(aws.StringValue(c.Key))
				if key == "" {
					continue
				}

				itemChan <- &Item{
					Key:          key,
					Etag:         aws.StringValue(c.ETag),
					LastModified: aws.TimeValue(c.LastModified),
					IsDirectory:  strings.HasSuffix(key, "/"),
					Size:         aws.Int64Value(c.Size),
					StorageClass: aws.StringValue(c.StorageClass),
				}

				itemFound = true
			}

			if itemFound && lastPage {
				itemChan <- SequenceEndMarker
			}

			return shouldPaginate && !lastPage
		})

		if err != nil {
			itemChan <- &Item{Err: err}
			return
		}

		if !itemFound {
			itemChan <- &Item{Err: ErrNoItemFound}
		}
	}()

	return itemChan
}

func (s *S3) Copy(ctx context.Context, from, to *s3url.S3Url, cls string) error {
	_, err := s.api.CopyObject(&s3.CopyObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Key),
		CopySource:   aws.String(to.Format()),
		StorageClass: aws.String(cls),
	})
	return err
}

func (s *S3) Get(ctx context.Context, from *s3url.S3Url, to io.WriterAt) error {
	_, err := s.downloader.DownloadWithContext(ctx, to, &s3.GetObjectInput{
		Bucket: aws.String(from.Bucket),
		Key:    aws.String(from.Key),
	}, func(u *s3manager.Downloader) {
		u.PartSize = s.opts.DownloadChunkSizeBytes
		u.Concurrency = s.opts.DownloadConcurrency
	})
	return err
}

func (s *S3) Put(ctx context.Context, reader io.Reader, to *s3url.S3Url, cls string) error {
	_, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Key),
		Body:         reader,
		StorageClass: aws.String(cls),
	}, func(u *s3manager.Uploader) {
		u.PartSize = s.opts.UploadChunkSizeBytes
		u.Concurrency = s.opts.UploadConcurrency
	})

	return err
}

func (s *S3) Delete(ctx context.Context, bucket string, keys ...string) error {
	var objects []*s3.ObjectIdentifier
	for _, key := range keys {
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(key)})
	}

	_, err := s.api.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{Objects: objects},
	})
	return err
}

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

func (s *S3) UpdateRegion(bucket string) error {
	o, err := s.api.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: &bucket,
	})
	if err == nil && o.LocationConstraint == nil {
		err = ErrNilResult
	}
	if err != nil {
		return err
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

// NewAwsSession initializes a new AWS session with region fallback and custom options
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
