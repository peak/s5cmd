package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

type S3 struct {
	api        s3iface.S3API
	downloader s3manageriface.DownloaderAPI
	uploader   s3manageriface.UploaderAPI
	opts       S3Opts
}

type S3Opts struct {
	MaxRetries           int
	EndpointURL          string
	Region               string
	NoVerifySSL          bool
	MultipartSize        int64
	MultipartConcurrency int
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

func (s *S3) Head(ctx context.Context, to, key string) (*Item, error) {
	output, err := s.api.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(to),
		Key:    aws.String(key),
	})

	if err != nil {
		return nil, err
	}

	return &Item{
		Content: &s3.Object{
			ETag:         output.ETag,
			LastModified: output.LastModified,
			Size:         output.ContentLength,
		},
		Key: key,
	}, nil
}

func (s *S3) List(ctx context.Context, url *s3url.S3Url) <-chan *ItemResponse {
	itemChan := make(chan *ItemResponse)
	inp := s3.ListObjectsV2Input{
		Bucket: aws.String(url.Bucket),
		Prefix: aws.String(url.Prefix),
	}
	if url.Delimiter != "" {
		inp.SetDelimiter(url.Delimiter)
	}

	go func() {
		defer close(itemChan)
		itemFound := false

		err := s.api.ListObjectsV2PagesWithContext(ctx, &inp, func(p *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, c := range p.CommonPrefixes {
				key, ok := url.Match(*c.Prefix)
				if !ok {
					continue
				}

				itemChan <- &ItemResponse{
					Item: &Item{
						Content:     &s3.Object{Key: c.Prefix},
						Key:         key,
						IsDirectory: true,
					},
				}
				itemFound = true
			}
			for _, c := range p.Contents {
				key, ok := url.Match(*c.Key)
				if !ok {
					continue
				}

				itemChan <- &ItemResponse{
					Item: &Item{
						Content:     c,
						Key:         key,
						IsDirectory: key[len(key)-1] == '/',
					},
				}
				itemFound = true
			}

			return !lastPage
		})

		if err != nil {
			itemChan <- &ItemResponse{Err: err}
			return
		}

		if !itemFound {
			itemChan <- &ItemResponse{Err: ErrNoItemFound}
		}
	}()

	return itemChan
}

func (s *S3) Copy(ctx context.Context, from, key, dst, cls string) error {
	_, err := s.api.CopyObject(&s3.CopyObjectInput{
		Bucket:       aws.String(from),
		Key:          aws.String(key),
		CopySource:   aws.String(dst),
		StorageClass: aws.String(cls),
	})
	return err
}

func (s *S3) Get(ctx context.Context, to io.WriterAt, from, key string) error {
	_, err := s.downloader.DownloadWithContext(ctx, to, &s3.GetObjectInput{
		Bucket: aws.String(from),
		Key:    aws.String(key),
	}, func(u *s3manager.Downloader) {
		u.PartSize = s.opts.MultipartSize
		u.Concurrency = s.opts.MultipartConcurrency
	})
	return err
}

func (s *S3) Put(ctx context.Context, content io.Reader, to, key, cls string) error {
	_, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:       aws.String(to),
		Key:          aws.String(key),
		Body:         content,
		StorageClass: aws.String(cls),
	}, func(u *s3manager.Uploader) {
		u.PartSize = s.opts.MultipartSize
		u.Concurrency = s.opts.MultipartConcurrency
	})

	return err
}

func (s *S3) Remove(ctx context.Context, from string, keys ...string) error {
	var objects []*s3.ObjectIdentifier
	for _, key := range keys {
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(key)})
	}

	_, err := s.api.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(from),
		Delete: &s3.Delete{
			Objects: objects,
		},
	})
	return err
}

func (s *S3) ListBuckets(ctx context.Context, prefix string) ([]string, error) {
	o, err := s.api.ListBucketsWithContext(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []string
	for _, b := range o.Buckets {
		if prefix == "" || strings.HasPrefix(*b.Name, prefix) {
			buckets = append(buckets, prefix)
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
		MaxRetries:           s.opts.MaxRetries,
		EndpointURL:          s.opts.EndpointURL,
		Region:               *o.LocationConstraint,
		NoVerifySSL:          s.opts.NoVerifySSL,
		MultipartSize:        s.opts.MultipartSize,
		MultipartConcurrency: s.opts.MultipartConcurrency,
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
		endpoint, err := url.Parse(opts.EndpointURL)
		if err != nil {
			return nil, err
		}

		awsCfg = awsCfg.WithEndpoint(opts.EndpointURL).WithS3ForcePathStyle(true)

		const acceleratedHost = "s3-accelerate.amazonaws.com"
		if endpoint.Hostname() == acceleratedHost {
			awsCfg = awsCfg.WithS3UseAccelerate(true).WithS3ForcePathStyle(false)
		}
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
