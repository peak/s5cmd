package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/peak/s5cmd/command"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	url "github.com/peak/s5cmd/storage/url"
	urlpkg "net/url"
	"strings"
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
	requestPayer     types.RequestPayer
	endpointURL      urlpkg.URL
	dryRun           bool
	useListObjectsV1 bool
}

type s3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	s3.HeadObjectAPIClient
	s3.ListObjectsV2APIClient
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
		//todo: did not export errHasCode, use commented line later
		//if storage.errHasCode(err, "NotFound") {
		//return nil, &storage.ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
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

func isGoogleEndpoint(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == gcsEndpoint
}

func main() {

	nurl, err := url.New("s3://bucket/s5cmd*")
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

	for object := range s3.listObjectsV2(context.TODO(), nurl) {

		msg := command.ListMessage{
			Object: object,
		}

		log.Init("debug", false)
		fmt.Println(object.URL)
		log.Info(msg)

	}

}
