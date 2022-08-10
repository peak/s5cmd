package storage

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/peak/s5cmd/log"
	url "github.com/peak/s5cmd/storage/url"
	"io"
	"net/http"
	urlpkg "net/url"
	"os"
	"strconv"
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

// Re-used AWS sessions dramatically improve performance.
var globalClientCache = &ClientCache{
	clients: map[Options]*s3.Client{},
	configs: map[Options]*aws.Config{},
}

type S3 struct {
	client           s3Client
	config           aws.Config
	downloader       downloader
	uploader         uploader
	endpointURL      urlpkg.URL
	dryRun           bool
	useListObjectsV1 bool
	requestPayer     types.RequestPayer
}
type s3Client interface {
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	s3.HeadObjectAPIClient
	s3.ListObjectsV2APIClient
	ListObjectsAPIClient
	manager.UploadAPIClient
	manager.DownloadAPIClient
	manager.DeleteObjectsAPIClient
	manager.HeadBucketAPIClient
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	SelectObjectContent(ctx context.Context, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error)
}

type downloader interface {
	Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*manager.Downloader)) (n int64, err error)
}

type uploader interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
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

func newS3Storage(ctx context.Context, opts Options) (*S3, error) {
	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	cfg, client, err := globalClientCache.newClient(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &S3{
		client:           client,
		config:           *cfg,
		downloader:       manager.NewDownloader(client),
		uploader:         manager.NewUploader(client),
		endpointURL:      endpointURL,
		requestPayer:     types.RequestPayer(opts.RequestPayer),
		useListObjectsV1: opts.UseListObjectsV1,
		dryRun:           opts.DryRun,
	}, nil

}

func (cc *ClientCache) newClient(ctx context.Context, opts Options) (*aws.Config, s3Client, error) {
	cc.Lock()
	defer cc.Unlock()

	if client, ok := cc.clients[opts]; ok {
		cfg := cc.configs[opts]
		return cfg, client, nil
	}
	var awsOpts []func(*config.LoadOptions) error

	if opts.NoSignRequest {
		// do not sign requests when making service API calls
		awsOpts = append(awsOpts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	} else if opts.CredentialFile != "" || opts.Profile != "" {
		awsOpts = append(awsOpts, config.WithSharedConfigProfile(opts.Profile))
		awsOpts = append(awsOpts, config.WithSharedCredentialsFiles([]string{opts.CredentialFile}))
	}

	loadCfg := os.Getenv("AWS_SDK_LOAD_CONFIG")
	// sdk does not check AWS_SDK_LOAD_CONFIG environment variable
	// check here explicitly for backwards compatibility
	// if AWS_SDK_LOAD_CONFIG is 0 (or a falsy value) disable shared configs

	if loadCfg != "" {
		if enable, _ := strconv.ParseBool(loadCfg); !enable {
			awsOpts = append(awsOpts, config.WithSharedConfigFiles([]string{}))
		}
	}
	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return &aws.Config{}, nil, err
	}

	endpoint, isVirtualHostStyle := getEndpointOpts(endpointURL)
	awsOpts = append(awsOpts, endpoint)

	useAccelerate := supportsTransferAcceleration(endpointURL)
	// AWS SDK handles transfer acceleration automatically. Setting the
	// Endpoint to a transfer acceleration endpoint would cause bucket
	// operations fail.
	if useAccelerate {
		endpointURL = sentinelURL
	}

	if opts.NoVerifySSL {
		httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
			tr.Proxy = http.ProxyFromEnvironment
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		})
		awsOpts = append(awsOpts, config.WithHTTPClient(httpClient))
	}

	if opts.LogLevel == log.LevelTrace {
		awsOpts = append(awsOpts, config.WithClientLogMode(aws.LogResponse|aws.LogRequest))
		awsOpts = append(awsOpts, config.WithLogger(SdkLogger{}))
	}
	awsOpts = append(awsOpts, config.WithRetryer(customRetryer(opts.MaxRetries)))

	awsOpts, err = getRegionOpts(ctx, opts, isVirtualHostStyle, awsOpts...)

	if err != nil {
		return &aws.Config{}, nil, err
	}

	cfg, err := config.LoadDefaultConfig(ctx, awsOpts...)

	if err != nil {
		return &aws.Config{}, nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = !isVirtualHostStyle
		o.UseAccelerate = useAccelerate
	})

	cc.clients[opts] = client
	cc.configs[opts] = &cfg

	return &cfg, client, nil
}

func (cc *ClientCache) clear() {
	cc.Lock()
	defer cc.Unlock()
	cc.clients = map[Options]*s3.Client{}
	cc.configs = map[Options]*aws.Config{}
}

func getEndpointOpts(endpointURL urlpkg.URL) (config.LoadOptionsFunc, bool) {
	// use virtual-host-style if the endpoint is known to support it,
	// otherwise use the path-style approach.
	isVirtualHostStyle := isVirtualHostStyle(endpointURL)

	endpoint := config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               endpointURL.String(),
				Source:            aws.EndpointSourceCustom,
				HostnameImmutable: !isVirtualHostStyle}, nil
		}))
	return endpoint, isVirtualHostStyle
}

func getRegionOpts(ctx context.Context, opts Options, isVirtualHostStyle bool, awsOpts ...func(*config.LoadOptions) error) ([]func(*config.LoadOptions) error, error) {
	// Detects the region according to the following order of priority:
	// 1. --source-region or --destination-region flags of cp command.
	// 2. AWS_REGION environment variable.
	// 3. Region section of AWS profile.
	// 4. Auto-detection from bucket region (via HeadBucket).
	// 5. us-east-1 as default region.
	_, isEnvSet := os.LookupEnv("AWS_REGION")
	awsOpts = append(awsOpts, config.WithDefaultRegion("us-east-1"))

	if opts.region != "" {
		awsOpts = append(awsOpts, config.WithRegion(opts.region))
	} else if isEnvSet {
		//default config will load environment variable itself.
		return awsOpts, nil
	} else if opts.bucket == "" {
		return awsOpts, nil

	} else {
		tmpCfg, err := config.LoadDefaultConfig(ctx, awsOpts...)
		if err != nil {
			return nil, err
		}

		tmpClient := s3.NewFromConfig(tmpCfg, func(o *s3.Options) {
			o.UsePathStyle = !isVirtualHostStyle
		})

		// auto-detection
		region, err := manager.GetBucketRegion(ctx, tmpClient, opts.bucket, func(o *s3.Options) {
			o.Credentials = tmpCfg.Credentials
			o.UsePathStyle = !isVirtualHostStyle
		})
		if err != nil {

			if ErrHasCode(err, "bucket not found") {
				return awsOpts, err
			}
			// don't deny any request to the service if region auto-fetching
			// receives an error. Delegate error handling to command execution.
			err = fmt.Errorf("client: fetching region failed: %v", err)
			msg := log.ErrorMessage{Err: err.Error()}
			log.Error(msg)
		} else {
			awsOpts = append(awsOpts, config.WithRegion(region))
			return awsOpts, nil
		}
	}

	return awsOpts, nil
}

func customRetryer(maxRetries int) func() aws.Retryer {
	// AWS SDK counts maxAttempts instead of maxRetries.
	// Increase maxRetries by one to get maxAttempts.
	maxAttempts := maxRetries + 1

	return func() aws.Retryer {
		retrier := retry.AddWithMaxAttempts(aws.NopRetryer{}, maxAttempts)
		retrier = retry.AddWithErrorCodes(retrier,
			"InternalError",
			"RequestError",
			"UseOfClosedNetworkConnection",
			"ConnectionResetByPeer",
			"RequestFailureRequestError",
			"RequestTimeout",
			"ResponseTimeout",
			"RequestTimeTooSkewed",
			"ProvisionedThroughputExceededException",
			"Throttling",
			"ThrottlingException",
			"RequestLimitExceeded",
			"RequestThrottled",
			"RequestThrottledException",
			"ConnectionReset",
			"ConnectionTimedOut",
			"BrokenPipe",
			"UnknownSDKError",
		)

		return retry.AddWithMaxBackoffDelay(retrier, time.Second*0)
	}
}

type SdkLogger struct{}

func (l SdkLogger) Logf(classification logging.Classification, format string, v ...interface{}) {
	_ = classification
	msg := log.TraceMessage{
		Message: fmt.Sprintf(format, v...),
	}
	log.Trace(msg)
}

// ClientCache holds client.Client according to s3Opts and it synchronizes
// access/modification.
type ClientCache struct {
	sync.Mutex
	clients map[Options]*s3.Client
	configs map[Options]*aws.Config
}

func (s *S3) Stat(ctx context.Context, url *url.URL) (*Object, error) {

	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.RequestPayer(),
	})
	if err != nil {
		if ErrHasCode(err, "NotFound") {
			return nil, &ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		}
		return nil, err
	}

	etag := aws.ToString(output.ETag)
	mod := aws.ToTime(output.LastModified)
	return &Object{
		URL:     url,
		Etag:    etag,
		ModTime: &mod,
		Size:    aws.ToInt64(&output.ContentLength),
	}, nil
}

// List is a non-blocking S3 list operation which paginates and filters S3
// keys. If no object found or an error is encountered during this period,
// it sends these errors to object channel.
func (s *S3) List(ctx context.Context, url *url.URL, _ bool) <-chan *Object {

	if isGoogleEndpoint(s.endpointURL) || s.useListObjectsV1 {
		return s.listObjects(ctx, url)
	}

	return s.listObjectsV2(ctx, url)
}

func (s *S3) listObjectsV2(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := s3.ListObjectsV2Input{
		Bucket:       aws.String(url.Bucket),
		Prefix:       aws.String(url.Prefix),
		RequestPayer: s.RequestPayer(),
	}

	if url.Delimiter != "" {
		listInput.Delimiter = &url.Delimiter
	}
	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		paginator := s3.NewListObjectsV2Paginator(s.client, &listInput)

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
					Type:         ObjectType{mode: objtype},
					Size:         c.Size,
					StorageClass: StorageClass(c.StorageClass),
				}

				objectFound = true
			}

		}

		if !objectFound {
			objCh <- &Object{Err: ErrNoObjectFound}
		}

	}()
	return objCh

}

// listObjects is used for cloud services that does not support S3
func (s *S3) listObjects(ctx context.Context, url *url.URL) <-chan *Object {

	listInput := s3.ListObjectsInput{
		Bucket:       aws.String(url.Bucket),
		Prefix:       aws.String(url.Prefix),
		RequestPayer: s.RequestPayer(),
	}

	if url.Delimiter != "" {
		listInput.Delimiter = &url.Delimiter
	}
	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		paginator := NewListObjectsPaginator(s.client, &listInput)
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
					Type:         ObjectType{mode: objtype},
					Size:         c.Size,
					StorageClass: StorageClass(c.StorageClass),
				}

				objectFound = true
			}

		}
		if !objectFound {
			objCh <- &Object{Err: ErrNoObjectFound}
		}

	}()
	return objCh
}

// ListObjectsAPIClient is a client that implements the ListObjectsV2 operation.
type ListObjectsAPIClient interface {
	ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
}

var _ ListObjectsAPIClient = (*s3.Client)(nil)

// ListObjectsPaginatorOptions is the paginator options for ListObjects
type ListObjectsPaginatorOptions struct {
	// Sets the maximum number of keys returned in the response. By default the action
	// returns up to 1,000 key names. The response might contain fewer keys but will
	// never contain more.
	Limit int32

	// Set to true if pagination should stop if the service returns a pagination token
	// that matches the most recent token provided to the service.
	StopOnDuplicateToken bool
}

// ListObjectsPaginator is a paginator for ListObjects
type ListObjectsPaginator struct {
	options    ListObjectsPaginatorOptions
	client     ListObjectsAPIClient
	params     *s3.ListObjectsInput
	nextMarker *string
	firstPage  bool
}

// NewListObjectsPaginator returns a new ListObjectsV2Paginator
func NewListObjectsPaginator(client ListObjectsAPIClient, params *s3.ListObjectsInput, optFns ...func(*ListObjectsPaginatorOptions)) *ListObjectsPaginator {
	if params == nil {
		params = &s3.ListObjectsInput{}
	}

	options := ListObjectsPaginatorOptions{}
	if params.MaxKeys != 0 {
		options.Limit = params.MaxKeys
	}

	for _, fn := range optFns {
		fn(&options)
	}

	return &ListObjectsPaginator{
		options:    options,
		client:     client,
		params:     params,
		firstPage:  true,
		nextMarker: params.Marker,
	}
}

// HasMorePages returns a boolean indicating whether more pages are available
func (p *ListObjectsPaginator) HasMorePages() bool {
	return p.firstPage || (p.nextMarker != nil && len(*p.nextMarker) != 0)
}

// NextPage retrieves the next ListObjectsV2 page.
func (p *ListObjectsPaginator) NextPage(ctx context.Context, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	if !p.HasMorePages() {
		return nil, fmt.Errorf("no more pages available")
	}

	params := *p.params
	params.Marker = p.nextMarker

	params.MaxKeys = p.options.Limit

	result, err := p.client.ListObjects(ctx, &params, optFns...)
	if err != nil {
		return nil, err
	}
	p.firstPage = false

	prevToken := p.nextMarker
	p.nextMarker = nil
	if result.IsTruncated {
		p.nextMarker = result.NextMarker
	}

	if p.options.StopOnDuplicateToken &&
		prevToken != nil &&
		p.nextMarker != nil &&
		*prevToken == *p.nextMarker {
		p.nextMarker = nil
	}

	return result, nil
}

func (s *S3) Copy(ctx context.Context, from, to *url.URL, metadata Metadata) error {

	if s.dryRun {
		return nil
	}
	// SDK expects CopySource like "bucket[/key]
	copySource := from.EscapedPath()

	input := s3.CopyObjectInput{
		Bucket:     aws.String(to.Bucket),
		Key:        aws.String(to.Path),
		CopySource: aws.String(copySource),
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
	metadata Metadata,
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
			resultch <- &Object{URL: url}
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
		resultch <- &Object{Err: err}
		return
	}

	for _, d := range o.Deleted {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.ToString(d.Key))
		url, _ := url.New(key)
		resultch <- &Object{URL: url}
	}

	for _, e := range o.Errors {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.ToString(e.Key))
		url, _ := url.New(key)
		resultch <- &Object{
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
	o, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{})
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

var insecureHTTPClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Proxy:           http.ProxyFromEnvironment,
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
func ErrHasCode(err error, code string) bool {
	if err == nil || code == "" {
		return false
	}

	var ae smithy.APIError
	if errors.As(err, &ae) {
		if ae.ErrorCode() == code {
			return true
		}
	}

	var bucketNotFoundErr manager.BucketNotFound
	if errors.As(err, &bucketNotFoundErr) {
		if err.Error() == code {
			return true
		}
	}

	return false

}

// IsCancelationError reports whether given error is a storage related
// cancelation error.
func IsCancelationError(err error) bool {
	return ErrHasCode(err, "RequestCanceled")
}
