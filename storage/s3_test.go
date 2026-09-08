package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	urlpkg "net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/assert"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

// mockS3API implements s3API. Only the operations a test exercises need to be
// set; calling any other panics, which surfaces an unexpected call clearly.
// The v2 SDK ships no generated mock, so this stands in for v1's s3iface.
type mockS3API struct {
	s3API

	copyObject    func(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	headObject    func(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	listObjects   func(context.Context, *s3.ListObjectsInput, ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
	listObjectsV2 func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func (m *mockS3API) CopyObject(ctx context.Context, in *s3.CopyObjectInput, o ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return m.copyObject(ctx, in, o...)
}

func (m *mockS3API) HeadObject(ctx context.Context, in *s3.HeadObjectInput, o ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return m.headObject(ctx, in, o...)
}

func (m *mockS3API) ListObjects(ctx context.Context, in *s3.ListObjectsInput, o ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	return m.listObjects(ctx, in, o...)
}

func (m *mockS3API) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, o ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return m.listObjectsV2(ctx, in, o...)
}

// mockUploader implements uploaderAPI.
type mockUploader struct {
	upload func(context.Context, *s3.PutObjectInput, ...func(*s3Uploader)) (*manager.UploadOutput, error)
}

func (m *mockUploader) Upload(ctx context.Context, in *s3.PutObjectInput, o ...func(*s3Uploader)) (*manager.UploadOutput, error) {
	return m.upload(ctx, in, o...)
}

// apiError builds an error shaped like one the SDK would surface for a given
// S3 error code.
func apiError(code, message string) error {
	return &smithy.GenericAPIError{Code: code, Message: message}
}

// TestMain initialises the logger once: several code paths log through it,
// and which test runs first must not decide whether that panics.
func TestMain(m *testing.M) {
	log.Init("error", false)
	os.Exit(m.Run())
}

func TestS3ImplementsStorageInterface(t *testing.T) {
	var i any = new(S3)
	if _, ok := i.(Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}

func TestNewSessionPathStyle(t *testing.T) {
	testcases := []struct {
		name            string
		endpoint        urlpkg.URL
		expectPathStyle bool
	}{
		{
			name:            "expect_virtual_host_style_when_missing_endpoint",
			endpoint:        urlpkg.URL{},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_transfer_accel",
			endpoint:        urlpkg.URL{Scheme: "https", Host: transferAccelEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_google_cloud_storage",
			endpoint:        urlpkg.URL{Scheme: "https", Host: gcsEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_path_style_for_localhost",
			endpoint:        urlpkg.URL{Scheme: "http", Host: "127.0.0.1"},
			expectPathStyle: true,
		},
		{
			name:            "expect_path_style_for_secure_localhost",
			endpoint:        urlpkg.URL{Scheme: "https", Host: "127.0.0.1"},
			expectPathStyle: true,
		},
		{
			name:            "expect_path_style_for_custom_endpoint",
			endpoint:        urlpkg.URL{Scheme: "https", Host: "example.com"},
			expectPathStyle: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// path style is a client option in the v2 SDK, not a config field
			optFn, err := s3Options(Options{Endpoint: tc.endpoint.String()})
			if err != nil {
				t.Fatal(err)
			}

			var opts s3.Options
			optFn(&opts)

			if got := opts.UsePathStyle; got != tc.expectPathStyle {
				t.Fatalf("expected: %v, got: %v", tc.expectPathStyle, got)
			}
		})
	}
}

func TestNewSessionWithRegionSetViaEnv(t *testing.T) {
	globalSessionCache.clear()

	const expectedRegion = "us-west-2"

	os.Setenv("AWS_REGION", expectedRegion)
	defer os.Unsetenv("AWS_REGION")

	cfg, err := globalSessionCache.newSession(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}

	if got := cfg.Region; got != expectedRegion {
		t.Fatalf("expected %v, got %v", expectedRegion, got)
	}
}

func TestNewSessionWithNoSignRequest(t *testing.T) {
	globalSessionCache.clear()

	cfg, err := globalSessionCache.newSession(context.Background(), Options{
		NoSignRequest: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// LoadDefaultConfig wraps providers in a cache, so compare by provider
	// identity rather than by concrete type.
	if !aws.IsCredentialsProvider(cfg.Credentials, (*aws.AnonymousCredentials)(nil)) {
		t.Fatalf("expected anonymous credentials, got %T", cfg.Credentials)
	}
}

func TestNewSessionWithProfileFromFile(t *testing.T) {
	// create a temporary credentials file
	file, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	profiles := `[default]
aws_access_key_id = default_profile_key_id
aws_secret_access_key = default_profile_access_key

[p1]
aws_access_key_id = p1_profile_key_id
aws_secret_access_key = p1_profile_access_key

[p2]
aws_access_key_id = p2_profile_key_id
aws_secret_access_key = p2_profile_access_key`

	_, err = file.Write([]byte(profiles))
	if err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		name               string
		fileName           string
		profileName        string
		expAccessKeyID     string
		expSecretAccessKey string
	}{
		{
			name:               "use default profile",
			fileName:           file.Name(),
			profileName:        "",
			expAccessKeyID:     "default_profile_key_id",
			expSecretAccessKey: "default_profile_access_key",
		},
		{
			name:               "use a non-default profile",
			fileName:           file.Name(),
			profileName:        "p1",
			expAccessKeyID:     "p1_profile_key_id",
			expSecretAccessKey: "p1_profile_access_key",
		},
		{
			name:               "use a non-existent profile",
			fileName:           file.Name(),
			profileName:        "non-existent-profile",
			expAccessKeyID:     "",
			expSecretAccessKey: "",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			globalSessionCache.clear()
			cfg, err := globalSessionCache.newSession(context.Background(), Options{
				Profile:        tc.profileName,
				CredentialFile: tc.fileName,
			})
			if err != nil {
				// a missing profile is reported when the config loads in v2
				if tc.expAccessKeyID != "" || tc.expSecretAccessKey != "" {
					t.Fatal(err)
				}
				return
			}

			got, err := cfg.Credentials.Retrieve(context.Background())
			if err != nil {
				// if there should be such a profile but received an error fail,
				// ignore the error otherwise.
				if tc.expAccessKeyID != "" || tc.expSecretAccessKey != "" {
					t.Fatal(err)
				}
			}

			if got.AccessKeyID != tc.expAccessKeyID || got.SecretAccessKey != tc.expSecretAccessKey {
				t.Errorf("Expected credentials does not match the credential we got!\nExpected: Access Key ID: %v, Secret Access Key: %v\nGot    : Access Key ID: %v, Secret Access Key: %v\n", tc.expAccessKeyID, tc.expSecretAccessKey, got.AccessKeyID, got.SecretAccessKey)
			}
		})
	}
}

func TestS3ListURL(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockS3 := &S3{
		api: &mockS3API{
			listObjectsV2: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{
					CommonPrefixes: []types.CommonPrefix{
						{Prefix: aws.String("key/a/")},
						{Prefix: aws.String("key/b/")},
					},
					Contents: []types.Object{
						{Key: aws.String("key/test.txt")},
						{Key: aws.String("key/test.pdf")},
					},
				}, nil
			},
		},
	}

	responses := []struct {
		isDir  bool
		url    string
		relurl string
	}{
		{
			isDir:  true,
			url:    "s3://bucket/key/a/",
			relurl: "a/",
		},
		{
			isDir:  true,
			url:    "s3://bucket/key/b/",
			relurl: "b/",
		},
		{
			isDir:  false,
			url:    "s3://bucket/key/test.txt",
			relurl: "test.txt",
		},
		{
			isDir:  false,
			url:    "s3://bucket/key/test.pdf",
			relurl: "test.pdf",
		},
	}

	index := 0
	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != nil {
			t.Errorf("unexpected error: %v", got.Err)
			continue
		}

		want := responses[index]
		if diff := cmp.Diff(want.isDir, got.Type.IsDir()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(want.url, got.URL.Absolute()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(want.relurl, got.URL.Relative()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		index++
	}
}

func TestS3ListError(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockErr := fmt.Errorf("mock error")

	mockS3 := &S3{
		api: &mockS3API{
			listObjectsV2: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return nil, mockErr
			},
		},
	}

	for got := range mockS3.List(context.Background(), url, true) {
		if !errors.Is(got.Err, mockErr) {
			t.Errorf("error got = %v, want %v", got.Err, mockErr)
		}
	}
}

func TestS3ListNoItemFound(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockS3 := &S3{
		api: &mockS3API{
			listObjectsV2: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				// output does not include keys that match with given key
				return &s3.ListObjectsV2Output{
					CommonPrefixes: []types.CommonPrefix{
						{Prefix: aws.String("anotherkey/a/")},
						{Prefix: aws.String("anotherkey/b/")},
					},
					Contents: []types.Object{
						{Key: aws.String("a/b/c/d/test.txt")},
						{Key: aws.String("unknown/test.pdf")},
					},
				}, nil
			},
		},
	}

	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != ErrNoObjectFound {
			t.Errorf("error got = %v, want %v", got.Err, ErrNoObjectFound)
		}
	}
}

func TestS3ListContextCancelled(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockS3 := &S3{
		api: &mockS3API{
			listObjectsV2: func(ctx context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				// the SDK surfaces the context error from the transport
				return nil, fmt.Errorf("operation error S3: ListObjectsV2: %w", ctx.Err())
			},
		},
	}

	for got := range mockS3.List(ctx, url, true) {
		if !IsCancelationError(got.Err) {
			t.Errorf("error got = %v, want a cancelation error", got.Err)
			continue
		}
	}
}

// TestS3RetryableErrors covers the retry decisions s5cmd adds on top of the
// SDK's own. The SDK's standard retryer is trusted for the rest.
func TestS3RetryableErrors(t *testing.T) {
	log.Init("debug", false)

	testcases := []struct {
		name        string
		err         error
		expectRetry bool
	}{
		// codes s5cmd adds
		{name: "InternalError", err: apiError("InternalError", "internal error"), expectRetry: true},
		{name: "RequestTimeTooSkewed", err: apiError("RequestTimeTooSkewed", "skewed"), expectRetry: true},
		{name: "SlowDown", err: apiError("SlowDown", "Please reduce your request rate."), expectRetry: true},
		{name: "ConnectionReset", err: errors.New("read tcp: connection reset by peer"), expectRetry: true},
		{name: "ConnectionTimedOut", err: errors.New("dial tcp: connection timed out"), expectRetry: true},

		// codes the SDK already knows about
		{name: "RequestTimeout", err: apiError("RequestTimeout", "request timeout"), expectRetry: true},
		{name: "Throttling", err: apiError("Throttling", "throttling"), expectRetry: true},
		{name: "ThrottlingException", err: apiError("ThrottlingException", "throttling exception"), expectRetry: true},
		{name: "RequestLimitExceeded", err: apiError("RequestLimitExceeded", "request limit exceeded"), expectRetry: true},
		{name: "ProvisionedThroughputExceededException", err: apiError("ProvisionedThroughputExceededException", "exceeded"), expectRetry: true},

		// token errors must never be retried
		{name: "ExpiredToken", err: apiError("ExpiredToken", "expired token"), expectRetry: false},
		{name: "ExpiredTokenException", err: apiError("ExpiredTokenException", "expired token exception"), expectRetry: false},
		{name: "InvalidToken", err: apiError("InvalidToken", "invalid token"), expectRetry: false},
	}

	retryer := newCustomRetryer(5)

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryer.IsErrorRetryable(tc.err); got != tc.expectRetry {
				t.Errorf("expected retryable %v, got %v", tc.expectRetry, got)
			}
		})
	}
}

// TestS3RetryCount checks that a retryable failure is attempted the
// configured number of times.
func TestS3RetryCount(t *testing.T) {
	log.Init("debug", false)

	const maxRetries = 3

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	globalSessionCache.clear()
	opts := Options{
		Endpoint:      server.URL,
		NoSignRequest: true,
		MaxRetries:    maxRetries,
		region:        "us-east-1",
	}

	s3storage, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}

	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Fatal(err)
	}

	for range s3storage.List(context.Background(), u, true) {
	}

	// the initial attempt plus maxRetries retries
	if got, want := int(attempts.Load()), maxRetries+1; got != want {
		t.Errorf("expected %v attempts, got %v", want, got)
	}
}

func TestS3RetryOnNoSuchUpload(t *testing.T) {
	log.Init("debug", false)

	noSuchUploadError := apiError(errCodeNoSuchUpload, "The specified upload does not exist.")

	testcases := []struct {
		name           string
		err            error
		retryCount     int
		expectedUpload int
		expectedStat   int
	}{
		{
			name:           "Don't retry",
			err:            noSuchUploadError,
			retryCount:     0,
			expectedUpload: 1,
			expectedStat:   0,
		}, {
			name:           "Retry 5 times on NoSuchUpload error",
			err:            noSuchUploadError,
			retryCount:     5,
			expectedUpload: 6,
			expectedStat:   5,
		}, {
			name:           "No error",
			err:            nil,
			retryCount:     0,
			expectedUpload: 1,
			expectedStat:   0,
		},
	}

	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var uploads, stats int32

			mockS3 := &S3{
				api: &mockS3API{
					headObject: func(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
						atomic.AddInt32(&stats, 1)
						return nil, noSuchUploadError
					},
				},
				uploader: &mockUploader{
					upload: func(context.Context, *s3.PutObjectInput, ...func(*s3Uploader)) (*manager.UploadOutput, error) {
						atomic.AddInt32(&uploads, 1)
						return &manager.UploadOutput{}, tc.err
					},
				},
				noSuchUploadRetryCount: tc.retryCount,
			}

			ctx := context.Background()

			//nolint:errcheck // the error is the point of the test
			mockS3.Put(ctx, strings.NewReader(""), url, Metadata{}, manager.DefaultUploadConcurrency, manager.DefaultUploadPartSize)

			if got := int(atomic.LoadInt32(&uploads)); got != tc.expectedUpload {
				t.Errorf("expected %d upload requests, got %d", tc.expectedUpload, got)
			}
			if got := int(atomic.LoadInt32(&stats)); got != tc.expectedStat {
				t.Errorf("expected %d stat requests, got %d", tc.expectedStat, got)
			}
		})
	}
}

func TestS3CopyEncryptionRequest(t *testing.T) {
	testcases := []struct {
		name     string
		sse      string
		sseKeyID string
		acl      string

		expectedSSE      string
		expectedSSEKeyID string
		expectedACL      string
	}{
		{
			name: "no encryption/no acl, by default",
		},
		{
			name:        "encryption with AES256",
			sse:         "AES256",
			expectedSSE: "AES256",
		},
		{
			name:             "encryption with aws:kms",
			sse:              "aws:kms",
			sseKeyID:         "sseKeyID",
			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sseKeyID",
		},
		{
			name:        "acl option",
			acl:         "public-read",
			expectedACL: "public-read",
		},
		{
			name:             "encryption and acl together",
			sse:              "aws:kms",
			sseKeyID:         "sseKeyID",
			acl:              "bucket-owner-full-control",
			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sseKeyID",
			expectedACL:      "bucket-owner-full-control",
		},
	}

	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var got *s3.CopyObjectInput

			mockS3 := &S3{
				api: &mockS3API{
					copyObject: func(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
						got = in
						return &s3.CopyObjectOutput{}, nil
					},
				},
			}

			metadata := Metadata{}
			metadata.EncryptionMethod = tc.sse
			metadata.EncryptionKeyID = tc.sseKeyID
			metadata.ACL = tc.acl

			err = mockS3.Copy(context.Background(), u, u, metadata)
			if err != nil {
				t.Errorf("Expected %v, but received %q", nil, err)
			}

			assert.Equal(t, string(got.ServerSideEncryption), tc.expectedSSE)
			assert.Equal(t, aws.ToString(got.SSEKMSKeyId), tc.expectedSSEKeyID)
			assert.Equal(t, string(got.ACL), tc.expectedACL)
		})
	}
}

func TestS3PutEncryptionRequest(t *testing.T) {
	testcases := []struct {
		name     string
		sse      string
		sseKeyID string
		acl      string

		expectedSSE      string
		expectedSSEKeyID string
		expectedACL      string
	}{
		{
			name: "no encryption/no acl, by default",
		},
		{
			name:        "encryption with AES256",
			sse:         "AES256",
			expectedSSE: "AES256",
		},
		{
			name:             "encryption with aws:kms",
			sse:              "aws:kms",
			sseKeyID:         "sseKeyID",
			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sseKeyID",
		},
		{
			name:        "acl option",
			acl:         "public-read",
			expectedACL: "public-read",
		},
		{
			name:             "encryption and acl together",
			sse:              "aws:kms",
			sseKeyID:         "sseKeyID",
			acl:              "bucket-owner-full-control",
			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sseKeyID",
			expectedACL:      "bucket-owner-full-control",
		},
	}

	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var got *s3.PutObjectInput

			mockS3 := &S3{
				uploader: &mockUploader{
					upload: func(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3Uploader)) (*manager.UploadOutput, error) {
						got = in
						return &manager.UploadOutput{}, nil
					},
				},
			}

			metadata := Metadata{}
			metadata.EncryptionMethod = tc.sse
			metadata.EncryptionKeyID = tc.sseKeyID
			metadata.ACL = tc.acl

			err = mockS3.Put(context.Background(), bytes.NewReader([]byte("")), u, metadata, 1, 5242880)
			if err != nil {
				t.Errorf("Expected %v, but received %q", nil, err)
			}

			assert.Equal(t, string(got.ServerSideEncryption), tc.expectedSSE)
			assert.Equal(t, aws.ToString(got.SSEKMSKeyId), tc.expectedSSEKeyID)
			assert.Equal(t, string(got.ACL), tc.expectedACL)
		})
	}
}

func TestS3listObjectsV2(t *testing.T) {
	const (
		numObjectsToReturn = 10100
		numObjectsToIgnore = 1127

		pre = "s3://bucket/key"
	)

	u, err := url.New(pre)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mapReturnObjNameToModtime := map[string]time.Time{}
	mapIgnoreObjNameToModtime := map[string]time.Time{}

	s3objs := make([]types.Object, 0, numObjectsToIgnore+numObjectsToReturn)

	for i := range numObjectsToReturn {
		fname := fmt.Sprintf("%s/%d", pre, i)
		now := time.Now()

		mapReturnObjNameToModtime[pre+"/"+fname] = now
		s3objs = append(s3objs, types.Object{
			Key:          aws.String("key/" + fname),
			LastModified: aws.Time(now),
		})
	}

	for i := range numObjectsToIgnore {
		fname := fmt.Sprintf("%s/%d", pre, numObjectsToReturn+i)
		later := time.Now().Add(time.Second * 10)

		mapIgnoreObjNameToModtime[pre+"/"+fname] = later
		s3objs = append(s3objs, types.Object{
			Key:          aws.String("key/" + fname),
			LastModified: aws.Time(later),
		})
	}

	// shuffle the objects array to remove possible assumptions about how objects
	// are stored.
	rand.Shuffle(len(s3objs), func(i, j int) {
		s3objs[i], s3objs[j] = s3objs[j], s3objs[i]
	})

	mockS3 := &S3{
		api: &mockS3API{
			listObjectsV2: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{Contents: s3objs}, nil
			},
		},
	}

	ouputCh := mockS3.listObjectsV2(context.Background(), u)

	for obj := range ouputCh {
		if _, ok := mapReturnObjNameToModtime[obj.String()]; ok {
			delete(mapReturnObjNameToModtime, obj.String())
			continue
		}
		t.Errorf("%v should not have been returned\n", obj)
	}
	assert.Equal(t, len(mapReturnObjNameToModtime), 0)
}

func TestSessionCreateAndCachingWithDifferentBuckets(t *testing.T) {
	log.Init("error", false)
	testcases := []struct {
		bucket         string
		alreadyCreated bool // sessions should not be created again if they already have been created before
	}{
		{bucket: "bucket"},
		{bucket: "bucket", alreadyCreated: true},
		{bucket: "test-bucket"},
	}

	sess := map[string]aws.Config{}

	for _, tc := range testcases {
		awsSess, err := globalSessionCache.newSession(context.Background(), Options{
			bucket: tc.bucket,
		})
		if err != nil {
			t.Error(err)
		}

		if tc.alreadyCreated {
			_, ok := sess[tc.bucket]
			assert.Check(t, ok, "session should not have been created again")
		} else {
			sess[tc.bucket] = awsSess
		}
	}
}

func TestSessionRegionDetection(t *testing.T) {
	bucketRegion := "sa-east-1"

	testcases := []struct {
		name           string
		bucket         string
		optsRegion     string
		envRegion      string
		expectedRegion string
	}{
		{
			name:           "RegionWithSourceRegionParameter",
			bucket:         "bucket",
			optsRegion:     "ap-east-1",
			envRegion:      "ca-central-1",
			expectedRegion: "ap-east-1",
		},
		{
			name:           "RegionWithEnvironmentVariable",
			bucket:         "bucket",
			optsRegion:     "",
			envRegion:      "ca-central-1",
			expectedRegion: "ca-central-1",
		},
		{
			name:           "RegionWithBucketRegion",
			bucket:         "bucket",
			optsRegion:     "",
			envRegion:      "",
			expectedRegion: bucketRegion,
		},
		{
			name:           "DefaultRegion",
			bucket:         "",
			optsRegion:     "",
			envRegion:      "",
			expectedRegion: "us-east-1",
		},
	}

	// ignore local profile loading
	os.Setenv("AWS_SDK_LOAD_CONFIG", "0")
	defer os.Unsetenv("AWS_SDK_LOAD_CONFIG")

	// mock auto bucket detection
	server := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Amz-Bucket-Region", bucketRegion)
			w.WriteHeader(http.StatusOK)
		}))
	}()
	defer server.Close()

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Endpoint: server.URL,

				// since profile loading disabled above, we need to provide
				// credentials to the session. NoSignRequest could be used
				// for anonymous credentials.
				NoSignRequest: true,
			}

			if tc.optsRegion != "" {
				opts.region = tc.optsRegion
			}

			if tc.envRegion != "" {
				os.Setenv("AWS_REGION", tc.envRegion)
				defer os.Unsetenv("AWS_REGION")
			}

			if tc.bucket != "" {
				opts.bucket = tc.bucket
			}

			globalSessionCache.clear()

			cfg, err := globalSessionCache.newSession(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}

			if got := cfg.Region; got != tc.expectedRegion {
				t.Fatalf("expected %v, got %v", tc.expectedRegion, got)
			}
		})
	}
}

func TestSessionAutoRegionValidateCredentials(t *testing.T) {
	var signed bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// the configured credentials must be used to sign the HeadBucket
		// request the region lookup makes
		if strings.Contains(r.Header.Get("Authorization"), "AKID") {
			signed = true
		}
		w.Header().Set("X-Amz-Bucket-Region", "")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// the region lookup only runs when no region is configured, so keep the
	// ambient environment and shared config out of it
	os.Setenv("AWS_SDK_LOAD_CONFIG", "0")
	os.Unsetenv("AWS_REGION")
	os.Unsetenv("AWS_DEFAULT_REGION")
	os.Setenv("AWS_ACCESS_KEY_ID", "AKID")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "SECRET")
	defer os.Unsetenv("AWS_SDK_LOAD_CONFIG")
	defer os.Unsetenv("AWS_ACCESS_KEY_ID")
	defer os.Unsetenv("AWS_SECRET_ACCESS_KEY")

	globalSessionCache.clear()
	opts := Options{Endpoint: server.URL, bucket: "bucket"}

	cfg, err := globalSessionCache.newSession(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	_ = cfg

	if !signed {
		t.Error("session credentials are expected to be used during HeadBucket request")
	}
}

func TestSessionAutoRegion(t *testing.T) {
	log.Init("error", false)

	testcases := []struct {
		name              string
		bucket            string
		region            string
		status            int
		expectedRegion    string
		expectedErrorCode string
	}{
		{
			name:           "NoLocationConstraint",
			bucket:         "bucket",
			region:         "",
			status:         http.StatusOK,
			expectedRegion: "us-east-1",
		},
		{
			name:           "LocationConstraintDefaultRegion",
			bucket:         "bucket",
			region:         "us-east-1",
			status:         http.StatusOK,
			expectedRegion: "us-east-1",
		},
		{
			name:           "LocationConstraintAnotherRegion",
			bucket:         "bucket",
			region:         "us-west-2",
			status:         http.StatusOK,
			expectedRegion: "us-west-2",
		},
		{
			name:              "BucketNotFoundErrorMustFail",
			bucket:            "bucket",
			status:            http.StatusNotFound,
			expectedRegion:    "us-east-1",
			expectedErrorCode: "NotFound",
		},
		{
			name:           "AccessDeniedErrorMustNotFail",
			bucket:         "bucket",
			status:         http.StatusForbidden,
			expectedRegion: "us-east-1",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.region != "" {
					w.Header().Set("X-Amz-Bucket-Region", tc.region)
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()

			cfg := aws.Config{Credentials: aws.AnonymousCredentials{}}
			opts := Options{Endpoint: server.URL, bucket: tc.bucket, NoSignRequest: true}

			err := setSessionRegion(context.Background(), &cfg, opts)
			if tc.expectedErrorCode != "" && !errHasCode(err, tc.expectedErrorCode) {
				t.Errorf("expected error code: %v, got error: %v", tc.expectedErrorCode, err)
				return
			}

			if expected, got := tc.expectedRegion, cfg.Region; expected != got {
				t.Errorf("expected: %v, got: %v", expected, got)
			}
		})
	}
}

func TestS3ListObjectsAPIVersions(t *testing.T) {
	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var calledV1, calledV2 bool

	mockS3 := &S3{
		api: &mockS3API{
			listObjects: func(context.Context, *s3.ListObjectsInput, ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
				calledV1 = true
				return &s3.ListObjectsOutput{}, nil
			},
			listObjectsV2: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				calledV2 = true
				return &s3.ListObjectsV2Output{}, nil
			},
		},
	}

	t.Run("list-objects-v2", func(t *testing.T) {
		calledV1, calledV2 = false, false
		mockS3.useListObjectsV1 = false
		for range mockS3.List(context.Background(), u, false) {
		}

		if !calledV2 || calledV1 {
			t.Errorf("expected ListObjectsV2 to be used; v1=%v v2=%v", calledV1, calledV2)
		}
	})

	t.Run("list-objects-v1", func(t *testing.T) {
		calledV1, calledV2 = false, false
		mockS3.useListObjectsV1 = true
		for range mockS3.List(context.Background(), u, false) {
		}

		if !calledV1 || calledV2 {
			t.Errorf("expected ListObjects to be used; v1=%v v2=%v", calledV1, calledV2)
		}
	})
}

func TestAWSLogLevel(t *testing.T) {
	testcases := []struct {
		name     string
		level    string
		expected aws.ClientLogMode
	}{
		{
			name:     "Trace: request and response bodies must be logged",
			level:    "trace",
			expected: aws.LogRequestWithBody | aws.LogResponseWithBody | aws.LogRetries,
		},
		{
			name:     "Debug: no SDK logging",
			level:    "debug",
			expected: 0,
		},
		{
			name:     "Info: no SDK logging",
			level:    "info",
			expected: 0,
		},
		{
			name:     "Error: no SDK logging",
			level:    "error",
			expected: 0,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			globalSessionCache.clear()
			cfg, err := globalSessionCache.newSession(context.Background(), Options{
				LogLevel: log.LevelFromString(tc.level),
			})
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(cfg.ClientLogMode, tc.expected); diff != "" {
				t.Errorf("%s: (-want +got):\n%v", tc.name, diff)
			}
		})
	}
}

func TestS3HeadObject(t *testing.T) {
	testcases := []struct {
		name string
		url  string
	}{
		{
			name: "HeadObject",
			url:  "s3://bucket/key",
		},
		{
			name: "HeadObject with different URL",
			url:  "s3://another-bucket/another-key",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.New(tc.url)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			var got *s3.HeadObjectInput

			mockS3 := &S3{
				api: &mockS3API{
					headObject: func(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
						got = in
						return &s3.HeadObjectOutput{}, nil
					},
				},
			}

			_, _, err = mockS3.HeadObject(context.Background(), u)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			assert.Equal(t, aws.ToString(got.Bucket), u.Bucket)
			assert.Equal(t, aws.ToString(got.Key), u.Path)
		})
	}
}
