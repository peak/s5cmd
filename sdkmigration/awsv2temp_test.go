package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	"gotest.tools/v3/assert"
	"math/rand"
	"net/http"
	"net/http/httptest"
	urlpkg "net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestS3ImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(S3)
	if _, ok := i.(storage.Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}

//todo: problematice
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
			endpoint:        urlpkg.URL{Host: transferAccelEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_google_cloud_storage",
			endpoint:        urlpkg.URL{Host: gcsEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_path_style_for_localhost",
			endpoint:        urlpkg.URL{Host: "127.0.0.1"},
			expectPathStyle: true,
		},
		{
			name:            "expect_path_style_for_custom_endpoint",
			endpoint:        urlpkg.URL{Host: "example.com"},
			expectPathStyle: true,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			opts := storage.Options{Endpoint: tc.endpoint.Hostname()}
			s3, err := newS3Storage(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			buckets, err := s3.ListBuckets(context.TODO(), "bucket")
			fmt.Println(buckets)

			epu := s3.endpointURL
			fmt.Println("epu", epu)
			bol := true
			//s3.Config.S3ForcePathStyle
			fmt.Println(s3.config.APIOptions)
			got := aws.ToBool(&bol)
			if got != tc.expectPathStyle {
				t.Fatalf("expected: %v, got: %v", tc.expectPathStyle, got)
			}
		})
	}
}

func TestNewSessionWithRegionSetViaEnv(t *testing.T) {

	const expectedRegion = "us-west-2"

	os.Setenv("AWS_REGION", expectedRegion)
	defer os.Unsetenv("AWS_REGION")

	opts := storage.Options{}
	s3, err := newS3Storage(context.Background(), opts)

	if err != nil {
		t.Fatal(err)
	}
	got := s3.config.Region
	s3.config.Region = "us-test-1"
	fmt.Println(s3.config.Region)
	if got != expectedRegion {
		t.Fatalf("expected %v, got %v", expectedRegion, got)
	}
}

func TestNewSessionWithNoSignRequest(t *testing.T) {

	opts := storage.Options{NoSignRequest: true}
	s3, err := newS3Storage(context.Background(), opts)

	if err != nil {
		t.Fatal(err)
	}

	_, gotErr := s3.config.Credentials.Retrieve(context.Background())

	expectedErr := "AnonymousCredentials is not a valid credential provider, and cannot be used to sign AWS requests with"
	//todo search if there is a better way to test this.
	if !strings.Contains(gotErr.Error(), expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, gotErr)
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
		expAccessKeyId     string
		expSecretAccessKey string
	}{
		{
			name:               "use default profile",
			fileName:           file.Name(),
			profileName:        "",
			expAccessKeyId:     "default_profile_key_id",
			expSecretAccessKey: "default_profile_access_key",
		},
		{
			name:               "use a non-default profile",
			fileName:           file.Name(),
			profileName:        "p1",
			expAccessKeyId:     "p1_profile_key_id",
			expSecretAccessKey: "p1_profile_access_key",
		},
		{

			name:               "use a non-existent profile",
			fileName:           file.Name(),
			profileName:        "non-existent-profile",
			expAccessKeyId:     "",
			expSecretAccessKey: "",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {

			s3, err := newS3Storage(context.Background(), storage.Options{
				Profile:        tc.profileName,
				CredentialFile: tc.fileName,
			})
			if err != nil {
				t.Fatal(err)
			}

			got, err := s3.config.Credentials.Retrieve(context.Background())
			if err != nil {
				// if there should be such a profile but received an error fail,
				// ignore the error otherwise.
				if tc.expAccessKeyId != "" || tc.expSecretAccessKey != "" {
					t.Fatal(err)
				}
			}

			if got.AccessKeyID != tc.expAccessKeyId || got.SecretAccessKey != tc.expSecretAccessKey {
				t.Errorf("Expected credentials does not match the credential we got!\nExpected: Access Key ID: %v, Secret Access Key: %v\nGot    : Access Key ID: %v, Secret Access Key: %v\n", tc.expAccessKeyId, tc.expSecretAccessKey, got.AccessKeyID, got.SecretAccessKey)
			}
		})
	}
}

func TestS3ListObjects(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMocks3Client(ctrl)
	mockS3 := &S3{
		client: m,
	}

	m.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(
		&s3.ListObjectsV2Output{
			CommonPrefixes: []types.CommonPrefix{
				{Prefix: aws.String("key/a/")},
				{Prefix: aws.String("key/b/")},
			},
			Contents: []types.Object{
				{Key: aws.String("key/test.txt")},
				{Key: aws.String("key/test.pdf")},
			},
		}, nil,
	)

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
	for got := range mockS3.listObjectsV2(context.Background(), url) {
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMocks3Client(ctrl)
	mockS3 := &S3{
		client: m,
	}
	mockErr := fmt.Errorf("mock error")

	m.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(
		nil, mockErr,
	)

	for got := range mockS3.listObjectsV2(context.Background(), url) {
		if got.Err != mockErr {
			t.Errorf("error got = %v, want %v", got.Err, mockErr)
		}
	}
}

func TestS3ListNoItemFound(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMocks3Client(ctrl)
	mockS3 := &S3{
		client: m,
	}

	m.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(
		// output does not include keys that match with given key
		&s3.ListObjectsV2Output{
			CommonPrefixes: []types.CommonPrefix{
				{Prefix: aws.String("anotherkey/a/")},
				{Prefix: aws.String("anotherkey/b/")},
			},
			Contents: []types.Object{
				{Key: aws.String("a/b/c/d/test.txt")},
				{Key: aws.String("unknown/test.pdf")},
			},
		}, nil,
	)

	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != storage.ErrNoObjectFound {
			t.Errorf("error got = %v, want %v", got.Err, storage.ErrNoObjectFound)
		}
	}
}

func TestS3Retry(t *testing.T) {

	testcases := []struct {
		name          string
		expectedRetry int
	}{
		// Internal error
		{
			name:          "InternalError",
			expectedRetry: 5,
		},

		// Request errors
		{
			name:          "RequestError",
			expectedRetry: 5,
		},
		{
			name:          "UseOfClosedNetworkConnection",
			expectedRetry: 5,
		},
		{
			name:          "ConnectionResetByPeer",
			expectedRetry: 5,
		},
		{
			name:          "RequestFailureRequestError",
			expectedRetry: 5,
		},
		{
			name:          "RequestTimeout",
			expectedRetry: 5,
		},
		{
			name:          "ResponseTimeout",
			expectedRetry: 5,
		},
		{
			name:          "RequestTimeTooSkewed",
			expectedRetry: 5,
		},

		// Throttling errors
		{
			name:          "ProvisionedThroughputExceededException",
			expectedRetry: 5,
		},
		{
			name:          "Throttling",
			expectedRetry: 5,
		},
		{
			name:          "ThrottlingException",
			expectedRetry: 5,
		},
		{
			name:          "RequestLimitExceeded",
			expectedRetry: 5,
		},
		{
			name:          "RequestThrottled",
			expectedRetry: 5,
		},
		{
			name:          "RequestThrottledException",
			expectedRetry: 5,
		},

		// Expired credential errors
		{
			name:          "ExpiredToken",
			expectedRetry: 0,
		},
		{
			name:          "ExpiredTokenException",
			expectedRetry: 0,
		},

		// Invalid Token errors
		{
			name:          "InvalidToken",
			expectedRetry: 0,
		},

		// Connection errors
		{
			name:          "ConnectionReset",
			expectedRetry: 5,
		},
		{
			name:          "ConnectionTimedOut",
			expectedRetry: 5,
		},
		{
			name:          "BrokenPipe",
			expectedRetry: 5,
		},

		// Unknown errors
		{
			name:          "UnknownSDKError",
			expectedRetry: 5,
		},
	}
	const expectedRetry = 5

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			var count int32
			mw := middleware.DeserializeMiddlewareFunc("GetObject", func(
				ctx context.Context,
				in middleware.DeserializeInput,
				next middleware.DeserializeHandler,
			) (
				out middleware.DeserializeOutput,
				metadata middleware.Metadata,
				err error,
			) {
				atomic.AddInt32(&count, 1)
				return out, metadata, &smithy.GenericAPIError{Code: tc.name}
			})

			s3c, err := newS3Storage(ctx, storage.Options{MaxRetries: expectedRetry})
			_, err = s3c.client.GetObject(
				context.Background(),
				&s3.GetObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")},
				func(options *s3.Options) {
					options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
						return stack.Deserialize.Add(mw, middleware.After)
					})
				},
			)
			fmt.Println(err.Error())

			got := int(atomic.LoadInt32(&count))
			expected := tc.expectedRetry

			if strings.Contains(err.Error(), "exceeded maximum number of attempts") {
				if got != expected {
					t.Errorf("expected %v retries, got %v", expected, got)
				}
			} else if expected != 0 {
				t.Errorf("expected %v retries, got %v", expected, got)
			}

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

	for i := 0; i < numObjectsToReturn; i++ {
		fname := fmt.Sprintf("%s/%d", pre, i)
		now := time.Now()

		mapReturnObjNameToModtime[pre+"/"+fname] = now
		s3objs = append(s3objs, types.Object{
			Key:          aws.String("key/" + fname),
			LastModified: aws.Time(now),
		})
	}

	for i := 0; i < numObjectsToIgnore; i++ {
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMocks3Client(ctrl)
	mockS3 := &S3{
		client: m,
	}

	m.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(&s3.ListObjectsV2Output{
		Contents: s3objs,
	}, nil)

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
			opts := storage.Options{
				LogLevel: log.LevelError,
				Endpoint: server.URL,

				// since profile loading disabled above, we need to provide
				// credentials to the session. NoSignRequest could be used
				// for anonymous credentials.
				NoSignRequest: false,
			}

			if tc.optsRegion != "" {
				opts.Region = tc.optsRegion
			}

			if tc.envRegion != "" {
				os.Setenv("AWS_REGION", tc.envRegion)
				defer os.Unsetenv("AWS_REGION")
			}

			if tc.bucket != "" {
				opts.Bucket = tc.bucket
			}

			s3c, err := newS3Storage(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}

			got := s3c.config.Region
			if got != tc.expectedRegion {
				t.Fatalf("expected %v, got %v", tc.expectedRegion, got)
			}
		})
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
			expectedErrorCode: "bucket not found",
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

			// mock auto bucket detection
			server := func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if tc.region != "" {
						w.Header().Set("X-Amz-Bucket-Region", tc.region)
					}
					w.WriteHeader(tc.status)
				}))
			}()
			defer server.Close()

			opts := storage.Options{
				LogLevel: log.LevelError,
				Endpoint: server.URL,

				// since profile loading disabled above, we need to provide
				// credentials to the session. NoSignRequest could be used
				// for anonymous credentials.
				NoSignRequest: false,
			}

			if tc.bucket != "" {
				opts.Bucket = tc.bucket
			}

			s3c, err := newS3Storage(context.Background(), opts)
			if tc.expectedErrorCode != "" {
				if !storage.ErrHasCode(err, tc.expectedErrorCode) {
					t.Errorf("expected error code: %v, got error: %v", tc.expectedErrorCode, err)
					return
				}
			} else if expected, got := tc.expectedRegion, s3c.config.Region; expected != got {
				t.Errorf("expected: %v, got: %v", expected, got)
			}

		})
	}
}

func TestAWSLogLevel(t *testing.T) {
	testcases := []struct {
		name     string
		level    string
		expected []aws.ClientLogMode
	}{
		{
			name:     "Trace: log level must be aws.LogResponse and aws.LogRequest",
			level:    "trace",
			expected: []aws.ClientLogMode{aws.LogResponse, aws.LogRequest},
		},
		{
			name:     "Debug: log level must be 0",
			level:    "debug",
			expected: []aws.ClientLogMode{0},
		},
		{
			name:     "Info: log level must be 0",
			level:    "info",
			expected: []aws.ClientLogMode{0},
		},
		{
			name:     "Error: log level must be 0",
			level:    "error",
			expected: []aws.ClientLogMode{0},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {

			s3c, err := newS3Storage(context.Background(), storage.Options{
				LogLevel: log.LevelFromString(tc.level),
			})

			if err != nil {
				t.Fatal(err)
			}

			cfgLogLevel := s3c.config.ClientLogMode
			for _, expectedLogLevel := range tc.expected {
				if expectedLogLevel == aws.LogRequest {
					assert.Equal(t, cfgLogLevel.IsRequest(), true)
				}
				if expectedLogLevel == aws.LogResponse {
					assert.Equal(t, cfgLogLevel.IsResponse(), true)
				}
				if expectedLogLevel == 0 {
					assert.Equal(t, int(expectedLogLevel), 0)
				}

			}

		})
	}
}
