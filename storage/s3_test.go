package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	urlpkg "net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/assert"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage/url"
)

func TestS3ImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(S3)
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

			opts := Options{Endpoint: tc.endpoint.Hostname()}
			sess, err := globalSessionCache.newSession(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}

			got := aws.BoolValue(sess.Config.S3ForcePathStyle)
			if got != tc.expectPathStyle {
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

	sess, err := globalSessionCache.newSession(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}

	got := aws.StringValue(sess.Config.Region)
	if got != expectedRegion {
		t.Fatalf("expected %v, got %v", expectedRegion, got)
	}
}

func TestNewSessionWithNoSignRequest(t *testing.T) {
	globalSessionCache.clear()

	sess, err := globalSessionCache.newSession(context.Background(), Options{
		NoSignRequest: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	got := sess.Config.Credentials
	expected := credentials.AnonymousCredentials

	if expected != got {
		t.Fatalf("expected %v, got %v", expected, got)
	}
}

func TestS3ListURL(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api: mockApi,
	}

	mockApi.Handlers.Send.Clear() // mock sending
	mockApi.Handlers.Unmarshal.Clear()
	mockApi.Handlers.UnmarshalMeta.Clear()
	mockApi.Handlers.ValidateResponse.Clear()
	mockApi.Handlers.Unmarshal.PushBack(func(r *request.Request) {
		r.Data = &s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("key/a/")},
				{Prefix: aws.String("key/b/")},
			},
			Contents: []*s3.Object{
				{Key: aws.String("key/test.txt")},
				{Key: aws.String("key/test.pdf")},
			},
		}
	})

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

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api: mockApi,
	}
	mockErr := fmt.Errorf("mock error")

	mockApi.Handlers.Unmarshal.Clear()
	mockApi.Handlers.UnmarshalMeta.Clear()
	mockApi.Handlers.ValidateResponse.Clear()
	mockApi.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = mockErr
	})

	for got := range mockS3.List(context.Background(), url, true) {
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

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api: mockApi,
	}

	mockApi.Handlers.Send.Clear() // mock sending
	mockApi.Handlers.Unmarshal.Clear()
	mockApi.Handlers.UnmarshalMeta.Clear()
	mockApi.Handlers.ValidateResponse.Clear()
	mockApi.Handlers.Unmarshal.PushBack(func(r *request.Request) {
		// output does not include keys that match with given key
		r.Data = &s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("anotherkey/a/")},
				{Prefix: aws.String("anotherkey/b/")},
			},
			Contents: []*s3.Object{
				{Key: aws.String("a/b/c/d/test.txt")},
				{Key: aws.String("unknown/test.pdf")},
			},
		}
	})

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

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api: mockApi,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockApi.Handlers.Unmarshal.Clear()
	mockApi.Handlers.UnmarshalMeta.Clear()
	mockApi.Handlers.ValidateResponse.Clear()
	mockApi.Handlers.Unmarshal.PushBack(func(r *request.Request) {
		r.Data = &s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("key/a/")},
			},
		}
	})

	for got := range mockS3.List(ctx, url, true) {
		reqErr, ok := got.Err.(awserr.Error)
		if !ok {
			t.Errorf("could not convert error")
			continue
		}

		if reqErr.Code() != request.CanceledErrorCode {
			t.Errorf("error got = %v, want %v", got.Err, context.Canceled)
			continue
		}
	}
}

func TestS3Retry(t *testing.T) {
	log.Init("debug", false)

	testcases := []struct {
		name string
		err  error
	}{
		// Internal error
		{
			name: "InternalError",
			err:  awserr.New("InternalError", "internal error", nil),
		},

		// Request errors
		{
			name: "RequestError",
			err:  awserr.New(request.ErrCodeRequestError, "request error", nil),
		},
		{
			name: "UseOfClosedNetworkConnection",
			err:  awserr.New(request.ErrCodeRequestError, "use of closed network connection", nil),
		},
		{
			name: "ConnectionResetByPeer",
			err:  awserr.New(request.ErrCodeRequestError, "connection reset by peer", nil),
		},
		{
			name: "RequestFailureRequestError",
			err: awserr.NewRequestFailure(
				awserr.New(request.ErrCodeRequestError, "request failure: request error", nil),
				400,
				"0",
			),
		},
		{
			name: "RequestTimeout",
			err:  awserr.New("RequestTimeout", "request timeout", nil),
		},
		{
			name: "ResponseTimeout",
			err:  awserr.New(request.ErrCodeResponseTimeout, "response timeout", nil),
		},
		{
			name: "RequestTimeTooSkewed",
			err:  awserr.New("RequestTimeTooSkewed", "The difference between the request time and the server's time is too large.", nil),
		},

		// Throttling errors
		{
			name: "ProvisionedThroughputExceededException",
			err:  awserr.New("ProvisionedThroughputExceededException", "provisioned throughput exceeded exception", nil),
		},
		{
			name: "Throttling",
			err:  awserr.New("Throttling", "throttling", nil),
		},
		{
			name: "ThrottlingException",
			err:  awserr.New("ThrottlingException", "throttling exception", nil),
		},
		{
			name: "RequestLimitExceeded",
			err:  awserr.New("RequestLimitExceeded", "request limit exceeded", nil),
		},
		{
			name: "RequestThrottled",
			err:  awserr.New("RequestThrottled", "request throttled", nil),
		},
		{
			name: "RequestThrottledException",
			err:  awserr.New("RequestThrottledException", "request throttled exception", nil),
		},

		// Expired credential errors
		{
			name: "ExpiredToken",
			err:  awserr.New("ExpiredToken", "expired token", nil),
		},
		{
			name: "ExpiredTokenException",
			err:  awserr.New("ExpiredTokenException", "expired token exception", nil),
		},

		// Connection errors
		{
			name: "ConnectionReset",
			err:  fmt.Errorf("connection reset by peer"),
		},
		{
			name: "BrokenPipe",
			err:  fmt.Errorf("broken pipe"),
		},

		// Unknown errors
		{
			name: "UnknownSDKError",
			err:  fmt.Errorf("an error that is not known to the SDK"),
		},
	}

	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	const expectedRetry = 5
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sess := unit.Session
			sess.Config.Retryer = newCustomRetryer(expectedRetry)

			mockApi := s3.New(sess)
			mockS3 := &S3{
				api: mockApi,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mockApi.Handlers.Send.Clear() // mock sending
			mockApi.Handlers.Unmarshal.Clear()
			mockApi.Handlers.UnmarshalMeta.Clear()
			mockApi.Handlers.ValidateResponse.Clear()
			mockApi.Handlers.Unmarshal.PushBack(func(r *request.Request) {
				r.Error = tc.err
				r.HTTPResponse = &http.Response{}
			})

			retried := -1
			// Add a request handler to the AfterRetry handler stack that is used by the
			// SDK to be executed after the SDK has determined if it will retry.
			mockApi.Handlers.AfterRetry.PushBack(func(_ *request.Request) {
				retried++
			})

			for range mockS3.List(ctx, url, true) {
			}

			if retried != expectedRetry {
				t.Errorf("expected retry %v, got %v", expectedRetry, retried)
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
		expectedAcl      string
	}{
		{
			name: "no encryption/no acl, by default",
		},
		{
			name: "aws:kms encryption with server side generated keys",
			sse:  "aws:kms",

			expectedSSE: "aws:kms",
		},
		{
			name:     "aws:kms encryption with user provided key",
			sse:      "aws:kms",
			sseKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",

			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",
		},
		{
			name:     "provide key without encryption flag, shall be ignored",
			sseKeyID: "1234567890",
		},
		{
			name:        "acl flag with a value",
			acl:         "bucket-owner-full-control",
			expectedAcl: "bucket-owner-full-control",
		},
	}

	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			mockApi := s3.New(unit.Session)

			mockApi.Handlers.Unmarshal.Clear()
			mockApi.Handlers.UnmarshalMeta.Clear()
			mockApi.Handlers.UnmarshalError.Clear()
			mockApi.Handlers.Send.Clear()

			mockApi.Handlers.Send.PushBack(func(r *request.Request) {

				r.HTTPResponse = &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(strings.NewReader("")),
				}

				params := r.Params
				sse := valueAtPath(params, "ServerSideEncryption")
				key := valueAtPath(params, "SSEKMSKeyId")

				if !(sse == nil && tc.expectedSSE == "") {
					assert.Equal(t, sse, tc.expectedSSE)
				}
				if !(key == nil && tc.expectedSSEKeyID == "") {
					assert.Equal(t, key, tc.expectedSSEKeyID)
				}

				aclVal := valueAtPath(r.Params, "ACL")

				if aclVal == nil && tc.expectedAcl == "" {
					return
				}
				assert.Equal(t, aclVal, tc.expectedAcl)
			})
			mockApi.Handlers.Unmarshal.PushBack(func(r *request.Request) {
				if r.Error != nil {
					if awsErr, ok := r.Error.(awserr.Error); ok {
						if awsErr.Code() == request.ErrCodeSerialization {
							r.Error = nil
						}
					}
				}
			})

			mockS3 := &S3{
				api: mockApi,
			}

			metadata := NewMetadata().SetSSE(tc.sse).SetSSEKeyID(tc.sseKeyID).SetACL(tc.acl)

			err = mockS3.Copy(context.Background(), u, u, metadata)

			if err != nil {
				t.Errorf("Expected %v, but received %q", nil, err)
			}
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
		expectedAcl      string
	}{
		{
			name: "no encryption, no acl flag",
		},
		{
			name:        "aws:kms encryption with server side generated keys",
			sse:         "aws:kms",
			expectedSSE: "aws:kms",
		},
		{
			name:     "aws:kms encryption with user provided key",
			sse:      "aws:kms",
			sseKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",

			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",
		},
		{
			name:     "provide key without encryption flag, shall be ignored",
			sseKeyID: "1234567890",
		},
		{
			name:        "acl flag with a value",
			acl:         "bucket-owner-full-control",
			expectedAcl: "bucket-owner-full-control",
		},
	}
	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			mockApi := s3.New(unit.Session)

			mockApi.Handlers.Unmarshal.Clear()
			mockApi.Handlers.UnmarshalMeta.Clear()
			mockApi.Handlers.UnmarshalError.Clear()
			mockApi.Handlers.Send.Clear()

			mockApi.Handlers.Send.PushBack(func(r *request.Request) {

				r.HTTPResponse = &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(strings.NewReader("")),
				}

				params := r.Params
				sse := valueAtPath(params, "ServerSideEncryption")
				key := valueAtPath(params, "SSEKMSKeyId")

				if !(sse == nil && tc.expectedSSE == "") {
					assert.Equal(t, sse, tc.expectedSSE)
				}
				if !(key == nil && tc.expectedSSEKeyID == "") {
					assert.Equal(t, key, tc.expectedSSEKeyID)
				}

				aclVal := valueAtPath(r.Params, "ACL")

				if aclVal == nil && tc.expectedAcl == "" {
					return
				}
				assert.Equal(t, aclVal, tc.expectedAcl)
			})

			mockS3 := &S3{
				uploader: s3manager.NewUploaderWithClient(mockApi),
			}

			metadata := NewMetadata().SetSSE(tc.sse).SetSSEKeyID(tc.sseKeyID).SetACL(tc.acl)

			err = mockS3.Put(context.Background(), bytes.NewReader([]byte("")), u, metadata, 1, 5242880)

			if err != nil {
				t.Errorf("Expected %v, but received %q", nil, err)
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

	s3objs := make([]*s3.Object, 0, numObjectsToIgnore+numObjectsToReturn)

	for i := 0; i < numObjectsToReturn; i++ {
		fname := fmt.Sprintf("%s/%d", pre, i)
		now := time.Now()

		mapReturnObjNameToModtime[pre+"/"+fname] = now
		s3objs = append(s3objs, &s3.Object{
			Key:          aws.String("key/" + fname),
			LastModified: aws.Time(now),
		})
	}

	for i := 0; i < numObjectsToIgnore; i++ {
		fname := fmt.Sprintf("%s/%d", pre, numObjectsToReturn+i)
		later := time.Now().Add(time.Second * 10)

		mapIgnoreObjNameToModtime[pre+"/"+fname] = later
		s3objs = append(s3objs, &s3.Object{
			Key:          aws.String("key/" + fname),
			LastModified: aws.Time(later),
		})
	}

	// shuffle the objects array to remove possible assumptions about how objects
	// are stored.
	rand.Shuffle(len(s3objs), func(i, j int) {
		s3objs[i], s3objs[j] = s3objs[j], s3objs[i]
	})

	mockApi := s3.New(unit.Session)

	mockApi.Handlers.Unmarshal.Clear()
	mockApi.Handlers.UnmarshalMeta.Clear()
	mockApi.Handlers.UnmarshalError.Clear()
	mockApi.Handlers.Send.Clear()

	mockApi.Handlers.Send.PushBack(func(r *request.Request) {
		r.HTTPResponse = &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(strings.NewReader("")),
		}

		r.Data = &s3.ListObjectsV2Output{
			Contents: s3objs,
		}
	})

	mockS3 := &S3{
		api: mockApi,
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

	sess := map[string]*session.Session{}

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

func TestSessionAutoRegionValidateCredentials(t *testing.T) {
	awsSess := unit.Session
	awsSess.Handlers.Unmarshal.Clear()
	awsSess.Handlers.Send.Clear()
	awsSess.Handlers.Send.PushBack(func(r *request.Request) {
		header := http.Header{}
		header.Set("X-Amz-Bucket-Region", "")
		r.HTTPResponse = &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
		}

		if r.Config.Credentials != awsSess.Config.Credentials {
			t.Error("session credentials are expected to be used during HeadBucket request")
		}
	})

	_ = setSessionRegion(context.Background(), awsSess, "bucket")
}

func TestSessionAutoRegion(t *testing.T) {
	log.Init("error", false)

	unitSession := func() *session.Session {
		return session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("AKID", "SECRET", "SESSION"),
			SleepDelay:  func(time.Duration) {},
		}))
	}

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
			awsSess := unitSession()
			awsSess.Handlers.Unmarshal.Clear()
			awsSess.Handlers.Send.Clear()
			awsSess.Handlers.Send.PushBack(func(r *request.Request) {
				header := http.Header{}
				if tc.region != "" {
					header.Set("X-Amz-Bucket-Region", tc.region)
				}
				r.HTTPResponse = &http.Response{
					StatusCode: tc.status,
					Header:     header,
					Body:       ioutil.NopCloser(strings.NewReader("")),
				}
			})

			err := setSessionRegion(context.Background(), awsSess, tc.bucket)
			if tc.expectedErrorCode != "" && !errHasCode(err, tc.expectedErrorCode) {
				t.Errorf("expected error code: %v, got error: %v", tc.expectedErrorCode, err)
				return
			}

			if expected, got := tc.expectedRegion, aws.StringValue(awsSess.Config.Region); expected != got {
				t.Errorf("expected: %v, got: %v", expected, got)
			}
		})
	}
}

func valueAtPath(i interface{}, s string) interface{} {
	v, err := awsutil.ValuesAtPath(i, s)
	if err != nil || len(v) == 0 {
		return nil
	}
	if _, ok := v[0].(io.Reader); ok {
		return v[0]
	}

	if rv := reflect.ValueOf(v[0]); rv.Kind() == reflect.Ptr {
		return rv.Elem().Interface()
	}

	return v[0]
}
