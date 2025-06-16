package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	urlpkg "net/url"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
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

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{Endpoint: tc.endpoint.String()}
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
	globalSessionCache.Clear()

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
	globalSessionCache.Clear()

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
			globalSessionCache.Clear()
			sess, err := globalSessionCache.newSession(context.Background(), Options{
				Profile:        tc.profileName,
				CredentialFile: tc.fileName,
			})
			if err != nil {
				t.Fatal(err)
			}

			got, err := sess.Config.Credentials.Get()
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

	mockAPI := s3.New(unit.Session)
	mockS3 := &S3{
		api: mockAPI,
	}

	mockAPI.Handlers.Send.Clear()
	mockAPI.Handlers.Unmarshal.Clear()
	mockAPI.Handlers.UnmarshalMeta.Clear()
	mockAPI.Handlers.ValidateResponse.Clear()
	mockAPI.Handlers.Unmarshal.PushBack(func(r *request.Request) {
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

	mockAPI := s3.New(unit.Session)
	mockS3 := &S3{
		api: mockAPI,
	}
	mockErr := fmt.Errorf("mock error")

	mockAPI.Handlers.Unmarshal.Clear()
	mockAPI.Handlers.UnmarshalMeta.Clear()
	mockAPI.Handlers.ValidateResponse.Clear()
	mockAPI.Handlers.Send.PushBack(func(r *request.Request) {
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

	mockAPI := s3.New(unit.Session)
	mockS3 := &S3{
		api: mockAPI,
	}

	mockAPI.Handlers.Send.Clear()
	mockAPI.Handlers.Unmarshal.Clear()
	mockAPI.Handlers.UnmarshalMeta.Clear()
	mockAPI.Handlers.ValidateResponse.Clear()
	mockAPI.Handlers.Unmarshal.PushBack(func(r *request.Request) {
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

	mockAPI := s3.New(unit.Session)
	mockS3 := &S3{
		api: mockAPI,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockAPI.Handlers.Unmarshal.Clear()
	mockAPI.Handlers.UnmarshalMeta.Clear()
	mockAPI.Handlers.ValidateResponse.Clear()
	mockAPI.Handlers.Unmarshal.PushBack(func(r *request.Request) {
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
		name          string
		err           error
		expectedRetry int
	}{
		// Internal error
		{
			name:          "InternalError",
			err:           awserr.New("InternalError", "internal error", nil),
			expectedRetry: 5,
		},

		// Request errors
		{
			name:          "RequestError",
			err:           awserr.New(request.ErrCodeRequestError, "request error", nil),
			expectedRetry: 5,
		},
		{
			name:          "UseOfClosedNetworkConnection",
			err:           awserr.New(request.ErrCodeRequestError, "use of closed network connection", nil),
			expectedRetry: 5,
		},
		{
			name:          "ConnectionResetByPeer",
			err:           awserr.New(request.ErrCodeRequestError, "connection reset by peer", nil),
			expectedRetry: 5,
		},
		{
			name: "RequestFailureRequestError",
			err: awserr.NewRequestFailure(
				awserr.New(request.ErrCodeRequestError, "request failure: request error", nil),
				400,
				"0",
			),
			expectedRetry: 5,
		},
		{
			name:          "RequestTimeout",
			err:           awserr.New("RequestTimeout", "request timeout", nil),
			expectedRetry: 5,
		},
		{
			name:          "ResponseTimeout",
			err:           awserr.New(request.ErrCodeResponseTimeout, "response timeout", nil),
			expectedRetry: 5,
		},
		{
			name:          "RequestTimeTooSkewed",
			err:           awserr.New("RequestTimeTooSkewed", "The difference between the request time and the server's time is too large.", nil),
			expectedRetry: 5,
		},
		{
			name:          "SlowDown",
			err:           awserr.New("SlowDown", "Please reduce your request rate.", nil),
			expectedRetry: 5,
		},

		// Throttling errors
		{
			name:          "ProvisionedThroughputExceededException",
			err:           awserr.New("ProvisionedThroughputExceededException", "provisioned throughput exceeded exception", nil),
			expectedRetry: 5,
		},
		{
			name:          "Throttling",
			err:           awserr.New("Throttling", "throttling", nil),
			expectedRetry: 5,
		},
		{
			name:          "ThrottlingException",
			err:           awserr.New("ThrottlingException", "throttling exception", nil),
			expectedRetry: 5,
		},
		{
			name:          "RequestLimitExceeded",
			err:           awserr.New("RequestLimitExceeded", "request limit exceeded", nil),
			expectedRetry: 5,
		},
		{
			name:          "RequestThrottled",
			err:           awserr.New("RequestThrottled", "request throttled", nil),
			expectedRetry: 5,
		},
		{
			name:          "RequestThrottledException",
			err:           awserr.New("RequestThrottledException", "request throttled exception", nil),
			expectedRetry: 5,
		},

		// Expired credential errors
		{
			name:          "ExpiredToken",
			err:           awserr.New("ExpiredToken", "expired token", nil),
			expectedRetry: 5,
		},
		{
			name:          "ExpiredTokenException",
			err:           awserr.New("ExpiredTokenException", "expired token exception", nil),
			expectedRetry: 5,
		},

		// Invalid Token errors
		{
			name:          "InvalidToken",
			err:           awserr.New("InvalidToken", "invalid token", nil),
			expectedRetry: 0,
		},

		// Connection errors
		{
			name:          "ConnectionReset",
			err:           fmt.Errorf("connection reset by peer"),
			expectedRetry: 5,
		},
		{
			name:          "ConnectionTimedOut",
			err:           awserr.New(request.ErrCodeRequestError, "", tempError{err: errors.New("connection timed out")}),
			expectedRetry: 5,
		},
		{
			name:          "BrokenPipe",
			err:           fmt.Errorf("broken pipe"),
			expectedRetry: 5,
		},

		// Unknown errors
		{
			name:          "UnknownSDKError",
			err:           fmt.Errorf("an error that is not known to the SDK"),
			expectedRetry: 5,
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
			sessionCache := &SessionCache{
				sessions: map[Options]*session.Session{},
			}

			sess := unit.Session
			sess.Config.Retryer = newCustomRetryer(sessionCache, expectedRetry)

			mockAPI := s3.New(sess)
			mockS3 := &S3{
				api: mockAPI,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mockAPI.Handlers.Send.Clear()
			mockAPI.Handlers.Unmarshal.Clear()
			mockAPI.Handlers.UnmarshalMeta.Clear()
			mockAPI.Handlers.ValidateResponse.Clear()
			mockAPI.Handlers.Unmarshal.PushBack(func(r *request.Request) {
				r.Error = tc.err
				r.HTTPResponse = &http.Response{}
			})

			retried := -1
			// Add a request handler to the AfterRetry handler stack that is used by the
			// SDK to be executed after the SDK has determined if it will retry.
			mockAPI.Handlers.AfterRetry.PushBack(func(_ *request.Request) {
				retried++
			})

			for range mockS3.List(ctx, url, true) {
			}

			if retried != tc.expectedRetry {
				t.Errorf("expected retry %v, got %v", tc.expectedRetry, retried)
			}
		})
	}
}

func TestS3RetryOnNoSuchUpload(t *testing.T) {
	log.Init("debug", false)

	noSuchUploadError := awserr.New(s3.ErrCodeNoSuchUpload, "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed. status code: 404, request id: PJXXXXX, host id: HOSTIDXX", nil)
	testcases := []struct {
		name       string
		err        error
		retryCount int32
	}{
		{
			name:       "Don't retry",
			err:        noSuchUploadError,
			retryCount: 0,
		}, {
			name:       "Retry 5 times on NoSuchUpload error",
			err:        noSuchUploadError,
			retryCount: 5,
		}, {
			name:       "No error",
			err:        nil,
			retryCount: 0,
		},
	}

	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mockAPI := s3.New(unit.Session)
			mockS3 := &S3{
				api: mockAPI,
				uploader: &s3manager.Uploader{
					S3:                mockAPI,
					PartSize:          s3manager.DefaultUploadPartSize,
					Concurrency:       s3manager.DefaultUploadConcurrency,
					LeavePartsOnError: false,
					MaxUploadParts:    s3manager.MaxUploadParts,
				},
				noSuchUploadRetryCount: int(tc.retryCount),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			atomicCounter := new(int32)
			atomic.StoreInt32(atomicCounter, 0)

			mockAPI.Handlers.Send.Clear()
			mockAPI.Handlers.Unmarshal.Clear()
			mockAPI.Handlers.UnmarshalMeta.Clear()
			mockAPI.Handlers.ValidateResponse.Clear()
			mockAPI.Handlers.Unmarshal.PushBack(func(r *request.Request) {
				r.Error = tc.err
				r.HTTPResponse = &http.Response{}
			})
			mockAPI.Handlers.Unmarshal.PushBack(func(r *request.Request) {
				atomic.AddInt32(atomicCounter, 1)
			})

			mockS3.Put(ctx, strings.NewReader(""), url, Metadata{}, s3manager.DefaultUploadConcurrency, s3manager.DefaultUploadPartSize)

			// +1 is for the original request
			// *2 is to account for the "Stat" requests that are made to obtain
			// retry code from object metada.
			want := 2*tc.retryCount + 1
			counter := atomic.LoadInt32(atomicCounter)
			if counter != want {
				t.Errorf("expected retry request count %d, got %d", want, counter)
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
			expectedACL: "bucket-owner-full-control",
		},
	}

	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mockAPI := s3.New(unit.Session)

			mockAPI.Handlers.Unmarshal.Clear()
			mockAPI.Handlers.UnmarshalMeta.Clear()
			mockAPI.Handlers.UnmarshalError.Clear()
			mockAPI.Handlers.Send.Clear()

			mockAPI.Handlers.Send.PushBack(func(r *request.Request) {
				r.HTTPResponse = &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
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

				if aclVal == nil && tc.expectedACL == "" {
					return
				}
				assert.Equal(t, aclVal, tc.expectedACL)
			})
			mockAPI.Handlers.Unmarshal.PushBack(func(r *request.Request) {
				if r.Error != nil {
					if awsErr, ok := r.Error.(awserr.Error); ok {
						if awsErr.Code() == request.ErrCodeSerialization {
							r.Error = nil
						}
					}
				}
			})

			mockS3 := &S3{
				api: mockAPI,
			}

			metadata := Metadata{}
			metadata.EncryptionMethod = tc.sse
			metadata.EncryptionKeyID = tc.sseKeyID
			metadata.ACL = tc.acl

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
		expectedACL      string
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
			expectedACL: "bucket-owner-full-control",
		},
	}
	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mockAPI := s3.New(unit.Session)

			mockAPI.Handlers.Unmarshal.Clear()
			mockAPI.Handlers.UnmarshalMeta.Clear()
			mockAPI.Handlers.UnmarshalError.Clear()
			mockAPI.Handlers.Send.Clear()

			mockAPI.Handlers.Send.PushBack(func(r *request.Request) {
				r.HTTPResponse = &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
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

				if aclVal == nil && tc.expectedACL == "" {
					return
				}
				assert.Equal(t, aclVal, tc.expectedACL)
			})

			mockS3 := &S3{
				uploader: s3manager.NewUploaderWithClient(mockAPI),
			}

			metadata := Metadata{}
			metadata.EncryptionMethod = tc.sse
			metadata.EncryptionKeyID = tc.sseKeyID
			metadata.ACL = tc.acl

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

	mockAPI := s3.New(unit.Session)

	mockAPI.Handlers.Unmarshal.Clear()
	mockAPI.Handlers.UnmarshalMeta.Clear()
	mockAPI.Handlers.UnmarshalError.Clear()
	mockAPI.Handlers.Send.Clear()

	mockAPI.Handlers.Send.PushBack(func(r *request.Request) {
		r.HTTPResponse = &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
		}

		r.Data = &s3.ListObjectsV2Output{
			Contents: s3objs,
		}
	})

	mockS3 := &S3{
		api: mockAPI,
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

			globalSessionCache.Clear()

			sess, err := globalSessionCache.newSession(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}

			got := aws.StringValue(sess.Config.Region)
			if got != tc.expectedRegion {
				t.Fatalf("expected %v, got %v", tc.expectedRegion, got)
			}
		})
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
					Body:       io.NopCloser(strings.NewReader("")),
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

func TestS3ListObjectsAPIVersions(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockAPI := s3.New(unit.Session)
	mockS3 := &S3{api: mockAPI}

	mockAPI.Handlers.Send.Clear()
	mockAPI.Handlers.Unmarshal.Clear()
	mockAPI.Handlers.UnmarshalMeta.Clear()
	mockAPI.Handlers.ValidateResponse.Clear()

	t.Run("list-objects-v2", func(t *testing.T) {
		var got interface{}
		mockAPI.Handlers.ValidateResponse.PushBack(func(r *request.Request) {
			got = r.Data
		})

		ctx := context.Background()
		mockS3.useListObjectsV1 = false
		for range mockS3.List(ctx, url, false) {
		}

		expected := &s3.ListObjectsV2Output{}

		if reflect.TypeOf(expected) != reflect.TypeOf(got) {
			t.Errorf("expected %T, got: %T", expected, got)
		}
	})

	t.Run("list-objects-v1", func(t *testing.T) {
		var got interface{}
		mockAPI.Handlers.ValidateResponse.PushBack(func(r *request.Request) {
			got = r.Data
		})

		ctx := context.Background()
		mockS3.useListObjectsV1 = true
		for range mockS3.List(ctx, url, false) {
		}

		expected := &s3.ListObjectsOutput{}

		if reflect.TypeOf(expected) != reflect.TypeOf(got) {
			t.Errorf("expected %T, got: %T", expected, got)
		}
	})
}

func TestAWSLogLevel(t *testing.T) {
	testcases := []struct {
		name     string
		level    string
		expected aws.LogLevelType
	}{
		{
			name:     "Trace: log level must be aws.LogDebug",
			level:    "trace",
			expected: aws.LogDebug,
		},
		{
			name:     "Debug: log level must be aws.LogOff",
			level:    "debug",
			expected: aws.LogOff,
		},
		{
			name:     "Info: log level must be aws.LogOff",
			level:    "info",
			expected: aws.LogOff,
		},
		{
			name:     "Error: log level must be aws.LogOff",
			level:    "error",
			expected: aws.LogOff,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			globalSessionCache.Clear()
			sess, err := globalSessionCache.newSession(context.Background(), Options{
				LogLevel: log.LevelFromString(tc.level),
			})
			if err != nil {
				t.Fatal(err)
			}

			cfgLogLevel := *sess.Config.LogLevel
			if diff := cmp.Diff(cfgLogLevel, tc.expected); diff != "" {
				t.Errorf("%s: (-want +got):\n%v", tc.name, diff)
			}
		})
	}
}

func TestS3HeadObject(t *testing.T) {
	testcases := []struct {
		name     string
		url      string
		expected string
	}{
		{
			name:     "HeadObject",
			url:      "s3://bucket/key",
			expected: "bucket/key",
		},
		{
			name:     "HeadObject with different URL",
			url:      "s3://another-bucket/another-key",
			expected: "another-bucket/another-key",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.New(tc.url)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			mockAPI := s3.New(unit.Session)
			mockS3 := &S3{api: mockAPI}

			mockAPI.Handlers.Send.Clear()
			mockAPI.Handlers.Unmarshal.Clear()
			mockAPI.Handlers.UnmarshalMeta.Clear()
			mockAPI.Handlers.ValidateResponse.Clear()

			mockAPI.Handlers.Send.PushBack(func(r *request.Request) {
				r.HTTPResponse = &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
				}
			})

			mockAPI.Handlers.ValidateResponse.PushBack(func(r *request.Request) {
				if r.Error != nil {
					t.Errorf("unexpected error: %v", r.Error)
				}
			})

			_, _, err = mockS3.HeadObject(context.Background(), u)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
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

// tempError is a wrapper error type that implements anonymous
// interface getting checked in url.Error.Temporary;
//
//	interface { Temporary() bool }
//
// see: https://github.com/golang/go/blob/2ebe77a2fda1ee9ff6fd9a3e08933ad1ebaea039/src/net/url/url.go#L38-L43
//
// AWS SDK checks if the underlying error in received url.Error implements it;
// see: https://github.com/aws/aws-sdk-go/blob/b8fe768e4ce7f8f7c002bd7b27f4f5a8723fb1a5/aws/request/retryer.go#L191-L208
//
// It's used to mimic errors like tls.permanentError that would
// be received in a url.Error when the connection timed out.
type tempError struct {
	err  error
	temp bool
}

func (e tempError) Error() string { return e.err.Error() }

func (e tempError) Temporary() bool { return e.temp }

func (e *tempError) Unwrap() error { return e.err }
