package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	urlpkg "net/url"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-cmp/cmp"

	"github.com/peak/s5cmd/storage/url"
)

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

			opts := S3Options{Endpoint: tc.endpoint.Hostname()}
			sess, err := newSession(opts)
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
	opts := S3Options{
		Region: "",
	}

	const expectedRegion = "us-west-2"

	os.Setenv("AWS_REGION", expectedRegion)
	defer os.Unsetenv("AWS_REGION")

	sess, err := newSession(opts)
	if err != nil {
		t.Fatal(err)
	}

	got := aws.StringValue(sess.Config.Region)
	if got != expectedRegion {
		t.Fatalf("expected %v, got %v", expectedRegion, got)
	}
}

func TestS3ListSuccess(t *testing.T) {
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
			name: "connection reset",
			err:  fmt.Errorf("connection reset by peer"),
		},
		{
			name: "broken pipe",
			err:  fmt.Errorf("broken pipe"),
		},

		// Unknown errors
		{
			name: "an unknown error is also retried by SDK",
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

// Credit to aws-sdk-go
func val(i interface{}, s string) interface{} {
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

// asserts equality; nil interface and empty string are considered equal.
func assertEqual(t *testing.T, expected string, got interface{}) {
	if got == nil {
		if expected != "" {
			t.Errorf("Expected %q, but received %q", "", got)
		}
	} else if expected != got {
		t.Errorf("Expected %q, but received %q", expected, got)
	}
}
func TestS3AclFlagOnCopy(t *testing.T) {
	testcases := []struct {
		name string
		acl  string

		expectedAcl string
		expectedErr error
	}{
		{
			name: "no acl flag",
		},
		{
			name: "acl flag without value, flag should be ignored",
		},
		{
			name:        "acl flag with a value",
			acl:         "bucket-owner-full-control",
			expectedAcl: "bucket-owner-full-control",
		},
	}
	const defaultReqErr = "request was canceled by the user"
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

				r.HTTPResponse = &http.Response{}
				r.Error = fmt.Errorf(defaultReqErr)

				aclVal := val(r.Params, "ACL")

				assertEqual(t, tc.expectedAcl, aclVal)
			})

			mockS3 := &S3{
				api: mockApi,
			}

			err = mockS3.Copy(context.Background(), u, u, map[string]string{
				"ACL": tc.acl,
			})

			if err != nil && err.Error() == defaultReqErr {
				return
			}

			if (err == nil || tc.expectedErr == nil) && tc.expectedErr != err {
				t.Errorf("Expected %q, but received %q", tc.expectedErr, err)
			} else if err.Error() != tc.expectedErr.Error() {
				t.Errorf("Expected %q, but received %q", tc.expectedErr, err)
			}
		})
	}
}
func TestS3AclFlagOnPut(t *testing.T) {
	testcases := []struct {
		name string
		acl  string

		expectedAcl string
		expectedErr error
	}{
		{
			name: "no acl flag",
		},
		{
			name: "acl flag without value, flag should be ignored",
		},
		{
			name:        "acl flag with a value",
			acl:         "bucket-owner-full-control",
			expectedAcl: "bucket-owner-full-control",
		},
	}
	const defaultReqErr = "request was canceled by the user"
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

				r.HTTPResponse = &http.Response{}
				r.Error = fmt.Errorf(defaultReqErr)

				aclVal := val(r.Params, "ACL")

				assertEqual(t, tc.expectedAcl, aclVal)
			})

			mockS3 := &S3{
				uploader: s3manager.NewUploaderWithClient(mockApi),
			}

			err = mockS3.Put(context.Background(), bytes.NewReader([]byte("")), u, map[string]string{
				"ACL": tc.acl,
			}, 1, 5242880)

			if err != nil && err.Error() == defaultReqErr {
				return
			}

			if (err == nil || tc.expectedErr == nil) && tc.expectedErr != err {
				t.Errorf("Expected %q, but received %q", tc.expectedErr, err)
			} else if err.Error() != tc.expectedErr.Error() {
				t.Errorf("Expected %q, but received %q", tc.expectedErr, err)
			}
		})
	}
}
