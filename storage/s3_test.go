package storage

import (
	"context"
	"fmt"
	urlpkg "net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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

func TestS3_List_success(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api:  mockApi,
		opts: S3Options{},
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

func TestS3_List_error(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api:  mockApi,
		opts: S3Options{},
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

func TestS3_List_no_item_found(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api:  mockApi,
		opts: S3Options{},
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

func TestS3_List_context_cancelled(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api:  mockApi,
		opts: S3Options{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// cancel on-flying request

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
		}

		if reqErr.Code() != request.CanceledErrorCode {
			t.Errorf("error got = %v, want %v", got.Err, context.Canceled)
		}
	}
}
