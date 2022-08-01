package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	urlpkg "net/url"
	"testing"
)

func TestS3ImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(S3)
	if _, ok := i.(storage.Storage); !ok {
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

			newS3Storage
			opts := Options{Endpoint: tc.endpoint.Hostname()}
			sess, err := globalSessionCache.newSession(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}

			got := aws.ToBool(sess.Config.S3ForcePathStyle)
			if got != tc.expectPathStyle {
				t.Fatalf("expected: %v, got: %v", tc.expectPathStyle, got)
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
