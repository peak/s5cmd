package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-cmp/cmp"

	"github.com/peak/s5cmd/objurl"
)

func TestS3_List_success(t *testing.T) {
	url, err := objurl.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api:  mockApi,
		opts: S3Opts{},
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
	for got := range mockS3.List(context.Background(), url, true, ListAllItems) {
		if got.Err != nil {
			t.Errorf("unexpected error: %v", got.Err)
		}

		if got.IsMarker() {
			continue
		}

		want := responses[index]
		if diff := cmp.Diff(want.isDir, got.Mode.IsDir()); diff != "" {
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
	url, err := objurl.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api:  mockApi,
		opts: S3Opts{},
	}
	mockErr := fmt.Errorf("mock error")

	mockApi.Handlers.Unmarshal.Clear()
	mockApi.Handlers.UnmarshalMeta.Clear()
	mockApi.Handlers.ValidateResponse.Clear()
	mockApi.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = mockErr
	})

	for got := range mockS3.List(context.Background(), url, true, ListAllItems) {
		if got.Err != mockErr {
			t.Errorf("error got = %v, want %v", got.Err, mockErr)
		}
	}
}

func TestS3_List_no_item_found(t *testing.T) {
	url, err := objurl.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api:  mockApi,
		opts: S3Opts{},
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

	for got := range mockS3.List(context.Background(), url, true, ListAllItems) {
		if got.Err != ErrNoObjectFound {
			t.Errorf("error got = %v, want %v", got.Err, ErrNoObjectFound)
		}
	}
}

func TestS3_List_context_cancelled(t *testing.T) {
	url, err := objurl.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockApi := s3.New(unit.Session)
	mockS3 := &S3{
		api:  mockApi,
		opts: S3Opts{},
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

	for got := range mockS3.List(ctx, url, true, ListAllItems) {
		reqErr, ok := got.Err.(awserr.Error)
		if !ok {
			t.Errorf("could not convert error")
		}

		if reqErr.Code() != request.CanceledErrorCode {
			t.Errorf("error got = %v, want %v", got.Err, context.Canceled)
		}
	}
}
