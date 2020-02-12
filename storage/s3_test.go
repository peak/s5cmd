package storage

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/peak/s5cmd/s3url"
)

func TestS3_List_success(t *testing.T) {
	url, err := s3url.New("s3://bucket/key")
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

	responses := []*Item{
		{
			IsDirectory: true,
			Key:         "a/",
		},
		{
			IsDirectory: true,
			Key:         "b/",
		},
		{
			IsDirectory: false,
			Key:         "test.txt",
		},
		{
			IsDirectory: false,
			Key:         "test.pdf",
		},
		SequenceEndMarker,
	}

	index := 0
	for got := range mockS3.List(context.Background(), url, ListAllItems) {
		if got.Err != nil {
			t.Errorf("unexpected error: %v", got.Err)
		}
		want := responses[index]
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got = %v, want %v", got, want)
		}
		index++
	}
}

func TestS3_List_error(t *testing.T) {
	url, err := s3url.New("s3://bucket/key")
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

	for got := range mockS3.List(context.Background(), url, ListAllItems) {
		if got.Err != mockErr {
			t.Errorf("error got = %v, want %v", got.Err, mockErr)
		}
	}
}

func TestS3_List_no_item_found(t *testing.T) {
	url, err := s3url.New("s3://bucket/key")
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

	for got := range mockS3.List(context.Background(), url, ListAllItems) {
		if got.Err != ErrNoItemFound {
			t.Errorf("error got = %v, want %v", got.Err, ErrNoItemFound)
		}
	}
}

func TestS3_List_context_cancelled(t *testing.T) {
	url, err := s3url.New("s3://bucket/key")
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

	for got := range mockS3.List(ctx, url, ListAllItems) {
		reqErr, ok := got.Err.(awserr.Error)
		if !ok {
			t.Errorf("could not convert error")
		}

		if reqErr.Code() != request.CanceledErrorCode {
			t.Errorf("error got = %v, want %v", got.Err, context.Canceled)
		}
	}
}
