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

	testClient := s3.New(unit.Session)
	mockS3 := &S3{
		api:        testClient,
		opts:       S3Opts{},
	}

	testClient.Handlers.Send.Clear() // mock sending
	testClient.Handlers.Unmarshal.Clear()
	testClient.Handlers.UnmarshalMeta.Clear()
	testClient.Handlers.ValidateResponse.Clear()
	testClient.Handlers.Unmarshal.PushBack(func(r *request.Request) {
		r.Data = &s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("key/a/")},
				{Prefix: aws.String("key/b/")},
			},
			Contents:               []*s3.Object{
				{Key: aws.String("key/test.txt")},
				{Key: aws.String("key/test.pdf")},
			},
		}
	})

	items := []Item{
		{
			IsDirectory: true,
			Key: "a/",
			Content:  &s3.Object{Key: aws.String("key/a/")},
		},
		{
			IsDirectory: true,
			Key: "b/",
			Content:  &s3.Object{Key: aws.String("key/b/")},
		},
		{
			IsDirectory: false,
			Key: "test.txt",
			Content:  &s3.Object{Key: aws.String("key/test.txt")},
		},
		{
			IsDirectory: false,
			Key: "test.pdf",
			Content:  &s3.Object{Key: aws.String("key/test.pdf")},
		},
	}

	index := 0
	for got := range mockS3.List(context.Background(), url) {
		if got.err != nil {
			t.Errorf("unexpected error: %v", got.err)
		}
		want := items[index]
		if !reflect.DeepEqual(got.item, want) {
			t.Errorf("got = %v, want %v", got.item, want)
		}
		index++
	}
}

func TestS3_List_error(t *testing.T) {
	url, err := s3url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	testClient := s3.New(unit.Session)
	mockS3 := &S3{
		api:        testClient,
		opts:       S3Opts{},
	}
	mockErr := fmt.Errorf("mock error")

	testClient.Handlers.Unmarshal.Clear()
	testClient.Handlers.UnmarshalMeta.Clear()
	testClient.Handlers.ValidateResponse.Clear()
	testClient.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = mockErr
	})

	for got := range mockS3.List(context.Background(), url) {
		if got.err != mockErr {
			t.Errorf("error got = %v, want %v", got.err, mockErr)
		}
	}
}

func TestS3_List_no_item_found(t *testing.T) {
	url, err := s3url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	testClient := s3.New(unit.Session)
	mockS3 := &S3{
		api:        testClient,
		opts:       S3Opts{},
	}

	testClient.Handlers.Send.Clear() // mock sending
	testClient.Handlers.Unmarshal.Clear()
	testClient.Handlers.UnmarshalMeta.Clear()
	testClient.Handlers.ValidateResponse.Clear()
	testClient.Handlers.Unmarshal.PushBack(func(r *request.Request) {
		// output does not include keys that match with given key
		r.Data = &s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("anotherkey/a/")},
				{Prefix: aws.String("anotherkey/b/")},
			},
			Contents:               []*s3.Object{
				{Key: aws.String("a/b/c/d/test.txt")},
				{Key: aws.String("unknown/test.pdf")},
			},
		}
	})

	for got := range mockS3.List(context.Background(), url) {
		if got.err != ErrNoItemFound {
			t.Errorf("error got = %v, want %v", got.err, ErrNoItemFound)
		}
	}
}

func TestS3_List_context_cancelled(t *testing.T) {
	url, err := s3url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	testClient := s3.New(unit.Session)
	mockS3 := &S3{
		api:        testClient,
		opts:       S3Opts{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// cancel request before handling it

	testClient.Handlers.Unmarshal.Clear()
	testClient.Handlers.UnmarshalMeta.Clear()
	testClient.Handlers.ValidateResponse.Clear()
	testClient.Handlers.Unmarshal.PushBack(func(r *request.Request) {
		r.Data = &s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("key/a/")},
			},
		}
	})

	for got := range mockS3.List(ctx, url) {
		reqErr, ok := got.err.(awserr.Error)
		if !ok {
			t.Errorf("could not convert error")
		}

		if reqErr.Code() != request.CanceledErrorCode {
			t.Errorf("error got = %v, want %v", got.err, context.Canceled)
		}
	}
}