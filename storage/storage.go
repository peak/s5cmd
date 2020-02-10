// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/peak/s5cmd/s3url"
)

type ItemResponse struct {
	Item *Item
	Err  error
}

type Item struct {
	Content     *s3.Object
	Key         string
	IsDirectory bool
}

type Bucket struct {
	CreationDate time.Time
	Name         string
}

func (i Item) String() string {
	return i.Key
}

type Storage interface {
	Head(context.Context, *s3url.S3Url) (*Item, error)
	List(context.Context, *s3url.S3Url) <-chan *ItemResponse
	Copy(context.Context, *s3url.S3Url, *s3url.S3Url, string) error
	Get(context.Context, *s3url.S3Url, io.WriterAt) error
	Put(context.Context, io.Reader, *s3url.S3Url, string) error
	Delete(context.Context, string, ...string) error
	ListBuckets(context.Context, string) ([]Bucket, error)
	UpdateRegion(string) error
}
