// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/service/s3"
	s3url "github.com/peak/s5cmd/url"
)

type Item struct {
	Content     *s3.Object
	Key         string
	IsDirectory bool
}

type Storage interface {
	Head(context.Context, string, string) (*Item, error)
	List(context.Context, *s3url.S3Url) (<-chan *Item, error)
	Copy(context.Context, string, string, string, string) error
	Get(context.Context, string, string, io.WriterAt) error
	Put(context.Context, string, string, io.Reader, string) error
	Remove(context.Context, string, ...string) error
	ListBuckets(context.Context, string) ([]string, error)
}
