package storage

import (
	"context"
	"io"

	"github.com/peak/s5cmd/s3url"
)

var _ Storage = (*File)(nil)

type File struct {
}

func (f File) Head(context.Context, *s3url.S3Url) (*Item, error) {
	panic("implement me")
}

func (f File) List(context.Context, *s3url.S3Url) <-chan *ItemResponse {
	panic("implement me")
}

func (f File) Copy(context.Context, *s3url.S3Url, *s3url.S3Url, string) error {
	panic("implement me")
}

func (f File) Get(context.Context, *s3url.S3Url, io.WriterAt) error {
	panic("implement me")
}

func (f File) Put(context.Context, io.Reader, *s3url.S3Url, string) error {
	panic("implement me")
}

func (f File) Delete(context.Context, string, ...string) error {
	panic("implement me")
}

func (f File) ListBuckets(context.Context, string) ([]Bucket, error) {
	panic("implement me")
}

func (f File) UpdateRegion(string) error {
	panic("implement me")
}
