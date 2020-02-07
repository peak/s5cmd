package storage

import (
	"context"
	"io"

	"github.com/peak/s5cmd/url"
)

var _ Storage = (*File)(nil)

type File struct {
}

func (F File) ListBuckets(context.Context, string) ([]string, error) {
	panic("implement me")
}

func NewFileStorage() *File {
	return &File{}
}

func (F File) Head(ctx context.Context, to string, key string) (*Item, error) {
	panic("implement me")
}

func (F File) List(ctx context.Context, s3url *url.S3Url) (<-chan *Item, error) {
	panic("implement me")
}

func (F File) Copy(ctx context.Context, from string, to string, dst string, cls string) error {
	panic("implement me")
}

func (F File) Get(ctx context.Context, from string, key string, writerAt io.WriterAt) error {
	panic("implement me")
}

func (F File) Put(ctx context.Context, to string, key string, file io.Reader, cls string) error {
	panic("implement me")
}

func (F File) Remove(ctx context.Context, from string, keys ...string) error {
	panic("implement me")
}
