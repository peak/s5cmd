package storage

import (
	"context"
	"io"

	"github.com/peak/s5cmd/url"
)

var _ Storage = (*File)(nil)

type File struct {
}

func (f File) Head(context.Context, string, string) (*Item, error) {
	panic("implement me")
}

func (f File) List(context.Context, *url.S3Url) (<-chan *Item, error) {
	panic("implement me")
}

func (f File) Copy(context.Context, string, string, string, string) error {
	panic("implement me")
}

func (f File) Get(context.Context, io.WriterAt, string, string) error {
	panic("implement me")
}

func (f File) Put(context.Context, io.Reader, string, string, string) error {
	panic("implement me")
}

func (f File) Remove(context.Context, string, ...string) error {
	panic("implement me")
}

func (f File) ListBuckets(context.Context, string) ([]string, error) {
	panic("implement me")
}

