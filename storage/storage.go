// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"io"
	"time"

	"github.com/peak/s5cmd/s3url"
)

type Item struct {
	Key          string
	Etag         string
	LastModified time.Time
	IsDirectory  bool
	Size         int64
	StorageClass string
	Err          error
}

type Bucket struct {
	CreationDate time.Time
	Name         string
}

func (i Item) String() string {
	return i.Key
}

func (i Item) IsGlacierObject() bool {
	return i.StorageClass == ObjectStorageClassGlacier
}

func (i *Item) IsMarkerObject() bool {
	return i == SequenceEndMarker
}

func (b Bucket) String() string {
	return b.Name
}

const (
	ObjectStorageClassStandard          = "STANDARD"
	ObjectStorageClassReducedRedundancy = "REDUCED_REDUNDANCY"
	ObjectStorageClassGlacier           = "GLACIER"
	TransitionStorageClassStandardIa    = "STANDARD_IA"
)

type Storage interface {
	Head(context.Context, *s3url.S3Url) (*Item, error)
	List(context.Context, *s3url.S3Url, int64) <-chan *Item
	Copy(context.Context, *s3url.S3Url, *s3url.S3Url, string) error
	Get(context.Context, *s3url.S3Url, io.WriterAt) error
	Put(context.Context, io.Reader, *s3url.S3Url, string) error
	Delete(context.Context, string, ...string) error
	ListBuckets(context.Context, string) ([]Bucket, error)
	UpdateRegion(string) error
}
