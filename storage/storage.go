// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"io"
	"time"

	"github.com/peak/s5cmd/s3url"
)

// Item is a generic type which contains metadata for storage items.
type Item struct {
	Key          string
	Etag         string
	LastModified time.Time
	IsDirectory  bool
	Size         int64
	StorageClass string
	Err          error
}

// Bucket is a container for storage items.
type Bucket struct {
	CreationDate time.Time
	Name         string
}

// String returns the string representation of Item.
func (i Item) String() string {
	return i.Key
}

// IsGlacierObject checks if the storage class of item is glacier.
func (i Item) IsGlacierObject() bool {
	return i.StorageClass == ObjectStorageClassGlacier
}

// IsMarkerObject checks if the item is a marker object to mark end of sequence.
func (i *Item) IsMarkerObject() bool {
	return i == SequenceEndMarker
}

// String returns the string representation of Bucket.
func (b Bucket) String() string {
	return b.Name
}

const (
	// ObjectStorageClassStandard is a standard storage class type.
	ObjectStorageClassStandard = "STANDARD"

	// ObjectStorageClassReducedRedundancy is a reduced redundancy storage class type.
	ObjectStorageClassReducedRedundancy = "REDUCED_REDUNDANCY"

	// ObjectStorageClassGlacier is a glacier storage class type.
	ObjectStorageClassGlacier = "GLACIER"

	// TransitionStorageClassStandardIa is a standard ia storage class type.
	TransitionStorageClassStandardIa = "STANDARD_IA"
)

// Storage is an interface for storage operations.
type Storage interface {
	Head(context.Context, *s3url.S3Url) (*Item, error)
	List(context.Context, *s3url.S3Url, int64) <-chan *Item
	Copy(context.Context, *s3url.S3Url, *s3url.S3Url, string) error
	Get(context.Context, *s3url.S3Url, io.WriterAt) error
	Put(context.Context, io.Reader, *s3url.S3Url, string) error
	Delete(context.Context, string, ...string) error
	ListBuckets(context.Context, string) ([]Bucket, error)
	UpdateRegion(string) error
	Stats() *Stats
}
