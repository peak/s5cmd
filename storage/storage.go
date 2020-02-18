// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/peak/s5cmd/objurl"
)

const dateFormat = "2006/01/02 15:04:05"

// Item is a generic type which contains metadata for storage items.
type Object struct {
	URL          *objurl.ObjectURL
	Etag         string
	LastModified time.Time
	IsDirectory  bool
	Size         int64
	StorageClass string
	Err          error
}

// String returns the string representation of Item.
func (o *Object) String() string {
	return o.URL.String()
}

// IsGlacierObject checks if the storage class of item is glacier.
func (o *Object) IsGlacierObject() bool {
	return o.StorageClass == ObjectStorageClassGlacier
}

// IsMarkerObject checks if the item is a marker object to mark end of sequence.
func (o *Object) IsMarkerObject() bool {
	return o == SequenceEndMarker
}

// Bucket is a container for storage items.
type Bucket struct {
	CreationDate time.Time
	Name         string
}

// String returns the string representation of Bucket.
func (b Bucket) String() string {
	return fmt.Sprintf("%s  s3://%s", b.CreationDate.Format(dateFormat), b.Name)
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
	Head(context.Context, *objurl.ObjectURL) (*Object, error)
	List(context.Context, *objurl.ObjectURL, int64) <-chan *Object
	Copy(context.Context, *objurl.ObjectURL, *objurl.ObjectURL, string) error
	Get(context.Context, *objurl.ObjectURL, io.WriterAt) error
	Put(context.Context, io.Reader, *objurl.ObjectURL, string) error
	Delete(context.Context, string, ...*objurl.ObjectURL) error
	ListBuckets(context.Context, string) ([]Bucket, error)
	UpdateRegion(string) error
	Stats() *Stats
}
