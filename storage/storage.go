// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/peak/s5cmd/objurl"
)

const dateFormat = "2006/01/02 15:04:05"

// Storage is an interface for storage operations.
type Storage interface {
	Stat(context.Context, *objurl.ObjectURL) (*Object, error)
	List(context.Context, *objurl.ObjectURL, bool, int64) <-chan *Object
	Copy(ctx context.Context, from, to *objurl.ObjectURL, class string) error
	Get(context.Context, *objurl.ObjectURL, io.WriterAt) error
	Put(context.Context, io.Reader, *objurl.ObjectURL, string) error
	Delete(context.Context, ...*objurl.ObjectURL) error
	ListBuckets(context.Context, string) ([]Bucket, error)
	UpdateRegion(string) error
	Statistics() *Stats
}

// Object is a generic type which contains metadata for storage items.
type Object struct {
	URL          *objurl.ObjectURL
	Etag         string
	ModTime      time.Time
	Type         os.FileMode
	Size         int64
	StorageClass storageClass
	Err          error
}

// String returns the string representation of Object.
func (o *Object) String() string {
	return o.URL.String()
}

// IsMarkerchecks if the object is a marker object to mark end of sequence.
func (o *Object) IsMarker() bool {
	return o == SequenceEndMarker
}

// Bucket is a container for storage objects.
type Bucket struct {
	CreationDate time.Time
	Name         string
}

// String returns the string representation of Bucket.
func (b Bucket) String() string {
	return fmt.Sprintf("%s  s3://%s", b.CreationDate.Format(dateFormat), b.Name)
}

type storageClass string

// IsGlacierObject checks if the storage class of object is glacier.
func (s storageClass) IsGlacier() bool {
	return s == ObjectStorageClassGlacier
}

const (
	// ObjectStorageClassStandard is a standard storage class type.
	ObjectStorageClassStandard storageClass = "STANDARD"

	// ObjectStorageClassReducedRedundancy is a reduced redundancy storage class type.
	ObjectStorageClassReducedRedundancy storageClass = "REDUCED_REDUNDANCY"

	// ObjectStorageClassGlacier is a glacier storage class type.
	ObjectStorageClassGlacier storageClass = "GLACIER"

	// TransitionStorageClassStandardIA is a Standard Infrequent-Access storage
	// class type.
	TransitionStorageClassStandardIA storageClass = "STANDARD_IA"
)

type notImplemented struct {
	apiType string
	method  string
}

func (e notImplemented) Error() string {
	return fmt.Sprintf("%q is not supported on %q storage", e.method, e.apiType)
}
