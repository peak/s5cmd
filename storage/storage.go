// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/peak/s5cmd/flags"
	"github.com/peak/s5cmd/objurl"
)

const dateFormat = "2006/01/02 15:04:05"

var (
	// ErrGivenObjectNotFound indicates a specified object is not found.
	ErrGivenObjectNotFound = fmt.Errorf("given object not found")

	// ErrNoObjectFound indicates there are no objects found from a given directory.
	ErrNoObjectFound = fmt.Errorf("no object found")
)

// Storage is an interface for storage operations.
type Storage interface {
	Stat(context.Context, *objurl.ObjectURL) (*Object, error)
	List(context.Context, *objurl.ObjectURL, bool, int64) <-chan *Object
	Copy(ctx context.Context, from, to *objurl.ObjectURL, metadata map[string]string) error
	Get(context.Context, *objurl.ObjectURL, io.WriterAt) error
	Put(context.Context, io.Reader, *objurl.ObjectURL, map[string]string) error
	Delete(context.Context, *objurl.ObjectURL) error
	MultiDelete(context.Context, <-chan *objurl.ObjectURL) <-chan *Object
	ListBuckets(context.Context, string) ([]Bucket, error)
	UpdateRegion(string) error
}

func NewClient(url *objurl.ObjectURL) (Storage, error) {
	if url.IsRemote() {
		opts := S3Opts{
			DownloadConcurrency:    *flags.DownloadConcurrency,
			DownloadChunkSizeBytes: *flags.DownloadPartSize,
			EndpointURL:            *flags.EndpointURL,
			MaxRetries:             *flags.RetryCount,
			NoVerifySSL:            *flags.NoVerifySSL,
			UploadChunkSizeBytes:   *flags.UploadPartSize,
			UploadConcurrency:      *flags.UploadConcurrency,
		}
		return NewS3Storage(opts)
	}

	return NewFilesystem(), nil
}

// Object is a generic type which contains metadata for storage items.
type Object struct {
	URL          *objurl.ObjectURL
	Etag         string
	ModTime      time.Time
	Mode         os.FileMode
	Size         int64
	StorageClass StorageClass
	Err          error
}

// String returns the string representation of Object.
func (o *Object) String() string {
	return o.URL.String()
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

type StorageClass string

// IsGlacierObject checks if the storage class of object is glacier.
func (s StorageClass) IsGlacier() bool {
	return s == StorageGlacier
}

const (
	// ObjectStorageClassStandard is a standard storage class type.
	StorageStandard StorageClass = "STANDARD"

	// ObjectStorageClassReducedRedundancy is a reduced redundancy storage class type.
	StorageReducedRedundancy StorageClass = "REDUCED_REDUNDANCY"

	// ObjectStorageClassGlacier is a glacier storage class type.
	StorageGlacier StorageClass = "GLACIER"

	// TransitionStorageClassStandardIA is a Standard Infrequent-Access storage
	// class type.
	StorageStandardIA StorageClass = "STANDARD_IA"
)

type notImplemented struct {
	apiType string
	method  string
}

func (e notImplemented) Error() string {
	return fmt.Sprintf("%q is not supported on %q storage", e.method, e.apiType)
}
