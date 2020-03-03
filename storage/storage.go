// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

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
	Get(context.Context, *objurl.ObjectURL, io.WriterAt) (int64, error)
	Put(context.Context, io.Reader, *objurl.ObjectURL, map[string]string) error
	Delete(context.Context, *objurl.ObjectURL) error
	MultiDelete(context.Context, <-chan *objurl.ObjectURL) <-chan *Object
	ListBuckets(context.Context, string) ([]Bucket, error)
	UpdateRegion(string) error
}

// NewClient returns new Storage client from given url. Storage implementation
// is infered from the url.
func NewClient(url *objurl.ObjectURL) (Storage, error) {
	if url.IsRemote() {
		return newCachedS3()
	}

	return NewFilesystem(), nil
}

// Object is a generic type which contains metadata for storage items.
type Object struct {
	URL          *objurl.ObjectURL `json:"key,omitempty"`
	Etag         string            `json:"etag,omitempty"`
	ModTime      *time.Time        `json:"last_modified,omitempty"`
	Type         ObjectType        `json:"type,omitempty"`
	Size         int64             `json:"size,omitempty"`
	StorageClass StorageClass      `json:"storage_class,omitempty"`
	Err          error             `json:"error,omitempty"`
}

// String returns the string representation of Object.
func (o *Object) String() string {
	return o.URL.String()
}

// JSON returns the JSON representation of Object.
func (o *Object) JSON() string {
	bytes, _ := json.Marshal(o)
	return string(bytes)
}

// ObjectType is the type of Object.
type ObjectType struct {
	mode os.FileMode
}

// String returns the string representation of ObjectType.
func (o ObjectType) String() string {
	switch mode := o.mode; {
	case mode.IsRegular():
		return "file"
	case mode.IsDir():
		return "directory"
	case mode&os.ModeSymlink != 0:
		return "symlink"
	}
	return ""
}

// MarshallJSON returns the stringer of ObjectType as a marshalled json.
func (o ObjectType) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.String())
}

// IsDir checks if the object is a directory.
func (o ObjectType) IsDir() bool {
	return o.mode.IsDir()
}

// Bucket is a container for storage objects.
type Bucket struct {
	CreationDate time.Time `json:"created_at"`
	Name         string    `json:"name"`
}

// String returns the string representation of Bucket.
func (b Bucket) String() string {
	return fmt.Sprintf("%s  s3://%s", b.CreationDate.Format(dateFormat), b.Name)
}

// String returns the JSON representation of Bucket.
func (b Bucket) JSON() string {
	bytes, _ := json.Marshal(b)
	return string(bytes)
}

type StorageClass string

// IsGlacierObject checks if the storage class of object is glacier.
func (s StorageClass) IsGlacier() bool {
	return s == StorageGlacier
}

func (s StorageClass) ShortCode() string {
	var code string
	switch s {
	case StorageStandard:
		code = ""
	case StorageGlacier:
		code = "G"
	case StorageReducedRedundancy:
		code = "R"
	case StorageStandardIA:
		code = "I"
	default:
		code = "?"
	}
	return code
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
