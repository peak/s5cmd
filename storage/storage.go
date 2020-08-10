// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/peak/s5cmd/storage/url"
	"github.com/peak/s5cmd/strutil"
)

var (
	// ErrGivenObjectNotFound indicates a specified object is not found.
	ErrGivenObjectNotFound = fmt.Errorf("given object not found")

	// ErrNoObjectFound indicates there are no objects found from a given directory.
	ErrNoObjectFound = fmt.Errorf("no object found")
)

// Storage is an interface for storage operations.
type Storage interface {
	// Stat returns the Object structure describing object. If src is not
	// found, ErrGivenObjectNotFound is returned.
	Stat(ctx context.Context, src *url.URL) (*Object, error)

	// List the objects and directories/prefixes in the src.
	List(ctx context.Context, src *url.URL, followSymlinks bool) <-chan *Object

	// Copy src to dst, optionally setting the given metadata. Src and dst
	// arguments are of the same type. If src is a remote type, server side
	// copying will be used.
	Copy(ctx context.Context, src, dst *url.URL, metadata Metadata) error

	// Get reads object content from src and writes to dst in parallel.
	Get(ctx context.Context, src *url.URL, dst io.WriterAt, concurrency int, partSize int64) (int64, error)

	// Put reads from src and writes content to dst.
	Put(ctx context.Context, src io.Reader, dst *url.URL, metadata Metadata, concurrency int, partSize int64) error

	// Delete deletes the given src.
	Delete(ctx context.Context, src *url.URL) error

	// MultiDelete deletes all items returned from given urls in batches.
	MultiDelete(ctx context.Context, urls <-chan *url.URL) <-chan *Object

	// ListBuckets returns bucket list. If prefix is given, results will be
	// filtered.
	ListBuckets(ctx context.Context, prefix string) ([]Bucket, error)

	// MakeBucket creates given bucket.
	MakeBucket(ctx context.Context, bucket string) error
}

// NewClient returns new Storage client from given url. Storage implementation
// is inferred from the url.
func NewClient(url *url.URL) (Storage, error) {
	if url.IsRemote() {
		return NewS3Storage(url)
	}

	return NewFilesystem(), nil
}

// Object is a generic type which contains metadata for storage items.
type Object struct {
	URL          *url.URL     `json:"key,omitempty"`
	Etag         string       `json:"etag,omitempty"`
	ModTime      *time.Time   `json:"last_modified,omitempty"`
	Type         ObjectType   `json:"type,omitempty"`
	Size         int64        `json:"size,omitempty"`
	StorageClass StorageClass `json:"storage_class,omitempty"`
	Err          error        `json:"error,omitempty"`
}

// String returns the string representation of Object.
func (o *Object) String() string {
	return o.URL.String()
}

// JSON returns the JSON representation of Object.
func (o *Object) JSON() string {
	return strutil.JSON(o)
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

// MarshalJSON returns the stringer of ObjectType as a marshalled json.
func (o ObjectType) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.String())
}

// IsDir checks if the object is a directory.
func (o ObjectType) IsDir() bool {
	return o.mode.IsDir()
}

// IsSymlink checks if the object is a symbolic link.
func (o ObjectType) IsSymlink() bool {
	return o.mode&os.ModeSymlink != 0
}

// ShouldProcessUrl returns true if follow symlinks is enabled.
// If follow symlinks is disabled we should not process the url.
// (this check is needed only for local files)
func ShouldProcessUrl(url *url.URL, followSymlinks bool) bool {
	if followSymlinks {
		return true
	}

	if url.IsRemote() {
		return true
	}
	fi, err := os.Lstat(url.Absolute())
	if err != nil {
		return false
	}

	// do not process symlinks
	return fi.Mode()&os.ModeSymlink == 0
}

// dateFormat is a constant time template for the bucket.
const dateFormat = "2006/01/02 15:04:05"

// Bucket is a container for storage objects.
type Bucket struct {
	CreationDate time.Time `json:"created_at"`
	Name         string    `json:"name"`
}

// String returns the string representation of Bucket.
func (b Bucket) String() string {
	return fmt.Sprintf("%s  s3://%s", b.CreationDate.Format(dateFormat), b.Name)
}

// JSON returns the JSON representation of Bucket.
func (b Bucket) JSON() string {
	return strutil.JSON(b)
}

// StorageClass represents the storage used to store an object.
type StorageClass string

func (s StorageClass) IsGlacier() bool {
	return s == "GLACIER"
}

// notImplemented is a structure which is used on the unsupported operations.
type notImplemented struct {
	apiType string
	method  string
}

// Error returns the string representation of Error for notImplemented.
func (e notImplemented) Error() string {
	return fmt.Sprintf("%q is not supported on %q storage", e.method, e.apiType)
}

type Metadata map[string]string

// NewMetadata will return an empty metadata object.
func NewMetadata() Metadata {
	return Metadata{}
}

func (m Metadata) ACL() string {
	return m["ACL"]
}

func (m Metadata) SetACL(acl string) Metadata {
	m["ACL"] = acl
	return m
}

func (m Metadata) StorageClass() string {
	return m["StorageClass"]
}

func (m Metadata) SetStorageClass(class string) Metadata {
	m["StorageClass"] = class
	return m
}

func (m Metadata) ContentType() string {
	return m["ContentType"]
}

func (m Metadata) SetContentType(contentType string) Metadata {
	m["ContentType"] = contentType
	return m
}

func (m Metadata) SSE() string {
	return m["EncryptionMethod"]
}

func (m Metadata) SetSSE(sse string) Metadata {
	m["EncryptionMethod"] = sse
	return m
}

func (m Metadata) SSEKeyID() string {
	return m["EncryptionKeyID"]
}

func (m Metadata) SetSSEKeyID(kid string) Metadata {
	m["EncryptionKeyID"] = kid
	return m
}
