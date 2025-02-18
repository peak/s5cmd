//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE Storage

// Package storage implements operations for s3 and fs.
package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/lanrat/extsort"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
	"github.com/peak/s5cmd/v2/strutil"
)

// ErrNoObjectFound indicates there are no objects found from a given directory.
var ErrNoObjectFound = fmt.Errorf("no object found")

// ErrGivenObjectNotFound indicates a specified object is not found.
type ErrGivenObjectNotFound struct {
	ObjectAbsPath string
}

func (e *ErrGivenObjectNotFound) Error() string {
	return fmt.Sprintf("given object %v not found", e.ObjectAbsPath)
}

// Storage is an interface for storage operations that is common
// to local filesystem and remote object storage.
type Storage interface {
	// Stat returns the Object structure describing object. If src is not
	// found, ErrGivenObjectNotFound is returned.
	Stat(ctx context.Context, src *url.URL) (*Object, error)

	// List the objects and directories/prefixes in the src.
	List(ctx context.Context, src *url.URL, followSymlinks bool) <-chan *Object

	// Delete deletes the given src.
	Delete(ctx context.Context, src *url.URL) error

	// MultiDelete deletes all items returned from given urls in batches.
	MultiDelete(ctx context.Context, urls <-chan *url.URL) <-chan *Object

	// Copy src to dst, optionally setting the given metadata. Src and dst
	// arguments are of the same type. If src is a remote type, server side
	// copying will be used.
	Copy(ctx context.Context, src, dst *url.URL, metadata Metadata) error
}

func NewLocalClient(opts Options) *Filesystem {
	return &Filesystem{dryRun: opts.DryRun}
}

func NewRemoteClient(ctx context.Context, url *url.URL, opts Options) (*S3, error) {
	newOpts := Options{
		MaxRetries:             opts.MaxRetries,
		NoSuchUploadRetryCount: opts.NoSuchUploadRetryCount,
		Endpoint:               opts.Endpoint,
		NoVerifySSL:            opts.NoVerifySSL,
		DryRun:                 opts.DryRun,
		NoSignRequest:          opts.NoSignRequest,
		UseListObjectsV1:       opts.UseListObjectsV1,
		RequestPayer:           opts.RequestPayer,
		Profile:                opts.Profile,
		CredentialFile:         opts.CredentialFile,
		LogLevel:               opts.LogLevel,
		bucket:                 url.Bucket,
		region:                 opts.region,
		AddressingStyle:        opts.AddressingStyle,
	}
	return newS3Storage(ctx, newOpts)
}

func NewClient(ctx context.Context, url *url.URL, opts Options) (Storage, error) {
	if url.IsRemote() {
		return NewRemoteClient(ctx, url, opts)
	}
	return NewLocalClient(opts), nil
}

// Options stores configuration for storage.
type Options struct {
	MaxRetries             int
	NoSuchUploadRetryCount int
	Endpoint               string
	NoVerifySSL            bool
	DryRun                 bool
	NoSignRequest          bool
	UseListObjectsV1       bool
	LogLevel               log.LogLevel
	RequestPayer           string
	Profile                string
	CredentialFile         string
	bucket                 string
	region                 string
	AddressingStyle        string
}

func (o *Options) SetRegion(region string) {
	o.region = region
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
	retryID      string

	// the VersionID field exist only for JSON Marshall, it must not be used for
	// any other purpose. URL.VersionID must be used instead.
	VersionID string `json:"version_id,omitempty"`
}

// String returns the string representation of Object.
func (o *Object) String() string {
	return o.URL.String()
}

// JSON returns the JSON representation of Object.
func (o *Object) JSON() string {
	if o.URL != nil {
		o.VersionID = o.URL.VersionID
	}
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

// IsRegular checks if the object is a regular file.
func (o ObjectType) IsRegular() bool {
	return o.mode.IsRegular()
}

// ShouldProcessURL returns true if follow symlinks is enabled.
// If follow symlinks is disabled we should not process the url.
// (this check is needed only for local files)
func ShouldProcessURL(url *url.URL, followSymlinks bool) bool {
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

type Metadata struct {
	ACL                string
	CacheControl       string
	Expires            string
	StorageClass       string
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	EncryptionMethod   string
	EncryptionKeyID    string

	UserDefined map[string]string

	// MetadataDirective is used to specify whether the metadata is copied from
	// the source object or replaced with metadata provided when copying S3
	// objects. If MetadataDirective is not set, it defaults to "COPY".
	Directive string
}

func (o Object) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 200))
	enc := gob.NewEncoder(buf)
	enc.Encode(o.URL.ToBytes())
	enc.Encode(o.ModTime.Format(time.RFC3339Nano))
	enc.Encode(o.Type.mode)
	enc.Encode(o.Size)

	return buf.Bytes()
}

func FromBytes(data []byte) extsort.SortType {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	var gobURL []byte
	dec.Decode(&gobURL)
	u := url.FromBytes(gobURL).(*url.URL)
	o := Object{
		URL: u,
	}
	str := ""
	dec.Decode(&str)
	tmp, _ := time.Parse(time.RFC3339Nano, str)
	o.ModTime = &tmp
	dec.Decode(&o.Type.mode)
	dec.Decode(&o.Size)
	return o
}

// Less returns if relative path of storage.Object a's URL comes before the one
// of b's in the lexicographic order.
// It assumes that both a, and b are the instances of Object
func Less(a, b extsort.SortType) bool {
	return a.(Object).URL.Relative() < b.(Object).URL.Relative()
}
