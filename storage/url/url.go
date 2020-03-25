// Package url abstracts local and remote file URLs.
package url

import (
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	globCharacters string = "?*"

	// s3Scheme is the schema used on s3 URLs
	s3Scheme string = "s3://"

	// s3Separator is the path separator for s3 URLs
	s3Separator string = "/"

	// matchAllRe is the regex to match everything
	matchAllRe string = ".*"
)

type urlType int

const (
	remoteObject urlType = iota
	localObject
)

// URL is the canonical representation of an object, either on local or remote
// storage.
type URL struct {
	Type      urlType
	Scheme    string
	Bucket    string
	Path      string
	Delimiter string
	Prefix    string

	relativePath string
	filter       string
	filterRegex  *regexp.Regexp
}

// New creates a new URL from given path string.
func New(s string) (*URL, error) {
	split := strings.Split(s, "://")

	if len(split) == 1 {
		url := &URL{
			Type:   localObject,
			Scheme: "",
			Path:   s,
		}
		if err := url.setPrefixAndFilter(); err != nil {
			return nil, err
		}

		return url, nil
	}

	if len(split) != 2 {
		return nil, fmt.Errorf("storage: unknown url format %q", s)
	}

	scheme, rest := split[0], split[1]

	if scheme != "s3" {
		return nil, fmt.Errorf("s3 url should start with %q", s3Scheme)
	}

	parts := strings.SplitN(rest, s3Separator, 2)

	key := ""
	bucket := parts[0]
	if len(parts) == 2 {
		key = strings.TrimLeft(parts[1], s3Separator)
	}

	if bucket == "" {
		return nil, fmt.Errorf("s3 url should have a bucket")
	}

	if hasGlobCharacter(bucket) {
		return nil, fmt.Errorf("bucket name cannot contain wildcards")
	}

	url := &URL{
		Type:   remoteObject,
		Scheme: "s3",
		Bucket: bucket,
		Path:   key,
	}

	if err := url.setPrefixAndFilter(); err != nil {
		return nil, err
	}
	return url, nil
}

// IsRemote reports whether the object is stored on a remote storage system.
func (u *URL) IsRemote() bool {
	return u.Type == remoteObject
}

// IsPrefix reports whether the remote object is an S3 prefix, and does not
// look like an object.
func (o *URL) IsPrefix() bool {
	return o.IsRemote() && strings.HasSuffix(o.Path, "/")
}

// IsBucket returns true if the object url contains only bucket name
func (u *URL) IsBucket() bool {
	return u.IsRemote() && u.Path == ""
}

// Absolute returns the absolute URL format of the object.
func (u *URL) Absolute() string {
	if !u.IsRemote() {
		return u.Path
	}

	return u.remoteURL()
}

// Relative returns a URI reference based on the calculated prefix.
func (o *URL) Relative() string {
	if o.relativePath != "" {
		return o.relativePath
	}
	return o.Absolute()
}

// Base returns the last element of object path.
func (u *URL) Base() string {
	basefn := filepath.Base
	if u.IsRemote() {
		basefn = path.Base
	}

	return basefn(u.Path)
}

// Dir returns all but the last element of path, typically the path's
// directory.
func (u *URL) Dir() string {
	basefn := filepath.Dir
	if u.IsRemote() {
		basefn = path.Dir
	}

	return basefn(u.Path)
}

// Join joins string and returns new URL.
func (u *URL) Join(s string) *URL {
	joinfn := filepath.Join
	if u.IsRemote() {
		joinfn = path.Join
	}

	clone := u.Clone()
	clone.Path = joinfn(clone.Path, s)
	return clone
}

func (u *URL) remoteURL() string {
	s := u.Scheme + "://"
	if u.Bucket != "" {
		s += u.Bucket
	}

	if u.Path != "" {
		s += "/" + u.Path
	}

	return s
}

// setPrefixAndFilter creates url metadata for both wildcard and non-wildcard
// operations.
//
// It converts wildcard strings to regex format
// and pre-compiles it for later usage. It is default to
// ".*" to match every key on S3.
//
// filter is the part that comes after the wildcard string.
// prefix is the part that comes before the wildcard string.
//
// Example:
//		key: a/b/test?/c/*.tsv
//		prefix: a/b/test
//		filter: ?/c/*
//		regex: ^a/b/test./c/.*?\\.tsv$
//		delimiter: ""
//
// It prepares delimiter, prefix and regex for regular strings.
// These are used in S3 listing operations.
// See: https://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html
//
// Example:
//		key: a/b/c
//		prefix: a/b/c
//		filter: ""
//		regex: ^a/b/c.*$
//		delimiter: "/"
//
func (u *URL) setPrefixAndFilter() error {
	loc := strings.IndexAny(u.Path, globCharacters)
	wildOperation := loc > -1
	if !wildOperation {
		u.Delimiter = s3Separator
		u.Prefix = u.Path
	} else {
		u.Prefix = u.Path[:loc]
		u.filter = u.Path[loc:]
	}

	filterRegex := matchAllRe
	if u.filter != "" {
		filterRegex = regexp.QuoteMeta(u.filter)
		filterRegex = strings.Replace(filterRegex, "\\?", ".", -1)
		filterRegex = strings.Replace(filterRegex, "\\*", ".*?", -1)
	}
	filterRegex = u.Prefix + filterRegex
	r, err := regexp.Compile("^" + filterRegex + "$")
	if err != nil {
		return err
	}
	u.filterRegex = r
	return nil
}

// Clone creates a copy of the receiver.
func (u *URL) Clone() *URL {
	return &URL{
		Type:      u.Type,
		Scheme:    u.Scheme,
		Bucket:    u.Bucket,
		Delimiter: u.Delimiter,
		Path:      u.Path,
		Prefix:    u.Prefix,

		relativePath: u.relativePath,
		filter:       u.filter,
		filterRegex:  u.filterRegex,
	}
}

func (u *URL) SetRelative(base string) {
	dir := filepath.Dir(base)
	u.relativePath, _ = filepath.Rel(dir, u.Absolute())
}

// Match checks if given key matches with the object.
func (u *URL) Match(key string) bool {
	if !u.filterRegex.MatchString(key) {
		return false
	}

	isBatch := u.filter != ""
	if isBatch {
		v := parseBatch(u.Prefix, key)
		u.relativePath = v
		return true
	}

	v := parseNonBatch(u.Prefix, key)
	u.relativePath = v
	return true
}

func (u *URL) String() string {
	if u.IsRemote() {
		return u.Absolute()
	}
	return u.Base()
}

func (u *URL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// HasGlob checks if a string contains any wildcard chars.
func (u *URL) HasGlob() bool {
	return hasGlobCharacter(u.Path)
}

// parseBatch parses keys for wildcard operations.
// It cuts the key starting from first directory before the
// wildcard part (filter)
//
// Example:
//		key: a/b/test2/c/example_file.tsv
//		prefix: a/b/
//		output: test2/c/example_file.tsv
//
func parseBatch(prefix string, key string) string {
	index := strings.LastIndex(prefix, s3Separator)
	if index < 0 || !strings.HasPrefix(key, prefix) {
		return key
	}
	trimmedKey := key[index:]
	trimmedKey = strings.TrimPrefix(trimmedKey, s3Separator)
	return trimmedKey
}

// parseNonBatch parses keys for non-wildcard operations.
// It substracts prefix part from the key and gets first
// path.
//
// Example:
//		key: a/b/c/d
//		prefix: a/b
//		output: c/
//
func parseNonBatch(prefix string, key string) string {
	if key == prefix || !strings.HasPrefix(key, prefix) {
		return key
	}
	parsedKey := strings.TrimSuffix(key, s3Separator)
	if loc := strings.LastIndex(parsedKey, s3Separator); loc < len(prefix) {
		if loc < 0 {
			return key
		}
		parsedKey = key[loc:]
		return strings.TrimPrefix(parsedKey, s3Separator)
	}
	parsedKey = strings.TrimPrefix(key, prefix)
	parsedKey = strings.TrimPrefix(parsedKey, s3Separator)
	index := strings.Index(parsedKey, s3Separator) + 1
	if index <= 0 || index >= len(parsedKey) {
		return parsedKey
	}
	trimmedKey := parsedKey[:index]
	return trimmedKey
}

// hasGlobCharacter checks if a string contains any wildcard chars.
func hasGlobCharacter(s string) bool {
	return strings.ContainsAny(s, globCharacters)
}
