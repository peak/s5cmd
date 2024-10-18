// Package url abstracts local and remote file URLs.
package url

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/lanrat/extsort"
	"github.com/peak/s5cmd/v2/strutil"
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
	Type        urlType
	Scheme      string
	Bucket      string
	Path        string
	Delimiter   string
	Prefix      string
	VersionID   string
	AllVersions bool

	relativePath string
	filter       string
	filterRegex  *regexp.Regexp
	raw          bool
}

type Option func(u *URL)

func WithRaw(mode bool) Option {
	return func(u *URL) {
		u.raw = mode
	}
}

func WithVersion(versionID string) Option {
	return func(u *URL) {
		u.VersionID = versionID
	}
}

func WithAllVersions(isAllVersions bool) Option {
	return func(u *URL) {
		u.AllVersions = isAllVersions
	}
}

// New creates a new URL from given path string.
func New(s string, opts ...Option) (*URL, error) {
	scheme, rest, isFound := strings.Cut(s, "://")
	if !isFound {
		url := &URL{
			Type:   localObject,
			Scheme: "",
			Path:   s,
		}

		for _, opt := range opts {
			opt(url)
		}

		if err := url.setPrefixAndFilter(); err != nil {
			return nil, err
		}

		if runtime.GOOS == "windows" {
			url.Path = filepath.ToSlash(url.Path)
		}
		return url, nil
	}

	if scheme != "s3" {
		return nil, fmt.Errorf("s3 url should start with %q", s3Scheme)
	}

	parts := strings.SplitN(rest, s3Separator, 2)

	key := ""
	bucket := parts[0]
	if len(parts) == 2 {
		key = parts[1]
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

	for _, opt := range opts {
		opt(url)
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
func (u *URL) IsPrefix() bool {
	return u.IsRemote() && strings.HasSuffix(u.Path, "/")
}

// IsBucket returns true if the object url contains only bucket name
func (u *URL) IsBucket() bool {
	return u.IsRemote() && u.Path == ""
}

// IsVersioned returns true if the URL has versioning related values
func (u *URL) IsVersioned() bool {
	return u.AllVersions || u.VersionID != ""
}

// Absolute returns the absolute URL format of the object.
func (u *URL) Absolute() string {
	if !u.IsRemote() {
		return u.Path
	}

	return u.remoteURL()
}

// Relative returns a URI reference based on the calculated prefix.
func (u *URL) Relative() string {
	if u.relativePath != "" {
		return u.relativePath
	}
	return u.Absolute()
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
	if runtime.GOOS == "windows" {
		s = filepath.ToSlash(s)
	}

	clone := u.Clone()
	if !clone.IsRemote() {
		// URL is local, thus clean the path by using path.Join which
		// removes adjacent slashes.
		clone.Path = path.Join(clone.Path, s)
		return clone
	}
	// URL is remote, keep them as it is and join using string.Join which
	// allows to use adjacent slashes
	clone.Path = strings.Join([]string{clone.Path, s}, "")
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
//
//	key: a/b/test?/c/*.tsv
//	prefix: a/b/test
//	filter: ?/c/*
//	regex: ^a/b/test./c/.*?\\.tsv$
//	delimiter: ""
//
// It prepares delimiter, prefix and regex for regular strings.
// These are used in S3 listing operations.
// See: https://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html
//
// Example:
//
//	key: a/b/c
//	prefix: a/b/c
//	filter: ""
//	regex: ^a/b/c.*$
//	delimiter: "/"
func (u *URL) setPrefixAndFilter() error {
	if u.raw {
		return nil
	}

	if loc := strings.IndexAny(u.Path, globCharacters); loc < 0 {
		u.Delimiter = s3Separator
		u.Prefix = u.Path
	} else {
		u.Prefix = u.Path[:loc]
		u.filter = u.Path[loc:]
	}

	filterRegex := matchAllRe
	if u.filter != "" {
		filterRegex = strutil.WildCardToRegexp(u.filter)
	}
	filterRegex = regexp.QuoteMeta(u.Prefix) + filterRegex
	filterRegex = strutil.MatchFromStartToEnd(filterRegex)
	filterRegex = strutil.AddNewLineFlag(filterRegex)
	r, err := regexp.Compile(filterRegex)
	if err != nil {
		return err
	}
	u.filterRegex = r
	return nil
}

// Clone creates a copy of the receiver.
func (u *URL) Clone() *URL {
	return &URL{
		Type:        u.Type,
		Scheme:      u.Scheme,
		Bucket:      u.Bucket,
		Path:        u.Path,
		Delimiter:   u.Delimiter,
		Prefix:      u.Prefix,
		VersionID:   u.VersionID,
		AllVersions: u.AllVersions,

		relativePath: u.relativePath,
		filter:       u.filter,
		filterRegex:  u.filterRegex,
		raw:          u.raw,
	}
}

// SetRelative explicitly sets the relative path of u against given base value.
// If the base path contains `globCharacters` then, the relative path is
// determined with respect to the parent directory of the so called wildcarded
// object.
func (u *URL) SetRelative(base *URL) {
	basePath := base.Absolute()
	if base.IsWildcard() {
		// When the basePath includes a wildcard character (globCharacters)
		// replace basePath with its substring up to the
		// index of the first instance of a wildcard character.
		//
		// If we don't handle this, the filepath.Dir()
		// will assume those wildcards as a part of the name.
		// Consequently the filepath.Rel() will determine
		// the relative path incorrectly since the wildcarded
		// path string won't match with the actual name of the
		// object.
		// e.g. base.Absolute(): "/a/*/n"
		//      u.Absolute()   : "/a/b/n"
		//
		// if we don't trim substring from globCharacters on
		// filepath.Dir() will give: "/a/*"
		// consequently the
		// filepath.Rel() will give: "../b/c" rather than "b/c"
		// since "b" and "*" are not the same.
		loc := strings.IndexAny(basePath, globCharacters)
		if loc >= 0 {
			basePath = basePath[:loc]
		}
	}
	baseDir := filepath.Dir(basePath)
	u.relativePath, _ = filepath.Rel(baseDir, u.Absolute())
}

// Match reports whether if given key matches with the object.
func (u *URL) Match(key string) bool {
	if u.filterRegex == nil {
		return false
	}

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

// String is the fmt.Stringer implementation of URL.
func (u *URL) String() string {
	return u.Absolute()
}

// MarshalJSON is the json.Marshaler implementation of URL.
func (u URL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

func (u URL) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	enc := gob.NewEncoder(buf)
	enc.Encode(u.Absolute())
	enc.Encode(u.relativePath)
	enc.Encode(u.raw)
	return buf.Bytes()
}

func FromBytes(data []byte) extsort.SortType {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var (
		abs, rel string
		raw      bool
	)
	dec.Decode(&abs)
	dec.Decode(&rel)
	dec.Decode(&raw)

	url, _ := New(abs, WithRaw(raw))
	url.relativePath = rel
	return url
}

// IsWildcard reports whether if a string contains any wildcard chars.
func (u *URL) IsWildcard() bool {
	return !u.raw && hasGlobCharacter(u.Path)
}

func (u *URL) IsRaw() bool {
	return u.raw
}

// parseBatch parses keys for wildcard operations.
// It cuts the key starting from first directory before the
// wildcard part (filter)
//
// Example:
//
//	key: a/b/test2/c/example_file.tsv
//	prefix: a/b/
//	output: test2/c/example_file.tsv
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
// It subtracts prefix part from the key and gets first
// path.
//
// Example:
//
//	key: a/b/c/d
//	prefix: a/b
//	output: c/
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

// hasGlobCharacter reports whether if a string contains any wildcard chars.
func hasGlobCharacter(s string) bool {
	return strings.ContainsAny(s, globCharacters)
}

func (u *URL) EscapedPath() string {
	sourceKey := strings.TrimPrefix(u.String(), "s3://")
	sourceKeyElements := strings.Split(sourceKey, "/")
	for i, element := range sourceKeyElements {
		sourceKeyElements[i] = url.QueryEscape(element)
	}

	return strings.Join(sourceKeyElements, "/") // nosem - silence semgrep's "use path.Join" error
}

// check if all fields of URL equal
func (u *URL) deepEqual(url *URL) bool {
	if url.Absolute() != u.Absolute() ||
		url.Type != u.Type ||
		url.Scheme != u.Scheme ||
		url.Bucket != u.Bucket ||
		url.Delimiter != u.Delimiter ||
		url.Path != u.Path ||
		url.Prefix != u.Prefix ||
		url.relativePath != u.relativePath ||
		url.filter != u.filter {
		return false
	}
	return true
}
