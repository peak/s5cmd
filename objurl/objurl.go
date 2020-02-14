// Package objurl abstracts local and remote file URLs.
package objurl

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	globCharacters string = "?*"

	// s3Schema is the schema used on s3 URLs
	s3Schema string = "s3://"

	// s3Separator is the path separator for s3 URLs
	s3Separator string = "/"

	// matchAllRe is the regex to match everything
	matchAllRe string = ".*"
)

type objurlType int

const (
	remoteObject objurlType = iota
	localObject
)

// ObjectURL is the canonical representation of an object, either on local or
// remote storage.
type ObjectURL struct {
	Type      objurlType
	Scheme    string
	Bucket    string
	Path      string
	Delimiter string
	Prefix    string

	filter      string
	filterRegex *regexp.Regexp
}

// New creates a new ObjectURL from given path string.
func New(s string) (*ObjectURL, error) {
	split := strings.Split(s, "://")
	if len(split) != 2 {
		return &ObjectURL{
			Type:   localObject,
			Scheme: "",
			Path:   s,
		}, nil
	}

	scheme, rest := split[0], split[1]

	if scheme != "s3" {
		return nil, fmt.Errorf("s3 url should start with %q", s3Schema)
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

	if HasGlobCharacter(bucket) {
		return nil, fmt.Errorf("bucket name cannot contain wildcards")
	}

	url := &ObjectURL{
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

func (o ObjectURL) IsRemote() bool {
	return o.Type == remoteObject
}

func (o ObjectURL) String() string {
	if !o.IsRemote() {
		return o.Path
	}

	return o.RemoteURL()
}

func (o ObjectURL) RemoteURL() string {
	s := o.Scheme + "://"
	if o.Bucket != "" {
		s += o.Bucket
	}

	if o.Path != "" {
		s += "/" + o.Path
	}

	return s
}

// Format formats the ObjectURL to the format "<bucket>[/<key>]".
func (o ObjectURL) Format() string {
	if o.Path == "" {
		return o.Bucket
	}
	return o.Bucket + s3Separator + o.Path
}

// setPrefixAndFilter creates url metadata for both wildcard and non-wildcard operations.
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
func (o *ObjectURL) setPrefixAndFilter() error {
	loc := strings.IndexAny(o.Path, globCharacters)
	wildOperation := loc > -1
	if !wildOperation {
		o.Delimiter = s3Separator
		o.Prefix = o.Path
	} else {
		o.Prefix = o.Path[:loc]
		o.filter = o.Path[loc:]
	}

	filterRegex := matchAllRe
	if o.filter != "" {
		filterRegex = regexp.QuoteMeta(o.filter)
		filterRegex = strings.Replace(filterRegex, "\\?", ".", -1)
		filterRegex = strings.Replace(filterRegex, "\\*", ".*?", -1)
	}
	filterRegex = o.Prefix + filterRegex
	r, err := regexp.Compile("^" + filterRegex + "$")
	if err != nil {
		return err
	}
	o.filterRegex = r
	return nil
}

// Clone creates a copy of the receiver.
func (o ObjectURL) Clone() ObjectURL {
	return ObjectURL{
		Bucket:      o.Bucket,
		Delimiter:   o.Delimiter,
		Path:        o.Path,
		Prefix:      o.Prefix,
		filter:      o.filter,
		filterRegex: o.filterRegex,
	}
}

// Match check if given key matches with regex and
// returns parsed key.
func (o ObjectURL) Match(key string) string {
	if !o.filterRegex.MatchString(key) {
		return ""
	}

	isBatch := o.filter != ""
	if isBatch {
		return parseBatch(o.Prefix, key)
	}
	return parseNonBatch(o.Prefix, key)
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

// HasGlobCharacter checks if a string contains any wildcard chars.
func HasGlobCharacter(s string) bool {
	return strings.ContainsAny(s, globCharacters)
}
