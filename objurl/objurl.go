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

// ObjectURL is the canonical representation of an object, either on local or
// remote storage.
type ObjectURL struct {
	Bucket      string
	Delimiter   string
	Key         string
	Prefix      string
	filter      string
	filterRegex *regexp.Regexp
}

// New creates a new ObjectURL from given path string.
func New(s string) (*ObjectURL, error) {
	if !strings.HasPrefix(s, s3Schema) {
		return nil, fmt.Errorf("s3 url should start with %s", s3Schema)
	}
	parts := strings.SplitN(s, s3Separator, 4)
	if parts[2] == "" {
		return nil, fmt.Errorf("s3 url should have a bucket")
	}
	if HasWild(parts[2]) {
		return nil, fmt.Errorf("bucket name cannot contain wildcards")
	}
	key := ""
	if len(parts) == 4 {
		key = strings.TrimLeft(parts[3], s3Separator)
	}

	url := &ObjectURL{Key: key, Bucket: parts[2]}

	if err := url.setPrefixAndFilter(); err != nil {
		return nil, err
	}
	return url, nil
}

func (o ObjectURL) String() string {
	return "s3://" + o.Bucket + "/" + o.Key
}

// Format formats the ObjectURL to the format "<bucket>[/<key>]".
func (o ObjectURL) Format() string {
	if o.Key == "" {
		return o.Bucket
	}
	return o.Bucket + s3Separator + o.Key
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
	loc := strings.IndexAny(o.Key, globCharacters)
	wildOperation := loc > -1
	if !wildOperation {
		o.Delimiter = s3Separator
		o.Prefix = o.Key
	} else {
		o.Prefix = o.Key[:loc]
		o.filter = o.Key[loc:]
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
		Key:         o.Key,
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

// HasWild checks if a string contains any S3 wildcard chars.
func HasWild(s string) bool {
	return strings.ContainsAny(s, globCharacters)
}
