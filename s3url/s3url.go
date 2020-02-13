// Package s3url defines URL types and has helper methods to parse a string into URLs.
package s3url

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	// s3WildCharacters is valid wildcard characters for a S3Url
	s3WildCharacters string = "?*"

	// s3Schema is the schema used on s3 URLs
	s3Schema string = "s3://"

	// s3Separator is the path separator for s3 URLs
	s3Separator string = "/"

	// matchAllRe is the regex to match everything
	matchAllRe string = ".*"
)

// S3Url is the metadata that is used on S3 operations (listing, querying).
type S3Url struct {
	Bucket      string
	Delimiter   string
	Key         string
	Prefix      string
	filter      string
	filterRegex *regexp.Regexp
}

func (s S3Url) String() string {
	return "s3://" + s.Bucket + "/" + s.Key
}

// Format formats the S3Url to the format "<bucket>[/<key>]".
func (s S3Url) Format() string {
	if s.Key == "" {
		return s.Bucket
	}
	return s.Bucket + s3Separator + s.Key
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
func (s *S3Url) setPrefixAndFilter() error {
	loc := strings.IndexAny(s.Key, s3WildCharacters)
	wildOperation := loc > -1
	if !wildOperation {
		s.Delimiter = s3Separator
		s.Prefix = s.Key
	} else {
		s.Prefix = s.Key[:loc]
		s.filter = s.Key[loc:]
	}

	filterRegex := matchAllRe
	if s.filter != "" {
		filterRegex = regexp.QuoteMeta(s.filter)
		filterRegex = strings.Replace(filterRegex, "\\?", ".", -1)
		filterRegex = strings.Replace(filterRegex, "\\*", ".*?", -1)
	}
	filterRegex = s.Prefix + filterRegex
	r, err := regexp.Compile("^" + filterRegex + "$")
	if err != nil {
		return err
	}
	s.filterRegex = r
	return nil
}

// Clone creates a new s3url with the values from the receiver.
func (s S3Url) Clone() S3Url {
	return S3Url{
		Bucket:      s.Bucket,
		Delimiter:   s.Delimiter,
		Key:         s.Key,
		Prefix:      s.Prefix,
		filter:      s.filter,
		filterRegex: s.filterRegex,
	}
}

// Match check if given key matches with regex and
// returns parsed key.
func (s S3Url) Match(key string) string {
	if !s.filterRegex.MatchString(key) {
		return ""
	}

	isBatch := s.filter != ""
	if isBatch {
		return parseBatch(s.Prefix, key)
	}
	return parseNonBatch(s.Prefix, key)
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
	return strings.ContainsAny(s, s3WildCharacters)
}

// New creates new S3Url by parsing given url.
func New(rawUrl string) (*S3Url, error) {
	if !strings.HasPrefix(rawUrl, s3Schema) {
		return nil, fmt.Errorf("s3 url should start with %s", s3Schema)
	}
	parts := strings.SplitN(rawUrl, s3Separator, 4)
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

	url := &S3Url{Key: key, Bucket: parts[2]}

	if err := url.setPrefixAndFilter(); err != nil {
		return nil, err
	}
	return url, nil
}
