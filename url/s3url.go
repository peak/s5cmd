// Package url defines URL types and has helper methods to parse a string into URLs.
package url

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	// s3WildCharacters is valid wildcard characters for a S3Url
	s3WildCharacters string = "?*"

	// s3Prefix is the prefix used on s3 URLs
	s3Prefix string = "s3://"

	// s3Separator is the path separator for s3 URLs
	s3Separator string = "/"

	// defaultRegex is the regex to match every key
	defaultRegex string = ".*"
)

// S3Url represents an S3 object (or bucket)
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

// Format formats the S3Url to the format "<bucket>[/<key>]"
func (s S3Url) Format() string {
	if s.Key == "" {
		return s.Bucket
	}
	return s.Bucket + s3Separator + s.Key
}

// setPrefixAndFilter sets prefix and filter parts on s3 key
//
// filter is the part that comes after the wildcard string
// prefix is the part that comes before the wildcard string
func (s *S3Url) setPrefixAndFilter() {
	loc := strings.IndexAny(s.Key, s3WildCharacters)
	wildOperation := loc > -1
	if !wildOperation {
		s.Delimiter = "/"
		s.Prefix = s.Key
	} else {
		s.Prefix = s.Key[:loc]
		s.filter = s.Key[loc:]
	}
}

// setFilterRegex converts wildcard strings to regex format
// and precompiles it for later usage. It is default to
// ".*" to match every key on S3
func (s *S3Url) setFilterRegex() error {
	filterRegex := defaultRegex
	if s.filter != "" {
		filterRegex = regexp.QuoteMeta(s.filter)
		filterRegex = strings.Replace(filterRegex, "\\?", ".", -1)
		filterRegex = strings.Replace(filterRegex, "\\*", ".*?", -1)
	}
	separator := ""
	if s.Prefix != "" && !strings.HasSuffix(s.Prefix, "/") {
		separator = "/"
	}
	filterRegex = s.Prefix + separator + filterRegex
	r, err := regexp.Compile("^" + filterRegex + "$")
	if err != nil {
		return err
	}
	s.filterRegex = r
	return nil
}

// Clone creates a new s3url with the values from the receiver
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

// Match check if given key matches with regex
func (s S3Url) Match(key string) bool {
	return s.filterRegex.MatchString(key)
}

// HasWild checks if a string contains any S3 wildcard chars
func HasWild(s string) bool {
	return strings.ContainsAny(s, s3WildCharacters)
}

// ParseS3Url parses a string into an S3Url
func ParseS3Url(object string) (*S3Url, error) {
	if !strings.HasPrefix(object, s3Prefix) {
		return nil, fmt.Errorf("s3 url should start with %s", s3Prefix)
	}
	parts := strings.SplitN(object, s3Separator, 4)
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
	url.setPrefixAndFilter()
	err := url.setFilterRegex()
	return url, err
}
