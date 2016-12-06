package url

import "strings"
import "errors"

const (
	// S3WildCharacters is valid wildcard characters for a S3Url
	S3WildCharacters string = "?*"
)

type S3Url struct {
	Bucket string
	Key    string
}

func (s S3Url) String() string {
	return "s3://" + s.Bucket + "/" + s.Key
}

func (s S3Url) Format() string {
	if s.Key == "" {
		return s.Bucket
	}
	return s.Bucket + "/" + s.Key
}

// Clone creates a new s3url with the values from the receiver
func (s S3Url) Clone() S3Url {
	return S3Url{s.Bucket, s.Key}
}

func HasWild(s string) bool {
	return strings.ContainsAny(s, S3WildCharacters)
}

func ParseS3Url(object string) (*S3Url, error) {
	if !strings.HasPrefix(object, "s3://") {
		return nil, errors.New("S3 url should start with s3://")
	}
	parts := strings.SplitN(object, "/", 4)
	if parts[2] == "" {
		return nil, errors.New("S3 url should have a bucket")
	}
	if HasWild(parts[2]) {
		return nil, errors.New("Bucket name cannot contain wildcards")
	}
	key := ""
	if len(parts) == 4 {
		key = strings.TrimLeft(parts[3], "/")
	}

	return &S3Url{
		parts[2],
		key,
	}, nil
}
