package core

import (
	"path/filepath"
	"strings"

	"github.com/peakgames/s5cmd/url"
)

// JobArgument is an argument of the job. Can be a file/directory, an s3 url ("s3" is set in this case) or an arbitrary string.
type JobArgument struct {
	arg string
	s3  *url.S3Url
}

// Clone duplicates a JobArgument and returns a pointer to a new one
func (a JobArgument) Clone() *JobArgument {
	var s url.S3Url
	if a.s3 != nil {
		s = a.s3.Clone()
	}
	return &JobArgument{a.arg, &s}
}

// StripS3 strips the S3 data from JobArgument and returns a new one
func (a JobArgument) StripS3() *JobArgument {
	return &JobArgument{a.arg, nil}
}

// Append appends a string to a JobArgument and returns itself
func (a *JobArgument) Append(s string, isS3path bool) *JobArgument {
	if a.s3 != nil && !isS3path {
		// a is an S3 object but s is not
		s = strings.Replace(s, string(filepath.Separator), "/", -1)
	}
	if a.s3 == nil && isS3path {
		// a is a not an S3 object but s is
		s = strings.Replace(s, "/", string(filepath.Separator), -1)
	}

	if a.s3 != nil {
		if a.s3.Key == "" {
			a.arg += "/" + s
		} else {
			a.arg += s
		}

		a.s3.Key += s
	} else {
		a.arg += s
	}

	return a
}
