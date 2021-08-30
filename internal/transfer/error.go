package transfer

import (
	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/storage/url"
)

// ReturnError returns error with given parameters.
func ReturnError(err error, op string, srcurl, dsturl *url.URL) error {
	if err != nil {
		return &errorpkg.Error{
			Op:  op,
			Src: srcurl,
			Dst: dsturl,
			Err: err,
		}
	}
	return nil
}
