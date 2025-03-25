package error

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

// Error is the type that implements error interface.
type Error struct {
	// Op is the operation being performed, usually the name of the method
	// being invoked (copy, move, etc.)
	Op string
	// Src is the source argument
	Src *url.URL
	// Dst is the destination argument
	Dst *url.URL
	// The underlying error if any
	Err error
}

// FullCommand returns the command string that occurred at.
func (e *Error) FullCommand() string {
	return fmt.Sprintf("%v %v %v", e.Op, e.Src, e.Dst)
}

// Error implements the error interface.
func (e *Error) Error() string {
	return e.Err.Error()
}

// Unwrap unwraps the error.
func (e *Error) Unwrap() error {
	return e.Err
}

// IsCancelation reports whether if given error is a cancelation error.
func IsCancelation(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	if storage.IsCancelationError(err) {
		return true
	}

	merr, ok := err.(*multierror.Error)
	if !ok {
		return false
	}

	for _, err := range merr.Errors {
		if IsCancelation(err) {
			return true
		}
	}

	return false
}

var (
	// ErrObjectExists indicates a specified object already exists.
	ErrObjectExists = fmt.Errorf("object already exists")

	// ErrObjectIsNewer indicates a specified object is newer or same age.
	ErrObjectIsNewer = fmt.Errorf("object is newer or same age")

	// ErrObjectSizesMatch indicates the sizes of objects match.
	ErrObjectSizesMatch = fmt.Errorf("object size matches")

	// ErrObjectEtagsMatch indicates the Etag of objects match.
	ErrObjectEtagsMatch = fmt.Errorf("object ETag matches")

	// ErrObjectIsNewerAndSizesMatch indicates the specified object is newer or same age and sizes of objects match.
	ErrObjectIsNewerAndSizesMatch = fmt.Errorf("%v and %v", ErrObjectIsNewer, ErrObjectSizesMatch)

	// ErrObjectIsGlacier indicates the object is in Glacier storage class.
	ErrorObjectIsGlacier = fmt.Errorf("object is in Glacier storage class")
)

// IsWarning checks if given error is either ErrObjectExists,
// ErrObjectIsNewer or ErrObjectSizesMatch.
func IsWarning(err error) bool {
	switch err {
	case ErrObjectExists, ErrObjectIsNewer, ErrObjectSizesMatch, ErrObjectEtagsMatch, ErrObjectIsNewerAndSizesMatch, ErrorObjectIsGlacier:
		return true
	}

	return false
}
