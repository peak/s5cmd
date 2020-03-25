package error

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"

	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

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

func (e *Error) FullCommand() string {
	return fmt.Sprintf("%v %v %v", e.Op, e.Src, e.Dst)
}

func (e *Error) Error() string {
	return e.Err.Error()
}

func (e *Error) Unwrap() error {
	return e.Err
}

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

//  OK-to-have error types (warnings) that is used when the job status is warning.
var (
	ErrObjectExists     = fmt.Errorf("object already exists")
	ErrObjectIsNewer    = fmt.Errorf("object is newer or same age")
	ErrObjectSizesMatch = fmt.Errorf("object size matches")
)

func IsWarning(err error) bool {
	switch err {
	case ErrObjectExists, ErrObjectIsNewer, ErrObjectSizesMatch:
		return true
	}

	return false
}
