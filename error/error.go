package error

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
)

type Error struct {
	// Op is the operation being performed, usually the name of the method
	// being invoked (copy, move, etc.)
	Op string
	// Src is the source argument
	Src *objurl.ObjectURL
	// Dst is the destination argument
	Dst *objurl.ObjectURL
	// The underlying error if any
	Err error
}

func (e *Error) FullCommand() string {
	return fmt.Sprintf("%v %v %v", e.Op, e.Src, e.Dst)
}

func (e *Error) Error() string {
	return e.Err.Error()
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
