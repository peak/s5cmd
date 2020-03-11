package error

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
)

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

type Error struct {
	Op       string
	Src      *objurl.ObjectURL
	Dst      *objurl.ObjectURL
	Original error
}

func (e *Error) FullCommand() string {
	return fmt.Sprintf("%v %v %v", e.Op, e.Src, e.Dst)
}

func (e *Error) Error() string {
	return e.Original.Error()
}
