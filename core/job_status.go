package core

import (
	"fmt"
)

// JobStatus is the status of Job.
type JobStatus int

const (
	statusSuccess JobStatus = iota
	statusErr
	statusWarning
)

//  OK-to-have error types (warnings) that is used when the job status is warning.
var (
	ErrObjectExists     = fmt.Errorf("object already exists")
	ErrObjectIsNewer    = fmt.Errorf("object is newer or same age")
	ErrObjectSizesMatch = fmt.Errorf("object size matches")
)

func isWarning(err error) bool {
	switch err {
	case ErrObjectExists, ErrObjectIsNewer, ErrObjectSizesMatch:
		return true
	}

	return false
}
