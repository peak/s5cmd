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

// String returns the string representation of JobStatus.
func (s JobStatus) String() string {
	switch s {
	case statusSuccess:
		return "+"
	case statusErr:
		return "-"
	case statusWarning:
		return "+?"
	default:
		return "?"
	}
}

//  OK-to-have error types (warnings) that is used when the job status is warning.
var (
	ErrObjectExists     = fmt.Errorf("object already exists")
	ErrObjectIsNewer    = fmt.Errorf("object is newer or same age")
	ErrObjectSizesMatch = fmt.Errorf("object size matches")
)
