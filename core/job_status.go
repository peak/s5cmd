package core

import (
	"fmt"
)

type JobStatus int

const (
	statusSuccess JobStatus = iota
	statusErr
	statusWarning
)

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

var (
	WarningObjectExists     = fmt.Errorf("Object already exists")
	WarningObjectIsNewer    = fmt.Errorf("Object is newer or same age")
	WarningObjectSizesMatch = fmt.Errorf("Object size matches")
)
