package log

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/strutil"
)

// Message is an interface to print structured logs.
type Message interface {
	fmt.Stringer
	JSON() string
}

// InfoMessage is a generic message structure for successful operations.
type InfoMessage struct {
	Operation   string            `json:"operation"`
	Success     bool              `json:"success"`
	Source      *objurl.ObjectURL `json:"source"`
	Destination *objurl.ObjectURL `json:"destination,omitempty"`
	Object      *storage.Object   `json:"object,omitempty"`
}

// String is the string representation of InfoMessage.
func (i InfoMessage) String() string {
	return fmt.Sprintf("%v %v", i.Operation, i.Source)
}

// JSON is the JSON representation of InfoMessage.
func (i InfoMessage) JSON() string {
	i.Success = true
	return strutil.JSON(i)
}

// ErrorMessage is a generic message structure for unsuccessful operations.
type ErrorMessage struct {
	Operation string `json:"operation,omitempty"`
	Command   string `json:"command,omitempty"`
	Err       string `json:"error"`

	format string
}

// String is the string representation of ErrorMessage.
func (e ErrorMessage) String() string {
	if e.Command == "" {
		return fmt.Sprintf(e.format, e.Err)
	}
	return fmt.Sprintf(e.format, e.Command, e.Err)
}

// JSON is the JSON representation of ErrorMessage.
func (e ErrorMessage) JSON() string {
	return strutil.JSON(e)
}

// ErrorMessage is a generic message structure for unsuccessful operations.
type WarningMessage struct {
	Operation string `json:"operation,omitempty"`
	Command   string `json:"job,omitempty"`
	Err       string `json:"error"`
}

// String is the string representation of ErrorMessage.
func (w WarningMessage) String() string {
	if w.Command == "" {
		return w.Err
	}
	return fmt.Sprintf("%q (%v)", w.Command, w.Err)
}

// JSON is the JSON representation of ErrorMessage.
func (w WarningMessage) JSON() string {
	return strutil.JSON(w)
}

// DebugMessage is a generic message structure for debugging logs.
type DebugMessage struct {
	Content string `json:"content"`
}

// String is the string representation of DebugMessage.
func (d DebugMessage) String() string {
	return d.Content
}

// JSON is the JSON representation of DebugMessage.
func (d DebugMessage) JSON() string {
	return strutil.JSON(d)
}

// printDebug is the helper function to log debug messages.
func printDebug(format string, args ...interface{}) {
	content := fmt.Sprintf(format, args...)
	msg := DebugMessage{Content: content}
	Debug(msg)
}

func isCancelationError(err error) bool {
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
		if isCancelationError(err) {
			return true
		}
	}

	return false
}
