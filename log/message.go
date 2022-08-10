package log

import (
	"fmt"

	"github.com/peak/s5cmd/storage/url"
	"github.com/peak/s5cmd/strutil"
)

// Message is an interface to print structured logs.
type Message interface {
	fmt.Stringer
	JSON() string
}

// InfoMessage is a generic message structure for successful operations.
type InfoMessage struct {
	Operation   string   `json:"operation"`
	Success     bool     `json:"success"`
	Source      *url.URL `json:"source"`
	Destination *url.URL `json:"destination,omitempty"`
	Object      Message  `json:"object,omitempty"`

	// the VersionID field exist only for JSON Marshall, it must not be used for
	// any other purpose.
	VersionId string `json:"version_id,omitempty"`
}

// String is the string representation of InfoMessage.
func (i InfoMessage) String() string {
	if i.Destination != nil {
		return fmt.Sprintf("%v %v %v", i.Operation, i.Source, i.Destination)
	}
	if i.Source != nil && i.Source.VersionID != "" {
		return fmt.Sprintf("%v %-50v %v", i.Operation, i.Source, i.Source.VersionID)
	}
	return fmt.Sprintf("%v %v", i.Operation, i.Source)
}

// JSON is the JSON representation of InfoMessage.
func (i InfoMessage) JSON() string {
	if i.Destination == nil && i.Source != nil {
		i.VersionId = i.Source.VersionID
	}
	i.Success = true
	return strutil.JSON(i)
}

// ErrorMessage is a generic message structure for unsuccessful operations.
type ErrorMessage struct {
	Operation string `json:"operation,omitempty"`
	Command   string `json:"command,omitempty"`
	Err       string `json:"error"`
}

// String is the string representation of ErrorMessage.
func (e ErrorMessage) String() string {
	if e.Command == "" {
		return fmt.Sprint(e.Err)
	}
	return fmt.Sprintf("%q: %v", e.Command, e.Err)
}

// JSON is the JSON representation of ErrorMessage.
func (e ErrorMessage) JSON() string {
	return strutil.JSON(e)
}

// DebugMessage is a generic message structure for unsuccessful operations.
type DebugMessage struct {
	Operation string `json:"operation,omitempty"`
	Command   string `json:"job,omitempty"`
	Err       string `json:"error"`
}

type TraceMessage struct {
	Message string `json:"message"`
}

func (t TraceMessage) String() string {
	return t.Message
}

func (t TraceMessage) JSON() string {
	return strutil.JSON(t)
}

// String is the string representation of ErrorMessage.
func (d DebugMessage) String() string {
	if d.Command == "" {
		return d.Err
	}
	return fmt.Sprintf("%q: %v", d.Command, d.Err)
}

// JSON is the JSON representation of ErrorMessage.
func (d DebugMessage) JSON() string {
	return strutil.JSON(d)
}
