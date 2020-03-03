package message

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
)

const dateFormat = "2006/01/02 15:04:05"

type Message interface {
	fmt.Stringer
	JSON() string
}

type Delete struct {
	URL  *objurl.ObjectURL `json:"source"`
	Size int64             `json:"size"`
}

func (d Delete) String() string {
	return fmt.Sprintf("Batch-delete %v", d.URL)
}

func (d Delete) JSON() string {
	bytes, _ := json.Marshal(d)
	return string(bytes)
}

type List struct {
	Object        *storage.Object `json:"object"`
	ShowEtag      bool            `json:"-"`
	ShowHumanized bool            `json:"-"`
}

func (l List) humanize() string {
	var size string
	if l.ShowHumanized {
		size = humanizeBytes(l.Object.Size)
	} else {
		size = fmt.Sprintf("%d", l.Object.Size)
	}
	return size
}

func (l List) String() string {
	if l.Object.Type.IsDir() {
		s := fmt.Sprintf(
			"%19s %1s %-38s  %12s  %s",
			"",
			"",
			"",
			"DIR",
			l.Object.URL.Relative(),
		)
		return s
	}

	var etag string
	if l.ShowEtag {
		etag = l.Object.Etag
	}

	s := fmt.Sprintf(
		"%19s %1s %-38s  %12s  %s",
		l.Object.ModTime.Format(dateFormat),
		l.Object.StorageClass.ShortCode(),
		etag,
		l.humanize(),
		l.Object.URL.Relative(),
	)
	return s
}

func (l List) JSON() string {
	b, _ := json.Marshal(l.Object)
	return string(b)
}

type Size struct {
	Source       string `json:"source"`
	StorageClass string `json:"storage_class"`
	Count        int64  `json:"count"`
	Bytes        string `json:"size"`

	Size          int64 `json:"-"`
	ShowHumanized bool  `json:"-"`
}

func (s Size) humanize() string {
	var size string
	if s.ShowHumanized {
		size = humanizeBytes(s.Size)
	} else {
		size = fmt.Sprintf("%d", s.Size)
	}
	return size
}

func (s Size) String() string {
	return fmt.Sprintf(
		"%s bytes in %d objects: %s [%s]",
		s.humanize(),
		s.Count,
		s.Source,
		s.StorageClass,
	)
}

func (s Size) JSON() string {
	s.Bytes = s.humanize()
	bytes, _ := json.Marshal(s)
	return string(bytes)
}

type JSON struct {
	Source      *objurl.ObjectURL `json:"source"`
	Destination *objurl.ObjectURL `json:"destination"`
	Object      *storage.Object   `json:"object,omitempty"`
	Error       error             `json:"error,omitempty"`
}

func (u JSON) String() string {
	return ""
}

func (u JSON) JSON() string {
	bytes, _ := json.Marshal(u)
	return string(bytes)
}

type Info struct {
	Operation string `json:"operation"`
	Target    string `json:"target"`
}

func (i Info) String() string {
	return fmt.Sprintf("%s %s...", i.Operation, i.Target)
}

func (i Info) JSON() string {
	return ""
}

type Error struct {
	Job string `json:"job"`
	Err string `json:"error,omitempty"`
}

func (e Error) String() string {
	err := cleanupSpaces(e.Err)
	return fmt.Sprintf("%q : %v", e.Job, err)
}

func (e Error) JSON() string {
	e.Err = cleanupSpaces(e.Err)
	b, _ := json.Marshal(e)
	return string(b)
}

type Warning struct {
	Job string `json:"job"`
	Err string `json:"error,omitempty"`
}

func (w Warning) String() string {
	err := cleanupSpaces(w.Err)
	return fmt.Sprintf("%q (%v)", w.Job, err)
}

func (w Warning) JSON() string {
	w.Err = cleanupSpaces(w.Err)
	b, _ := json.Marshal(w)
	return string(b)
}

type Debug struct {
	Content string `json:"content"`
}

func (d Debug) String() string {
	return d.Content
}

func (d Debug) JSON() string {
	b, _ := json.Marshal(d)
	return string(b)
}

// cleanupError converts multiline error messages generated by aws-sdk-go into
// a single line.
func cleanupSpaces(s string) string {
	s = strings.Replace(s, "\n", " ", -1)
	s = strings.Replace(s, "\t", " ", -1)
	s = strings.Replace(s, "  ", " ", -1)
	s = strings.TrimSpace(s)
	return s
}
