package core

import (
	"context"
	"fmt"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

func ListBuckets(ctx context.Context, job *Job) *JobResponse {
	// set as remote storage
	url := &objurl.ObjectURL{Type: 0}
	client, err := storage.NewClient(url)
	if err != nil {
		return jobResponse(err)
	}

	buckets, err := client.ListBuckets(ctx, "")
	if err != nil {
		return jobResponse(err)
	}

	for _, b := range buckets {
		log.Logger.Info(b)
	}

	return jobResponse(nil)
}

func List(ctx context.Context, job *Job) *JobResponse {
	src := job.args[0]

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	for object := range client.List(ctx, src, true, storage.ListAllItems) {
		if object.Err != nil {
			// TODO(ig): expose or log the error
			continue
		}

		res := ListMessage{
			Object:        object,
			showEtag:      job.opts.Has(opt.ListETags),
			showHumanized: job.opts.Has(opt.HumanReadable),
		}
		log.Logger.Info(res)
	}

	return jobResponse(nil)
}

// ListMessage is a structure for logging ls results.
type ListMessage struct {
	Object *storage.Object `json:"object"`

	showEtag      bool
	showHumanized bool
}

// humanize is a helper function to humanize bytes.
func (l ListMessage) humanize() string {
	var size string
	if l.showHumanized {
		size = humanizeBytes(l.Object.Size)
	} else {
		size = fmt.Sprintf("%d", l.Object.Size)
	}
	return size
}

const (
	listFormat = "%19s %1s %-6s %12s %s"
	dateFormat = "2006/01/02 15:04:05"
)

// String returns the string representation of ListMessage.
func (l ListMessage) String() string {
	if l.Object.Type.IsDir() {
		s := fmt.Sprintf(
			listFormat,
			"",
			"",
			"",
			"DIR",
			l.Object.URL.Relative(),
		)
		return s
	}

	var etag string
	if l.showEtag {
		etag = l.Object.Etag
	}

	s := fmt.Sprintf(
		listFormat,
		l.Object.ModTime.Format(dateFormat),
		l.Object.StorageClass.ShortCode(),
		etag,
		l.humanize(),
		l.Object.URL.Relative(),
	)
	return s
}

// JSON returns the JSON representation of ListMessage.
func (l ListMessage) JSON() string {
	return jsonMarshal(l.Object)
}
