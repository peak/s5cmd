package core

import (
	"context"
	"fmt"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

type sizeAndCount struct {
	size  int64
	count int64
}

func (s *sizeAndCount) addObject(obj *storage.Object) {
	s.size += obj.Size
	s.count++
}

func Size(ctx context.Context, job *Job) *JobResponse {
	src := job.args[0]

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	storageTotal := map[string]sizeAndCount{}
	total := sizeAndCount{}

	for object := range client.List(ctx, src, true, storage.ListAllItems) {
		if object.Type.IsDir() || object.Err != nil {
			// TODO(ig): expose or log the error
			continue
		}
		storageClass := string(object.StorageClass)
		s := storageTotal[storageClass]
		s.addObject(object)
		storageTotal[storageClass] = s

		total.addObject(object)
	}

	if !job.opts.Has(opt.GroupByClass) {
		m := SizeMessage{
			Source:       src.String(),
			Count:        total.count,
			Size:         total.size,
			shoHumanized: job.opts.Has(opt.HumanReadable),
		}
		log.Logger.Info(m)
		return jobResponse(err)
	}

	for k, v := range storageTotal {
		m := SizeMessage{
			Source:       src.String(),
			StorageClass: k,
			Count:        v.count,
			Size:         v.size,
			shoHumanized: job.opts.Has(opt.HumanReadable),
		}
		log.Logger.Info(m)
	}

	return jobResponse(nil)
}

// SizeMessage is the structure for logging disk usage.
type SizeMessage struct {
	Source       string `json:"source"`
	StorageClass string `json:"storage_class,omitempty"`
	Count        int64  `json:"count"`
	Size         int64  `json:"size"`

	shoHumanized bool
}

// humanize is a helper function to humanize bytes.
func (s SizeMessage) humanize() string {
	var size string
	if s.shoHumanized {
		size = humanizeBytes(s.Size)
	} else {
		size = fmt.Sprintf("%d", s.Size)
	}
	return size
}

// String returns the string representation of SizeMessage.
func (s SizeMessage) String() string {
	storageCls := ""
	if s.StorageClass != "" {
		storageCls = fmt.Sprintf(" [%s]", s.StorageClass)
	}
	return fmt.Sprintf(
		"%s bytes in %d objects: %s%s",
		s.humanize(),
		s.Count,
		s.Source,
		storageCls,
	)
}

// JSON returns the JSON representation of SizeMessage.
func (s SizeMessage) JSON() string {
	return jsonMarshal(s)
}
