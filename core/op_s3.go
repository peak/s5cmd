package core

import (
	"fmt"
	"os"
	"strings"

	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

func S3Copy(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.src[0], job.dst

	client, err := wp.newClient()
	if err != nil {
		return jobResponse(err)
	}

	err = client.Copy(
		wp.ctx,
		dst,
		src,
		job.cls,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(wp.ctx, src.Bucket, src)
	}

	return jobResponse(err)
}

func S3Delete(job *Job, wp *WorkerParams) *JobResponse {
	client, err := wp.newClient()
	if err != nil {
		return jobResponse(err)
	}

	src := job.src
	err = client.Delete(wp.ctx, src.Bucket, job.dst)
	return jobResponse(err)
}

func S3Download(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.src, job.dst

	client, err := wp.newClient()
	if err != nil {
		return jobResponse(err)
	}

	srcFn := src.Base()
	destFn := dst.Absolute()

	f, err := os.Create(destFn)
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	infoLog("Downloading %s...", srcFn)
	err = client.Get(wp.ctx, src, f)
	if err != nil {
		os.Remove(destFn) // Remove partly downloaded file
	} else if job.opts.Has(opt.DeleteSource) {
		err = client.Delete(wp.ctx, src.Bucket, src)
	}

	return jobResponse(err)
}

func S3Upload(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.src, job.dst
	srcFn := src.Base()

	f, err := os.Open(src.Absolute())
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	client, err := wp.newClient()
	if err != nil {
		return jobResponse(err)
	}

	infoLog("Uploading %s...", srcFn)

	err = client.Put(
		wp.ctx,
		f,
		dst,
		job.cls,
	)

	return jobResponse(err)
}

func S3ListBuckets(_ *Job, wp *WorkerParams) *JobResponse {
	client, err := wp.newClient()
	if err != nil {
		return jobResponse(err)
	}

	buckets, err := client.ListBuckets(wp.ctx, "")
	if err != nil {
		return jobResponse(err)
	}

	var msg []string
	for _, b := range buckets {
		msg = append(msg, b.String())
	}

	return jobResponse(err, msg...)
}

func S3List(job *Job, wp *WorkerParams) *JobResponse {
	showETags := job.opts.Has(opt.ListETags)
	humanize := job.opts.Has(opt.HumanReadable)

	src := job.src

	client, err := wp.newClient()
	if err != nil {
		return jobResponse(err)
	}

	var msg []string
	for item := range client.List(wp.ctx, src, storage.ListAllItems) {
		if item.IsMarkerObject() || item.Err != nil {
			continue
		}

		if item.IsDirectory {
			msg = append(msg, fmt.Sprintf("%19s %1s %-38s  %12s  %s", "", "", "", "DIR", item.URL.Relative()))
		} else {
			var cls, etag, size string

			switch item.StorageClass {
			case storage.ObjectStorageClassStandard:
				cls = ""
			case storage.ObjectStorageClassGlacier:
				cls = "G"
			case storage.ObjectStorageClassReducedRedundancy:
				cls = "R"
			case storage.TransitionStorageClassStandardIa:
				cls = "I"
			default:
				cls = "?"
			}

			if showETags {
				etag = strings.Trim(item.Etag, `"`)
			}
			if humanize {
				size = HumanizeBytes(item.Size)
			} else {
				size = fmt.Sprintf("%d", item.Size)
			}

			msg = append(
				msg,
				fmt.Sprintf("%s %1s %-38s %12s  %s",
					item.LastModified.Format(dateFormat),
					cls,
					etag,
					size,
					item.URL.Relative(),
				),
			)
		}
	}

	return jobResponse(nil, msg...)
}

func S3Size(job *Job, wp *WorkerParams) *JobResponse {
	type sizeAndCount struct {
		size  int64
		count int64
	}
	src := job.src

	client, err := wp.newClient()
	if err != nil {
		return jobResponse(err)
	}

	totals := map[string]sizeAndCount{}

	for item := range client.List(wp.ctx, src, storage.ListAllItems) {
		if item.IsMarkerObject() || item.IsDirectory {
			continue
		}
		storageClass := item.StorageClass
		s := totals[storageClass]
		s.size += item.Size
		s.count++
		totals[storageClass] = s
	}

	sz := sizeAndCount{}
	if !job.opts.Has(opt.GroupByClass) {
		for k, v := range totals {
			sz.size += v.size
			sz.count += v.count
			delete(totals, k)
		}
		totals["Total"] = sz
	}

	var msg []string
	for k, v := range totals {
		if job.opts.Has(opt.HumanReadable) {
			msg = append(msg, fmt.Sprintf("%s bytes in %d objects: %s [%s]", HumanizeBytes(v.size), v.count, src, k))
		} else {
			msg = append(msg, fmt.Sprintf("%d bytes in %d objects: %s [%s]", v.size, v.count, src, k))
		}
	}

	return jobResponse(nil, msg...)
}
