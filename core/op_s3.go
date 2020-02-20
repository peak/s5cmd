package core

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

func S3Copy(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.src, job.dst
	client := job.client

	err := client.Copy(
		ctx,
		dst,
		src,
		job.cls,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(ctx, src.Bucket, src)
	}

	return jobResponse(err)
}

func S3Delete(ctx context.Context, job *Job) *JobResponse {
	// TODO: FIX
	src := job.src
	err := job.client.Delete(ctx, src.Bucket, job.dst)
	return jobResponse(err)
}

func S3Download(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.src, job.dst

	srcFn := src.Base()
	destFn := dst.Absolute()
	client := job.client

	f, err := os.Create(destFn)
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	infoLog("Downloading %s...", srcFn)
	err = client.Get(ctx, src, f)
	if err != nil {
		os.Remove(destFn) // Remove partly downloaded file
	} else if job.opts.Has(opt.DeleteSource) {
		err = client.Delete(ctx, src.Bucket, src)
	}

	return jobResponse(err)
}

func S3Upload(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.src, job.dst
	srcFn := src.Base()

	f, err := os.Open(src.Absolute())
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	infoLog("Uploading %s...", srcFn)

	err = job.client.Put(
		ctx,
		f,
		dst,
		job.cls,
	)

	return jobResponse(err)
}

func S3ListBuckets(ctx context.Context, job *Job) *JobResponse {
	buckets, err := job.client.ListBuckets(ctx, "")
	if err != nil {
		return jobResponse(err)
	}

	var msg []string
	for _, b := range buckets {
		msg = append(msg, b.String())
	}

	return jobResponse(err, msg...)
}

func S3List(ctx context.Context, job *Job) *JobResponse {
	const opType = stats.S3Op

	showETags := job.opts.Has(opt.ListETags)
	humanize := job.opts.Has(opt.HumanReadable)

	src := job.src
	client := job.client

	var msg []string
	for item := range client.List(ctx, src, storage.ListAllItems) {
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

func S3Size(ctx context.Context, job *Job) *JobResponse {
	type sizeAndCount struct {
		size  int64
		count int64
	}

	src := job.src
	totals := map[string]sizeAndCount{}

	for item := range job.client.List(ctx, src, storage.ListAllItems) {
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
