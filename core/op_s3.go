package core

import (
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

func S3Copy(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.src[0], job.dst

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return response
	}

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

	srcFn := src.Base()
	infoLog("Copying %s...", srcFn)

	err = client.Copy(
		wp.ctx,
		dst,
		src,
		job.cls,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(wp.ctx, src)
	}

	return jobResponse(err)
}

func S3Delete(job *Job, wp *WorkerParams) *JobResponse {
	src := job.src[0]

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

	err = client.Delete(wp.ctx, src)
	return jobResponse(err)
}

func S3Download(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.src[0], job.dst

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return response
	}

	client, err := wp.newClient(src)
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
		err = client.Delete(wp.ctx, src)
	}

	return jobResponse(err)
}

func S3Upload(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.src[0], job.dst

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return response
	}

	f, err := os.Open(src.Absolute())
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	// infer the client based on destination, which is a remote storage.
	client, err := wp.newClient(dst)
	if err != nil {
		return jobResponse(err)
	}

	srcFn := src.Base()
	infoLog("Uploading %s...", srcFn)

	metadata := map[string]string{
		"StorageClass": job.cls,
		"ContentType":  "", // guess the mimetype (see: #33)
	}

	err = client.Put(
		wp.ctx,
		f,
		dst,
		metadata,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = os.Remove(src.Absolute())
	}

	return jobResponse(err)
}

func S3BatchDelete(job *Job, wp *WorkerParams) *JobResponse {
	src := job.src[0]

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

	// do object->objurl transformation
	urlch := make(chan *objurl.ObjectURL)
	go func() {
		defer close(urlch)
		for obj := range client.List(wp.ctx, src, true, storage.ListAllItems) {
			urlch <- obj.URL
		}
	}()

	resultch := client.MultiDelete(wp.ctx, urlch)

	// closed errch indicates that MultiDelete operation is finished.
	var merror error
	var msg []string
	for obj := range resultch {
		if obj.Err != nil {
			merror = multierror.Append(merror, err)
			msg = append(msg, fmt.Sprintf(`Batch-delete %v: %v`, obj.URL, err))
		} else {
			msg = append(msg, fmt.Sprintf("Batch-delete %v", obj.URL))
		}
	}

	return jobResponse(merror, msg...)
}

func S3ListBuckets(_ *Job, wp *WorkerParams) *JobResponse {
	// set as remote storage
	url := &objurl.ObjectURL{Type: 0}
	client, err := wp.newClient(url)
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

	src := job.src[0]

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

	var msg []string
	for object := range client.List(wp.ctx, src, true, storage.ListAllItems) {
		if object.Err != nil {
			continue
		}

		if object.Mode.IsDir() {
			msg = append(msg, fmt.Sprintf("%19s %1s %-38s  %12s  %s", "", "", "", "DIR", object.URL.Relative()))
		} else {
			var cls, etag, size string

			switch object.StorageClass {
			case storage.StorageStandard:
				cls = ""
			case storage.StorageGlacier:
				cls = "G"
			case storage.StorageReducedRedundancy:
				cls = "R"
			case storage.StorageStandardIA:
				cls = "I"
			default:
				cls = "?"
			}

			if showETags {
				etag = strings.Trim(object.Etag, `"`)
			}
			if humanize {
				size = HumanizeBytes(object.Size)
			} else {
				size = fmt.Sprintf("%d", object.Size)
			}

			msg = append(
				msg,
				fmt.Sprintf("%s %1s %-38s %12s  %s",
					object.ModTime.Format(dateFormat),
					cls,
					etag,
					size,
					object.URL.Relative(),
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
	src := job.src[0]

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

	totals := map[string]sizeAndCount{}

	for object := range client.List(wp.ctx, src, true, storage.ListAllItems) {
		if object.Mode.IsDir() || object.Err != nil {
			continue
		}
		storageClass := string(object.StorageClass)
		s := totals[storageClass]
		s.size += object.Size
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

	return jobResponse(err, msg...)
}
