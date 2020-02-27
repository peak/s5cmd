package core

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

func S3Copy(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.args[0], job.args[1]

	response := CheckConditions(ctx, src, dst, job.opts)
	if response != nil {
		return response
	}

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	srcFilename := src.Base()
	infoLog("Copying %s...", srcFilename)

	metadata := map[string]string{
		"StorageClass": string(job.storageClass),
	}

	err = client.Copy(
		ctx,
		dst,
		src,
		metadata,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(ctx, src)
	}

	return jobResponse(err)
}

func S3Delete(ctx context.Context, job *Job) *JobResponse {
	src := job.args[0]

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	err = client.Delete(ctx, src)
	return jobResponse(err)
}

func S3Download(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.args[0], job.args[1]

	response := CheckConditions(ctx, src, dst, job.opts)
	if response != nil {
		return response
	}

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	srcFilename := src.Base()
	destFilename := dst.Absolute()

	f, err := os.Create(destFilename)
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	infoLog("Downloading %s...", srcFilename)

	err = client.Get(ctx, src, f)
	if err != nil {
		os.Remove(destFilename)
	} else if job.opts.Has(opt.DeleteSource) {
		err = client.Delete(ctx, src)
	}

	return jobResponse(err)
}

func S3Upload(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.args[0], job.args[1]

	response := CheckConditions(ctx, src, dst, job.opts)
	if response != nil {
		return response
	}

	f, err := os.Open(src.Absolute())
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	// infer the client based on destination, which is a remote storage.
	client, err := storage.NewClient(dst)
	if err != nil {
		return jobResponse(err)
	}

	srcFilename := src.Base()
	infoLog("Uploading %s...", srcFilename)

	metadata := map[string]string{
		"StorageClass": string(job.storageClass),
		"ContentType":  "", // TODO(ig): guess the mimetype (see: #33)
	}

	err = client.Put(
		ctx,
		f,
		dst,
		metadata,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = os.Remove(src.Absolute())
	}

	return jobResponse(err)
}

func S3BatchDelete(ctx context.Context, job *Job) *JobResponse {
	sources := job.args

	client, err := storage.NewClient(sources[0])
	if err != nil {
		return jobResponse(err)
	}

	// do object->objurl transformation
	urlch := make(chan *objurl.ObjectURL)

	go func() {
		defer close(urlch)

		// there are multiple source files which are received from batch-rm
		// command.
		if len(sources) > 1 {
			for _, url := range sources {
				select {
				case <-ctx.Done():
					return
				case urlch <- url:
				}
			}
		} else {
			// src is a glob
			src := sources[0]
			for obj := range client.List(ctx, src, true, storage.ListAllItems) {
				if obj.Err != nil {
					// TODO(ig): add proper logging
					continue
				}
				urlch <- obj.URL
			}
		}
	}()

	resultch := client.MultiDelete(ctx, urlch)

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

func S3ListBuckets(ctx context.Context, _ *Job) *JobResponse {
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

	var msg []string
	for _, b := range buckets {
		msg = append(msg, b.String())
	}

	return jobResponse(err, msg...)
}

func S3List(ctx context.Context, job *Job) *JobResponse {
	showETags := job.opts.Has(opt.ListETags)
	humanize := job.opts.Has(opt.HumanReadable)

	src := job.args[0]

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	var msg []string
	for object := range client.List(ctx, src, true, storage.ListAllItems) {
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

func S3Size(ctx context.Context, job *Job) *JobResponse {
	type sizeAndCount struct {
		size  int64
		count int64
	}
	src := job.args[0]

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	totals := map[string]sizeAndCount{}

	for object := range client.List(ctx, src, true, storage.ListAllItems) {
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
