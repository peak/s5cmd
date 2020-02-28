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

func Copy(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.args[0], job.args[1]

	response := CheckConditions(ctx, src, dst, job.opts)
	if response != nil {
		return response
	}

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	infoLog("Copying %v...", src.Base())

	metadata := map[string]string{
		"StorageClass": string(job.storageClass),
	}

	err = client.Copy(
		ctx,
		src,
		dst,
		metadata,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(ctx, src)
	}

	return jobResponse(err)
}

func Delete(ctx context.Context, job *Job) *JobResponse {
	src := job.args[0]

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	infoLog("Deleting %v...", src.Base())

	err = client.Delete(ctx, src)
	return jobResponse(err)
}

func Download(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.args[0], job.args[1]

	response := CheckConditions(ctx, src, dst, job.opts)
	if response != nil {
		return response
	}

	srcClient, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return jobResponse(err)
	}

	srcFilename := src.Base()
	destFilename := dst.Absolute()

	// TODO(ig): use storage abstraction
	f, err := os.Create(destFilename)
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	infoLog("Downloading %s...", srcFilename)

	err = srcClient.Get(ctx, src, f)
	if err != nil {
		err = dstClient.Delete(ctx, dst)
	} else if job.opts.Has(opt.DeleteSource) {
		err = srcClient.Delete(ctx, src)
	}

	return jobResponse(err)
}

func Upload(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.args[0], job.args[1]

	response := CheckConditions(ctx, src, dst, job.opts)
	if response != nil {
		return response
	}

	// TODO(ig): use storage abstraction
	f, err := os.Open(src.Absolute())
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return jobResponse(err)
	}

	srcClient, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	srcFilename := src.Base()
	infoLog("Uploading %s...", srcFilename)

	metadata := map[string]string{
		"StorageClass": string(job.storageClass),
		"ContentType":  "", // TODO(ig): guess the mimetype (see: #33)
	}

	err = dstClient.Put(
		ctx,
		f,
		dst,
		metadata,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = srcClient.Delete(ctx, src)
	}

	return jobResponse(err)
}

func BatchDelete(ctx context.Context, job *Job) *JobResponse {
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
	for obj := range resultch {
		if obj.Err != nil {
			merror = multierror.Append(merror, err)
			msg := message{
				status: statusErr,
				err:    fmt.Errorf(`batch-delete %v: %v`, obj.URL, err),
				job:    job.String(),
			}
			sendMessage(ctx, msg)
			continue
		}

		msg := message{
			job: job.String(),
			s:   fmt.Sprintf(`Batch-delete %v`, obj.URL),
		}
		sendMessage(ctx, msg)
	}

	return jobResponse(merror)
}

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
		msg := message{
			s:   b.String(),
			job: job.String(),
		}
		sendMessage(ctx, msg)
	}

	return jobResponse(err)
}

func List(ctx context.Context, job *Job) *JobResponse {
	showETags := job.opts.Has(opt.ListETags)
	humanize := job.opts.Has(opt.HumanReadable)

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

		if object.Mode.IsDir() {
			msg := message{
				job:    job.String(),
				status: statusSuccess,
				s: fmt.Sprintf(
					"%19s %1s %-38s  %12s  %s",
					"",
					"",
					"",
					"DIR",
					object.URL.Relative(),
				),
			}
			sendMessage(ctx, msg)
			continue
		}

		var etag, size string

		if showETags {
			etag = strings.Trim(object.Etag, `"`)
		}
		if humanize {
			size = HumanizeBytes(object.Size)
		} else {
			size = fmt.Sprintf("%d", object.Size)
		}

		msg := message{
			job:    job.String(),
			status: statusSuccess,
			s: fmt.Sprintf(
				"%19s %1s %-38s  %12s  %s",
				object.ModTime.Format(dateFormat),
				object.StorageClass.ShortCode(),
				etag,
				size,
				object.URL.Relative(),
			),
		}
		sendMessage(ctx, msg)
	}

	return jobResponse(nil)
}

func Size(ctx context.Context, job *Job) *JobResponse {
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
			// TODO(ig): expose or log the error
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

	for k, v := range totals {
		var msg string
		if job.opts.Has(opt.HumanReadable) {
			msg = fmt.Sprintf("%s bytes in %d objects: %s [%s]", HumanizeBytes(v.size), v.count, src, k)
		} else {
			msg = fmt.Sprintf("%d bytes in %d objects: %s [%s]", v.size, v.count, src, k)
		}

		m := message{s: msg, job: job.String()}
		sendMessage(ctx, m)
	}

	return jobResponse(err)
}
