package core

import (
	"context"
	"os"

	"github.com/hashicorp/go-multierror"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/message"
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

	msg := message.Info{
		Operation: "Copying",
		Target:    src.Base(),
	}
	log.Logger.Info(msg)

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

	msg := message.Info{
		Operation: "Deleting",
		Target:    src.Base(),
	}
	log.Logger.Info(msg)

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

	destFilename := dst.Absolute()

	// TODO(ig): use storage abstraction
	f, err := os.Create(destFilename)
	if err != nil {
		return jobResponse(err)
	}
	defer f.Close()

	msg := message.Info{
		Operation: "Downloading",
		Target:    src.Base(),
	}
	log.Logger.Info(msg)

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

	metadata := map[string]string{
		"StorageClass": string(job.storageClass),
		"ContentType":  "", // TODO(ig): guess the mimetype (see: #33)
	}

	msg := message.Info{
		Operation: "Uploading",
		Target:    src.Base(),
	}
	log.Logger.Info(msg)

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
			merror = multierror.Append(merror, obj.Err)
			err := message.Error{
				Job: job.String(),
				Err: obj.Err.Error(),
			}

			log.Logger.Error(err)
			continue
		}

		msg := message.Delete{
			URL:  obj.URL,
			Size: obj.Size,
		}

		log.Logger.Success(msg)
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
		log.Logger.Success(b)
	}

	return jobResponse(err)
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

		res := message.List{
			Object:        object,
			ShowEtag:      job.opts.Has(opt.ListETags),
			ShowHumanized: job.opts.Has(opt.HumanReadable),
		}
		log.Logger.Success(res)
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
		if object.Type.IsDir() || object.Err != nil {
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
		m := message.Size{
			Source:        src.String(),
			StorageClass:  k,
			Count:         v.count,
			Size:          v.size,
			ShowHumanized: job.opts.Has(opt.HumanReadable),
		}
		log.Logger.Success(m)
	}

	return jobResponse(err)
}
