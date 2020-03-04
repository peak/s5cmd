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

	if err != nil {
		return jobResponse(err)
	}

	log.Logger.Info(message.Info{
		Operation:   job.operation.String(),
		Source:      src,
		Destination: dst,
		Object: &storage.Object{
			URL:          dst,
			StorageClass: job.storageClass,
		},
	})

	return jobResponse(nil)
}

func Delete(ctx context.Context, job *Job) *JobResponse {
	src := job.args[0]

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	err = client.Delete(ctx, src)
	if err != nil {
		return jobResponse(err)
	}

	log.Logger.Info(message.Info{
		Operation: job.operation.String(),
		Source:    src,
	})

	return jobResponse(nil)
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

	size, err := srcClient.Get(ctx, src, f)

	if err != nil {
		err = dstClient.Delete(ctx, dst)
	} else if job.opts.Has(opt.DeleteSource) {
		err = srcClient.Delete(ctx, src)
	}

	if err != nil {
		return jobResponse(err)
	}

	log.Logger.Info(message.Info{
		Operation:   job.operation.String(),
		Source:      src,
		Destination: dst,
		Object:      &storage.Object{Size: size},
	})

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

	err = dstClient.Put(
		ctx,
		f,
		dst,
		metadata,
	)

	obj, _ := srcClient.Stat(ctx, src)
	size := obj.Size

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = srcClient.Delete(ctx, src)
	}

	if err != nil {
		return jobResponse(err)
	}

	log.Logger.Info(message.Info{
		Operation:   job.operation.String(),
		Source:      src,
		Destination: dst,
		Object:      &storage.Object{Size: size},
	})

	return jobResponse(nil)
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

		log.Logger.Info(message.Info{
			Operation: job.operation.String(),
			Source:    obj.URL,
		})
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
		log.Logger.Info(b)
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
		log.Logger.Info(res)
	}

	return jobResponse(nil)
}

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
		m := message.Size{
			Source:        src.String(),
			Count:         total.count,
			Size:          total.size,
			ShowHumanized: job.opts.Has(opt.HumanReadable),
		}
		log.Logger.Info(m)
		return jobResponse(err)
	}

	for k, v := range storageTotal {
		m := message.Size{
			Source:        src.String(),
			StorageClass:  k,
			Count:         v.count,
			Size:          v.size,
			ShowHumanized: job.opts.Has(opt.HumanReadable),
		}
		log.Logger.Info(m)
	}

	return jobResponse(err)
}

func MakeBucket(ctx context.Context, job *Job) *JobResponse {
	bucket := job.args[0]

	client, err := storage.NewClient(bucket)
	if err != nil {
		return jobResponse(err)
	}

	err = client.MakeBucket(ctx, bucket.Bucket)
	if err != nil {
		return jobResponse(err)
	}

	log.Logger.Info(message.Info{
		Operation: job.operation.String(),
		Source:    bucket,
	})

	return jobResponse(nil)
}
