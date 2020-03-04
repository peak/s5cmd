package core

import (
	"context"
	"os"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

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

	log.Logger.Info(InfoMessage{
		Operation:   job.operation.String(),
		Source:      src,
		Destination: dst,
		Object:      &storage.Object{Size: size},
	})

	return jobResponse(nil)
}
