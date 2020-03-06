package core

import (
	"context"
	"os"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

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

	log.Logger.Info(InfoMessage{
		Operation:   job.operation.String(),
		Source:      src,
		Destination: dst,
		Object:      &storage.Object{Size: size},
	})

	return jobResponse(nil)
}
