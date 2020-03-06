package core

import (
	"context"

	"github.com/peak/s5cmd/log"
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

	log.Logger.Info(InfoMessage{
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
