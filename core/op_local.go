package core

import (
	"context"

	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

func LocalCopy(ctx context.Context, job *Job) *JobResponse {
	src, dst := job.args[0], job.args[1]

	response := CheckConditions(ctx, src, dst, job.opts)
	if response != nil {
		return response
	}

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	infoLog("Copying %s...", src.Base())

	err = client.Copy(
		ctx,
		src,
		dst,
		nil,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(ctx, src)
	}

	return jobResponse(err)
}

func LocalDelete(ctx context.Context, job *Job) *JobResponse {
	src := job.args[0]

	client, err := storage.NewClient(src)
	if err != nil {
		return jobResponse(err)
	}

	infoLog("Deleting %s...", src.Base())

	err = client.Delete(ctx, src)
	return jobResponse(err)
}
