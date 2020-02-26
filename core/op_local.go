package core

import (
	"github.com/peak/s5cmd/opt"
)

func LocalCopy(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.args[0], job.args[1]

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return response
	}

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

	infoLog("Copying %s...", src.Base())
	err = client.Copy(
		wp.ctx,
		src,
		dst,
		job.cls,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(wp.ctx, src)
	}

	return jobResponse(err)
}

func LocalDelete(job *Job, wp *WorkerParams) *JobResponse {
	src := job.args[0]

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

	infoLog("Deleting %s...", src.Base())

	err = client.Delete(wp.ctx, src)
	return jobResponse(err)
}
