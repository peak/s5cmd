package core

import (
	"github.com/peak/s5cmd/opt"
)

func LocalCopy(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.src[0], job.dst

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return response
	}

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

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
	src := job.src[0]

	client, err := wp.newClient(src)
	if err != nil {
		return jobResponse(err)
	}

	err = client.Delete(wp.ctx, src)
	return jobResponse(err)
}
