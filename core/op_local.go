package core

import (
	"os"

	"github.com/peak/s5cmd/opt"
	"github.com/termie/go-shutil"
)

func LocalCopy(job *Job, wp *WorkerParams) *JobResponse {
	src, dst := job.src, job.dst
	srcPath := src.Absolute()
	dstPath := dst.Absolute()

	var err error
	if job.opts.Has(opt.DeleteSource) {
		err = os.Rename(srcPath, dstPath)
	} else {
		_, err = shutil.Copy(srcPath, dstPath, true)
	}

	return jobResponse(err)
}

func LocalDelete(job *Job, wp *WorkerParams) *JobResponse {
	srcPath := job.src.Absolute()
	err := os.Remove(srcPath)
	return jobResponse(err)
}
