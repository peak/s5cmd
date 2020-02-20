package core

import (
	"context"
	"os"

	"github.com/peak/s5cmd/opt"
	"github.com/termie/go-shutil"
)

func LocalCopy(_ context.Context, job *Job) *JobResponse {
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

func LocalDelete(_ context.Context, job *Job) *JobResponse {
	srcPath := job.src.Absolute()
	err := os.Remove(srcPath)
	return jobResponse(err)
}
