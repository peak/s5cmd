package core

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

func LocalCopy(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.FileOp

	src, dst := job.args[0], job.args[1]

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return opType, response
	}

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	err = client.Copy(
		wp.ctx,
		src.url,
		dst.url,
		job.getStorageClass(),
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(wp.ctx, src.url)
	}

	return opType, jobResponse(err)
}

func LocalDelete(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.FileOp

	src := job.args[0]

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	err = client.Delete(wp.ctx, src.url)
	return opType, jobResponse(err)
}

func BatchLocalCopy(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.FileOp

	subCmd := "cp"
	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	src, dst := job.args[0], job.args[1]

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	// src.url could contain glob, or could be a directory.
	// err is not important here
	obj, err := client.Stat(wp.ctx, src.url)
	walkMode := err == nil && obj.Mode.IsDir()

	trimPrefix := src.url.Absolute()

	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return opType, jobResponse(fmt.Errorf("internal error, not a glob: %s", trimPrefix))
		}
		trimPrefix = trimPrefix[:loc]
	}

	trimPrefix = path.Dir(trimPrefix)
	if trimPrefix == "." {
		trimPrefix = ""
	} else {
		trimPrefix += string(filepath.Separator)
	}

	isRecursive := job.opts.Has(opt.Recursive)

	err = wildOperation(client, src.url, isRecursive, wp, func(object *storage.Object) *Job {
		if object.IsMarker() || object.Mode.IsDir() {
			return nil
		}

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = object.URL.Absolute()
			if strings.Index(dstFn, trimPrefix) == 0 {
				dstFn = dstFn[len(trimPrefix):]
			}
		} else {
			dstFn = object.URL.Base()
		}

		arg1 := NewJobArgument(object.URL)
		arg2 := dst.Clone().Join(dstFn)

		dir := filepath.Dir(arg2.url.Absolute())
		os.MkdirAll(dir, os.ModePerm)

		return job.MakeSubJob(subCmd, op.LocalCopy, []*JobArgument{arg1, arg2}, job.opts)
	})

	return opType, jobResponse(err)
}

func BatchLocalUpload(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.FileOp

	subCmd := "cp"
	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	src, dst := job.args[0], job.args[1]

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	// src.url could contain glob, or could be a directory.
	// err is not important here
	obj, err := client.Stat(wp.ctx, src.url)
	walkMode := err == nil && obj.Mode.IsDir()

	trimPrefix := src.url.Absolute()
	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return opType, jobResponse(fmt.Errorf("internal error, not a glob: %s", trimPrefix))
		}
		trimPrefix = trimPrefix[:loc]
	}
	trimPrefix = path.Dir(trimPrefix)
	if trimPrefix == "." {
		trimPrefix = ""
	} else {
		trimPrefix += string(filepath.Separator)
	}

	err = wildOperation(client, src.url, true, wp, func(object *storage.Object) *Job {
		if object.IsMarker() || object.Mode.IsDir() {
			return nil
		}

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = object.URL.Absolute()
			if strings.Index(dstFn, trimPrefix) == 0 {
				dstFn = dstFn[len(trimPrefix):]
			}
		} else {
			dstFn = object.URL.Base()
		}

		arg1 := NewJobArgument(object.URL)
		arg2 := dst.Clone().Join(dstFn)

		return job.MakeSubJob(subCmd, op.Upload, []*JobArgument{arg1, arg2}, job.opts)
	})

	return opType, jobResponse(err)
}
