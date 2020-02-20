package core

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/objurl"
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
	walkMode := err == nil && obj.Type.IsDir()

	trimPrefix := src.url.Absolute()
	globStart := src.url.Absolute()

	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return opType, jobResponse(fmt.Errorf("internal error, not a glob: %s", trimPrefix))
		}
		trimPrefix = trimPrefix[:loc]
	} else {
		if !strings.HasSuffix(globStart, string(filepath.Separator)) {
			globStart += string(filepath.Separator)
		}
		globStart = globStart + "*"
	}
	trimPrefix = path.Dir(trimPrefix)
	if trimPrefix == "." {
		trimPrefix = ""
	} else {
		trimPrefix += string(filepath.Separator)
	}

	recurse := job.opts.Has(opt.Recursive)

	err = wildOperationLocal(wp, func(ch chan<- interface{}) error {
		defer func() {
			ch <- nil // send EOF
		}()

		matchedFiles, err := filepath.Glob(globStart)
		if err != nil {
			return err
		}
		if len(matchedFiles) == 0 {
			if walkMode {
				return nil // Directory empty
			}

			return errors.New("could not find match for glob")
		}

		for _, f := range matchedFiles {
			s := f // copy
			st, _ := os.Stat(s)
			if !st.IsDir() {
				ch <- &s
			} else if recurse {
				err = filepath.Walk(s, func(path string, st os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if st.IsDir() {
						return nil
					}
					ch <- &path
					return nil
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, func(data interface{}) *Job {
		if data == nil {
			return nil
		}
		fn := data.(*string)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = *fn
			if strings.Index(dstFn, trimPrefix) == 0 {
				dstFn = dstFn[len(trimPrefix):]
			}
		} else {
			dstFn = filepath.Base(*fn)
		}

		url, _ := objurl.New(*fn)
		arg1 := NewJobArgument(url)
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
	walkMode := err == nil && obj.Type.IsDir()

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

	// fmt.Println("*** trimprefix:", trimPrefix)
	// fmt.Println("*** walkmode:", walkMode)

	err = wildOperation(client, src.url, true, wp, func(item *storage.Object) *Job {
		if item.IsMarkerObject() || item.Type.IsDir() {
			return nil
		}

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = item.URL.Absolute()
			if strings.Index(dstFn, trimPrefix) == 0 {
				dstFn = dstFn[len(trimPrefix):]
			}
		} else {
			dstFn = item.URL.Base()
		}

		arg1 := NewJobArgument(item.URL)
		arg2 := dst.Clone().Join(dstFn)

		// fmt.Println("*** arg1", arg1.url)
		// fmt.Println("*** arg2", arg2.url)

		return job.MakeSubJob(subCmd, op.Upload, []*JobArgument{arg1, arg2}, job.opts)
	})

	return opType, jobResponse(err)
}
