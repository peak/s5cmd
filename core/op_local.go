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
	"github.com/termie/go-shutil"
)

func LocalCopy(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.FileOp

	src, dst := job.args[0], job.args[1]

	err := CheckConditionals(src, dst, wp, job.opts)
	if err != nil {
		return opType, err
	}

	srcpath := src.url.String()
	dstpath := dst.url.String()

	if job.opts.Has(opt.DeleteSource) {
		err = os.Rename(srcpath, dstpath)
	} else {
		_, err = shutil.Copy(srcpath, dstpath, true)
	}

	return opType, err
}

func LocalDelete(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.FileOp

	srcpath := job.args[0].url.String()
	return opType, os.Remove(srcpath)
}

func BatchLocalCopy(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.FileOp

	subCmd := "cp"
	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	src, dst := job.args[0], job.args[1]

	st, err := os.Stat(src.url.String())
	walkMode := err == nil && st.IsDir() // walk or glob?

	trimPrefix := src.url.String()
	globStart := src.url.String()

	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return opType, fmt.Errorf("internal error, not a glob: %s", trimPrefix)
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
		arg2 := dst.Clone().Join(dstFn, false)

		dir := filepath.Dir(arg2.url.String())
		os.MkdirAll(dir, os.ModePerm)

		return job.MakeSubJob(subCmd, op.LocalCopy, []*JobArgument{arg1, arg2}, job.opts)
	})

	return opType, err
}

func BatchLocalUpload(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.FileOp

	subCmd := "cp"
	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	src, dst := job.args[0], job.args[1]

	st, err := os.Stat(src.url.String())
	walkMode := err == nil && st.IsDir() // walk or glob?

	trimPrefix := src.url.String()
	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return opType, fmt.Errorf("internal error, not a glob: %s", trimPrefix)
		}
		trimPrefix = trimPrefix[:loc]
	}
	trimPrefix = path.Dir(trimPrefix)
	if trimPrefix == "." {
		trimPrefix = ""
	} else {
		trimPrefix += string(filepath.Separator)
	}

	err = wildOperationLocal(wp, func(ch chan<- interface{}) error {
		defer func() {
			ch <- nil // send EOF
		}()
		if walkMode {
			err := filepath.Walk(src.url.String(), func(path string, st os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if st.IsDir() {
					return nil
				}
				ch <- &path
				return nil
			})
			return err
		} else {
			matchedFiles, err := filepath.Glob(src.url.String())
			if err != nil {
				return err
			}
			if len(matchedFiles) == 0 {
				return errors.New("could not find match for glob")
			}

			for _, f := range matchedFiles {
				s := f // copy
				st, _ = os.Stat(s)
				if !st.IsDir() {
					ch <- &s
				}
			}
			return nil
		}
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
		arg2 := dst.Clone().Join(dstFn, false)

		return job.MakeSubJob(subCmd, op.Upload, []*JobArgument{arg1, arg2}, job.opts)
	})

	return opType, err
}
