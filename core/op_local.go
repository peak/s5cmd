package core

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/termie/go-shutil"
)

func LocalDelete(job *Job, wp *WorkerParams) error {
	return wp.st.IncrementIfSuccess(stats.FileOp, os.Remove(job.args[0].arg))
}

func LocalCopy(job *Job, wp *WorkerParams) error {
	var err error

	err = job.args[1].CheckConditionals(wp, job.args[0], job.opts)
	if err != nil {
		return err
	}

	if job.opts.Has(opt.DeleteSource) {
		err = os.Rename(job.args[0].arg, job.args[1].arg)
	} else {
		_, err = shutil.Copy(job.args[0].arg, job.args[1].arg, true)
	}

	wp.st.IncrementIfSuccess(stats.FileOp, err)
	return err
}

func BatchLocalCopy(job *Job, wp *WorkerParams) error {
	subCmd := "cp"
	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	st, err := os.Stat(job.args[0].arg)
	walkMode := err == nil && st.IsDir() // walk or glob?

	trimPrefix := job.args[0].arg
	globStart := job.args[0].arg
	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return fmt.Errorf("internal error, not a glob: %s", trimPrefix)
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

	err = wildOperation(wp, func(ch chan<- interface{}) error {
		defer func() {
			ch <- nil // send EOF
		}()

		// lister
		ma, err := filepath.Glob(globStart)
		if err != nil {
			return err
		}
		if len(ma) == 0 {
			if walkMode {
				return nil // Directory empty
			}

			return errors.New("could not find match for glob")
		}

		for _, f := range ma {
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
		// callback
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

		arg1 := NewJobArgument(*fn, nil)
		arg2 := job.args[1].Clone().Append(dstFn, false)

		dir := filepath.Dir(arg2.arg)
		os.MkdirAll(dir, os.ModePerm)

		return job.MakeSubJob(subCmd, op.LocalCopy, []*JobArgument{arg1, arg2}, job.opts)
	})

	return wp.st.IncrementIfSuccess(stats.FileOp, err)
}

func BatchUpload(job *Job, wp *WorkerParams) error {
	subCmd := "cp"
	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	st, err := os.Stat(job.args[0].arg)
	walkMode := err == nil && st.IsDir() // walk or glob?

	trimPrefix := job.args[0].arg
	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return fmt.Errorf("internal error, not a glob: %s", trimPrefix)
		}
		trimPrefix = trimPrefix[:loc]
	}
	trimPrefix = path.Dir(trimPrefix)
	if trimPrefix == "." {
		trimPrefix = ""
	} else {
		trimPrefix += string(filepath.Separator)
	}

	err = wildOperation(wp, func(ch chan<- interface{}) error {
		defer func() {
			ch <- nil // send EOF
		}()
		// lister
		if walkMode {
			err := filepath.Walk(job.args[0].arg, func(path string, st os.FileInfo, err error) error {
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
			ma, err := filepath.Glob(job.args[0].arg)
			if err != nil {
				return err
			}
			if len(ma) == 0 {
				return errors.New("could not find match for glob")
			}

			for _, f := range ma {
				s := f // copy
				st, _ = os.Stat(s)
				if !st.IsDir() {
					ch <- &s
				}
			}
			return nil
		}
	}, func(data interface{}) *Job {
		// callback
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

		arg1 := NewJobArgument(*fn, nil)
		arg2 := job.args[1].Clone().Append(dstFn, false)
		return job.MakeSubJob(subCmd, op.Upload, []*JobArgument{arg1, arg2}, job.opts)
	})

	return wp.st.IncrementIfSuccess(stats.FileOp, err)
}
