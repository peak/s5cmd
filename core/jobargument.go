package core

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/s3url"
	"github.com/peak/s5cmd/stats"
)

var (
	// ErrObjectExistsButOk is used when a destination object already exists and opt.IfNotExists is set.
	ErrObjectExistsButOk = NewAcceptableError("Object already exists")
	// ErrObjectIsNewerButOk is used when a destination object is newer than the source and opt.IfSourceNewer is set.
	ErrObjectIsNewerButOk = NewAcceptableError("Object is newer or same age")
	// ErrObjectSizesMatchButOk is used when a destination object size matches the source and opt.IfSizeDiffers is set.
	ErrObjectSizesMatchButOk = NewAcceptableError("Object size matches")
)

// JobArgument is an argument of the job. Can be a file/directory, an s3 url ("s3" is set in this case) or an arbitrary string.
type JobArgument struct {
	arg string
	s3  *s3url.S3Url

	filled  bool
	exists  bool
	size    int64
	modTime time.Time
}

func NewJobArgument(arg string, s3 *s3url.S3Url) *JobArgument {
	return &JobArgument{arg: arg, s3: s3}
}

// Clone duplicates a JobArgument and returns a pointer to a new one
func (a *JobArgument) Clone() *JobArgument {
	var s s3url.S3Url
	if a.s3 != nil {
		s = a.s3.Clone()
	}
	return NewJobArgument(a.arg, &s)
}

// StripS3 strips the S3 data from JobArgument and returns a new one
func (a *JobArgument) StripS3() *JobArgument {
	return NewJobArgument(a.arg, nil)
}

// Append appends a string to a JobArgument and returns itself
func (a *JobArgument) Append(s string, isS3path bool) *JobArgument {
	if a.s3 != nil && !isS3path {
		// a is an S3 object but s is not
		s = strings.Replace(s, string(filepath.Separator), "/", -1)
	}
	if a.s3 == nil && isS3path {
		// a is a not an S3 object but s is
		s = strings.Replace(s, "/", string(filepath.Separator), -1)
	}

	if a.s3 != nil {
		if a.s3.Key == "" {
			a.arg += "/" + s
		} else {
			a.arg += s
		}

		a.s3.Key += s
	} else {
		a.arg += s
	}

	return a
}

func (a *JobArgument) CheckConditionals(wp *WorkerParams, src *JobArgument, opts opt.OptionList) (ret error) {
	if opts.Has(opt.IfNotExists) {
		ex, err := a.Exists(wp)
		if err != nil {
			return err
		}
		if ex {
			ret = ErrObjectExistsButOk
		} else {
			ret = nil
		}
	}

	if opts.Has(opt.IfSizeDiffers) {
		sDest, err := a.Size(wp)
		if err != nil {
			return err
		}

		sSrc, err := src.Size(wp)
		if err != nil {
			return err
		}

		if sDest == sSrc {
			ret = ErrObjectSizesMatchButOk
		} else {
			ret = nil
		}
	}

	if opts.Has(opt.IfSourceNewer) {
		tDest, err := a.ModTime(wp)
		if err != nil {
			return err
		}

		tSrc, err := src.ModTime(wp)
		if err != nil {
			return err
		}

		if !tSrc.After(tDest) {
			ret = ErrObjectIsNewerButOk
		} else {
			ret = nil
		}
	}

	return ret
}

func (a *JobArgument) fillData(wp *WorkerParams) error {
	if a.filled {
		return nil
	}

	if a.s3 == nil {
		st, err := os.Stat(a.arg)
		if err != nil {
			if os.IsNotExist(err) {
				a.filled = true
				a.exists = false
				return nil
			}
			// error
			return err
		} else {
			a.filled = true
			a.exists = true
			a.size = st.Size()
			a.modTime = st.ModTime()
			return nil
		}

	}

	client, err := wp.newClient()
	if err != nil {
		return err
	}

	item, err := client.Head(wp.ctx, a.s3)
	wp.st.IncrementIfSuccess(stats.S3Op, err)

	if err != nil {
		a.filled = true
		a.exists = false
		return nil
	}

	a.filled = true
	a.exists = true
	a.modTime = item.LastModified
	a.size = item.Size
	return nil
}

func (a *JobArgument) Size(wp *WorkerParams) (int64, error) {
	err := a.fillData(wp)
	return a.size, err
}

func (a *JobArgument) Exists(wp *WorkerParams) (bool, error) {
	err := a.fillData(wp)
	return a.exists, err
}

func (a *JobArgument) ModTime(wp *WorkerParams) (time.Time, error) {
	err := a.fillData(wp)
	return a.modTime, err
}
