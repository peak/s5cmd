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

// CheckConditions checks if the job satisfies the conditions if the job has -n, -s and -u flags.
// It returns error-embedded JobResponse with status "warning" if none of the requirements are met.
// It returns nil if any warning or error is encountered during this check.
func CheckConditions(src, dst *JobArgument, wp *WorkerParams, opts opt.OptionList) *JobResponse {
	var res *JobResponse

	if opts.Has(opt.IfNotExists) {
		ex, err := dst.Exists(wp)
		if err != nil {
			return jobResponse(err)
		}
		if ex {
			res = &JobResponse{status: statusWarning, err: ErrObjectExists}
		} else {
			res = nil
		}
	}

	if opts.Has(opt.IfSizeDiffers) {
		sDest, err := dst.Size(wp)
		if err != nil {
			return jobResponse(err)
		}

		sSrc, err := src.Size(wp)
		if err != nil {
			return jobResponse(err)
		}

		if sDest == sSrc {
			res = &JobResponse{status: statusWarning, err: ErrObjectSizesMatch}
		} else {
			res = nil
		}
	}

	if opts.Has(opt.IfSourceNewer) {
		tDest, err := dst.ModTime(wp)
		if err != nil {
			return jobResponse(err)
		}

		tSrc, err := src.ModTime(wp)
		if err != nil {
			return jobResponse(err)
		}

		if !tSrc.After(tDest) {
			res = &JobResponse{status: statusWarning, err: ErrObjectIsNewer}
		} else {
			res = nil
		}
	}

	return res
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
