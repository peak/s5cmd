package core

import (
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
)

// JobArgument is an argument of the job. Can be a file/directory, an s3 url
// ("s3" is set in this case) or an arbitrary string.
type JobArgument struct {
	url *objurl.ObjectURL

	filled  bool
	exists  bool
	size    int64
	modTime time.Time
}

func NewJobArgument(url *objurl.ObjectURL) *JobArgument {
	return &JobArgument{url: url}
}

// Clone duplicates a JobArgument and returns a pointer to a new one.
func (a *JobArgument) Clone() *JobArgument {
	return NewJobArgument(a.url.Clone())
}

// Join appends the given s to the JobArgument.
func (a *JobArgument) Join(s string) *JobArgument {
	joinfn := filepath.Join
	if a.url.IsRemote() {
		joinfn = path.Join
	}

	clone := a.Clone()
	clone.url.Path = joinfn(clone.url.Path, s)
	return clone
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

	if !a.url.IsRemote() {
		fpath := a.url.Absolute()

		st, err := os.Stat(fpath)
		if err == nil {
			a.filled = true
			a.exists = true
			a.size = st.Size()
			a.modTime = st.ModTime()
			return nil
		}

		if os.IsNotExist(err) {
			a.filled = true
			a.exists = false
			return nil
		}

		if err != nil {
			return err
		}
	}

	client, err := wp.newClient(a.url)
	if err != nil {
		return err
	}

	object, err := client.Stat(wp.ctx, a.url)
	wp.st.IncrementIfSuccess(stats.S3Op, err)

	if err != nil {
		a.filled = true
		a.exists = false
		return nil
	}

	a.filled = true
	a.exists = true
	a.modTime = object.ModTime
	a.size = object.Size
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
