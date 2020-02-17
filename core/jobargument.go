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

var (
	// ErrObjectExistsButOk is used when a destination object already exists and opt.IfNotExists is set.
	ErrObjectExistsButOk = NewAcceptableError("Object already exists")
	// ErrObjectIsNewerButOk is used when a destination object is newer than the source and opt.IfSourceNewer is set.
	ErrObjectIsNewerButOk = NewAcceptableError("Object is newer or same age")
	// ErrObjectSizesMatchButOk is used when a destination object size matches the source and opt.IfSizeDiffers is set.
	ErrObjectSizesMatchButOk = NewAcceptableError("Object size matches")
	// ErrDisplayedHelp is used when a command is invoked with "-h"
	ErrDisplayedHelp = NewAcceptableError("Displayed help for command")
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

// Join joins a string to a JobArgument and returns itself.
func (a *JobArgument) Join(s string, isS3path bool) *JobArgument {
	joinfn := filepath.Join
	if a.url.IsRemote() {
		joinfn = path.Join
	}

	clone := a.Clone()
	clone.url.Path = joinfn(clone.url.Path, s)
	return clone
}

func CheckConditionals(src, dst *JobArgument, wp *WorkerParams, opts opt.OptionList) (ret error) {
	if opts.Has(opt.IfNotExists) {
		ex, err := dst.Exists(wp)
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
		sDest, err := dst.Size(wp)
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
		tDest, err := dst.ModTime(wp)
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

	if !a.url.IsRemote() {
		fpath := a.url.String()

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

	client, err := wp.newClient()
	if err != nil {
		return err
	}

	item, err := client.Head(wp.ctx, a.url)
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
