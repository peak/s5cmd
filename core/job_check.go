package core

import (
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

// CheckConditions checks if the job satisfies the conditions if the job has -n, -s and -u flags.
// It returns error-embedded JobResponse with status "warning" if none of the requirements are met.
// It returns nil if any warning or error is encountered during this check.
func CheckConditions(src, dst *objurl.ObjectURL, wp *WorkerParams, opts opt.OptionList) *JobResponse {
	exists := opts.Has(opt.IfNotExists)
	sizeDiffers := opts.Has(opt.IfSizeDiffers)
	sourceNewer := opts.Has(opt.IfSourceNewer)

	// if has no flag, return nil
	if !exists && !sizeDiffers && !sourceNewer {
		return nil
	}

	srcObj, err := getObject(src, wp)
	if err != nil {
		return jobResponse(err)
	}

	dstObj, err := getObject(dst, wp)
	if err != nil {
		return jobResponse(err)
	}

	var res *JobResponse
	if exists {
		res = &JobResponse{status: statusWarning, err: ErrObjectExists}
	}

	if sizeDiffers && srcObj.Size == dstObj.Size {
		res = &JobResponse{status: statusWarning, err: ErrObjectSizesMatch}
	}

	srcMod, dstMod := srcObj.ModTime, dstObj.ModTime
	if sourceNewer && !srcMod.After(dstMod) {
		res = &JobResponse{status: statusWarning, err: ErrObjectIsNewer}
	}

	return res
}

// getObjects creates new storage client and sends stat request.
func getObject(url *objurl.ObjectURL, wp *WorkerParams) (*storage.Object, error) {
	client, err := wp.newClient(url)
	if err != nil {
		return nil, err
	}

	obj, err := client.Stat(wp.ctx, url)
	return obj, err
}
