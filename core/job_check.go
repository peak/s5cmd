package core

import (
	"context"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

// CheckConditions checks if the job satisfies the conditions if the job has -n, -s and -u flags.
// It returns error-embedded JobResponse with status "warning" if none of the requirements are met.
// It returns nil if any warning or error is encountered during this check.
func CheckConditions(ctx context.Context, src, dst *objurl.ObjectURL, opts opt.OptionList) *JobResponse {
	condIsExist := opts.Has(opt.IfNotExists)
	condSizeDiffers := opts.Has(opt.IfSizeDiffers)
	condSourceNewer := opts.Has(opt.IfSourceNewer)

	// if has no flag, return nil
	if !condIsExist && !condSizeDiffers && !condSourceNewer {
		return nil
	}

	srcObj, err := getObject(ctx, src)
	if err != nil {
		return jobResponse(err)
	}

	dstObj, err := getObject(ctx, dst)
	if err != nil {
		return jobResponse(err)
	}

	// if destination is not exists, no conditions apply.
	if dstObj == nil {
		return nil
	}

	var res *JobResponse
	if condIsExist {
		res = &JobResponse{status: statusWarning, err: ErrObjectExists}
	}

	if condSizeDiffers {
		if srcObj.Size == dstObj.Size {
			res = &JobResponse{status: statusWarning, err: ErrObjectSizesMatch}
		} else {
			res = nil
		}
	}

	if condSourceNewer {
		srcMod, dstMod := srcObj.ModTime, dstObj.ModTime

		if !srcMod.After(dstMod) {
			res = &JobResponse{status: statusWarning, err: ErrObjectIsNewer}
		} else {
			res = nil
		}
	}

	return res
}

// getObject checks if the object from given url exists. If no object is
// found, error and returning object would be nil.
func getObject(ctx context.Context, url *objurl.ObjectURL) (*storage.Object, error) {
	client, err := storage.NewClient(url)
	if err != nil {
		return nil, err
	}

	obj, err := client.Stat(ctx, url)
	if err == storage.ErrGivenObjectNotFound {
		return nil, nil
	}

	return obj, err
}
