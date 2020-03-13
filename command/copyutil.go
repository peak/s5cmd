package command

import (
	"context"
	"fmt"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
)

// shouldOverride is a closure to check if the destination should be
// overriden if the source-destination pair and given copy flags conform to the
// override criteria. For example; "cp -n -s <src> <dst>" should not override
// the <dst> if <src> and <dst> filenames are the same, except if the size
// differs.
func shouldOverride(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
) error {
	// if not asked to override, ignore.
	if !noClobber && !ifSizeDiffer && !ifSourceNewer {
		return nil
	}

	srcObj, err := getObject(ctx, src)
	if err != nil {
		return err
	}

	dstObj, err := getObject(ctx, dst)
	if err != nil {
		return err
	}

	// if destination not exists, no conditions apply.
	if dstObj == nil {
		return nil
	}

	var stickyErr error
	if noClobber {
		stickyErr = ErrObjectExists
	}

	if ifSizeDiffer {
		if srcObj.Size == dstObj.Size {
			stickyErr = ErrObjectSizesMatch
		} else {
			stickyErr = nil
		}
	}

	if ifSourceNewer {
		srcMod, dstMod := srcObj.ModTime, dstObj.ModTime

		if !srcMod.After(*dstMod) {
			stickyErr = ErrObjectIsNewer
		} else {
			stickyErr = nil
		}
	}

	return stickyErr
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

//  OK-to-have error types (warnings) that is used when the job status is warning.
var (
	ErrObjectExists     = fmt.Errorf("object already exists")
	ErrObjectIsNewer    = fmt.Errorf("object is newer or same age")
	ErrObjectSizesMatch = fmt.Errorf("object size matches")
)

func isWarning(err error) bool {
	switch err {
	case ErrObjectExists, ErrObjectIsNewer, ErrObjectSizesMatch:
		return true
	}

	return false
}
