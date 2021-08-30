package strategy

import (
	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/storage"
)

// SizeOnly strategy is used when --size-only flag is set.
// It only checks the sizes of objects.
type SizeOnly struct{}

// Compare compares sizes of objects.
func (s *SizeOnly) Compare(srcObj, dstObj *storage.Object) error {
	if srcObj.Size == dstObj.Size {
		return errorpkg.ErrObjectSizesMatch
	}
	return nil
}

// SizeAndModification strategy checks the sizes
// and modification time of the objects.
type SizeAndModification struct{}

// Compare compares size and mod times of objects.
func (sm *SizeAndModification) Compare(srcObj, dstObj *storage.Object) error {
	var stickyErr = errorpkg.ErrObjectSizesMatch
	// check size of objects
	if srcObj.Size != dstObj.Size {
		stickyErr = nil
	}

	srcMod, dstMod := srcObj.ModTime, dstObj.ModTime
	if !srcMod.After(*dstMod) {
		stickyErr = errorpkg.ErrObjectIsNewer
	} else {
		stickyErr = nil
	}

	return stickyErr
}
