package command

import (
	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/storage"
)

// SyncStrategy is the interface to compare whether given objects should be considered as same
// within 'sync' command.
type SyncStrategy interface {
	Compare(srcObject, dstObject *storage.Object) error
}

func NewStrategy(sizeOnly bool) SyncStrategy {
	if sizeOnly {
		return &SizeOnlyStrategy{}
	} else {
		return &SizeAndModificationStrategy{}
	}
}

// SizeOnlyStrategy only compares given objects' sizes.
type SizeOnlyStrategy struct{}

func (s *SizeOnlyStrategy) Compare(srcObj, dstObj *storage.Object) error {
	if srcObj.Size == dstObj.Size {
		return errorpkg.ErrObjectSizesMatch
	}
	return nil
}

// SizeAndModificationStrategy compares given objects' both sizes and modification times.
// It treats 'srcObj' as the source of truth; if 'dstObj' is newer than 'srcObj' or has the same exact size with it,
// returns a sentinel error to indicate not to 'sync'. Even if 'srcObj' older than 'dstObj' it returns without error
// if file sizes would not match.
//
// time: src > dst        size: src != dst    should sync: yes
// time: src > dst        size: src == dst    should sync: yes
// time: src <= dst       size: src != dst    should sync: yes
// time: src <= dst       size: src == dst    should sync: no
type SizeAndModificationStrategy struct{}

func (sm *SizeAndModificationStrategy) Compare(srcObj, dstObj *storage.Object) error {
	srcMod, dstMod := srcObj.ModTime, dstObj.ModTime
	if srcMod.After(*dstMod) {
		return nil
	}

	if srcObj.Size != dstObj.Size {
		return nil
	}

	return errorpkg.ErrObjectIsNewerAndSizesMatch
}
