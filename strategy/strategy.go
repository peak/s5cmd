package strategy

import "github.com/peak/s5cmd/storage"

type Strategy interface {
	Compare(srcObject, dstObject *storage.Object) error
}

func New(sizeOnly bool) Strategy {
	if sizeOnly {
		return &SizeOnly{}
	} else {
		return &SizeAndModification{}
	}
}
