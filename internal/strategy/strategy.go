package strategy

import "github.com/peak/s5cmd/storage"

// Strategy defines the interface for the comparison method.
type Strategy interface {
	Compare(srcObject, dstObject *storage.Object) error
}

// New returns a new comparison strategy.
func New(sizeOnly bool) Strategy {
	if sizeOnly {
		return &SizeOnly{}
	} else {
		return &SizeAndModification{}
	}
}
