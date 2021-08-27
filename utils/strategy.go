package utils

import "github.com/peak/s5cmd/storage"

type Strategy interface {
	Compare(srcObject, dstObject *storage.Object) error
}
