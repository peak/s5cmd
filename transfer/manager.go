package transfer

import (
	"github.com/peak/s5cmd/storage"
)

type Manager struct {
	storageOptions storage.Options
	flatten        bool
	isBatch        bool
	concurrency    int
	partSize       int64
}

func NewManager(options storage.Options, flatten bool, isBatch bool, concurrency int, partSize int64) *Manager {
	return &Manager{
		storageOptions: options,
		flatten:        flatten,
		isBatch:        isBatch,
		concurrency:    concurrency,
		partSize:       partSize,
	}
}
