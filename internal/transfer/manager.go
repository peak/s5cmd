// Package transfer handles all of transfer operations for sync
package transfer

import (
	"github.com/peak/s5cmd/storage"
)

// Manager handles all of the transfer operations.
type Manager struct {
	storageOptions storage.Options
	flatten        bool
	isBatch        bool
	concurrency    int
	partSize       int64
}

// NewManager returns a new manager.
func NewManager(options storage.Options, flatten bool, isBatch bool, concurrency int, partSize int64) *Manager {
	return &Manager{
		storageOptions: options,
		flatten:        flatten,
		isBatch:        isBatch,
		concurrency:    concurrency,
		partSize:       partSize,
	}
}
