// Package stats provides atomic counters for operations.
package stats

import (
	"sync/atomic"
	"time"
)

var global = &stats{}

// StatType is an enum for our various types of stats.
type StatType int

const (
	// Fail is failed jobs
	Fail StatType = iota

	// S3Op is successful S3 operations
	S3Op

	// FileOp is successful File operations
	FileOp

	// ShellOp is successful shell invocations
	ShellOp
)

// Stats contain the number of operations of each StatType.
type stats struct {
	ops       [4]uint64
	startedAt time.Time
}

// Increment atomically increments the StatType's counter.
func Increment(t StatType) {
	atomic.AddUint64(&(global.ops[t]), 1)
}

// Get atomically reads the StatType's number of operations value.
func Get(t StatType) uint64 {
	return atomic.LoadUint64(&(global.ops[t]))
}

func StartTimer() {
	global.startedAt = time.Now().UTC()
}

func Elapsed() time.Duration {
	return time.Since(global.startedAt)
}
