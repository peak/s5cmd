package main

import "sync/atomic"

// StatType is an enum for our various types of stats.
type StatType int

const (
	// StatsFail is failed jobs
	StatsFail StatType = iota

	// StatsS3Op is successful S3 operations
	StatsS3Op

	// StatsFileOp is sucessful File operations
	StatsFileOp

	// StatsShellOp is successful shell invocations
	StatsShellOp

	// StatsRetryOp is retried operations
	StatsRetryOp
)

// Stats contain the number of operations of each StatType
type Stats struct {
	ops [5]uint64
}

// IncrementIfSuccess atomically increments the StatType's counter in Stats if err is nil
func (s *Stats) IncrementIfSuccess(t StatType, err error) error {
	if err == nil {
		atomic.AddUint64(&(s.ops[t]), 1)
	}
	return err
}

// Increment atomically increments the StatType's counter
func (s *Stats) Increment(t StatType) {
	atomic.AddUint64(&(s.ops[t]), 1)
}

// Get atomically reads the StatType's number of operations value
func (s *Stats) Get(t StatType) (value uint64) {
	value = atomic.LoadUint64(&(s.ops[t]))
	return
}
