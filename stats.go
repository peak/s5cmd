package main

import "sync/atomic"

type StatType int

const (
	// Failed jobs
	STATS_FAIL StatType = iota

	// Successful S3 operations
	STATS_S3OP

	// Sucessful File operations
	STATS_FILEOP

	// Successful shell invocations
	STATS_SHELLOP

	// Retried operations
	STATS_RETRYOP
)

type Stats struct {
	ops [5]uint64
}

func (s *Stats) IncrementIfSuccess(t StatType, err error) error {
	if err == nil {
		atomic.AddUint64(&(s.ops[t]), 1)
	}
	return err
}

func (s *Stats) Increment(t StatType) {
	atomic.AddUint64(&(s.ops[t]), 1)
}

func (s *Stats) Get(t StatType) (value uint64) {
	value = atomic.LoadUint64(&(s.ops[t]))
	return
}
