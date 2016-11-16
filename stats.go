package s5cmd

import "sync/atomic"

type StatType int

const (
	STATS_FAIL StatType = iota
	STATS_S3OP
	STATS_FILEOP
	STATS_SHELLOP
)

type Stats struct {
	ops [4]uint64
}

func (s *Stats) CountOp(t StatType, err error) error {
	if err == nil {
		atomic.AddUint64(&(s.ops[t]), 1)
	} else {
		atomic.AddUint64(&(s.ops[STATS_FAIL]), 1)
	}
	return err
}

func (s *Stats) Get(t StatType) (value uint64) {
	value = atomic.LoadUint64(&(s.ops[t]))
	return
}
