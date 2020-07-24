package statutil

import (
	"fmt"
	"sync"
	"time"
)

const (
	totalCount = iota
	succCount
	timeInfo
)

var (
	enabled bool
	stats   [3]Stat
)

// InitStat initializes collecting program statistics.
func InitStat() {
	enabled = true
	for i := range stats {
		stats[i] = Stat{
			mu:          sync.Mutex{},
			mapStrInt64: map[string]int64{},
		}
	}
}

// Stat is a statically typed and synchronized map.
type Stat struct {
	mu          sync.Mutex
	mapStrInt64 map[string]int64
}

func (s *Stat) putStat(statName string, val int64) {
	s.mu.Lock()
	s.mapStrInt64[statName] += val
	s.mu.Unlock()
}

func statSuccCount(path string) {
	stats[succCount].putStat(path, 1)
}

// StatCollect collects function execution data.
func StatCollect(path string, t time.Time, err *error) func() {
	return func() {
		if !enabled {
			return
		}
		if err == nil || *err == nil {
			statSuccCount(path)
		}
		stats[totalCount].putStat(path, 1)
		stats[timeInfo].putStat(path, time.Since(t).Milliseconds())
	}
}

// StatDisplay will output statistics that has been collected so far.
func StatDisplay() {
	if !enabled {
		return
	}
	for path, cnt := range stats[totalCount].mapStrInt64 {
		cntSucc := stats[succCount].mapStrInt64[path]
		cntErr := cnt - cntSucc
		avgTime := float64(stats[timeInfo].mapStrInt64[path]) / float64(cnt)
		fmt.Printf("=> visited %q %d time with %d ERROR and %d SUCCESS returns; average execution time: %f msec.\n",
			path, cnt, cntErr, cntSucc, avgTime)
	}
}
