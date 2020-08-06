package stat

import (
	"fmt"
	"sync"
	"time"

	"github.com/peak/s5cmd/strutil"
)

const (
	totalCount = iota
	succCount
	timeInfo
)

var (
	enabled bool
	stats   statistics
)

type statistics [3]syncMap

// InitStat initializes collecting program statistics.
func InitStat() {
	enabled = true
	for i := range stats {
		stats[i] = syncMap{
			mu:          sync.Mutex{},
			mapStrInt64: map[string]int64{},
		}
	}
}

// syncMap is a statically typed and synchronized map.
type syncMap struct {
	mu          sync.Mutex
	mapStrInt64 map[string]int64
}

func (s *syncMap) add(key string, val int64) {
	s.mu.Lock()
	s.mapStrInt64[key] += val
	s.mu.Unlock()
}

// Stat implements log.Message interface.
type Stat struct {
	Visited     string  `json:"visited"`
	SuccVisits  int64   `json:"success visits"`
	ErrVisits   int64   `json:"error visits"`
	AvgExecTime float64 `json:"avg. execution time"`
}

func (s Stat) String() string {
	return fmt.Sprintf("visited %q %d time with %d ERROR and %d SUCCESS returns; average execution time: %f msec.",
		s.Visited, s.ErrVisits+s.SuccVisits, s.ErrVisits, s.SuccVisits, s.AvgExecTime)
}

func (s Stat) JSON() string {
	return strutil.JSON(s)
}

// Collect collects function execution data.
func Collect(path string, t time.Time, err *error) func() {
	return func() {
		if !enabled {
			return
		}
		if err == nil || *err == nil {
			stats[succCount].add(path, 1)
		}
		stats[totalCount].add(path, 1)
		stats[timeInfo].add(path, time.Since(t).Milliseconds())
	}
}

// Statistics will return statistics that has been collected so far.
func Statistics() []Stat {
	if !enabled {
		return make([]Stat, 0)
	}

	result := make([]Stat, 0)
	for path, cnt := range stats[totalCount].mapStrInt64 {
		cntSucc := stats[succCount].mapStrInt64[path]
		avgTime := float64(stats[timeInfo].mapStrInt64[path]) / float64(cnt)

		result = append(result, Stat{
			Visited:     path,
			SuccVisits:  cntSucc,
			ErrVisits:   cnt - cntSucc,
			AvgExecTime: avgTime,
		})
	}
	return result
}
