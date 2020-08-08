package stat

import (
	"bytes"
	"fmt"
	"sync"
	"text/tabwriter"
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
			Mutex:       sync.Mutex{},
			mapStrInt64: map[string]int64{},
		}
	}
}

// syncMap is a statically typed and synchronized map.
type syncMap struct {
	sync.Mutex
	mapStrInt64 map[string]int64
}

func (s *syncMap) add(key string, val int64) {
	s.Lock()
	defer s.Unlock()

	s.mapStrInt64[key] += val
}

// Stat is for storing a particular statistics.
type Stat struct {
	Visited     string  `json:"visited"`
	SuccVisits  int64   `json:"success visits"`
	ErrVisits   int64   `json:"error visits"`
	AvgExecTime float64 `json:"avg. execution time"`
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

// Stats implements log.Message interface.
type Stats []Stat

func (s Stats) String() string {
	b := bytes.Buffer{}

	w := tabwriter.NewWriter(&b, 5, 0, 5, ' ', tabwriter.AlignRight)

	fmt.Fprintf(w, "\n%s\t%s\t%s\t%s\t%s\t\n", "Operation", "Total", "Error", "Success", "Duration(ms)")
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t\n", "=========", "=====", "=====", "=======", "============")
	for _, stat := range s {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%f\t\n", stat.Visited, stat.ErrVisits+stat.SuccVisits, stat.ErrVisits, stat.SuccVisits, stat.AvgExecTime)
	}

	w.Flush()
	return b.String()
}

func (s Stats) JSON() string {
	return strutil.JSON(s)
}

// Statistics will return statistics that has been collected so far.
func Statistics() Stats {
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
