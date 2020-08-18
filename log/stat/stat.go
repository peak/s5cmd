package stat

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/peak/s5cmd/strutil"
)

const (
	totalCount = iota
	succCount
)

var (
	enabled bool
	stats   statistics
)

type statistics [2]syncMapStrInt64

// InitStat initializes collecting program statistics.
func InitStat() {
	enabled = true
	for i := range stats {
		stats[i] = syncMapStrInt64{
			Mutex:       sync.Mutex{},
			mapStrInt64: map[string]int64{},
		}
	}
}

// syncMapStrInt64 is a statically typed and synchronized map.
type syncMapStrInt64 struct {
	sync.Mutex
	mapStrInt64 map[string]int64
}

func (s *syncMapStrInt64) add(key string, val int64) {
	s.Lock()
	defer s.Unlock()

	s.mapStrInt64[key] += val
}

// Stat is for storing a particular statistics.
type Stat struct {
	Operation string `json:"operation"`
	Success   int64  `json:"success"`
	Error     int64  `json:"error"`
}

// Collect collects function execution data.
func Collect(op string, err *error) func() {
	return func() {
		if !enabled {
			return
		}
		if err == nil || *err == nil {
			stats[succCount].add(op, 1)
		}
		stats[totalCount].add(op, 1)
	}
}

// Stats implements log.Message interface.
type Stats []Stat

func (s Stats) String() string {
	var buf bytes.Buffer

	w := tabwriter.NewWriter(&buf, 0, 8, 1, '\t', tabwriter.AlignRight)

	fmt.Fprintf(w, "\n%s\t%s\t%s\t%s\t\n", "Operation", "Total", "Error", "Success")
	for _, stat := range s {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t\n", stat.Operation, stat.Error+stat.Success, stat.Error, stat.Success)
	}

	w.Flush()
	return buf.String()
}

func (s Stats) JSON() string {
	var builder strings.Builder

	for _, stat := range s {
		builder.WriteString(strutil.JSON(stat) + "\n")
	}
	return builder.String()
}

// Statistics will return statistics that has been collected so far.
func Statistics() Stats {
	if !enabled {
		return Stats{}
	}

	var result Stats
	for op, total := range stats[totalCount].mapStrInt64 {
		success := stats[succCount].mapStrInt64[op]

		result = append(result, Stat{
			Operation: op,
			Success:   success,
			Error:     total - success,
		})
	}
	return result
}
