// Package stats provides atomic counters for operations.
package stats

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/peak/s5cmd/flags"
	"github.com/peak/s5cmd/log"
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
)

// Stats contain the number of operations of each StatType.
type stats struct {
	ops       [3]uint64
	startedAt time.Time
}

// formatStats creates a stats string.
func formatStats(name string, counter uint64, elapsed time.Duration, extra string) string {
	secs := elapsed.Seconds()
	if secs == 0 {
		secs = 1
	}

	ops := uint64(math.Floor((float64(counter) / secs) + 0.5))
	return fmt.Sprintf("# Stats: %-7s %10d %4d ops/sec%s", name, counter, ops, extra)
}

// String returns the string representation of stats.
func (s stats) String() string {
	elapsed := elapsed()

	s3ops := get(S3Op)
	fileops := get(FileOp)
	failops := get(Fail)
	total := s3ops + fileops + failops
	extra := fmt.Sprintf(" %v", elapsed)

	var output []string
	output = appendIfExists(output, "S3", s3ops, elapsed, "")
	output = appendIfExists(output, "File", fileops, elapsed, "")
	output = appendIfExists(output, "Failed", failops, elapsed, "")
	output = appendIfExists(output, "Total", total, elapsed, extra)
	return strings.Join(output, "\n")
}

// appendIfExists appends formatted output if the counter is bigger than 0.
func appendIfExists(output []string, opName string, count uint64, elapsed time.Duration, extra string) []string {
	if count == 0 {
		return output
	}
	output = append(output, formatStats(opName, count, elapsed, extra))
	return output
}

// JSON returns the JSON representation of stats.
func (s stats) JSON() string {
	var statsJson struct {
		Op      string `json:"type"`
		Success struct {
			S3   uint64 `json:"s3"`
			File uint64 `json:"file"`
		} `json:"success"`
		Fail        uint64        `json:"fail_count"`
		ElapsedTime time.Duration `json:"elapsed_time"`
		TimeUnit    string        `json:"time_unit"`
	}

	statsJson.Op = "stats"
	statsJson.Success.File = get(FileOp)
	statsJson.Success.S3 = get(S3Op)
	statsJson.ElapsedTime = elapsed()
	statsJson.TimeUnit = "nanosecond"

	r, _ := json.Marshal(statsJson)
	return string(r)
}

// Increment atomically increments the StatType's counter.
func Increment(t StatType) {
	atomic.AddUint64(&global.ops[t], 1)
}

// Get atomically reads the StatType's number of operations value.
func get(t StatType) uint64 {
	return atomic.LoadUint64(&global.ops[t])
}

// StartTimer starts timer for global stats.
func StartTimer() {
	global.startedAt = time.Now().UTC()
}

// elapsed returns the elapsed time for stats.
func elapsed() time.Duration {
	return time.Since(global.startedAt)
}

// HasFailed checks if any failed operation exist.
func HasFailed() bool {
	failops := get(Fail)
	return failops > 0
}

// Print sends stats message to the logger.
func Print(force bool) {
	if force || *flags.PrintStats {
		log.Logger.Info(global)
	}
}
