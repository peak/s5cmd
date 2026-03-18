package progressbar

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cheggaaa/pb/v3"
)

type ProgressBar interface {
	Start()
	Finish()
	IncrementCompletedObjects()
	IncrementTotalObjects()
	AddCompletedBytes(bytes int64)
	AddTotalBytes(bytes int64)
}

type NoOp struct{}

func (pb *NoOp) Start() {}

func (pb *NoOp) Finish() {}

func (pb *NoOp) IncrementCompletedObjects() {}

func (pb *NoOp) IncrementTotalObjects() {}

func (pb *NoOp) AddCompletedBytes(bytes int64) {}

func (pb *NoOp) AddTotalBytes(bytes int64) {}

type CommandProgressBar struct {
	totalObjects     int64
	completedObjects int64
	progressbar      *pb.ProgressBar
}

var _ ProgressBar = (*CommandProgressBar)(nil)

const progressbarTemplate = `{{percent . | green}} {{bar . " " "━" "━" "─" " " | green}} {{counters . | green}} {{speed . "(%s/s)" | red}} {{rtime . "%s left" | blue}} {{ string . "objects" | yellow}}`

func New() *CommandProgressBar {
	return &CommandProgressBar{
		progressbar: pb.New64(0).
			Set(pb.Bytes, true).
			Set(pb.SIBytesPrefix, true).
			SetWidth(128).
			Set("objects", fmt.Sprintf("(%d/%d)", 0, 0)).
			SetTemplateString(progressbarTemplate),
	}
}

func (cp *CommandProgressBar) Start() {
	cp.progressbar.Start()
}

func (cp *CommandProgressBar) Finish() {
	cp.progressbar.Finish()
}

func (cp *CommandProgressBar) IncrementCompletedObjects() {
	completed := atomic.AddInt64(&cp.completedObjects, 1)
	total := atomic.LoadInt64(&cp.totalObjects)
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", completed, total))
}

func (cp *CommandProgressBar) IncrementTotalObjects() {
	total := atomic.AddInt64(&cp.totalObjects, 1)
	completed := atomic.LoadInt64(&cp.completedObjects)
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", completed, total))
}

func (cp *CommandProgressBar) AddCompletedBytes(bytes int64) {
	cp.progressbar.Add64(bytes)
}

func (cp *CommandProgressBar) AddTotalBytes(bytes int64) {
	cp.progressbar.AddTotal(bytes)
}

// LogProgressBar outputs progress to stderr in a log-friendly format
// (newline-separated, no ANSI codes, works in pipes and non-TTY environments).
type LogProgressBar struct {
	totalObjects     int64
	completedObjects int64
	totalBytes       int64
	completedBytes   int64
	startTime        time.Time
	mu               sync.Mutex
	lastUpdate       time.Time
	updateInterval   time.Duration
}

var _ ProgressBar = (*LogProgressBar)(nil)

func NewLogProgressBar() *LogProgressBar {
	return &LogProgressBar{
		updateInterval: 2 * time.Second,
	}
}

func (lp *LogProgressBar) Start() {
	lp.startTime = time.Now()
	lp.lastUpdate = lp.startTime
}

func (lp *LogProgressBar) Finish() {
	lp.print()
	fmt.Fprintln(os.Stderr, "Transfer complete")
}

func (lp *LogProgressBar) IncrementCompletedObjects() {
	atomic.AddInt64(&lp.completedObjects, 1)
	lp.maybeUpdate()
}

func (lp *LogProgressBar) IncrementTotalObjects() {
	atomic.AddInt64(&lp.totalObjects, 1)
}

func (lp *LogProgressBar) AddCompletedBytes(bytes int64) {
	atomic.AddInt64(&lp.completedBytes, bytes)
	lp.maybeUpdate()
}

func (lp *LogProgressBar) AddTotalBytes(bytes int64) {
	atomic.AddInt64(&lp.totalBytes, bytes)
}

func (lp *LogProgressBar) maybeUpdate() {
	now := time.Now()
	lp.mu.Lock()
	if now.Sub(lp.lastUpdate) >= lp.updateInterval {
		lp.lastUpdate = now
		lp.mu.Unlock()
		lp.print()
		return
	}
	lp.mu.Unlock()
}

func (lp *LogProgressBar) print() {
	completed := atomic.LoadInt64(&lp.completedBytes)
	total := atomic.LoadInt64(&lp.totalBytes)
	completedObjs := atomic.LoadInt64(&lp.completedObjects)
	totalObjs := atomic.LoadInt64(&lp.totalObjects)

	if total == 0 {
		return
	}

	percent := float64(completed) / float64(total) * 100
	elapsed := time.Since(lp.startTime).Seconds()
	var speed float64
	var eta string

	if elapsed > 0 {
		speed = float64(completed) / elapsed
		if speed > 0 {
			remaining := float64(total-completed) / speed
			eta = formatDuration(time.Duration(remaining) * time.Second)
		} else {
			eta = "unknown"
		}
	} else {
		eta = "calculating..."
	}

	fmt.Fprintf(os.Stderr, "%.1f%% - %s/%s @ %s/s - ETA: %s (%d/%d files)\n",
		percent,
		formatBytes(completed),
		formatBytes(total),
		formatBytes(int64(speed)),
		eta,
		completedObjs,
		totalObjs,
	)
}

func formatBytes(bytes int64) string {
	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "kMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
