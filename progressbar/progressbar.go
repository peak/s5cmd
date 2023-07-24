package progressbar

import (
	"fmt"
	"sync"

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

type NoOpProgressBar struct{}

func (pb *NoOpProgressBar) Start() {}

func (pb *NoOpProgressBar) Finish() {}

func (pb *NoOpProgressBar) IncrementCompletedObjects() {}

func (pb *NoOpProgressBar) IncrementTotalObjects() {}

func (pb *NoOpProgressBar) AddCompletedBytes(bytes int64) {}

func (pb *NoOpProgressBar) AddTotalBytes(bytes int64) {}

type CommandProgressBar struct {
	totalObjects     int64
	completedObjects int64
	totalBytes       int64
	completedBytes   int64
	mu               sync.RWMutex
	progressbar      *pb.ProgressBar
}

var _ ProgressBar = (*CommandProgressBar)(nil)

const progressbarTemplate = `{{percent . | green}} {{bar . " " "━" "━" "─" " " | green}} {{counters . | green}} {{speed . "(%s/s)" | red}} {{rtime . "%s left" | blue}} {{ string . "objects" | yellow}}`

func NewCommandProgressBar() *CommandProgressBar {
	cp := &CommandProgressBar{}
	cp.progressbar = pb.New64(0)
	cp.progressbar.Set(pb.Bytes, true)
	cp.progressbar.Set(pb.SIBytesPrefix, true)
	cp.progressbar.SetWidth(128)
	cp.progressbar.SetTemplateString(progressbarTemplate)
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", 0, 0))
	return cp
}

func (cp *CommandProgressBar) Start() {
	cp.progressbar.Start()
}

func (cp *CommandProgressBar) Finish() {
	cp.progressbar.Finish()
}

func (cp *CommandProgressBar) IncrementCompletedObjects() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.completedObjects += 1
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", cp.completedObjects, cp.totalObjects))
}

func (cp *CommandProgressBar) IncrementTotalObjects() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.totalObjects += 1
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", cp.completedObjects, cp.totalObjects))
}

func (cp *CommandProgressBar) AddCompletedBytes(bytes int64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.completedBytes += bytes
	cp.progressbar.Add64(bytes)
}

func (cp *CommandProgressBar) AddTotalBytes(bytes int64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.totalBytes += bytes
	cp.progressbar.SetTotal(cp.totalBytes)
}
