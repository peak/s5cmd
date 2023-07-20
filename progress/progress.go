package progress

import (
	"fmt"
	"sync"

	"github.com/cheggaaa/pb/v3"
)

type ProgressBar interface {
	InitializeProgressBar()
	Finish()
	IncrementCompletedObjects()
	IncrementTotalObjects()
	AddCompletedBytesInt64(bytes int64)
	AddCompletedBytes(bytes int)
	AddTotalBytes(bytes int64)
}

type DummyProgress struct{}

func (dp *DummyProgress) InitializeProgressBar() {}

func (dp *DummyProgress) Finish() {}

func (dp *DummyProgress) IncrementCompletedObjects() {}

func (dp *DummyProgress) IncrementTotalObjects() {}

func (dp *DummyProgress) AddCompletedBytesInt64(bytes int64) {}

func (dp *DummyProgress) AddCompletedBytes(bytes int) {}

func (dp *DummyProgress) AddTotalBytes(bytes int64) {}

type CommandProgress struct {
	totalObjects     int64
	completedObjects int64
	totalBytes       int64
	completedBytes   int64
	mu               sync.RWMutex
	progressbar      *pb.ProgressBar
}

const progressbarTemplate = `{{percent . | green}} {{bar . " " "━" "━" "─" " " | green}} {{counters . | green}} {{speed . "(%s/s)" | red}} {{rtime . "%s left" | blue}} {{ string . "objects" | yellow}}`

func (cp *CommandProgress) InitializeProgressBar() {
	cp.progressbar = pb.New64(0)
	cp.progressbar.Set(pb.Bytes, true)
	cp.progressbar.Set(pb.SIBytesPrefix, true)
	cp.progressbar.SetWidth(128)
	cp.progressbar.SetTemplateString(progressbarTemplate)
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", 0, 0))
	cp.progressbar.Start()
}

func (cp *CommandProgress) Finish() {
	cp.progressbar.Finish()
}

func (cp *CommandProgress) IncrementCompletedObjects() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.completedObjects += 1
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", cp.completedObjects, cp.totalObjects))
}

func (cp *CommandProgress) IncrementTotalObjects() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.totalObjects += 1
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", cp.completedObjects, cp.totalObjects))
}

func (cp *CommandProgress) AddCompletedBytesInt64(bytes int64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.completedBytes += bytes
	cp.progressbar.Add64(bytes)
}

func (cp *CommandProgress) AddCompletedBytes(bytes int) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.completedBytes += int64(bytes)
	cp.progressbar.Add(bytes)
}

func (cp *CommandProgress) AddTotalBytes(bytes int64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.totalBytes += bytes
	cp.progressbar.SetTotal(cp.totalBytes)
}
