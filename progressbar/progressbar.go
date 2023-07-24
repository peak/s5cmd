package progressbar

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
	AddCompletedBytes(bytes int)
	AddTotalBytes(bytes int64)
}

type MockProgressBar struct{}

func (pb *MockProgressBar) InitializeProgressBar() {}

func (pb *MockProgressBar) Finish() {}

func (pb *MockProgressBar) IncrementCompletedObjects() {}

func (pb *MockProgressBar) IncrementTotalObjects() {}

func (pb *MockProgressBar) AddCompletedBytes(bytes int) {}

func (pb *MockProgressBar) AddTotalBytes(bytes int64) {}

type CommandProgressBar struct {
	totalObjects     int64
	completedObjects int64
	totalBytes       int64
	completedBytes   int64
	mu               sync.RWMutex
	progressbar      *pb.ProgressBar
}

const progressbarTemplate = `{{percent . | green}} {{bar . " " "━" "━" "─" " " | green}} {{counters . | green}} {{speed . "(%s/s)" | red}} {{rtime . "%s left" | blue}} {{ string . "objects" | yellow}}`

func (cp *CommandProgressBar) InitializeProgressBar() {
	cp.progressbar = pb.New64(0)
	cp.progressbar.Set(pb.Bytes, true)
	cp.progressbar.Set(pb.SIBytesPrefix, true)
	cp.progressbar.SetWidth(128)
	cp.progressbar.SetTemplateString(progressbarTemplate)
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", 0, 0))
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

func (cp *CommandProgressBar) AddCompletedBytes(bytes int) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.completedBytes += int64(bytes)
	cp.progressbar.Add(bytes)
}

func (cp *CommandProgressBar) AddTotalBytes(bytes int64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.totalBytes += bytes
	cp.progressbar.SetTotal(cp.totalBytes)
}
