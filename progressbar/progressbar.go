package progressbar

import (
	"fmt"
	"sync/atomic"

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
	atomic.AddInt64(&cp.completedObjects, 1)
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", cp.completedObjects, cp.totalObjects))
}

func (cp *CommandProgressBar) IncrementTotalObjects() {
	atomic.AddInt64(&cp.totalObjects, 1)
	cp.progressbar.Set("objects", fmt.Sprintf("(%d/%d)", cp.completedObjects, cp.totalObjects))
}

func (cp *CommandProgressBar) AddCompletedBytes(bytes int64) {
	cp.progressbar.Add64(bytes)
}

func (cp *CommandProgressBar) AddTotalBytes(bytes int64) {
	cp.progressbar.AddTotal(bytes)
}
