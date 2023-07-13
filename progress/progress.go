package progress

import (
	"fmt"
	"sync"

	"github.com/cheggaaa/pb/v3"
)

type CommandProgress struct {
	// task status
	totalObjects     int64
	completedObjects int64
	totalBytes       int64
	completedBytes   int64
	mutex            sync.RWMutex
	progressbar      *pb.ProgressBar
}

const progressbarTemplate = `{{ string . "taskStatus" }} {{percent .}} {{ bar .  | green}} {{counters .}} {{speed . "(%s/s)" }} {{rtime . "%s left"}}`

func (cp *CommandProgress) InitializeProgressBar() {
	cp.progressbar = pb.New64(0)
	cp.progressbar.Set(pb.Bytes, true)
	cp.progressbar.Set(pb.SIBytesPrefix, true)
	cp.progressbar.SetWidth(128)
	cp.progressbar.SetTemplateString(progressbarTemplate)
	cp.progressbar.Set("taskStatus", fmt.Sprintf("%d/%d items are completed.", 0, 0))
	cp.progressbar.Start()
}

func (cp *CommandProgress) Finish() {
	cp.progressbar.Finish()
}

func (cp *CommandProgress) IncrementCompletedObjects() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.completedObjects += 1
	cp.progressbar.Set("taskStatus", fmt.Sprintf("%d/%d items are completed.", cp.completedObjects, cp.totalObjects))
}

func (cp *CommandProgress) IncrementTotalObjects() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.totalObjects += 1
	cp.progressbar.Set("taskStatus", fmt.Sprintf("%d/%d items are completed.", cp.completedObjects, cp.totalObjects))
}

func (cp *CommandProgress) AddCompletedBytesInt64(bytes int64) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.completedBytes += bytes
	cp.progressbar.Add64(bytes)
}

func (cp *CommandProgress) AddCompletedBytes(bytes int) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.completedBytes += int64(bytes)
	cp.progressbar.Add(bytes)
}

func (cp *CommandProgress) AddTotalBytes(bytes int64) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.totalBytes += bytes
	cp.progressbar.SetTotal(cp.totalBytes)
}
