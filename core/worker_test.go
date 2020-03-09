package core

/*

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestWorkerManager_semaphore(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	wm := NewWorkerManager(cancelFunc)

	var numJobs uint64 = 1000000
	var counter uint64 = 0

	for i := uint64(0); i < numJobs; i++ {
		wm.runJob(ctx, &incrementJob{counter: &counter})
	}
	wm.close()
	if counter != numJobs {
		t.Errorf("expected counter value %v got %v", numJobs, counter)
	}
}

type incrementJob struct {
	counter *uint64
}

func (i *incrementJob) Run(ctx context.Context) {
	atomic.AddUint64(i.counter, 1)
}
*/
