package core

import (
	"fmt"
	"sync"
)

// TODO(ig):
var globalWorkerManager *WorkerManager

func InitWorker(workercount int) {
	globalWorkerManager = NewWorkerManager(workercount)
}

func CloseWorker() {
	globalWorkerManager.close()
}

func RunFunc(fn Func) {
	globalWorkerManager.Run(fn)
}

type Func func() error

// WorkerManager is the manager to run and manage workers.
type WorkerManager struct {
	wg        *sync.WaitGroup
	semaphore chan bool
}

// NewWorkerManager creates a new WorkerManager.
func NewWorkerManager(workercount int) *WorkerManager {
	return &WorkerManager{
		wg:        &sync.WaitGroup{},
		semaphore: make(chan bool, workercount),
	}
}

// acquire acquires the semaphore and blocks until resources are available.
// It also increments the WaitGroup counter by one.
func (w *WorkerManager) acquire() {
	w.semaphore <- true
	w.wg.Add(1)
}

// release decrements the WaitGroup counter by one and releases the semaphore.
func (w *WorkerManager) release() {
	w.wg.Done()
	<-w.semaphore
}

// runJob acquires semaphore and creates new goroutine for the job.
// It exits goroutine after the job is done and releases the semaphore.
func (w *WorkerManager) Run(fn Func) {
	w.acquire()
	go func() {
		defer w.release()
		err := fn()
		if err != nil {
			// FIXME(ig): don't log here
			fmt.Println(err)
			return
		}
	}()
}

// close waits all jobs to finish and closes semaphore.
func (w *WorkerManager) close() {
	w.wg.Wait()
	close(w.semaphore)
}
