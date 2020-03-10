package parallel

import (
	"fmt"
	"sync"
)

type Task func() error

func Run(task Task) {
	global.Run(task)
}

// TODO(ig):
var global *parallelManager

func Init(workercount int) {
	global = newParallelManager(workercount)
}

func Close() {
	global.close()
}

func RunFunc(fn Task) {
	global.Run(fn)
}

// parallelManager is the manager to run and manage workers.
type parallelManager struct {
	wg        *sync.WaitGroup
	semaphore chan bool
}

// newParallelManager creates a new parallelManager.
func newParallelManager(workercount int) *parallelManager {
	return &parallelManager{
		wg:        &sync.WaitGroup{},
		semaphore: make(chan bool, workercount),
	}
}

// acquire acquires the semaphore and blocks until resources are available.
// It also increments the WaitGroup counter by one.
func (p *parallelManager) acquire() {
	p.semaphore <- true
	p.wg.Add(1)
}

// release decrements the WaitGroup counter by one and releases the semaphore.
func (p *parallelManager) release() {
	p.wg.Done()
	<-p.semaphore
}

// runJob acquires semaphore and creates new goroutine for the job.
// It exits goroutine after the job is done and releases the semaphore.
func (p *parallelManager) Run(fn Task) {
	p.acquire()
	go func() {
		defer p.release()
		err := fn()
		if err != nil {
			// FIXME(ig): don't log here
			fmt.Println(err)
			return
		}
	}()
}

// close waits all jobs to finish and closes semaphore.
func (p *parallelManager) close() {
	p.wg.Wait()
	close(p.semaphore)
}
