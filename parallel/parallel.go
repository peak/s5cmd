package parallel

import (
	"runtime"
	"sync"
	"syscall"
)

const minNumWorkers = 2
const minOpenFilesLimit = 1000

var global *Manager

// Task is a function type for parallel manager.
type Task func() error

// Init tries to increase the soft limit of open files and
// creates new global ParallelManager.
func Init(workercount int) {
	adjustOpenFilesLimit()
	global = New(workercount)
}

// Close waits all jobs to finish and
// closes the semaphore of global ParallelManager.
func Close() { global.Close() }

// Run runs global ParallelManager.
func Run(task Task, waiter *Waiter) { global.Run(task, waiter) }

// Manager is a structure for running tasks in parallel.
type Manager struct {
	wg        *sync.WaitGroup
	semaphore chan bool
}

// New creates a new parallel.Manager.
func New(workercount int) *Manager {
	if workercount < 0 {
		workercount = runtime.NumCPU() * -workercount
	}

	if workercount < minNumWorkers {
		workercount = minNumWorkers
	}

	return &Manager{
		wg:        &sync.WaitGroup{},
		semaphore: make(chan bool, workercount),
	}
}

// acquire limits concurrency by trying to acquire the semaphore.
func (p *Manager) acquire() {
	p.semaphore <- true
	p.wg.Add(1)
}

// release releases the acquired semaphore to signal that a task is finished.
func (p *Manager) release() {
	p.wg.Done()
	<-p.semaphore
}

// Run runs the given task while limiting the concurrency.
func (p *Manager) Run(fn Task, waiter *Waiter) {
	waiter.wg.Add(1)
	p.acquire()
	go func() {
		defer waiter.wg.Done()
		defer p.release()

		if err := fn(); err != nil {
			waiter.errch <- err
		}
	}()
}

// Close waits all tasks to finish.
func (p *Manager) Close() {
	p.wg.Wait()
	close(p.semaphore)
}

// Waiter is a structure for waiting and reading
// error messages created by Manager.
type Waiter struct {
	wg    sync.WaitGroup
	errch chan error
}

// NewWaiter creates a new parallel.Waiter.
func NewWaiter() *Waiter {
	return &Waiter{
		errch: make(chan error),
	}
}

// Wait blocks until the WaitGroup counter is zero
// and closes error channel.
func (w *Waiter) Wait() {
	w.wg.Wait()
	close(w.errch)
}

// Err returns read-only error channel.
func (w *Waiter) Err() <-chan error {
	return w.errch
}

// adjustOpenFilesLimit tries to increase the soft limit of open files.
func adjustOpenFilesLimit() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return
	}

	if rLimit.Cur >= minOpenFilesLimit {
		return
	}

	if rLimit.Max < minOpenFilesLimit {
		return
	}

	rLimit.Cur = minOpenFilesLimit

	_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
}
