package parallel

import (
	"runtime"
	"sync"
)

const minNumWorkers = 2

var global *Manager

type Task func() error

func Init(workercount int) {
	global = New(workercount)
}

func Close() { global.Close() }

func Run(task Task, waiter *Waiter) { global.Run(task, waiter) }

// parallel is the manager to run and manage workers.
type Manager struct {
	wg        *sync.WaitGroup
	semaphore chan bool
}

// New creates a new parallel manager.
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

// acquire acquires the semaphore and blocks until resources are available.
// It also increments the WaitGroup counter by one.
func (p *Manager) acquire() {
	p.semaphore <- true
	p.wg.Add(1)
}

// release decrements the WaitGroup counter by one and releases the semaphore.
func (p *Manager) release() {
	p.wg.Done()
	<-p.semaphore
}

// runJob acquires semaphore and creates new goroutine for the job.
// It exits goroutine after the job is done and releases the semaphore.
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

// Close waits all jobs to finish and closes semaphore.
func (p *Manager) Close() {
	p.wg.Wait()
	close(p.semaphore)
}

type Waiter struct {
	wg    sync.WaitGroup
	errch chan error
}

func NewWaiter() *Waiter {
	return &Waiter{
		errch: make(chan error),
	}
}

func (w *Waiter) Wait() {
	w.wg.Wait()
	close(w.errch)
}

func (w *Waiter) Err() <-chan error {
	return w.errch
}
