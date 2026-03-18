package parallel

import "github.com/peak/s5cmd/v2/parallel/fdlimit"

var global *Manager

// Init tries to increase the soft limit of open files and
// creates new global ParallelManager.
func Init(workercount int) {
	_ = fdlimit.Raise()
	global = New(workercount)
}

// Close waits all jobs to finish and
// closes the semaphore of global ParallelManager.
func Close() {
	if global != nil {
		global.Close()
	}
}

// Run runs global ParallelManager.
// Init must be called before Run, otherwise this will panic.
func Run(task Task, waiter *Waiter) {
	if global == nil {
		panic("parallel: Run called before Init")
	}
	global.Run(task, waiter)
}
