package parallel

import "github.com/peak/s5cmd/parallel/fdlimit"

var global *Manager

// Init tries to increase the soft limit of open files and
// creates new global ParallelManager.
func Init(workercount int) {
	_ = fdlimit.Raise()
	global = New(workercount)
}

// Close waits all jobs to finish and
// closes the semaphore of global ParallelManager.
func Close() { global.Close() }

// Run runs global ParallelManager.
func Run(task Task, waiter *Waiter) { global.Run(task, waiter) }

func Size() int {
	return global.Size
}
