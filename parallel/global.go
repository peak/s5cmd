package parallel

import "syscall"

const (
	minOpenFilesLimit = 1000
)

var global *Manager

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
