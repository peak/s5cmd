package core

import (
	"context"
	"errors"
	"io"
	stdlog "log"
	"os"
	"sync"

	"github.com/peak/s5cmd/flags"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
)

// WorkerManager is the manager to run and manage workers.
type WorkerManager struct {
	wg         *sync.WaitGroup
	cancelFunc context.CancelFunc
	semaphore  chan bool
	donech     chan bool
}

// NewWorkerManager creates a new WorkerManager.
func NewWorkerManager(cancelFunc context.CancelFunc) *WorkerManager {
	return &WorkerManager{
		wg:         &sync.WaitGroup{},
		cancelFunc: cancelFunc,
		semaphore:  make(chan bool, *flags.WorkerCount),
		donech:     make(chan bool),
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
func (w *WorkerManager) runJob(ctx context.Context, job Runnable) {
	w.acquire()
	go func() {
		defer w.release()
		job.Run(ctx)
	}()
}

// RunCmd will run a single command in the worker manager, wait for it to
// finish, clean up and return.
func (w *WorkerManager) RunCmd(ctx context.Context, cmd string) {
	stats.StartTimer()

	defer w.closeStdout(false)
	defer w.close()

	command := w.parseCommand(cmd)
	if command == nil {
		return
	}

	if command.opts.Has(opt.Help) {
		command.displayHelp()
		return
	}

	producer := &Producer{runJob: w.runJob}
	producer.Run(ctx, command)
}

// parseCommand parses command.
func (w *WorkerManager) parseCommand(cmd string) *Command {
	command, err := ParseCommand(cmd)
	if err != nil {
		log.Logger.Error(ErrorMessage{
			Job:    cmd,
			Err:    err.Error(),
			format: `"%s": %v`,
		})
		stats.Increment(stats.Fail)
		return nil
	}
	return command
}

// close waits all jobs to finish and closes semaphore.
func (w *WorkerManager) close() {
	w.wg.Wait()
	close(w.semaphore)
}

// closeStdout prints stats message if it is enabled
// and closes stdout channel.
func (w *WorkerManager) closeStdout(force bool) {
	// WorkerManager is responsible for logging, hence we run the close routine
	// here.
	stats.Print(force)
	log.Logger.Close()
}

// Run runs the commands in filename in the worker manager, on EOF
// it will wait for all jobs to finish, clean up and return.
func (w *WorkerManager) Run(ctx context.Context, filename string) {
	stats.StartTimer()

	defer w.closeStdout(true)
	defer w.close()

	var r io.ReadCloser
	var err error

	if filename == "-" {
		r = os.Stdin
	} else {
		r, err = os.Open(filename)
		if err != nil {
			stdlog.Fatal(err)
		}
		defer r.Close()
	}

	w.produceWithScanner(ctx, r)
}

// produceWithScanner reads content from io.ReadCloser and
// produces jobs for valid commands.
func (w *WorkerManager) produceWithScanner(ctx context.Context, r io.ReadCloser) {
	scanner := NewScanner(ctx, r)
	producer := &Producer{runJob: w.runJob}

	for cmd := range scanner.Scan() {
		command := w.parseCommand(cmd)
		if command != nil {
			producer.Run(ctx, command)
		}
	}

	if err := scanner.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		stdlog.Printf("-ERR Error reading: %v", err)
	}
}
