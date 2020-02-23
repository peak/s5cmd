package core

import (
	"context"
	"io"
	"log"
	"os"
	"sync"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

// ClientFunc is the function type to create new storage objects.
type ClientFunc func(*objurl.ObjectURL) (storage.Storage, error)

// WorkerPoolParams is the common parameters of all worker pools.
type WorkerManagerParams struct {
	MaxWorkers             int
	UploadChunkSizeBytes   int64
	UploadConcurrency      int
	DownloadChunkSizeBytes int64
	DownloadConcurrency    int
	Retries                int
	EndpointURL            string
	NoVerifySSL            bool
}

// WorkerManager is the manager to run and manage workers.
type WorkerManager struct {
	ctx        context.Context
	params     *WorkerManagerParams
	wg         *sync.WaitGroup
	newClient  ClientFunc
	cancelFunc context.CancelFunc
	st         *stats.Stats
	semaphore  chan bool
}

// WorkerParams is the params/state of a single worker.
type WorkerParams struct {
	ctx        context.Context
	poolParams *WorkerManagerParams
	st         *stats.Stats
	newClient  ClientFunc
}

// NewWorkerManager creates a new WorkerManager.
func NewWorkerManager(ctx context.Context, params *WorkerManagerParams, st *stats.Stats) *WorkerManager {
	newClient := func(url *objurl.ObjectURL) (storage.Storage, error) {
		if url.IsRemote() {
			s3, err := storage.NewS3Storage(storage.S3Opts{
				MaxRetries:           params.Retries,
				EndpointURL:          params.EndpointURL,
				Region:               "",
				NoVerifySSL:          params.NoVerifySSL,
				UploadChunkSizeBytes: params.UploadChunkSizeBytes,
				UploadConcurrency:    params.UploadConcurrency,
			})
			return s3, err
		}

		return storage.NewFilesystem(), nil
	}

	cancelFunc := ctx.Value(CancelFuncKey).(context.CancelFunc)

	w := &WorkerManager{
		ctx:        ctx,
		params:     params,
		wg:         &sync.WaitGroup{},
		cancelFunc: cancelFunc,
		st:         st,
		newClient:  newClient,
		semaphore:  make(chan bool, params.MaxWorkers),
	}

	return w
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
// It exits goroutine after all jobs are done and releases the semaphore.
func (w *WorkerManager) runJob(job *Job) {
	w.acquire()
	go func() {
		defer w.release()
		wp := WorkerParams{
			ctx:        w.ctx,
			poolParams: w.params,
			st:         w.st,
			newClient:  w.newClient,
		}
		job.Run(wp)
	}()
}

// RunCmd will run a single command (and subsequent sub-commands) in the worker manager,
// wait for it to finish, clean up and return.
func (w *WorkerManager) RunCmd(cmd string) {
	defer w.close()

	command := w.parseCommand(cmd)
	if command == nil {
		return
	}

	if command.opts.Has(opt.Help) {
		command.displayHelp()
		return
	}

	producer := &Producer{newClient: w.newClient, runJob: w.runJob}
	producer.Run(w.ctx, command)
}

// parseCommand parses command.
func (w *WorkerManager) parseCommand(cmd string) *Command {
	command, err := ParseCommand(cmd)
	if err != nil {
		log.Println(`-ERR "`, cmd, `": `, err)
		w.st.Increment(stats.Fail)
		return nil
	}
	return command
}

// close waits all jobs to finish and closes jobQueue channel.
func (w *WorkerManager) close() {
	w.wg.Wait()
	close(w.semaphore)
}

// Run runs the commands in filename in the worker manager, on EOF
// it will wait for all jobs to finish, clean up and return.
func (w *WorkerManager) Run(filename string) {
	defer w.close()

	var r io.ReadCloser
	var err error

	if filename == "-" {
		r = os.Stdin
	} else {
		r, err = os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer r.Close()
	}

	w.produceWithScanner(r)
}

// produceWithScanner reads content from io.ReadCloser and
// produces jobs for valid commands.
func (w *WorkerManager) produceWithScanner(r io.ReadCloser) {
	scanner := NewScanner(w.ctx, r)
	producer := &Producer{newClient: w.newClient, runJob: w.runJob}

	for cmd := range scanner.Scan() {
		command := w.parseCommand(cmd)
		if command != nil {
			producer.Run(w.ctx, command)
		}
	}

	if err := scanner.Err(); err != nil {
		if err == context.Canceled {
			return
		}
		log.Printf("-ERR Error reading: %v", err)
	}
}
