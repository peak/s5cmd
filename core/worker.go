package core

import (
	"context"
	"io"
	"log"
	"os"
	"sync"

	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

// ClientFunc is the function type to create new storage objects.
type ClientFunc func() (storage.Storage, error)

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
	ctx         context.Context
	params      *WorkerManagerParams
	wg          *sync.WaitGroup
	newClient   ClientFunc
	cancelFunc  context.CancelFunc
	st          *stats.Stats
	jobQueue    chan *Job
	jobProducer *Producer
	semaphore   chan bool
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
	newClient := func() (storage.Storage, error) {
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

	cancelFunc := ctx.Value(CancelFuncKey).(context.CancelFunc)

	wg := &sync.WaitGroup{}
	jobQueue := make(chan *Job)

	enqueueJob := func(job *Job) {
		wg.Add(1)
		jobQueue <- job
	}

	// TODO(os): handle error and move
	producerClient, _ := newClient()
	w := &WorkerManager{
		ctx:        ctx,
		params:     params,
		wg:         wg,
		newClient:  newClient,
		cancelFunc: cancelFunc,
		st:         st,
		jobQueue:   jobQueue,
		semaphore:  make(chan bool, params.MaxWorkers),
		jobProducer: &Producer{
			client:     producerClient,
			enqueueJob: enqueueJob,
		},
	}

	return w
}

// acquire acquires the semaphore and blocks until resources are available.
// It also increments the WaitGroup counter by one.
func (w *WorkerManager) acquire() {
	w.semaphore <- true
}

// release decrements the WaitGroup counter by one and releases the semaphore.
func (w *WorkerManager) release() {
	w.wg.Done()
	<-w.semaphore
}

// watchJobs listens for the jobs and starts new worker for produced jobs.
func (w *WorkerManager) watchJobs() {
	for {
		select {
		case j, ok := <-w.jobQueue:
			if !ok {
				return
			}
			w.runWorker(j)
		case <-w.ctx.Done():
			return
		}
	}
}

// runWorker acquires semaphore and creates new worker (goroutine) for the job.
// Worker is closed after all jobs are done and releases the semaphore.
func (w *WorkerManager) runWorker(job *Job) {
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
	command := w.parseCommand(cmd)
	if command == nil {
		return
	}

	if command.opts.Has(opt.Help) {
		command.displayHelp()
		return
	}

	go func() {
		defer w.close()
		w.jobProducer.Produce(w.ctx, command)
	}()

	w.watchJobs()
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
	close(w.jobQueue)
}

// Run runs the commands in filename in the worker manager, on EOF
// it will wait for all jobs to finish, clean up and return.
func (w *WorkerManager) Run(filename string) {
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

	go w.produceWithScanner(r)
	w.watchJobs()
}

// produceWithScanner reads content from io.ReadCloser and
// produces jobs for valid commands.
func (w *WorkerManager) produceWithScanner(r io.ReadCloser) {
	defer w.close()

	scanner := NewScanner(w.ctx, r)

	for cmd := range scanner.Scan() {
		command := w.parseCommand(cmd)
		if command != nil {
			w.jobProducer.Produce(w.ctx, command)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("-ERR Error reading: %v\n", err)
	}
}
