package core

import (
	"context"
	"fmt"
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

// WorkerFunc is the function type to create and run workers.
type WorkerFunc func(*Job)

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
}

// WorkerParams is the params/state of a single worker.
type WorkerParams struct {
	ctx        context.Context
	poolParams *WorkerManagerParams
	st         *stats.Stats
	newClient  ClientFunc
	runWorker  WorkerFunc
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
	jobQueue := make(chan *Job, params.MaxWorkers)

	enqueueJob := func(job *Job) {
		wg.Add(1)
		jobQueue <- job
	}

	producerClient, _ := newClient()
	w := &WorkerManager{
		ctx:        ctx,
		params:     params,
		wg:         wg,
		newClient:  newClient,
		cancelFunc: cancelFunc,
		st:         st,
		jobQueue:   jobQueue,
		jobProducer: &Producer{
			client:     producerClient,
			enqueueJob: enqueueJob,
		},
	}

	return w
}

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

// runWorker creates new worker (goroutine) for the job.
// Worker is closed after all jobs done.
func (w *WorkerManager) runWorker(job *Job) {
	go func() {
		defer w.wg.Done()
		wp := WorkerParams{
			ctx:        w.ctx,
			poolParams: w.params,
			st:         w.st,
			newClient:  w.newClient,
			runWorker:  w.runWorker,
		}
		job.Run(wp)
	}()
}

// RunCmd will run a single command (and subsequent sub-commands) in the worker pool,
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
		err := w.jobProducer.Produce(w.ctx, command)
		if err != nil {
			log.Printf("%v\n", err)
		}
		w.wg.Wait()
		close(w.jobQueue)
	}()

	w.watchJobs()
}

func (w *WorkerManager) parseCommand(cmd string) *Command {
	command, err := ParseCommand(cmd)
	if err != nil {
		log.Println(`-ERR "`, cmd, `": `, err)
		w.st.Increment(stats.Fail)
		return nil
	}
	return command
}

// Run runs the commands in filename in the worker pool, on EOF
// it will wait for all commands to finish, clean up and return.
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

	go func() {
		scanner := NewScanner(w.ctx, r)

		for cmd := range scanner.Scan() {
			command := w.parseCommand(cmd)
			if command != nil {
				err := w.jobProducer.Produce(w.ctx, command)
				if err != nil {
					fmt.Println(err)
				}
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("-ERR Error reading: %v\n", err)
		}

		w.wg.Wait()
		close(w.jobQueue)
	}()

	w.watchJobs()
}
