package core

import (
	"context"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peak/s5cmd/storage"

	"github.com/peak/s5cmd/stats"
)

// WorkerPoolParams is the common parameters of all worker pools.
type WorkerPoolParams struct {
	NumWorkers             int
	UploadChunkSizeBytes   int64
	UploadConcurrency      int
	DownloadChunkSizeBytes int64
	DownloadConcurrency    int
	Retries                int
	EndpointURL            string
	NoVerifySSL            bool
}

// WorkerPool is the state of our worker pool.
type WorkerPool struct {
	ctx           context.Context
	params        *WorkerPoolParams
	jobQueue      chan *Job
	subJobQueue   chan *Job
	wg            *sync.WaitGroup
	storage       storage.Storage
	cancelFunc    context.CancelFunc
	st            *stats.Stats
	idlingCounter int32
}

// WorkerParams is the params/state of a single worker.
type WorkerParams struct {
	ctx           context.Context
	poolParams    *WorkerPoolParams
	st            *stats.Stats
	subJobQueue   *chan *Job
	idlingCounter *int32
	storage       storage.Storage
}

// NewWorkerPool creates a new worker pool and start the workers.
func NewWorkerPool(ctx context.Context, params *WorkerPoolParams, st *stats.Stats) *WorkerPool {
	s3, err := storage.NewS3Storage(storage.S3Opts{
		MaxRetries:           params.Retries,
		EndpointURL:          params.EndpointURL,
		Region:               "",
		NoVerifySSL:          params.NoVerifySSL,
		UploadChunkSizeBytes: params.UploadChunkSizeBytes,
		UploadConcurrency:    params.UploadConcurrency,
	})

	if err != nil {
		log.Fatal(err)
	}

	cancelFunc := ctx.Value(CancelFuncKey).(context.CancelFunc)

	p := &WorkerPool{
		ctx:         ctx,
		params:      params,
		jobQueue:    make(chan *Job),
		subJobQueue: make(chan *Job),
		wg:          &sync.WaitGroup{},
		storage:     s3,
		cancelFunc:  cancelFunc,
		st:          st,
	}

	for i := 0; i < params.NumWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(st, &p.idlingCounter, i)
	}

	return p
}

// runWorker is the main function of a single worker.
func (p *WorkerPool) runWorker(st *stats.Stats, idlingCounter *int32, id int) {
	defer p.wg.Done()
	defer verboseLog("Exiting goroutine %d", id)

	wp := WorkerParams{
		ctx:           p.ctx,
		poolParams:    p.params,
		st:            st,
		subJobQueue:   &p.subJobQueue,
		idlingCounter: idlingCounter,
		storage:       p.storage,
	}

	lastSetIdle := false
	setIdle := func() {
		if !lastSetIdle {
			atomic.AddInt32(idlingCounter, 1)
			lastSetIdle = true
		}
	}
	setWorking := func() {
		if lastSetIdle {
			atomic.AddInt32(idlingCounter, ^int32(0))
			lastSetIdle = false
		}
	}
	defer setIdle()

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			setIdle()

		case job, ok := <-p.jobQueue:
			if !ok {
				verboseLog("Channel closed %d", id)
				return
			}
			setWorking()

			if err := job.Run(&wp); err != nil {
				if IsAcceptableError(err) {
					job.Notify(true)
				} else {
					job.PrintErr(err)
					wp.st.Increment(stats.Fail)
					job.Notify(false)
				}
			} else {
				job.PrintOK()
				job.Notify(true)
			}

		case <-p.ctx.Done():
			return
		}
	}
}

func (p *WorkerPool) parseJob(line string) *Job {
	job, err := ParseJob(line)
	if err != nil {
		log.Print(`-ERR "`, line, `": `, err)
		p.st.Increment(stats.Fail)
		return nil
	}
	return job
}

func (p *WorkerPool) queueJob(job *Job) bool {
	select {
	case <-p.ctx.Done():
		return false
	case p.jobQueue <- job:
		return true
	}
}

func queueSubJob(job *Job, wp *WorkerParams) bool {
	select {
	case <-wp.ctx.Done():
		return false
	case *wp.subJobQueue <- job:
		return true
	}
}

func (p *WorkerPool) pumpJobQueues() {
	verboseLog("Start pumping...")

	var idc int32
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			idc = atomic.LoadInt32(&p.idlingCounter)
			verboseLog("idlingCounter is %d, expected to be %d", idc, p.params.NumWorkers)

			if idc == int32(p.params.NumWorkers) {
				log.Print("# All workers idle, finishing up...")
				return
			}
		case <-p.ctx.Done():
			return
		case j, ok := <-p.subJobQueue:
			if ok {
				select {
				case p.jobQueue <- j:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}
}

// RunCmd will run a single command (and subsequent sub-commands) in the worker pool, wait for it to finish, clean up and return.
func (p *WorkerPool) RunCmd(commandLine string) {
	j := p.parseJob(commandLine)
	if j != nil {
		p.queueJob(j)
		if j.operation.IsBatch() {
			p.pumpJobQueues()
		}
	}

	close(p.jobQueue)
	p.wg.Wait()
}

// Run runs the commands in filename in the worker pool, on EOF it will wait for all commands to finish, clean up and return.
func (p *WorkerPool) Run(filename string) {
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

	s := NewCancelableScanner(p.ctx, r).Start()

	var j *Job
	isAnyBatch := false
	for {
		line, err := s.ReadOne(p.subJobQueue, p.jobQueue)
		if err != nil {
			if err == context.Canceled || err == io.EOF {
				break
			}
			log.Printf("-ERR Error reading: %v", err)
			break
		}

		j = p.parseJob(line)
		if j != nil {
			isAnyBatch = isAnyBatch || j.operation.IsBatch()
			p.queueJob(j)
		}
	}
	if isAnyBatch {
		p.pumpJobQueues()
	}
	close(p.jobQueue)

	//log.Print("# Waiting...")
	p.wg.Wait()
}
