package core

import (
	"context"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

// ClientFunc is the function type to create new storage objects.
type ClientFunc func(*objurl.ObjectURL) (storage.Storage, error)

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
	newClient     ClientFunc
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
	newClient     ClientFunc
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(ctx context.Context, params *WorkerPoolParams, st *stats.Stats) *WorkerPool {
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

	p := &WorkerPool{
		ctx:         ctx,
		params:      params,
		jobQueue:    make(chan *Job),
		subJobQueue: make(chan *Job),
		wg:          &sync.WaitGroup{},
		newClient:   newClient,
		cancelFunc:  cancelFunc,
		st:          st,
	}

	return p
}

// startWorkers starts the workers.
func (p *WorkerPool) startWorkers() {
	for i := 0; i < p.params.NumWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(p.st, &p.idlingCounter, i)
	}
}

// runWorker is the main function of a single worker.
func (p *WorkerPool) runWorker(st *stats.Stats, idlingCounter *int32, id int) {
	defer p.wg.Done()

	wp := WorkerParams{
		ctx:           p.ctx,
		poolParams:    p.params,
		st:            st,
		subJobQueue:   &p.subJobQueue,
		idlingCounter: idlingCounter,
		newClient:     p.newClient,
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
				return
			}
			setWorking()
			job.Run(wp)
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

func (p *WorkerPool) pumpJobQueues() {
	var idc int32
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			idc = atomic.LoadInt32(&p.idlingCounter)

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
					j.subJobData.Done()
					return
				}
			}
		}
	}
}

// closeAndWait closes the channel and waits all jobs to finish.
func (p *WorkerPool) closeAndWait() {
	close(p.jobQueue)
	p.wg.Wait()
}

// RunCmd will run a single command (and subsequent sub-commands) in the worker pool, wait for it to finish, clean up and return.
func (p *WorkerPool) RunCmd(commandLine string) {
	defer p.closeAndWait()

	j := p.parseJob(commandLine)

	if j == nil {
		return
	}

	if j.opts.Has(opt.Help) {
		j.displayHelp()
		return
	}

	p.startWorkers()
	p.queueJob(j)
	if j.operation.IsBatch() {
		p.pumpJobQueues()
	}
}

// Run runs the commands in filename in the worker pool, on EOF it will wait for all commands to finish, clean up and return.
func (p *WorkerPool) Run(filename string) {
	defer p.closeAndWait()

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

	p.startWorkers()
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
}
