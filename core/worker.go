package core

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cenkalti/backoff"
	"github.com/peakgames/s5cmd/stats"
)

// WorkerPoolParams is the common parameters of all worker pools.
type WorkerPoolParams struct {
	NumWorkers     uint32
	ChunkSizeBytes int64
	Retries        int
}

// WorkerPool is the state of our worker pool.
type WorkerPool struct {
	ctx           context.Context
	params        *WorkerPoolParams
	jobQueue      chan *Job
	subJobQueue   chan *Job
	wg            *sync.WaitGroup
	awsSession    *session.Session
	cancelFunc    context.CancelFunc
	st            *stats.Stats
	idlingCounter int32
}

// WorkerParams is the params/state of a single worker.
type WorkerParams struct {
	s3svc         *s3.S3
	s3dl          *s3manager.Downloader
	s3ul          *s3manager.Uploader
	ctx           context.Context
	poolParams    *WorkerPoolParams
	st            *stats.Stats
	subJobQueue   *chan *Job
	idlingCounter *int32
}

// NewWorkerPool creates a new worker pool and start the workers.
func NewWorkerPool(ctx context.Context, params *WorkerPoolParams, st *stats.Stats) *WorkerPool {
	newSession := func(c *aws.Config) *session.Session {
		useSharedConfig := session.SharedConfigEnable

		// Reverse of what the SDK does: if AWS_SDK_LOAD_CONFIG is 0 (or a falsy value) disable shared configs
		loadCfg := os.Getenv("AWS_SDK_LOAD_CONFIG")
		if loadCfg != "" {
			if enable, _ := strconv.ParseBool(loadCfg); !enable {
				useSharedConfig = session.SharedConfigDisable
			}
		}
		ses, err := session.NewSessionWithOptions(session.Options{Config: *c, SharedConfigState: useSharedConfig})
		if err != nil {
			log.Fatal(err)
		}
		return ses
	}
	awsCfg := aws.NewConfig().WithMaxRetries(params.Retries) //.WithLogLevel(aws.LogDebug))
	ses := newSession(awsCfg)

	if (*ses).Config.Region == nil || *(*ses).Config.Region == "" { // No region specified in env or config, fallback to us-east-1
		awsCfg = awsCfg.WithRegion("us-east-1")
		ses = newSession(awsCfg)
	}

	cancelFunc := ctx.Value(CancelFuncKey).(context.CancelFunc)

	p := &WorkerPool{
		ctx:           ctx,
		params:        params,
		jobQueue:      make(chan *Job),
		subJobQueue:   make(chan *Job),
		wg:            &sync.WaitGroup{},
		awsSession:    ses,
		cancelFunc:    cancelFunc,
		st:            st,
		idlingCounter: 0,
	}

	var i uint32
	for i = 0; i < params.NumWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(st, &p.idlingCounter, int(i))
	}

	return p
}

// runWorker is the main function of a single worker.
func (p *WorkerPool) runWorker(st *stats.Stats, idlingCounter *int32, id int) {
	defer p.wg.Done()
	if Verbose {
		defer fmt.Printf("VERBOSE: Exiting goroutine %d\n", id)
	}

	wp := WorkerParams{
		s3.New(p.awsSession),
		// Give each worker its own s3manager
		s3manager.NewDownloader(p.awsSession),
		s3manager.NewUploader(p.awsSession),
		p.ctx,
		p.params,
		st,
		&p.subJobQueue,
		idlingCounter,
	}

	bkf := backoff.NewExponentialBackOff()
	bkf.InitialInterval = time.Second
	bkf.MaxInterval = time.Minute
	bkf.Multiplier = 2
	bkf.MaxElapsedTime = 0
	bkf.Reset()

	run := true
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

	for run {
		select {
		case <-time.After(100 * time.Millisecond):
			setIdle()

		case job, ok := <-p.jobQueue:
			if !ok {
				if Verbose {
					fmt.Printf("VERBOSE: Channel closed %d\n", id)
				}
				return
			}
			setWorking()

			tries := 0
			for job != nil {
				err := job.Run(&wp)
				var acceptableErr AcceptableError
				if err != nil {
					acceptableErr = IsAcceptableError(err)
					if acceptableErr != nil {
						err = nil
					}
				}

				if err != nil {
					errCode, doRetry := IsRetryableError(err)
					if doRetry && p.params.Retries > 0 && tries < p.params.Retries {
						tries++
						sleepTime := bkf.NextBackOff()
						log.Printf(`?%s "%s", sleep for %v`, errCode, job, sleepTime)
						select {
						case <-time.After(sleepTime):
							wp.st.Increment(stats.RetryOp)
							continue
						case <-p.ctx.Done():
							run = false // if Canceled during sleep, report ERR and immediately process failCommand
						}
					}

					job.PrintErr(err)
					wp.st.Increment(stats.Fail)
					job.Notify(p.ctx, err)
					job = job.failCommand
				} else {
					job.PrintOK(acceptableErr)
					job.Notify(p.ctx, nil)
					job = job.successCommand
				}
				tries = 0
				bkf.Reset()
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

func (p *WorkerPool) pumpJobQueues() {
	if Verbose {
		fmt.Println("VERBOSE: Start pumping...")
	}
	var idc int32
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			idc = atomic.LoadInt32(&p.idlingCounter)
			if Verbose {
				fmt.Printf("VERBOSE: idlingCounter is %d, expected to be %d\n", idc, p.params.NumWorkers)
			}
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
