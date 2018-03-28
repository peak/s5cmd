package core

import (
	"context"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cenkalti/backoff"
	"github.com/peakgames/s5cmd/stats"
)

// WorkerPoolParams is the common parameters of all worker pools.
type WorkerPoolParams struct {
	NumWorkers             int
	UploadChunkSizeBytes   int64
	UploadConcurrency      int
	DownloadChunkSizeBytes int64
	DownloadConcurrency    int
	Retries                int
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

// NewAwsSession initializes a new AWS session with region fallback and custom options
func NewAwsSession(maxRetries int, region string) (*session.Session, error) {
	newSession := func(c *aws.Config) (*session.Session, error) {
		useSharedConfig := session.SharedConfigEnable

		// Reverse of what the SDK does: if AWS_SDK_LOAD_CONFIG is 0 (or a falsy value) disable shared configs
		loadCfg := os.Getenv("AWS_SDK_LOAD_CONFIG")
		if loadCfg != "" {
			if enable, _ := strconv.ParseBool(loadCfg); !enable {
				useSharedConfig = session.SharedConfigDisable
			}
		}
		return session.NewSessionWithOptions(session.Options{Config: *c, SharedConfigState: useSharedConfig})
	}

	awsCfg := aws.NewConfig().WithMaxRetries(maxRetries) //.WithLogLevel(aws.LogDebug))
	if region != "" {
		awsCfg = awsCfg.WithRegion(region)
		return newSession(awsCfg)
	}

	ses, err := newSession(awsCfg)
	if err != nil {
		return nil, err
	}
	if (*ses).Config.Region == nil || *(*ses).Config.Region == "" { // No region specified in env or config, fallback to us-east-1
		awsCfg = awsCfg.WithRegion(endpoints.UsEast1RegionID)
		ses, err = newSession(awsCfg)
	}

	return ses, err
}

// NewWorkerPool creates a new worker pool and start the workers.
func NewWorkerPool(ctx context.Context, params *WorkerPoolParams, st *stats.Stats) *WorkerPool {
	ses, err := NewAwsSession(params.Retries, "")
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
		awsSession:  ses,
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
				verboseLog("Channel closed %d", id)
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
					job.Notify(false)
					job = job.failCommand
				} else {
					if acceptableErr != ErrDisplayedHelp {
						job.PrintOK(acceptableErr)
					}
					job.Notify(true)
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
