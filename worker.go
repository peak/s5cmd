package main

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cenkalti/backoff"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerPoolParams struct {
	NumWorkers     uint32
	ChunkSizeBytes int64
	Retries        int
}

type WorkerPool struct {
	ctx           context.Context
	params        *WorkerPoolParams
	jobQueue      chan *Job
	subJobQueue   chan *Job
	wg            *sync.WaitGroup
	awsSession    *session.Session
	cancelFunc    context.CancelFunc
	stats         *Stats
	idlingCounter int32
}

type WorkerParams struct {
	s3svc         *s3.S3
	s3dl          *s3manager.Downloader
	s3ul          *s3manager.Uploader
	ctx           context.Context
	poolParams    *WorkerPoolParams
	stats         *Stats
	subJobQueue   *chan *Job
	idlingCounter *int32
}

func NewWorkerPool(ctx context.Context, params *WorkerPoolParams, stats *Stats) *WorkerPool {
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
		stats:         stats,
		idlingCounter: int32(params.NumWorkers),
	}

	var i uint32
	for i = 0; i < params.NumWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(stats, &p.idlingCounter, int(i))
	}

	return p
}

func (p *WorkerPool) runWorker(stats *Stats, idlingCounter *int32, id int) {
	defer p.wg.Done()
	//defer log.Print("# Exiting goroutine", id)

	wp := WorkerParams{
		s3.New(p.awsSession),
		// Give each worker its own s3manager
		s3manager.NewDownloader(p.awsSession),
		s3manager.NewUploader(p.awsSession),
		p.ctx,
		p.params,
		stats,
		&p.subJobQueue,
		idlingCounter,
	}

	bkf := backoff.NewExponentialBackOff()
	bkf.InitialInterval = time.Second
	bkf.MaxInterval = time.Minute
	bkf.Multiplier = 2
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

	for run {
		setWorking()
		select {
		case <-time.After(1 * time.Second):
			setIdle()

		case job, ok := <-p.jobQueue:
			if !ok {
				//log.Print("Channel closed", id)
				goto endfor
			}

			tries := 0
			for job != nil {
				err := job.Run(&wp)
				if err != nil {
					errCode, doRetry := IsRetryableError(err)
					if doRetry && p.params.Retries > 0 && tries < p.params.Retries {
						tries++
						sleepTime := bkf.NextBackOff()
						log.Printf(`?%s "%s", sleep for %v`, errCode, job, sleepTime)
						select {
						case <-time.After(sleepTime):
							wp.stats.Increment(StatsRetryOp)
							continue
						case <-p.ctx.Done():
							run = false // if Canceled during sleep, report ERR and immediately process failCommand
						}
					}

					log.Printf(`-ERR "%s": %s`, job, CleanupError(err))
					wp.stats.Increment(StatsFail)
					job.Notify(p.ctx, err)
					job = job.failCommand
				} else {
					job.PrintOK()
					job.Notify(p.ctx, nil)
					job = job.successCommand
				}
				tries = 0
				bkf.Reset()
			}

		case <-p.ctx.Done():
			goto endfor
		}
	}
endfor:
	setIdle()
}

func (p *WorkerPool) parseJob(line string) *Job {
	job, err := ParseJob(line)
	if err != nil {
		log.Print(`-ERR "`, line, `": `, err)
		p.stats.Increment(StatsFail)
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
		case <-time.After(2 * time.Second):
			idc = atomic.LoadInt32(&p.idlingCounter)
			if idc == int32(p.params.NumWorkers) {
				log.Print("# All workers idle, finishing up...")
				return
			}
		case <-p.ctx.Done():
			return
		case j, ok := <-p.subJobQueue:
			if ok {
				p.jobQueue <- j
			}
		}
	}
}

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
