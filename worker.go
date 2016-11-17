package s5cmd

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
	"sync"
	"time"
)

type WorkerPoolParams struct {
	NumWorkers     int
	ChunkSizeBytes int64
	Retries        int
}

type WorkerPool struct {
	ctx        context.Context
	params     *WorkerPoolParams
	jobQueue   chan *Job
	wg         *sync.WaitGroup
	awsSession *session.Session
	cancelFunc context.CancelFunc
}

type WorkerParams struct {
	s3svc      *s3.S3
	s3dl       *s3manager.Downloader
	s3ul       *s3manager.Uploader
	ctx        context.Context
	poolParams *WorkerPoolParams
	stats      *Stats
}

func NewWorkerPool(ctx context.Context, params *WorkerPoolParams, stats *Stats) *WorkerPool {
	ses, err := session.NewSession(aws.NewConfig().WithMaxRetries(params.Retries))
	if err != nil {
		log.Fatal(err)
	}

	cancelFunc := ctx.Value("cancelFunc").(context.CancelFunc)

	p := &WorkerPool{
		ctx:        ctx,
		params:     params,
		jobQueue:   make(chan *Job),
		wg:         &sync.WaitGroup{},
		awsSession: ses,
		cancelFunc: cancelFunc,
	}

	for i := 0; i < params.NumWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(stats)
	}

	return p
}

func (p *WorkerPool) runWorker(stats *Stats) {
	defer p.wg.Done()

	wp := WorkerParams{
		s3.New(p.awsSession),
		// Give each worker its own s3manager
		s3manager.NewDownloader(p.awsSession),
		s3manager.NewUploader(p.awsSession, func(u *s3manager.Uploader) {
			u.PartSize = p.params.ChunkSizeBytes
		}),
		p.ctx,
		p.params,
		stats,
	}

	bkf := backoff.NewExponentialBackOff()
	bkf.InitialInterval = time.Second
	bkf.MaxInterval = time.Minute
	bkf.Multiplier = 2
	bkf.Reset()

	run := true
	for run {
		select {
		case job, ok := <-p.jobQueue:
			if !ok { // channel closed
				run = false
				break
			}
			tries := 0
			for job != nil {
				err := job.Run(&wp)
				if err != nil {
					if IsRatelimitError(err) && p.params.Retries > 0 && tries < p.params.Retries {
						tries++
						sleepTime := bkf.NextBackOff()
						log.Printf(`?Ratelimit "%s", sleep for %v`, job, sleepTime)
						select {
						case <-time.After(sleepTime):
							continue
						case <-p.ctx.Done():
							run = false // if Canceled during sleep, report ERR and immediately process failCommand
						}
					}

					log.Printf(`-ERR "%s": %v`, job, err)
					job = job.failCommand
				} else {
					log.Printf(`+OK "%s"`, job)
					job = job.successCommand
				}
				tries = 0
				bkf.Reset()
			}
		case <-p.ctx.Done():
			run = false
			break
		}
	}

	//log.Print("Exiting goroutine")
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

	defer close(p.jobQueue)

	s := NewCancelableScanner(p.ctx, r).Start()

	run := true
	for run {
		line, err := s.ReadOne()
		if err != nil {
			if err == context.Canceled || err == io.EOF {
				if err == io.EOF {
					p.cancelFunc()
				}
				run = false
				break
			}
			log.Printf("Error reading: %v", err)
			run = false
			break
		}

		job, err := ParseJob(line)
		if err != nil {
			log.Print(`Could not parse line "`, line, `": `, err)
			continue
		}
		select {
		case <-p.ctx.Done():
			run = false
			break
		case p.jobQueue <- job:
		}
	}

	//log.Print("Waiting...")
	p.wg.Wait()
}
