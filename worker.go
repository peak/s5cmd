package s5cmd

import (
	"context"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"os"
	"sync"
)

type WorkerPoolParams struct {
	NumWorkers int
}

type WorkerPool struct {
	ctx        context.Context
	params     *WorkerPoolParams
	jobQueue   chan *Job
	wg         *sync.WaitGroup
	awsSession *session.Session
	cancelFunc context.CancelFunc
}

func NewWorkerPool(ctx context.Context, params *WorkerPoolParams) *WorkerPool {
	ses, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFunc := context.WithCancel(ctx)

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
		go p.runWorker()
	}

	return p
}

func (p *WorkerPool) runWorker() {
	defer p.wg.Done()

	s3svc := s3.New(p.awsSession)

	run := true
	for run {
		select {
		case job, ok := <-p.jobQueue:
			if !ok { // channel closed
				run = false
				break
			}
			for job != nil {
				err := job.Run(s3svc)
				if err != nil {
					log.Printf(`-ERR "%s": %v`, job, err)
					job = job.failCommand
				} else {
					log.Printf(`+OK "%s"`, job)
					job = job.successCommand
				}
			}
		case <-p.ctx.Done():
			run = false
			break
		}
	}

	//log.Println("Exiting goroutine")
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
		p.jobQueue <- job
	}

	//log.Print("Waiting...")
	p.wg.Wait()
}
