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
}

func NewWorkerPool(ctx context.Context, params *WorkerPoolParams) *WorkerPool {
	ses, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}

	p := &WorkerPool{
		ctx:        ctx,
		params:     params,
		jobQueue:   make(chan *Job),
		wg:         &sync.WaitGroup{},
		awsSession: ses,
	}

	p.wg.Add(1)
	for i := 0; i < params.NumWorkers; i++ {
		go p.runWorker()
	}

	return p
}

func (p *WorkerPool) runWorker() {
	defer p.wg.Done()

	s3svc := s3.New(p.awsSession)

	for {
		select {
		case job, ok := <-p.jobQueue:
			if !ok { // channel closed
				break
			}
			if job == nil {
				continue
			}
			err := job.Run(s3svc)
			if err != nil {
				log.Printf(`-ERR "%s": %v`, job, err)
			} else {
				log.Printf(`+OK "%s"`, job)
			}
		case <-p.ctx.Done():
			break
		}
	}

	log.Println("Exiting goroutine")
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

	for {
		line, err := s.ReadOne()
		if err != nil {
			if err == context.Canceled || err == io.EOF {
				break
			}
			log.Printf("Error reading: %v", err)
			break
		}

		job, err := ParseJob(line)
		if err != nil {
			log.Print(`Could not parse line "`, line, `": `, err)
			continue
		}
		p.jobQueue <- job
	}
}
