package core

import (
	"context"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

type producerFunc func(*Command, ...*objurl.ObjectURL) *Job

type producerOp struct {
	fn       producerFunc
	fullScan bool
}

var producerRegistry = map[op.Operation]producerOp{
	op.BatchDownload:  {S3BatchDownload, false},
	op.AliasBatchGet:  {S3BatchDownload, false},
	op.BatchCopy:      {S3BatchCopy, false},
	op.BatchDelete:    {S3BatchDelete, true},
	op.BatchUpload:    {BatchLocalUpload, false},
	op.BatchLocalCopy: {BatchLocalCopy, false},
}

type Producer struct {
	newClient ClientFunc
	runJob    func(*Job)
}

func (p *Producer) Run(ctx context.Context, command *Command) {
	if command.IsBatch() {
		p.batchProduce(ctx, command)
		return
	}

	job := command.toJob()
	p.runJob(job)
}

func (p *Producer) batchProduce(ctx context.Context, command *Command) {
	producerOp, ok := producerRegistry[command.operation]
	if !ok {
		return
	}
	if producerOp.fullScan {
		p.fullScan(ctx, command, producerOp.fn)
	} else {
		p.lookup(ctx, command, producerOp.fn)
	}
}

func (p *Producer) fullScan(ctx context.Context, command *Command, fn producerFunc) {
	// TODO(os): handle errors
	client, _ := p.newClient(command.src)
	isRecursive := command.opts.Has(opt.Recursive)

	var urls []*objurl.ObjectURL
	for object := range client.List(ctx, command.src, isRecursive, storage.ListAllItems) {
		if object.Err != nil || object.Mode.IsDir() {
			continue
		}

		urls = append(urls, object.URL)
	}

	job := fn(command, urls...)
	p.runJob(job)
}

func (p *Producer) lookup(ctx context.Context, command *Command, fn producerFunc) {
	// TODO(os): handle errors
	client, _ := p.newClient(command.src)
	isRecursive := command.opts.Has(opt.Recursive)

	for object := range client.List(ctx, command.src, isRecursive, storage.ListAllItems) {
		if object.Err != nil || object.Mode.IsDir() {
			continue
		}

		job := fn(command, object.URL)
		p.runJob(job)
	}
}
