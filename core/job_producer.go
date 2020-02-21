package core

import (
	"context"

	"github.com/peak/s5cmd/objurl"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/storage"
)

type producerFunc func(command *Command, sources ...*objurl.ObjectURL) *Job

type producerOp struct {
	fn       producerFunc
	fullScan bool
}

var producerRegistry = map[op.Operation]producerOp{
	op.BatchDownload: {S3BatchDownload, false},
	op.AliasBatchGet: {S3BatchDownload, false},
	op.BatchCopy:     {S3BatchCopy, false},
	op.BatchDelete:   {S3BatchDelete, true},
}

type Producer struct {
	client     storage.Storage
	enqueueJob func(*Job)
}

func (p *Producer) Produce(ctx context.Context, command *Command) {
	if command.IsBatch() {
		p.batchProduce(ctx, command)
		return
	}

	job := command.toJob()
	p.enqueueJob(job)
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
	var urls []*objurl.ObjectURL
	for object := range p.client.List(ctx, command.src, storage.ListAllItems) {
		// TODO(os): handle error
		if object.Err != nil {
			continue
		}

		urls = append(urls, object.URL)
	}

	job := fn(command, urls...)
	p.enqueueJob(job)
}

func (p *Producer) lookup(ctx context.Context, command *Command, fn producerFunc) {
	for object := range p.client.List(ctx, command.src, storage.ListAllItems) {
		// TODO(os): handle error
		if object.Err != nil || object.IsMarkerObject() {
			continue
		}

		job := fn(command, object.URL)
		p.enqueueJob(job)
	}
}
