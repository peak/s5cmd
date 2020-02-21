package core

import (
	"context"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/storage"
)

type producerFunc func(command *Command, sources ...*storage.Object) *Job

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
	newClient  ClientFunc
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
	// TODO(os): handle error
	client, _ := p.newClient(command.src)

	var objects []*storage.Object
	for object := range client.List(ctx, command.src, true, storage.ListAllItems) {
		// TODO(os): handle error
		if object.Err != nil {
			continue
		}

		objects = append(objects, object)
	}

	job := fn(command, objects...)
	p.enqueueJob(job)
}

func (p *Producer) lookup(ctx context.Context, command *Command, fn producerFunc) {
	// TODO(os): handle error
	client, _ := p.newClient(command.src)

	for object := range client.List(ctx, command.src, true, storage.ListAllItems) {
		// TODO(os): handle error
		if object.Err != nil {
			continue
		}

		job := fn(command, object)
		p.enqueueJob(job)
	}
}
