package core

import (
	"context"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/storage"
)

type producerFunc func(command *Command, object *storage.Object) *Job

var producerRegistry = map[op.Operation]producerFunc{
	op.BatchDownload: S3BatchDownload,
	op.AliasBatchGet: S3BatchDownload,
	op.BatchCopy:     S3BatchCopy,
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
	for object := range p.client.List(ctx, command.src, storage.ListAllItems) {
		// TODO(os): handle error
		if object.Err != nil || object.IsMarkerObject() {
			continue
		}

		producerFunc, ok := producerRegistry[command.operation]
		if ok {
			job := producerFunc(command, object)
			p.enqueueJob(job)
		}
	}
}
