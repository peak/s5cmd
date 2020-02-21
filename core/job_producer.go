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

func (p *Producer) Produce(ctx context.Context, command *Command) error {
	if command.IsBatch() {
		return p.batchProduce(ctx, command)
	}

	job := command.toJob()
	p.enqueueJob(job)
	return nil
}

func (p *Producer) batchProduce(ctx context.Context, command *Command) error {
	var err error
	for object := range p.client.List(ctx, command.src, storage.ListAllItems) {
		if object.Err != nil {
			err = object.Err
			continue
		}

		if object.IsMarkerObject() {
			continue
		}

		producerFunc, ok := producerRegistry[command.operation]
		if ok {
			job := producerFunc(command, object)
			p.enqueueJob(job)
		}
	}

	return err
}
