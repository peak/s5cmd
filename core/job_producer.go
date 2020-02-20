package core

import (
	"context"
	"fmt"

	"github.com/peak/s5cmd/op"

	"github.com/peak/s5cmd/storage"
)

type producerFunc func(command *Command, client storage.Storage, object *storage.Object) *Job

var producerRegistry = map[op.Operation]producerFunc{
	op.BatchLocalCopy:    BatchLocalCopy,
	op.BatchUpload:       BatchLocalUpload,
	op.BatchDelete:       S3BatchDelete,
	op.BatchDeleteActual: S3BatchDeleteActual,
	op.BatchDownload:     S3BatchDownload,
	op.AliasBatchGet:     S3BatchDownload,
	op.BatchCopy:         S3BatchCopy,
}

type Producer struct {
	newClient  ClientFunc
	batchSize  int
	enqueueJob func(*Job)
}

func (p *Producer) Produce(ctx context.Context, command *Command) error {
	client, err := p.newClient()
	if err != nil {
		return err
	}

	if command.IsBatch() {
		return p.batchProduce(ctx, command, client)
	}

	job := command.makeJob(client, command.src, command.dst)
	p.enqueueJob(job)
	return nil
}

func (p *Producer) batchProduce(ctx context.Context, command *Command, client storage.Storage) error {
	var err error
	for object := range client.List(ctx, command.src, storage.ListAllItems) {
		if object.Err != nil {
			err = object.Err
			continue
		}

		if object.IsMarkerObject() {
			continue
		}

		producerFunc, ok := producerRegistry[command.operation]
		if !ok {
			fmt.Println("not ok")
		}

		job := producerFunc(command, client, object)
		p.enqueueJob(job)
	}

	return err
}
