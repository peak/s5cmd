package core

import (
	"context"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

type producerFunc func(*Command, *objurl.ObjectURL) *Job

var producerRegistry = map[op.Operation]producerFunc{
	op.BatchDownload:  S3BatchDownload,
	op.AliasBatchGet:  S3BatchDownload,
	op.BatchCopy:      S3BatchCopy,
	op.BatchUpload:    BatchLocalUpload,
	op.BatchLocalCopy: BatchLocalCopy,
}

type Producer struct {
	runJob func(context.Context, *Job)
}

func (p *Producer) Run(ctx context.Context, command *Command) {
	if command.IsBatch() && !command.SupportsAggregation() {
		p.batchProduce(ctx, command)
		return
	}

	job := command.toJob()
	p.runJob(ctx, job)
}

func (p *Producer) batchProduce(ctx context.Context, command *Command) {
	fn, ok := producerRegistry[command.operation]
	if !ok {
		return
	}

	src := command.args[0]

	// TODO(os): handle errors
	client, _ := storage.NewClient(src)
	isRecursive := command.opts.Has(opt.Recursive)

	for object := range client.List(ctx, src, isRecursive, storage.ListAllItems) {
		// TODO(ig): log error
		if object.Err != nil || object.Mode.IsDir() {
			continue
		}

		job := fn(command, object.URL)
		p.runJob(ctx, job)
	}
}
