package core

import (
	"context"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
)

func MakeBucket(ctx context.Context, job *Job) *JobResponse {
	bucket := job.args[0]

	client, err := storage.NewClient(bucket)
	if err != nil {
		return jobResponse(err)
	}

	err = client.MakeBucket(ctx, bucket.Bucket)
	if err != nil {
		return jobResponse(err)
	}

	log.Logger.Info(InfoMessage{
		Operation: job.operation.String(),
		Source:    bucket,
	})

	return jobResponse(nil)
}
