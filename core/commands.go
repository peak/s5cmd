package core

import (
	"context"

	"github.com/peak/s5cmd/op"
)

type commandFunc func(context.Context, *Job) *JobResponse

var globalCmdRegistry = map[op.Operation]commandFunc{
	op.Copy:        Copy,
	op.LocalCopy:   Copy,
	op.Delete:      Delete,
	op.LocalDelete: Delete,
	op.BatchDelete: S3BatchDelete,
	op.Download:    S3Download,
	op.AliasGet:    S3Download,
	op.Upload:      S3Upload,
	op.List:        S3List,
	op.ListBuckets: S3ListBuckets,
	op.Size:        S3Size,
}
