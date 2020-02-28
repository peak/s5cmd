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
	op.BatchDelete: BatchDelete,
	op.Download:    Download,
	op.AliasGet:    Download,
	op.Upload:      Upload,
	op.List:        List,
	op.ListBuckets: ListBuckets,
	op.Size:        Size,
}
