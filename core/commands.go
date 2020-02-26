package core

import (
	"github.com/peak/s5cmd/op"
)

type commandFunc func(*Job, *WorkerParams) *JobResponse

var globalCmdRegistry = map[op.Operation]commandFunc{
	op.LocalCopy:   LocalCopy,
	op.LocalDelete: LocalDelete,
	op.Download:    S3Download,
	op.AliasGet:    S3Download,
	op.Upload:      S3Upload,
	op.List:        S3List,
	op.ListBuckets: S3ListBuckets,
	op.Size:        S3Size,
	op.Copy:        S3Copy,
	op.Delete:      S3Delete,
	op.BatchDelete: S3BatchDelete,
	op.ShellExec:   ShellExec,
	op.Abort:       ShellAbort,
}
