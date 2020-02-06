package core

import "github.com/peak/s5cmd/op"

type commandFunc func(*Job, *WorkerParams) error

var globalCmdRegistry = map[op.Operation]commandFunc{
	op.LocalCopy:         LocalCopy,
	op.LocalDelete:       LocalDelete,
	op.BatchLocalCopy:    BatchLocalCopy,
	op.BatchUpload:       BatchLocalUpload,
	op.Download:          S3Download,
	op.AliasGet:          S3Download,
	op.Upload:            S3Upload,
	op.List:              S3List,
	op.ListBuckets:       S3ListBuckets,
	op.Size:              S3Size,
	op.Copy:              S3Copy,
	op.Delete:            S3Delete,
	op.BatchDelete:       S3BatchDelete,
	op.BatchDeleteActual: S3BatchDeleteActual,
	op.BatchDownload:     S3BatchDownload,
	op.AliasBatchGet:     S3BatchDownload,
	op.BatchCopy:         S3BatchCopy,
	op.ShellExec:         ShellExec,
	op.Abort:             ShellAbort,
}
