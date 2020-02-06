package core

import "github.com/peak/s5cmd/op"

type commandFunc func(*Job, *WorkerParams) error

type commandRegistry struct {
	m map[op.Operation]commandFunc
}

var globalCmdRegistry = map[op.Operation]commandFunc{
	op.LocalDelete:       LocalDelete,
	op.LocalCopy:         LocalCopy,
	op.ShellExec:         ShellExec,
	op.Copy:              S3Copy,
	op.BatchLocalCopy:    BatchLocalCopy,
	op.Delete:            S3Delete,
	op.BatchDelete:       S3BatchDelete,
	op.BatchDeleteActual: S3BatchDeleteActual,
	op.BatchDownload:     S3BatchDownload,
	op.AliasBatchGet:     S3BatchDownload,
	op.BatchUpload:       BatchUpload,
	op.Download:          S3Download,
	op.AliasGet:          S3Download,
	op.Upload:            S3Upload,
	op.BatchCopy:         S3BatchCopy,
	op.ListBuckets:       S3ListBuckets,
	op.List:              S3List,
	op.Size:              S3Size,
	op.Abort:             ShellAbort,
}
