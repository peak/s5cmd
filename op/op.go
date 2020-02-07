// Package op defines types of operations and their accepted options.
package op

import (
	"fmt"

	"github.com/peak/s5cmd/opt"
)

// Operation is a type of our operations.
type Operation int

// List of Operations
const (
	Abort             Operation = iota // Abort program
	Download                           // Download from S3 to local
	BatchDownload                      // Batch download from S3 to local
	Upload                             // Upload from local to S3
	BatchUpload                        // Batch upload from local to S3
	Copy                               // Copy from S3 to S3
	BatchCopy                          // Batch copy from S3 to S3
	Delete                             // Delete from S3
	Size                               // List S3 and get object sizes
	BatchDelete                        // "ls" and submit batched multi-delete operations
	BatchDeleteActual                  // AWS deleteObjects call
	List                               // List S3 objects
	ListBuckets                        // List S3 buckets
	LocalCopy                          // Copy from local to local
	BatchLocalCopy                     // Batch copy from local to local
	LocalDelete                        // Delete local file
	ShellExec                          // Execute shell command
	AliasGet                           // Alias for Download
	AliasBatchGet                      // Alias for BatchDownload
)

// IsBatch returns true if this operation creates sub-jobs
func (o Operation) IsBatch() bool {
	return o == BatchDownload || o == BatchUpload || o == BatchDelete || o == BatchLocalCopy || o == BatchCopy || o == AliasBatchGet
}

// IsInternal returns true if this operation is considered internal. Internal operations are not shown in +OK messages
func (o Operation) IsInternal() bool {
	return o == BatchDeleteActual
}

func (o Operation) String() string {
	switch o {
	case Abort:
		return "abort"
	case Download:
		return "download"
	case BatchDownload:
		return "batch-download"
	case Upload:
		return "upload"
	case BatchUpload:
		return "batch-upload"
	case Copy:
		return "copy"
	case BatchCopy:
		return "batch-copy"
	case Delete:
		return "delete"
	case BatchDelete:
		return "batch-delete"
	case BatchDeleteActual:
		return "batch-delete-actual"
	case ListBuckets:
		return "ls-buckets"
	case List:
		return "ls"
	case Size:
		return "du"
	case LocalCopy:
		return "local-copy"
	case BatchLocalCopy:
		return "batch-local-copy"
	case LocalDelete:
		return "local-delete"
	case ShellExec:
		return "shell-exec"
	case AliasGet:
		return "get"
	case AliasBatchGet:
		return "batch-get"
	}

	return fmt.Sprintf("Unknown:%d", o)
}

// Describe returns string description of the Operation given a specific OptionList
func (o Operation) Describe(l opt.OptionList) string {
	switch o {
	case Abort:
		return "Exit program"
	case Download, AliasGet:
		if l.Has(opt.DeleteSource) {
			return "Download from S3 and delete source objects"
		}
		return "Download from S3"
	case BatchDownload, AliasBatchGet:
		if l.Has(opt.DeleteSource) {
			return "Batch download from S3 and delete source objects"
		}
		return "Batch download from S3"
	case Upload:
		if l.Has(opt.DeleteSource) {
			return "Upload to S3 and delete source files"
		}
		return "Upload to S3"
	case BatchUpload:
		if l.Has(opt.DeleteSource) {
			return "Batch upload to S3 and delete source files"
		}
		return "Batch upload to S3"
	case Copy:
		if l.Has(opt.DeleteSource) {
			return "Move S3 object"
		}
		return "Copy S3 object"
	case BatchCopy:
		if l.Has(opt.DeleteSource) {
			return "Batch move S3 objects"
		}
		return "Batch copy S3 objects"
	case Delete:
		return "Delete from S3"
	case BatchDelete:
		return "Batch delete from S3"
	case ListBuckets:
		return "List buckets"
	case List:
		return "List objects"
	case Size:
		return "Count objects and size"
	case LocalCopy:
		if l.Has(opt.DeleteSource) {
			return "Move local files"
		}
		return "Copy local files"
	case BatchLocalCopy:
		if l.Has(opt.DeleteSource) {
			return "Batch move local files"
		}
		return "Batch copy local files"
	case LocalDelete:
		return "Delete local files"
	case ShellExec:
		return "Arbitrary shell-execute"
	}

	return fmt.Sprintf("Unknown:%d", o)
}

// GetAcceptedOpts returns an opt.OptionList of optional parameters for a specific Operation
func (o Operation) GetAcceptedOpts() *opt.OptionList {
	l := opt.OptionList{opt.Help}

	switch o {
	case Download, Upload, Copy, LocalCopy, BatchDownload, BatchUpload, BatchLocalCopy, BatchCopy, AliasGet, AliasBatchGet:
		l = append(l, opt.IfNotExists, opt.IfSizeDiffers, opt.IfSourceNewer)
	}

	switch o {
	case BatchDownload, BatchUpload, BatchLocalCopy, BatchCopy, AliasBatchGet:
		l = append(l, opt.Parents)
	}

	switch o {
	case Upload, BatchUpload, Copy, BatchCopy:
		l = append(l, opt.RR, opt.IA)
	}

	switch o {
	case BatchLocalCopy:
		l = append(l, opt.Recursive)
	}

	switch o {
	case List:
		l = append(l, opt.ListETags)
	}

	switch o {
	case List, Size:
		l = append(l, opt.HumanReadable)
	}

	switch o {
	case Size:
		l = append(l, opt.GroupByClass)
	}

	return &l
}
