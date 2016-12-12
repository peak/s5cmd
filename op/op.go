// Op package defines types of operations and their accepted options.
package op

import (
	"fmt"

	"github.com/peakgames/s5cmd/opt"
)

// Operation is a type of our operations.
type Operation int

const (
	Abort             Operation = iota // Abort program
	Download                           // Download from S3 to local
	BatchDownload                      // Batch download from S3 to local
	Upload                             // Upload from local to S3
	BatchUpload                        // Batch upload from local to S3
	Copy                               // Copy from S3 to S3
	Delete                             // Delete from S3
	Size                               // List S3 and get object sizes
	BatchDelete                        // "ls" and submit batched multi-delete operations
	BatchDeleteActual                  // AWS deleteObjects call
	List                               // List S3 objects
	ListBuckets                        // List S3 buckets
	LocalCopy                          // Copy from local to local
	LocalDelete                        // Delete local file
	ShellExec                          // Execute shell command
)

// IsBatch returns true if this operation creates sub-jobs
func (o Operation) IsBatch() bool {
	return o == BatchDownload || o == BatchUpload || o == BatchDelete
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
	case LocalDelete:
		return "local-delete"
	case ShellExec:
		return "shell-exec"
	}

	return fmt.Sprintf("Unknown:%d", o)
}

// Describe returns string description of the Operation given a specific OptionList
func (o Operation) Describe(l opt.OptionList) string {
	switch o {
	case Abort:
		return "Exit program"
	case Download:
		if l.Has(opt.DeleteSource) {
			return "Download from S3 and delete source objects"
		}
		return "Download from S3"
	case BatchDownload:
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
	case LocalDelete:
		return "Delete local files"
	case ShellExec:
		return "Arbitrary shell-execute"
	}

	return fmt.Sprintf("Unknown:%d", o)
}

// GetAcceptedOpts returns an opt.OptionList of optional parameters for a specific Operation
func (o Operation) GetAcceptedOpts() *opt.OptionList {
	l := opt.OptionList{}

	switch o {
	case Download, Upload, Copy, LocalCopy, BatchDownload, BatchUpload:
		l = append(l, opt.IfNotExists)
	}

	switch o {
	case BatchDownload, BatchUpload:
		l = append(l, opt.Parents)
	}

	switch o {
	case Upload, BatchUpload, Copy:
		l = append(l, opt.RR, opt.IA)
	}

	return &l
}
