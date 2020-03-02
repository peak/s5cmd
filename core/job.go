package core

import (
	"context"
	"errors"
	stdlog "log"

	"github.com/hashicorp/go-multierror"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

const dateFormat = "2006/01/02 15:04:05"

type Runnable interface {
	Run(ctx context.Context)
}

// Job is the job type that is executed for each command.
type Job struct {
	opts         opt.OptionList
	operation    op.Operation
	args         []*objurl.ObjectURL
	storageClass storage.StorageClass
	command      string
	statType     stats.StatType
}

// JobResponse is the response type.
type JobResponse struct {
	status JobStatus
	err    error
}

// jobResponse creates a new JobResponse by setting job status, message and error.
func jobResponse(err error) *JobResponse {
	if err == nil {
		return &JobResponse{status: statusSuccess}
	}
	return &JobResponse{status: statusErr, err: err}
}

// String formats the job using its command and arguments.
func (j Job) String() string {
	s := j.command

	for _, src := range j.args {
		s += " " + src.Absolute()
	}

	return s
}

// Run runs the Job, gets job response and logs the job status.
func (j *Job) Run(ctx context.Context) {
	cmdFunc, ok := globalCmdRegistry[j.operation]
	if !ok {
		// TODO(ig): log and continue
		stdlog.Fatalf("unhandled operation %v", j.operation)
		return
	}

	response := cmdFunc(ctx, j)
	if response == nil {
		return
	}

	switch response.status {
	case statusErr:
		stats.Increment(stats.Fail)
		log.Logger.Error("%q: %v", j, response.err)
	case statusWarning:
		log.Logger.Warning("%q (%v)", j, response.err)
		fallthrough
	default:
		stats.Increment(j.statType)
	}
}

func isCancelationError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}

	if storage.IsCancelationError(err) {
		return true
	}

	merr, ok := err.(*multierror.Error)
	if !ok {
		return false
	}

	for _, err := range merr.Errors {
		if isCancelationError(err) {
			return true
		}
	}

	return false
}
