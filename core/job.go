package core

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/hashicorp/go-multierror"

	"github.com/peak/s5cmd/flags"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

var Stdout = make(chan message, 10000)

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
	response     *JobResponse
	statType     stats.StatType
}

// JobResponse is the response type.
type JobResponse struct {
	status  JobStatus
	message []string
	err     error
}

type message struct {
	job    string
	status JobStatus
	s      string
	err    error
}

func (m message) String() string {
	if m.status == statusSuccess {
		return fmt.Sprint("                   ", m.status, m.s)
	}

	errStr := ""
	if m.err != nil {
		if !*flags.Verbose && isCancelationError(m.err) {
			return ""
		}

		errStr = CleanupError(m.err)
		errStr = fmt.Sprintf(" (%s)", errStr)
	}

	if m.status == statusErr {
		return fmt.Sprintf(`-ERR "%s": %s`, m.job, errStr)
	}

	return fmt.Sprintf(`"%v%s"%s`, m.status, m.job, errStr)
}

func sendMessage(ctx context.Context, msg message) {
	select {
	case <-ctx.Done():
	case Stdout <- msg:
	}
}

func newMessage(date, storageclass, etag, size, url string) message {
	return message{
		status: statusSuccess,
		s: fmt.Sprintf(
			"%19s %1s %-38s  %12s  %s",
			date,
			storageclass,
			etag,
			size,
			url,
		),
	}
}

// jobResponse creates a new JobResponse by setting job status, message and error.
func jobResponse(err error, msg ...string) *JobResponse {
	if err == nil {
		return &JobResponse{status: statusSuccess, message: msg}
	}
	return &JobResponse{status: statusErr, err: err, message: msg}
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
		log.Fatalf("unhandled operation %v", j.operation)
		return
	}

	response := cmdFunc(ctx, j)
	if response != nil {
		if response.status == statusErr {
			stats.Increment(stats.Fail)
		} else {
			stats.Increment(j.statType)
		}
		j.response = response
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
