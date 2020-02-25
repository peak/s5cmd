package core

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

const dateFormat = "2006/01/02 15:04:05"

// Job is the job type that is executed for each command.
type Job struct {
	opts      opt.OptionList
	operation op.Operation
	src       []*objurl.ObjectURL
	dst       *objurl.ObjectURL
	cls       string
	command   string
	response  *JobResponse
	statType  stats.StatType
}

// JobResponse is the response type.
type JobResponse struct {
	status  JobStatus
	message []string
	err     error
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

	for _, src := range j.src {
		s += " " + src.Absolute()
	}

	if j.dst != nil {
		s += " " + j.dst.Absolute()
	}
	return s
}

// Log prints the results of jobs.
func (j *Job) Log() {
	status := j.response.status
	err := j.response.err

	for _, m := range j.response.message {
		fmt.Println("                   ", status, m)
	}

	errStr := ""
	if err != nil {
		if !Verbose {
			if errors.Is(err, context.Canceled) {
				return
			}

			if storage.IsCancelationError(err) {
				return
			}
		}

		errStr = CleanupError(err)
		errStr = fmt.Sprintf(" (%s)", errStr)
	}

	if status == statusErr {
		log.Printf(`-ERR "%s": %s`, j, errStr)
		return
	}

	if j.operation.IsInternal() {
		return
	}

	m := fmt.Sprintf(`"%s"%s`, j, errStr)
	if status != statusSuccess {
		fmt.Println(status, m)
	}
}

// Run runs the Job, gets job response and logs the job status.
func (j *Job) Run(wp *WorkerParams) {
	cmdFunc, ok := globalCmdRegistry[j.operation]
	if !ok {
		log.Fatalf("unhandled operation %v", j.operation)
		return
	}

	// runner will get cmdFunc
	response := cmdFunc(j, wp)
	if response != nil {
		if response.status == statusErr {
			wp.st.Increment(stats.Fail)
		} else {
			wp.st.Increment(j.statType)
		}
		j.response = response
		j.Log()
	}
}
