package core

import (
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
	client    storage.Storage
	opts      opt.OptionList
	operation op.Operation
	src       *objurl.ObjectURL
	dst       *objurl.ObjectURL
	cls       string
	command   string
	response  *JobResponse
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
	s += " " + j.src.Absolute()
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
		errStr = CleanupError(err)
	}

	if status == statusErr {
		log.Printf(`-ERR "%s": %s`, j, errStr)
		return
	}

	okStr := "OK"
	if status == statusWarning {
		errStr = fmt.Sprintf(" (%s)", errStr)
		okStr = "OK?"
	}

	log.Printf(`+%s "%s"%s`, okStr, j, errStr)
}

// Run runs the Job and returns error.
func (j *Job) run(wp *WorkerParams) *JobResponse {
	cmdFunc, ok := globalCmdRegistry[j.operation]
	if !ok {
		fmt.Printf("unsupported op.... %s", j.operation)
		return &JobResponse{
			status: statusErr,
			err:    fmt.Errorf("unhandled operation %v", j.operation),
		}
	}

	// runner will get cmdFunc
	response := cmdFunc(wp.ctx, j)
	if response != nil {
		j.response = response
		j.Log()
	}
	return response
}

// Run runs the Job, logs the results and returns sub-job of parent job.
func (j *Job) Run(wp WorkerParams) {
	response := j.run(&wp)
	switch response.status {
	case statusSuccess, statusWarning:
		return
	case statusErr:
		wp.st.Increment(stats.Fail)
		return
	}
}
