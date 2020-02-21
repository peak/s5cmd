package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

const dateFormat = "2006/01/02 15:04:05"

// Job is the job type that is executed for each command.
type Job struct {
	sourceDesc string
	command    string
	operation  op.Operation
	args       []*JobArgument
	opts       opt.OptionList
	subJobData *subjobStatsType
	isSubJob   bool
	response   *JobResponse
}

// JobResponse is the response type.
type JobResponse struct {
	status  JobStatus
	message []string
	err     error
}

type subjobStatsType struct {
	sync.WaitGroup
	numSuccess uint32 // FIXME is it possible to use job.numSuccess instead?
}

// String formats the job using its command and arguments.
func (j Job) String() string {
	s := j.command
	for _, a := range j.args {
		s += " " + a.url.Absolute()
	}
	return s
}

// jobResponse creates a new JobResponse by setting job status, message and error.
func jobResponse(err error, msg ...string) *JobResponse {
	if err == nil {
		return &JobResponse{status: statusSuccess, message: msg}
	}
	return &JobResponse{status: statusErr, err: err, message: msg}
}

// MakeSubJob creates a sub-job linked to the original. sourceDesc is copied, numSuccess/numFails are linked. Returns a pointer to the new job.
func (j Job) MakeSubJob(command string, operation op.Operation, args []*JobArgument, opts opt.OptionList) *Job {
	ptr := args
	return &Job{
		sourceDesc: j.sourceDesc,
		command:    command,
		operation:  operation,
		args:       ptr,
		opts:       opts,
		isSubJob:   true,
	}
}

// Log prints the results of jobs.
func (j *Job) Log() {
	status := j.response.status
	err := j.response.err

	for _, m := range j.response.message {
		fmt.Println("                   ", status, m)
	}

	if j.operation.IsInternal() {
		return
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
	}

	if j.isSubJob {
		m := fmt.Sprintf(`"%s"`, j)
		if err != nil {
			m += fmt.Sprintf(`(%s)`, errStr)
		}
		fmt.Println("                   ", status, m)
		return
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

func (j *Job) getStorageClass() string {
	var cls string
	if j.opts.Has(opt.RR) {
		cls = string(storage.ObjectStorageClassReducedRedundancy)
	} else if j.opts.Has(opt.IA) {
		cls = string(storage.TransitionStorageClassStandardIA)
	} else {
		cls = string(storage.ObjectStorageClassStandard)
	}
	return cls
}

// Notify informs the parent/issuer job if the job succeeded or failed.
func (j *Job) Notify(success bool) {
	if j.subJobData == nil {
		return
	}
	if success {
		atomic.AddUint32(&(j.subJobData.numSuccess), 1)
	}
	j.subJobData.Done()
}

// displayHelp displays help text.
func (j *Job) displayHelp() {
	fmt.Fprintf(os.Stderr, "%v\n\n", UsageLine())

	cl, opts, cnt := CommandHelps(j.command)

	if ol := opt.OptionHelps(opts); ol != "" {
		fmt.Fprintf(os.Stderr, "\"%v\" command options:\n", j.command)
		fmt.Fprint(os.Stderr, ol)
		fmt.Fprint(os.Stderr, "\n\n")
	}

	if cnt > 1 {
		fmt.Fprintf(os.Stderr, "Help for \"%v\" commands:\n", j.command)
	}
	fmt.Fprint(os.Stderr, cl)
	fmt.Fprint(os.Stderr, "\nTo list available general options, run without arguments.\n")
}

// Run runs the Job and returns error.
func (j *Job) run(wp *WorkerParams) *JobResponse {
	cmdFunc, ok := globalCmdRegistry[j.operation]
	if !ok {
		return &JobResponse{
			status: statusErr,
			err:    fmt.Errorf("unhandled operation %v", j.operation),
		}
	}

	kind, response := cmdFunc(j, wp)
	if response != nil {
		wp.st.IncrementIfSuccess(kind, response.err)
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
		j.Notify(true)
		return
	case statusErr:
		wp.st.Increment(stats.Fail)
		j.Notify(false)
		return
	}
}

type makeJobFunc func(*storage.Object) *Job

// wildOperation is the cornerstone of sub-job launching for S3.
//
// It will run storage.List() and creates jobs from produced items by
// running makeJob function. Generated jobs will be send to subJobQueue
// for sub-job launching.
//
// After all sub-jobs created and executed, it waits all jobs to finish.
func wildOperation(
	client storage.Storage,
	url *objurl.ObjectURL,
	isRecursive bool,
	wp *WorkerParams,
	makeJob makeJobFunc,
) error {
	var subJobCounter uint32
	subjobStats := subjobStatsType{}

	for object := range client.List(wp.ctx, url, isRecursive, storage.ListAllItems) {
		if object.Err != nil {
			verboseLog("wildcard: listing has error: %v", object.Err)
			continue
		}

		job := makeJob(object)
		if job != nil {
			job.subJobData = &subjobStats
			subjobStats.Add(1)
			subJobCounter++
			select {
			case *wp.subJobQueue <- job:
			case <-wp.ctx.Done():
				subjobStats.Done()
				break
			}
		}
	}

	subjobStats.Wait()

	s := atomic.LoadUint32(&(subjobStats.numSuccess))
	verboseLog("wildOperation all subjobs finished: %d/%d", s, subJobCounter)

	if s != subJobCounter {
		return fmt.Errorf("not all jobs completed successfully: %d/%d", s, subJobCounter)
	}
	return nil
}
