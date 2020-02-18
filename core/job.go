package core

import (
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
	sourceDesc     string
	command        string
	operation      op.Operation
	args           []*JobArgument
	opts           opt.OptionList
	successCommand *Job
	failCommand    *Job
	subJobData     *subjobStatsType
	isSubJob       bool
	response       *JobResponse
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
		cls = storage.ObjectStorageClassReducedRedundancy
	} else if j.opts.Has(opt.IA) {
		cls = storage.TransitionStorageClassStandardIa
	} else {
		cls = storage.ObjectStorageClassStandard
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
func (j *Job) Run(wp WorkerParams) *Job {
	response := j.run(&wp)
	switch response.status {
	case statusSuccess, statusWarning:
		j.Notify(true)
		return j.successCommand
	case statusErr:
		wp.st.Increment(stats.Fail)
		j.Notify(false)
		return j.failCommand
	default:
		return nil
	}
}

type (
	wildCallback func(*storage.Object) *Job
	wildLister   func(chan<- interface{}) error
)

// wildOperation is the cornerstone of sub-job launching for S3.
//
// It will run storage.List() and creates jobs from produced items by
// running callback function. Generated jobs will be pumped to subJobQueue
// for sub-job launching.
//
// After all sub-jobs created and executed, it waits all jobs to finish.
func wildOperation(client storage.Storage, url *objurl.ObjectURL, wp *WorkerParams, callback wildCallback) error {
	var subJobCounter uint32
	subjobStats := subjobStatsType{}

	for item := range client.List(wp.ctx, url, storage.ListAllItems) {
		if item.Err != nil {
			verboseLog("wildOperation lister is done with error: %v", item.Err)
			continue
		}

		j := callback(item)
		if j != nil {
			j.subJobData = &subjobStats
			subjobStats.Add(1)
			subJobCounter++
			select {
			case *wp.subJobQueue <- j:
			case <-wp.ctx.Done():
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

// TODO: Remove this function after implementing file storage
// wildOperationLocal is the cornerstone of sub-job launching for local filesystem.
//
// It will run lister() when ready and expect data from ch. On EOF, a single
// nil should be passed into ch. Data received from ch will be passed to
// callback() which in turn will create a *Job entry (or nil for no job)
// Then this entry is submitted to the subJobQueue chan.
//
// After lister() completes, the sub-jobs are tracked
// The fn will return when all jobs are processed, and it will return with
// error if even a single sub-job was not successful
//
// Midway-failing lister() fns are not thoroughly tested and may hang or panic.
func wildOperationLocal(wp *WorkerParams, lister wildLister, callback func(interface{}) *Job) error {
	itemCh := make(chan interface{})
	doneCh := make(chan struct{})

	subjobStats := subjobStatsType{} // Tally successful and total processed sub-jobs here
	var subJobCounter uint32         // number of total subJobs issued

	// This goroutine will read ls results from ch and issue new subJobs
	go func() {
		defer close(doneCh)

		// If channel closed early: err returned from s3list?
		for data := range itemCh {
			j := callback(data)
			if j != nil {
				j.subJobData = &subjobStats
				subjobStats.Add(1)
				subJobCounter++
				select {
				case *wp.subJobQueue <- j:
				case <-wp.ctx.Done():
					return
				}
			}
			if data == nil {
				// End of listing
				return
			}
		}
	}()

	// Do the actual work
	err := lister(itemCh)
	if err == nil {
		verboseLog("wildOperation lister is done without error")
		// This select ensures that we don't return to the main loop without
		// completely getting the list results (and queueing up operations on
		// subJobQueue)
		<-doneCh
		verboseLog("wildOperation all subjobs sent")

		doneCh := make(chan struct{})
		go func() {
			subjobStats.Wait() // Wait for all jobs to finish
			close(doneCh)
		}()

		// Block until waitgroup is finished or we're cancelled (then it won't finish)
		select {
		case <-doneCh:
		case <-wp.ctx.Done():
		}

		s := atomic.LoadUint32(&(subjobStats.numSuccess))
		verboseLog("wildOperation all subjobs finished: %d/%d", s, subJobCounter)

		if s != subJobCounter {
			err = fmt.Errorf("not all jobs completed successfully: %d/%d", s, subJobCounter)
		}
	} else {
		verboseLog("wildOperation lister is done with error: %v", err)
	}
	close(itemCh)
	return err
}
