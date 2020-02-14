package core

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/s3url"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

const dateFormat = "2006/01/02 15:04:05"

// Job is our basic job type.
type Job struct {
	sourceDesc         string // Source job description which we parsed this from
	command            string // Different from operation, as multiple commands can map to the same op
	operation          op.Operation
	args               []*JobArgument
	opts               opt.OptionList
	successCommand     *Job             // Next job to run if this one is successful
	failCommand        *Job             // ... if unsuccessful
	subJobData         *subjobStatsType // WaitGroup and success counter for sub-jobs launched from this job using wildOperation()
	isSubJob           bool
	numSuccess         *uint32 // Number of affected objects (only on batch operations)
	numFails           *uint32
	numAcceptableFails *uint32
}

type subjobStatsType struct {
	sync.WaitGroup
	numSuccess uint32 // FIXME is it possible to use job.numSuccess instead?
}

// String formats the job using its command and arguments.
func (j Job) String() (s string) {
	s = j.command
	for _, a := range j.args {
		s += " " + a.arg
	}
	return
}

// MakeSubJob creates a sub-job linked to the original. sourceDesc is copied, numSuccess/numFails are linked. Returns a pointer to the new job.
func (j Job) MakeSubJob(command string, operation op.Operation, args []*JobArgument, opts opt.OptionList) *Job {
	ptr := args
	return &Job{
		sourceDesc:         j.sourceDesc,
		command:            command,
		operation:          operation,
		args:               ptr,
		opts:               opts,
		isSubJob:           true,
		numSuccess:         j.numSuccess,
		numFails:           j.numFails,
		numAcceptableFails: j.numAcceptableFails,
	}
}

func (j *Job) out(short shortCode, format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...)
	fmt.Println("                   ", short, s)
	if j.numSuccess != nil && short == shortOk {
		atomic.AddUint32(j.numSuccess, 1)
	}
	if j.numAcceptableFails != nil && short == shortOkWithError {
		atomic.AddUint32(j.numAcceptableFails, 1)
	}
	if j.numFails != nil && short == shortErr {
		atomic.AddUint32(j.numFails, 1)
	}
}

// PrintOK notifies the user about the positive outcome of the job. Internal operations are not shown, sub-jobs use short syntax.
func (j *Job) PrintOK(err AcceptableError) {
	if j.operation.IsInternal() {
		return
	}

	if j.isSubJob {
		if err != nil {
			j.out(shortOkWithError, `"%s" (%s)`, j, err.Error())
		} else {
			j.out(shortOk, `"%s"`, j)
		}
		return
	}

	errStr := ""
	okStr := "OK"
	if err != nil {
		errStr = " (" + err.Error() + ")"
		okStr = "OK?"
	}

	// Add successful jobs and considered-successful (finished with AcceptableError) jobs together
	var totalSuccess uint32
	if j.numSuccess != nil {
		totalSuccess += *j.numSuccess
	}
	if j.numAcceptableFails != nil {
		totalSuccess += *j.numAcceptableFails
		if *j.numAcceptableFails > 0 {
			okStr = "OK?"
		}
	}

	if totalSuccess > 0 {
		if j.numFails != nil && *j.numFails > 0 {
			log.Printf(`+%s "%s"%s (%d, %d failed)`, okStr, j, errStr, totalSuccess, *j.numFails)
		} else {
			log.Printf(`+%s "%s"%s (%d)`, okStr, j, errStr, totalSuccess)
		}
	} else if j.numFails != nil && *j.numFails > 0 {
		log.Printf(`+%s "%s"%s (%d failed)`, okStr, j, errStr, *j.numFails)
	} else {
		log.Printf(`+%s "%s"%s`, okStr, j, errStr)
	}
}

// PrintErr prints the error response from a Job.
func (j *Job) PrintErr(err error) {
	if j.operation.IsInternal() {
		// TODO are we sure about ignoring errors from internal jobs?
		return
	}

	errStr := CleanupError(err)

	if j.isSubJob {
		j.out(shortErr, `"%s": %s`, j, errStr)
	} else {
		log.Printf(`-ERR "%s": %s`, j, errStr)
	}
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

// run runs the Job and returns error.
func (j *Job) run(wp *WorkerParams) error {
	cmdFunc, ok := globalCmdRegistry[j.operation]
	if !ok {
		return fmt.Errorf("unhandled operation %v", j.operation)
	}

	kind, err := cmdFunc(j, wp)
	return wp.st.IncrementIfSuccess(kind, err)
}

// Run runs the Job, logs the results and returns sub-job of parent job.
func (j *Job) Run(wp WorkerParams) *Job {
	err := j.run(&wp)
	if err == nil {
		j.PrintOK(nil)
		j.Notify(true)
		return j.successCommand
	}
	if acceptableErr := IsAcceptableError(err); acceptableErr != nil {
		j.PrintOK(acceptableErr)
		j.Notify(true)
		return j.successCommand
	}
	j.PrintErr(err)
	wp.st.Increment(stats.Fail)
	j.Notify(false)
	return j.failCommand
}

type (
	wildCallback func(*storage.Item) *Job
	wildLister   func(chan<- interface{}) error
)

// wildOperation is the cornerstone of sub-job launching for S3.
//
// It will run storage.List() and creates jobs from produced items by
// running callback function. Generated jobs will be pumped to subJobQueue
// for sub-job launching.
//
// After all sub-jobs created and executed, it waits all jobs to finish.
func wildOperation(url *s3url.S3Url, wp *WorkerParams, callback wildCallback) error {
	subjobStats := subjobStatsType{}
	var subJobCounter uint32

	client, err := wp.newClient()
	if err != nil {
		return err
	}

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
