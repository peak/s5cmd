package s5cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/termie/go-shutil"
	"gopkg.in/cheggaaa/pb.v1"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

const DATE_FORMAT string = "2006/01/02 15:04:05"

type JobArgument struct {
	arg string
	s3  *s3url
}

type Job struct {
	sourceDesc     string // Source job description which we parsed this from
	command        string // Different from operation, as multiple commands can map to the same op
	operation      Operation
	args           []*JobArgument
	successCommand *Job       // Next job to run if this one is successful
	failCommand    *Job       // .. if unsuccessful
	notifyChan     *chan bool // Ptr to chan to notify the job's success or fail
	isSubJob       bool
	numSuccess     *uint32 // Number of affected objects (only on batch operations)
	numFails       *uint32
}

type ShortCode int

const (
	SHORT_ERR = iota
	SHORT_OK
)

func (s ShortCode) String() string {
	if s == SHORT_OK {
		return "+"
	}
	if s == SHORT_ERR {
		return "-"
	}
	return "?"
}

func (j Job) String() (s string) {
	s = j.command
	for _, a := range j.args {
		s += " " + a.arg
	}
	//s += " # from " + j.sourceDesc
	return
}

func (j Job) MakeSubJob(command string, operation Operation, args []*JobArgument) *Job {
	ptr := args
	return &Job{
		sourceDesc: j.sourceDesc,
		command:    command,
		operation:  operation,
		args:       ptr,
		isSubJob:   true,
		numSuccess: j.numSuccess,
		numFails:   j.numFails,
	}
}

func (a JobArgument) Clone() JobArgument {
	var s s3url
	if a.s3 != nil {
		s = a.s3.Clone()
	}
	return JobArgument{a.arg, &s}
}
func (a JobArgument) Append(s string) JobArgument {
	a.arg += s
	if a.s3 != nil {
		a.s3.key += s
	}
	return a
}

func (j *Job) out(short ShortCode, format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...)
	fmt.Println("                   ", short, s)
	if j.numSuccess != nil && short == SHORT_OK {
		atomic.AddUint32(j.numSuccess, 1)
	}
	if j.numFails != nil && short == SHORT_ERR {
		atomic.AddUint32(j.numFails, 1)
	}
}

func (j *Job) PrintOK() {
	if j.operation.IsInternal() {
		return
	}
	if j.isSubJob {
		j.out(SHORT_OK, `"%s"`, j)
		return
	}

	if j.numSuccess != nil && *j.numSuccess > 0 {
		if j.numFails != nil && *j.numFails > 0 {
			log.Printf(`+OK "%s" (%d, %d failed)`, j, *j.numSuccess, *j.numFails)
		} else {
			log.Printf(`+OK "%s" (%d)`, j, *j.numSuccess)
		}
	} else if j.numFails != nil && *j.numFails > 0 {
		log.Printf(`+OK "%s" (%d failed)`, j, *j.numFails)
	} else {
		log.Printf(`+OK "%s"`, j)
	}
}

func (j *Job) Notify(ctx context.Context, err error) {
	if j.notifyChan == nil {
		return
	}
	res := err == nil
	select {
	case <-ctx.Done():
		return
	case *j.notifyChan <- res:
	}
}

func (j *Job) Run(wp *WorkerParams) error {
	//log.Printf("Running %v", j)

	switch j.operation {

	// Local operations
	case OP_LOCAL_DELETE:
		return wp.stats.IncrementIfSuccess(STATS_FILEOP, os.Remove(j.args[0].arg))

	case OP_LOCAL_MOVE:
		return wp.stats.IncrementIfSuccess(STATS_FILEOP, os.Rename(j.args[0].arg, j.args[1].arg))

	case OP_LOCAL_COPY:
		_, err := shutil.Copy(j.args[0].arg, j.args[1].arg, true)
		wp.stats.IncrementIfSuccess(STATS_FILEOP, err)
		return err

	case OP_SHELL_EXEC:
		strArgs := make([]string, 0)

		for i, a := range j.args {
			if i == 0 {
				continue
			}
			strArgs = append(strArgs, a.arg)
		}
		cmd := exec.CommandContext(wp.ctx, j.args[0].arg, strArgs...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return wp.stats.IncrementIfSuccess(STATS_SHELLOP, cmd.Run())

	// S3 operations
	case OP_COPY:
		_, err := s3copy(wp.s3svc, j.args[0].s3, j.args[1].s3)
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_MOVE:
		_, err := s3copy(wp.s3svc, j.args[0].s3, j.args[1].s3)
		wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		if err == nil {
			_, err = s3delete(wp.s3svc, j.args[0].s3)
			wp.stats.IncrementIfSuccess(STATS_S3OP, err)
			// FIXME if err != nil try to rollback by deleting j.args[1].s3 ? What if we don't have permission to delete?
		}

		return err

	case OP_DELETE:
		_, err := s3delete(wp.s3svc, j.args[0].s3)
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_BATCH_DELETE:
		var jobArgs []*JobArgument
		srcBucket := j.args[0].Clone()
		srcBucket.arg = "s3://" + srcBucket.s3.bucket

		maxArgs := 1000

		initArgs := func() {
			jobArgs = make([]*JobArgument, 0, maxArgs+1)
			jobArgs = append(jobArgs, &srcBucket)
		}

		addArg := func(key *string) *Job {
			var subJob *Job

			if jobArgs == nil {
				initArgs()
			}

			if (key == nil || len(jobArgs) == maxArgs) && len(jobArgs) > 1 {
				subJob = j.MakeSubJob("batch-rm", OP_BATCH_DELETE_ACTUAL, jobArgs)
				initArgs()
			}

			if key != nil {
				jobArgs = append(jobArgs, &JobArgument{arg: *key})
			}

			return subJob
		}

		err := s3wildOperation(j.args[0].s3, wp, func(li *s3listItem) *Job {
			if li == nil {
				return addArg(nil)
			}

			if li.isCommonPrefix {
				return nil
			}

			return addArg(li.key)
		})

		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_BATCH_DELETE_ACTUAL:
		obj := make([]*s3.ObjectIdentifier, len(j.args)-1)
		for i, a := range j.args {
			if i == 0 {
				continue
			}
			obj[i-1] = &s3.ObjectIdentifier{Key: aws.String(a.arg)}
		}
		o, err := wp.s3svc.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(j.args[0].s3.bucket),
			Delete: &s3.Delete{
				Objects: obj,
			},
		})
		for _, o := range o.Deleted {
			j.out(SHORT_OK, `Batch-delete s3://%s/%s`, j.args[0].s3.bucket, *o.Key)
		}
		for _, e := range o.Errors {
			j.out(SHORT_ERR, `Batch-delete s3://%s/%s: %s`, j.args[0].s3.bucket, *e.Key, *e.Message)
			if err != nil {
				err = errors.New(*e.Message)
			}
		}
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_BATCH_DOWNLOAD:
		dst_dir := ""
		if len(j.args) > 1 {
			dst_dir = j.args[1].arg
		}

		err := s3wildOperation(j.args[0].s3, wp, func(li *s3listItem) *Job {
			if li == nil || li.isCommonPrefix {
				return nil
			}

			arg1 := JobArgument{
				"s3://" + j.args[0].s3.bucket + "/" + *li.key,
				&s3url{j.args[0].s3.bucket, *li.key},
			}
			arg2 := JobArgument{
				dst_dir + li.parsedKey,
				nil,
			}

			j = j.MakeSubJob("get", OP_DOWNLOAD, []*JobArgument{&arg1, &arg2})
			if *li.class == s3.ObjectStorageClassGlacier {
				j.out(SHORT_ERR, `"%s": Cannot download glacier object`, j)
				return nil
			}
			dir := filepath.Dir(arg2.arg)
			os.MkdirAll(dir, os.ModePerm)
			return j
		})

		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_BATCH_UPLOAD:
		err := wildOperation(wp, func(ch chan<- interface{}) error {
			// lister
			st, err := os.Stat(j.args[0].arg)
			if err == nil && st.IsDir() {
				err = filepath.Walk(j.args[0].arg, func(path string, st os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if st.IsDir() {
						return nil
					}
					ch <- &path
					return nil
				})
				ch <- nil // send EOF
				return err
			} else {
				ma, err := filepath.Glob(j.args[0].arg)
				if err != nil {
					return err
				}
				if len(ma) == 0 {
					return errors.New("Could not find match for glob")
				}

				for _, f := range ma {
					st, _ = os.Stat(f)
					if !st.IsDir() {
						ch <- &f
					}
				}
				ch <- nil // send EOF
				return nil
			}
		}, func(data interface{}) *Job {
			if data == nil {
				return nil
			}
			// callback
			fn := data.(*string)

			arg1 := JobArgument{
				*fn,
				nil,
			}
			arg2 := j.args[1].Clone().Append(*fn)
			return j.MakeSubJob("put", OP_UPLOAD, []*JobArgument{&arg1, &arg2})
		})

		return wp.stats.IncrementIfSuccess(STATS_FILEOP, err)

	case OP_DOWNLOAD:
		src_fn := filepath.Base(j.args[0].arg)
		dest_fn := src_fn
		if len(j.args) > 1 {
			dest_fn = j.args[1].arg
		}

		o, err := s3head(wp.s3svc, j.args[0].s3)
		if err != nil {
			return err
		}

		bar := pb.New64(*o.ContentLength).SetUnits(pb.U_BYTES).Prefix(src_fn)
		bar.Start()

		f, err := os.Create(dest_fn)
		if err != nil {
			return err
		}

		wap := NewWriterAtProgress(f, func(n int64) {
			bar.Add64(n)
		})

		ch := make(chan error)

		go func() {
			_, err := wp.s3dl.Download(wap, &s3.GetObjectInput{
				Bucket: aws.String(j.args[0].s3.bucket),
				Key:    aws.String(j.args[0].s3.key),
			})

			select {
			case ch <- err:
			}
		}()

		select {
		case <-wp.ctx.Done():
			err = ErrInterrupted
		case err = <-ch:
			break
		}
		close(ch)
		ch = nil

		f.Close()

		if err == ErrInterrupted {
			bar.NotPrint = true
		}
		bar.Finish()

		wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		if err != nil {
			os.Remove(dest_fn) // Remove partly downloaded file
		}

		return err

	case OP_UPLOAD:
		src_fn := filepath.Base(j.args[0].arg)
		s, err := os.Stat(j.args[0].arg)
		if err != nil {
			return err
		}

		f, err := os.Open(j.args[0].arg)
		if err != nil {
			return err
		}

		defer f.Close()

		bar := pb.New64(s.Size()).SetUnits(pb.U_BYTES).Prefix(src_fn)
		bar.Start()

		r := bar.NewProxyReader(f)

		ch := make(chan error)

		go func() {
			_, err := wp.s3ul.Upload(&s3manager.UploadInput{
				Bucket: aws.String(j.args[1].s3.bucket),
				Key:    aws.String(j.args[1].s3.key),
				Body:   r,
			})

			select {
			case ch <- err:
			}
		}()

		select {
		case <-wp.ctx.Done():
			err = ErrInterrupted
		case err = <-ch:
			break
		}
		close(ch)
		ch = nil

		f.Close()

		if err == ErrInterrupted {
			bar.NotPrint = true
		}
		bar.Finish()

		wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		return err

	case OP_LISTBUCKETS:
		o, err := wp.s3svc.ListBuckets(&s3.ListBucketsInput{})
		if err == nil {
			for _, b := range o.Buckets {
				j.out(SHORT_OK, "%s  s3://%s", b.CreationDate.Format(DATE_FORMAT), *b.Name)
			}
		}
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_LIST:
		err := s3wildOperation(j.args[0].s3, wp, func(li *s3listItem) *Job {
			if li == nil {
				return nil
			}

			if li.isCommonPrefix {
				j.out(SHORT_OK, "%19s %1s  %12s  %s", "", "", "DIR", li.parsedKey)
			} else {
				var cls string

				switch *li.class {
				case s3.ObjectStorageClassStandard:
					cls = ""
				case s3.ObjectStorageClassGlacier:
					cls = "G"
				case s3.ObjectStorageClassReducedRedundancy:
					cls = "R"
				default:
					cls = "?"
				}
				j.out(SHORT_OK, "%s %1s  %12d  %s", li.lastModified.Format(DATE_FORMAT), cls, li.size, li.parsedKey)
			}

			return nil
		})

		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_ABORT:
		var (
			exitCode int64 = -1
			err      error
		)

		if len(j.args) > 0 {
			exitCode, err = strconv.ParseInt(j.args[0].arg, 10, 8)
			if err != nil {
				exitCode = 255
			}
		}

		ef := wp.ctx.Value("exitFunc").(func(int))
		ef(int(exitCode))

		return nil

	// Unhandled
	default:
		return fmt.Errorf("Unhandled operation %v", j.operation)
	}

}

type wildLister func(chan<- interface{}) error
type wildCallback func(interface{}) *Job

/*
wildOperation is the cornerstone of sub-job launching.

It will run lister() when ready and expect data from ch. On EOF, a single nil should be passed into ch.
Data received from ch will be passed to callback() which in turn will create a *Job entry (or nil for no job)
Then this entry is submitted to the subJobQueue chan.

After lister() completes, the sub-jobs are tracked
The fn will return when all jobs are processed, and it will return with error if even a single sub-job was not successful

Midway-failing lister() fns are not thoroughly tested and may hang or panic
*/

func wildOperation(wp *WorkerParams, lister wildLister, callback wildCallback) error {
	ch := make(chan interface{})
	closer := make(chan bool)
	notifyChan := make(chan bool)
	var subJobCounter uint32 // number of total subJobs issued

	// This goroutine will read ls results from ch and issue new subJobs
	go func() {
		defer close(closer) // Close closer when goroutine exits
		for {
			select {
			case data, ok := <-ch:
				if !ok {
					// Channel closed early: err returned from s3list?
					return
				}
				j := callback(data)
				if j != nil {
					j.notifyChan = &notifyChan
					subJobCounter++
					*wp.subJobQueue <- j
				}
				if data == nil {
					// End of listing
					return
				}
			}
		}
	}()

	var (
		successfulSubJobs uint32
		processedSubJobs  uint32
	)
	// This goroutine will tally successful and total processed sub-jobs
	go func() {
		for {
			select {
			case res, ok := <-notifyChan:
				if !ok {
					return
				}
				atomic.AddUint32(&processedSubJobs, 1)
				if res == true {
					atomic.AddUint32(&successfulSubJobs, 1)
				}
			}
		}
	}()

	// Do the actual work
	err := lister(ch)
	if err == nil {
		// This select ensures that we don't return to the main loop without completely getting the list results (and queueing up operations on subJobQueue)
		select {
		case <-closer: // Wait for EOF on goroutine
		}

		var p, s uint32
		for { // wait for all jobs to finish
			p = atomic.LoadUint32(&processedSubJobs)
			if p < subJobCounter {
				time.Sleep(time.Second)
			} else {
				break
			}
		}

		s = atomic.LoadUint32(&successfulSubJobs)
		if s != subJobCounter {
			err = fmt.Errorf("Not all jobs completed successfully: %d/%d", s, subJobCounter)
		}
	}
	close(ch)
	close(notifyChan)
	return err
}
