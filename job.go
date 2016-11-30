package s5cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/termie/go-shutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
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
	opts           OptionList
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
	SHORT_INFO
)

func (s ShortCode) String() string {
	if s == SHORT_OK {
		return "+"
	}
	if s == SHORT_ERR {
		return "-"
	}
	if s == SHORT_INFO {
		return "?"
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

func (j Job) MakeSubJob(command string, operation Operation, args []*JobArgument, opts OptionList) *Job {
	ptr := args
	return &Job{
		sourceDesc: j.sourceDesc,
		command:    command,
		operation:  operation,
		args:       ptr,
		opts:       opts,
		isSubJob:   true,
		numSuccess: j.numSuccess,
		numFails:   j.numFails,
	}
}

func (a JobArgument) Clone() *JobArgument {
	var s s3url
	if a.s3 != nil {
		s = a.s3.Clone()
	}
	return &JobArgument{a.arg, &s}
}
func (a *JobArgument) Append(s string, isS3path bool) *JobArgument {
	if a.s3 != nil && !isS3path {
		// a is an S3 object but s is not
		s = strings.Replace(s, string(filepath.Separator), "/", -1)
	}
	if a.s3 == nil && isS3path {
		// a is a not an S3 object but s is
		s = strings.Replace(s, "/", string(filepath.Separator), -1)
	}

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

var (
	ErrFileExists = errors.New("File already exists")
	ErrS3Exists   = errors.New("Object already exists")
)

func (j *Job) Run(wp *WorkerParams) error {
	//log.Printf("Running %v", j)

	doesFileExist := func(filename string) error {
		_, err := os.Stat(filename)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		return ErrFileExists
	}

	switch j.operation {

	// Local operations
	case OP_LOCAL_DELETE:
		return wp.stats.IncrementIfSuccess(STATS_FILEOP, os.Remove(j.args[0].arg))

	case OP_LOCAL_COPY:
		var err error
		if j.opts.Has(OPT_IF_NOT_EXISTS) {
			err = doesFileExist(j.args[1].arg)
			if err != nil {
				return err
			}
		}

		if j.opts.Has(OPT_DELETE_SOURCE) {
			err = os.Rename(j.args[0].arg, j.args[1].arg)
		} else {
			_, err = shutil.Copy(j.args[0].arg, j.args[1].arg, true)
		}
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
		var err error
		if j.opts.Has(OPT_IF_NOT_EXISTS) {
			_, err := s3head(wp.s3svc, j.args[1].s3)
			if err == nil {
				wp.stats.IncrementIfSuccess(STATS_S3OP, err)
				return ErrS3Exists
			}
		}

		var cls string

		if j.opts.Has(OPT_RR) {
			cls = s3.ObjectStorageClassReducedRedundancy
		} else if j.opts.Has(OPT_IA) {
			cls = s3.TransitionStorageClassStandardIa
		} else {
			cls = s3.ObjectStorageClassStandard
		}

		_, err = wp.s3svc.CopyObject(&s3.CopyObjectInput{
			Bucket:       aws.String(j.args[1].s3.bucket),
			Key:          aws.String(j.args[1].s3.key),
			CopySource:   aws.String(j.args[0].s3.format()),
			StorageClass: aws.String(cls),
		})
		wp.stats.IncrementIfSuccess(STATS_S3OP, err)

		if j.opts.Has(OPT_DELETE_SOURCE) && err == nil {
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
		srcBucket := *j.args[0].Clone()
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
				subJob = j.MakeSubJob("batch-rm", OP_BATCH_DELETE_ACTUAL, jobArgs, OptionList{})
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
		subCmd := "cp"
		if j.opts.Has(OPT_DELETE_SOURCE) {
			subCmd = "mv"
		}
		subCmd += j.opts.GetParams()

		err := s3wildOperation(j.args[0].s3, wp, func(li *s3listItem) *Job {
			if li == nil || li.isCommonPrefix {
				return nil
			}

			arg1 := JobArgument{
				"s3://" + j.args[0].s3.bucket + "/" + *li.key,
				&s3url{j.args[0].s3.bucket, *li.key},
			}

			var dstFn string
			if j.opts.Has(OPT_PARENTS) {
				dstFn = li.parsedKey
			} else {
				dstFn = path.Base(li.parsedKey)
			}

			arg2 := j.args[1].Clone().Append(dstFn, true)
			subJob := j.MakeSubJob(subCmd, OP_DOWNLOAD, []*JobArgument{&arg1, arg2}, j.opts)
			if *li.class == s3.ObjectStorageClassGlacier {
				subJob.out(SHORT_ERR, `"%s": Cannot download glacier object`, arg1.arg)
				return nil
			}
			dir := filepath.Dir(arg2.arg)
			os.MkdirAll(dir, os.ModePerm)
			return subJob
		})

		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_BATCH_UPLOAD:
		subCmd := "cp"
		if j.opts.Has(OPT_DELETE_SOURCE) {
			subCmd = "mv"
		}
		subCmd += j.opts.GetParams()

		st, err := os.Stat(j.args[0].arg)
		walkMode := err == nil && st.IsDir() // walk or glob?

		trimPrefix := j.args[0].arg
		if !walkMode {
			loc := strings.IndexAny(trimPrefix, GLOB_CHARACTERS)
			trimPrefix = trimPrefix[:loc]
		}
		trimPrefix = path.Dir(trimPrefix)
		if trimPrefix == "." {
			trimPrefix = ""
		} else {
			trimPrefix += string(filepath.Separator)
		}

		err = wildOperation(wp, func(ch chan<- interface{}) error {
			// lister
			st, err := os.Stat(j.args[0].arg)
			if walkMode {
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
			// callback
			if data == nil {
				return nil
			}
			fn := data.(*string)

			var dstFn string
			if j.opts.Has(OPT_PARENTS) {
				dstFn = *fn
				if strings.Index(dstFn, trimPrefix) == 0 {
					dstFn = dstFn[len(trimPrefix):]
				}
			} else {
				dstFn = filepath.Base(*fn)
			}

			arg1 := JobArgument{
				*fn,
				nil,
			}
			arg2 := j.args[1].Clone().Append(dstFn, false)
			return j.MakeSubJob(subCmd, OP_UPLOAD, []*JobArgument{&arg1, arg2}, j.opts)
		})

		return wp.stats.IncrementIfSuccess(STATS_FILEOP, err)

	case OP_DOWNLOAD:
		src_fn := path.Base(j.args[0].arg)
		dest_fn := j.args[1].arg

		if j.opts.Has(OPT_IF_NOT_EXISTS) {
			err := doesFileExist(dest_fn)
			if err != nil {
				return err
			}
		}

		f, err := os.Create(dest_fn)
		if err != nil {
			return err
		}

		j.out(SHORT_INFO, "Downloading %s...", src_fn)

		ch := make(chan error)

		go func() {
			_, err := wp.s3dl.Download(f, &s3.GetObjectInput{
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

		wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		if err != nil {
			os.Remove(dest_fn) // Remove partly downloaded file
		} else if j.opts.Has(OPT_DELETE_SOURCE) {
			_, err = s3delete(wp.s3svc, j.args[0].s3)
			wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		}

		return err

	case OP_UPLOAD:
		const bytesInMb = float64(1024 * 1024)

		src_fn := filepath.Base(j.args[0].arg)
		s, err := os.Stat(j.args[0].arg)
		if err != nil {
			return err
		}

		if j.opts.Has(OPT_IF_NOT_EXISTS) {
			_, err = s3head(wp.s3svc, j.args[1].s3)
			if err == nil {
				wp.stats.IncrementIfSuccess(STATS_S3OP, err)
				return ErrS3Exists
			}
		}

		f, err := os.Open(j.args[0].arg)
		if err != nil {
			return err
		}

		defer f.Close()

		filesize := s.Size()

		numPartsNeeded := filesize / wp.poolParams.ChunkSizeBytes
		chunkSize := int64(wp.poolParams.ChunkSizeBytes / int64(bytesInMb))
		if numPartsNeeded > s3manager.MaxUploadParts {
			cSize := float64(filesize / s3manager.MaxUploadParts)
			chunkSize = int64(math.Ceil(cSize / bytesInMb))
			j.out(SHORT_INFO, "Uploading %s... (%d bytes) (chunk size %d MB)", src_fn, filesize, chunkSize)
		} else {
			j.out(SHORT_INFO, "Uploading %s... (%d bytes)", src_fn, filesize)
		}

		ch := make(chan error)

		go func(chunkSizeInBytes int64) {
			var cls string

			if j.opts.Has(OPT_RR) {
				cls = s3.ObjectStorageClassReducedRedundancy
			} else if j.opts.Has(OPT_IA) {
				cls = s3.TransitionStorageClassStandardIa
			} else {
				cls = s3.ObjectStorageClassStandard
			}

			_, err := wp.s3ul.Upload(&s3manager.UploadInput{
				Bucket:       aws.String(j.args[1].s3.bucket),
				Key:          aws.String(j.args[1].s3.key),
				Body:         f,
				StorageClass: aws.String(cls),
			}, func(u *s3manager.Uploader) {
				u.PartSize = chunkSizeInBytes
			})

			select {
			case ch <- err:
			}
		}(chunkSize * int64(bytesInMb))

		select {
		case <-wp.ctx.Done():
			err = ErrInterrupted
		case err = <-ch:
			break
		}
		close(ch)
		ch = nil

		f.Close()

		wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		if j.opts.Has(OPT_DELETE_SOURCE) && err == nil {
			err = wp.stats.IncrementIfSuccess(STATS_FILEOP, os.Remove(j.args[0].arg))
		}
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
				case s3.TransitionStorageClassStandardIa:
					cls = "I"
				default:
					cls = "?"
				}
				j.out(SHORT_OK, "%s %1s  %12d  %s", li.lastModified.Format(DATE_FORMAT), cls, li.size, li.parsedKey)
			}

			return nil
		})

		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_SIZE:
		var size, count int64
		err := s3wildOperation(j.args[0].s3, wp, func(li *s3listItem) *Job {
			if li == nil || li.isCommonPrefix {
				return nil
			}
			size += li.size
			count++
			return nil
		})
		if err == nil {
			j.out(SHORT_OK, "%d bytes in %d objects: %s", size, count, j.args[0].s3)
		}
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
