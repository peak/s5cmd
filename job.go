package s5cmd

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/termie/go-shutil"
	"gopkg.in/cheggaaa/pb.v1"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
}

func (j Job) String() (s string) {
	s = j.command
	for _, a := range j.args {
		s += " " + a.arg
	}
	//s += " # from " + j.sourceDesc
	return
}

func out(shortCode, format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...)
	fmt.Println("                   ", shortCode, s)
}

func (j *Job) Notify(ctx context.Context, err error) {
	if j.notifyChan == nil {
		return
	}
	res := err == nil
	select {
	case <-ctx.Done():
		return
	default:
		select {
		case <-ctx.Done():
			return
		case *j.notifyChan <- res:
		}
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
			// FIXME if err != nil try to rollback by deleting j.args[1].s3 ? What if we don't have permission to delete?
		}

		return err

	case OP_DELETE:
		_, err := s3delete(wp.s3svc, j.args[0].s3)
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_BATCH_DOWNLOAD:
		dst_dir := ""
		if len(j.args) > 1 {
			dst_dir = j.args[1].arg
		}

		err := s3wildOperation(j.args[0].s3, wp, func(li *s3listItem) *Job {
			if li.isCommonPrefix {
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
			cmd := "get " + arg1.arg + " " + arg2.arg
			if *li.class == s3.ObjectStorageClassGlacier {
				out("-ERR", `"%s": Cannot download glacier object`, cmd)
				return nil
			}
			dir := filepath.Dir(arg2.arg)
			os.MkdirAll(dir, os.ModePerm)
			return &Job{
				sourceDesc: j.sourceDesc,
				command:    cmd,
				operation:  OP_DOWNLOAD,
				args:       []*JobArgument{&arg1, &arg2},
			}
		})

		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

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
				out("+", "%s  s3://%s", b.CreationDate.Format(DATE_FORMAT), *b.Name)
			}
		}
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_LIST:
		ch := make(chan *s3listItem)
		closer := make(chan bool)
		go func() {
			defer close(closer) // Close closer when goroutine exits
			var cls string
			for {
				select {
				case li, ok := <-ch:
					if !ok {
						// Channel closed early: err returned from s3list?
						return
					}
					if li == nil {
						out("?EOF", "End of listing")
						return
					}
					if li.isCommonPrefix {
						out("+", "%19s %1s  %12s  %s", "", "", "DIR", li.parsedKey)
					} else {
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
						out("+", "%s %1s  %12d   %s", li.lastModified.Format(DATE_FORMAT), cls, li.size, li.parsedKey)
					}
				}
			}
		}()
		err := s3list(wp.ctx, wp.s3svc, j.args[0].s3, ch)
		wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		if err == nil {
			select {
			case <-closer: // Wait for EOF on goroutine
			}
		}
		close(ch)
		return err

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
