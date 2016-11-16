package s5cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/termie/go-shutil"
	"os"
	"os/exec"
	"path/filepath"
)

type JobArgument struct {
	arg string
	s3  *s3url
}

type Job struct {
	sourceDesc     string // Source job description which we parsed this from
	command        string // Different from operation, as multiple commands can map to the same op
	operation      Operation
	args           []*JobArgument
	successCommand *Job
	failCommand    *Job
}

func (j Job) String() (s string) {
	s = j.command
	for _, a := range j.args {
		s += " " + a.arg
	}
	//s += " # from " + j.sourceDesc
	return
}

func s3copy(svc *s3.S3, src, dst *s3url) error {
	_, err := svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(dst.bucket),
		Key:        aws.String(dst.key),
		CopySource: aws.String(src.format()),
	})
	return err
}

func s3delete(svc *s3.S3, obj *s3url) error {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(obj.bucket),
		Key:    aws.String(obj.key),
	})
	return err
}

func (j *Job) Run(wp *WorkerParams) error {
	//log.Printf("Running %v", j)

	switch j.operation {

	// Local operations
	case OP_LOCAL_DELETE:
		return os.Remove(j.args[0].arg)

	case OP_LOCAL_MOVE:
		return os.Rename(j.args[0].arg, j.args[1].arg)

	case OP_LOCAL_COPY:
		_, err := shutil.Copy(j.args[0].arg, j.args[1].arg, true)
		return err

	case OP_SHELL_EXEC:
		strArgs := make([]string, 0)

		for i, a := range j.args {
			if i == 0 {
				continue
			}
			strArgs = append(strArgs, a.arg)
		}
		return exec.Command(j.args[0].arg, strArgs...).Run()

	// S3 operations
	case OP_COPY:
		return s3copy(wp.s3svc, j.args[0].s3, j.args[1].s3)

	case OP_MOVE:
		err := s3copy(wp.s3svc, j.args[0].s3, j.args[1].s3)
		if err == nil {
			err = s3delete(wp.s3svc, j.args[0].s3)
			// FIXME if err != nil try to rollback by deleting j.args[1].s3 ? What if we don't have permission to delete?
		}

		return err

	case OP_DELETE:
		return s3delete(wp.s3svc, j.args[0].s3)

	case OP_DOWNLOAD:
		dest_fn := filepath.Base(j.args[0].arg)
		if len(j.args) > 1 {
			dest_fn = j.args[1].arg
		}

		f, err := os.Create(dest_fn)
		if err != nil {
			return err
		}

		_, err = wp.s3dl.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(j.args[0].s3.bucket),
			Key:    aws.String(j.args[0].s3.key),
		})

		f.Close()
		if err != nil {
			os.Remove(dest_fn) // Remove partly downloaded file
		}

		return err

	case OP_UPLOAD:
		f, err := os.Open(j.args[0].arg)
		if err != nil {
			return err
		}

		defer f.Close()
		_, err = wp.s3ul.Upload(&s3manager.UploadInput{
			Bucket: aws.String(j.args[1].s3.bucket),
			Key:    aws.String(j.args[1].s3.key),
			Body:   f,
		})
		return err

	// Unhandled
	default:
		return fmt.Errorf("Unhandled operation %v", j.operation)
	}

	return nil
}
