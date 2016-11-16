package s5cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/termie/go-shutil"
	"os"
	"os/exec"
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

func (j *Job) Run(svc *s3.S3) error {
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
		return s3copy(svc, j.args[0].s3, j.args[1].s3)

	case OP_MOVE:
		err := s3copy(svc, j.args[0].s3, j.args[1].s3)
		if err == nil {
			err = s3delete(svc, j.args[0].s3)
			// FIXME if err != nil try to rollback by deleting j.args[1].s3 ?
		}

		return err

	case OP_DELETE:
		return s3delete(svc, j.args[0].s3)

	// Unhandled
	default:
		return fmt.Errorf("Unhandled operation %v", j.operation)
	}

	return nil
}
