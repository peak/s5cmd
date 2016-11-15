package s5cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/termie/go-shutil"
	"log"
	"os"
	"os/exec"
	"strings"
)

type JobArgument struct {
	arg string
	s3  *s3url
}

type Job struct {
	sourceDesc     string
	operation      Operation
	args           []*JobArgument
	successCommand *Job
	failCommand    *Job
}

func (j Job) String() string {
	return j.sourceDesc
}

func (j *Job) Run(svc *s3.S3) error {
	log.Printf("Running %v", j)

	// TODO Run successCommand or failCommand if any, if not, return
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
		if len(j.args) > 1 {
			parts := strings.Split(j.args[1].arg, " ")
			return exec.Command(j.args[0].arg, parts...).Run()
		}
		return exec.Command(j.args[0].arg).Run()

	// S3 operations
	case OP_COPY:
		res, err := svc.CopyObject(&s3.CopyObjectInput{
			Bucket:     aws.String(j.args[0].s3.bucket),
			Key:        aws.String(j.args[0].s3.key),
			CopySource: aws.String(j.args[1].s3.format()),
		})
		log.Printf("Result Output: %v", res)
		log.Printf("Result Result: %v", res.CopyObjectResult)

		return err

	// Unhandled
	default:
		return fmt.Errorf("Unhandled operation %v", j.operation)
	}

	return nil
}
