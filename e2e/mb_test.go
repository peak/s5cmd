package e2e

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"gotest.tools/v3/icmd"
)

func Test_MakeBucket_success(t *testing.T) {
	t.Parallel()
	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "test-bucket"
	src := fmt.Sprintf("s3://%s", bucketName)

	cmd := s5cmd("mb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`make-bucket %v`, src),
		1: equals(""),
	})

	_, err := s3client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func Test_MakeBucket_failure(t *testing.T) {
	t.Parallel()
	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "invalid/bucket/name"
	src := fmt.Sprintf("s3://%s", bucketName)
	cmd := s5cmd("mb", src)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 127})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`-ERR "mb %v": invalid parameters to "mb": invalid s3 bucket`, src),
		1: equals(""),
	})
}
