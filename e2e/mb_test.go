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

func Test_MakeBucket_success_json(t *testing.T) {
	t.Parallel()
	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "test-bucket"
	src := fmt.Sprintf("s3://%s", bucketName)

	cmd := s5cmd("-json", "mb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	jsonText := `
		{
			"operation": "make-bucket",
			"success": true,
			"source": "%v"
		}
	`

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: json(jsonText, src),
		1: equals(""),
	}, jsonCheck(true))

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

	// FIXME(os): errors should be written into stderr
	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`ERROR "mb %v": invalid parameters to "mb": invalid s3 bucket`, src),
		1: equals(""),
	})
}

func Test_MakeBucket_failure_json(t *testing.T) {
	t.Parallel()
	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "invalid/bucket/name"
	src := fmt.Sprintf("s3://%s", bucketName)
	cmd := s5cmd("-json", "mb", src)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 127})

	// FIXME(os): errors should be written into stderr
	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`{"job":"mb %v","error":"invalid parameters to \"mb\": invalid s3 bucket"}`, src),
		1: equals(""),
	}, jsonCheck(true))
}
