package e2e

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"gotest.tools/v3/icmd"
)

func TestMakeBucket_success(t *testing.T) {
	t.Parallel()
	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "test-bucket"
	src := fmt.Sprintf("s3://%s", bucketName)

	cmd := s5cmd("mb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`mb %v`, src),
	})

	_, err := s3client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func TestMakeBucket_success_json(t *testing.T) {
	t.Parallel()
	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "test-bucket"
	src := fmt.Sprintf("s3://%s", bucketName)

	cmd := s5cmd("--json", "mb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	jsonText := `
		{
			"operation": "mb",
			"success": true,
			"source": "%v"
		}
	`

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: json(jsonText, src),
	}, jsonCheck(true))

	_, err := s3client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func TestMakeBucket_failure(t *testing.T) {
	t.Parallel()
	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "invalid/bucket/name"
	src := fmt.Sprintf("s3://%s", bucketName)
	cmd := s5cmd("mb", src)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "mb %v": invalid s3 bucket`, src),
	})
}

func TestMakeBucket_failure_json(t *testing.T) {
	t.Parallel()
	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "invalid/bucket/name"
	src := fmt.Sprintf("s3://%s", bucketName)
	cmd := s5cmd("--json", "mb", src)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`{"operation":"mb","command":"mb %v","error":"invalid s3 bucket"}`, src),
	}, jsonCheck(true))
}
