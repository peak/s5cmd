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

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	src := fmt.Sprintf("s3://%s", bucket)

	cmd := s5cmd("mb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`mb %v`, src),
	})

	_, err := s3client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	// cleanup the bucket later:
	_, err = s3client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMakeBucket_success_json(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	src := fmt.Sprintf("s3://%s", bucket)

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

	_, err := s3client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	// cleanup the bucket later:
	_, err = s3client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMakeBucket_failure(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	bucket := "invalid/bucket/name"
	src := fmt.Sprintf("s3://%s", bucket)
	cmd := s5cmd("mb", src)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "mb %v": invalid s3 bucket`, src),
	})
}

func TestMakeBucket_failure_json(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	bucket := "invalid/bucket/name"
	src := fmt.Sprintf("s3://%s", bucket)
	cmd := s5cmd("--json", "mb", src)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`{"operation":"mb","command":"mb %v","error":"invalid s3 bucket"}`, src),
	}, jsonCheck(true))
}
