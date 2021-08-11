package e2e

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"gotest.tools/v3/icmd"
)

func TestRemoveBucketSuccess(t *testing.T) {
	t.Parallel()

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "bucket"
	src := fmt.Sprintf("s3://%v", bucketName)

	createBucket(t, s3client, bucketName)

	cmd := s5cmd("rb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rb %v`, src),
	})

	_, err := s3client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucketName)})

	if err == nil {
		t.Errorf("bucket still exists after remove bucket operation\n")
	}
}

func TestRemoveBucketSuccessJson(t *testing.T) {
	t.Parallel()

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "bucket"
	src := fmt.Sprintf("s3://%v", bucketName)

	createBucket(t, s3client, bucketName)

	cmd := s5cmd("--json", "rb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	jsonText := `
		{
			"operation": "rb",
			"success": true,
			"source": "%v"
		}
	`

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: json(jsonText, src),
	}, jsonCheck(true))

	_, err := s3client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err == nil {
		t.Errorf("bucket still exists after remove bucket operation\n")
	}
}

func TestRemoveBucketFailure(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "invalid/bucket/name"
	src := fmt.Sprintf("s3://%s", bucketName)
	cmd := s5cmd("rb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "rb %v": invalid s3 bucket`, src),
	})
}

func TestRemoveBucketFailureJson(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucketName := "invalid/bucket/name"
	src := fmt.Sprintf("s3://%s", bucketName)
	cmd := s5cmd("--json", "rb", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`{"operation":"rb","command":"rb %v","error":"invalid s3 bucket"}`, src),
	}, jsonCheck(true))
}

func TestRemoveBucketWithObject(t *testing.T) {
	t.Parallel()

	const (
		bucket      = "test-bucket"
		fileContent = "this is a file content"
		fileName    = "file1.txt"
	)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, fileName, fileContent)

	bucketName := fmt.Sprintf("s3://%v", bucket)
	cmd := s5cmd("rb", bucketName)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	expected := fmt.Sprintf(`ERROR "rb %v": BucketNotEmpty:`, bucketName) // error due to non-empty bucket.

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: match(expected),
	})
}
