package e2e

import (
	"fmt"
	"testing"

	"gotest.tools/v3/icmd"
)

func TestPresign(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)

	createBucket(t, s3client, bucket)

	const (
		filename = "test.txt"
		content  = "file content"
	)
	putFile(t, s3client, bucket, filename, content)

	src := fmt.Sprintf("s3://%v/%v", bucket, filename)

	cmd := s5cmd("presign", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: contains(filename),
	})
}
