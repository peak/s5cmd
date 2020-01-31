package e2e

import (
	"testing"

	"gotest.tools/v3/icmd"
)

func TestDiskUsageSingleS3Object(t *testing.T) {
	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	// create 2 files, expect 1.
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")

	cmd := s5cmd("du", "s3://"+bucket+"/testfile1.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "du s3://%v/testfile1.txt" (1)`, bucket),
	}, strictLineCheck(false))

	assertLines(t, result.Stdout(), map[int]compareFunc{
		// 0: suffix("317 testfile1.txt"),
	})
}

func TestDiskUsageWildcardS3Objects(t *testing.T) {
}

func TestDiskUsageWildcardS3ObjectsWithDashH(t *testing.T) {
}
