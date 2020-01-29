package e2e

import (
	"fmt"
	"testing"

	"gotest.tools/v3/icmd"
)

func TestListBuckets(t *testing.T) {
	const dbname = "test-list-buckets"

	s3client, s5cmd, cleanup := setup(t, dbname)
	defer cleanup()

	// alphabetically unordered list of buckets
	createBucket(t, s3client, "test-list-buckets-1")
	createBucket(t, s3client, "test-list-buckets-2")
	createBucket(t, s3client, "test-list-buckets-4")
	createBucket(t, s3client, "test-list-buckets-3")

	cmd := s5cmd("ls")

	result := icmd.RunCmd(
		cmd,
		icmd.WithEnv(
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
		),
	)

	result.Assert(t, icmd.Success)
	result.Assert(t, icmd.Expected{Err: `+OK "ls"`})

	// expect and ordered list
	assert(t, result.Stdout(), map[int]compareFunc{
		0: suffix("s3://test-list-buckets-1"),
		1: suffix("s3://test-list-buckets-2"),
		2: suffix("s3://test-list-buckets-3"),
		3: suffix("s3://test-list-buckets-4"),
		4: equals(""),
	}, true)
}

func TestListSingleS3Object(t *testing.T) {
	const (
		bucket = "test-list-single-s3-object"
		dbname = bucket
	)

	s3client, s5cmd, cleanup := setup(t, dbname)
	defer cleanup()

	createBucket(t, s3client, bucket)

	// create 2 files, expect 1.
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")

	cmd := s5cmd("ls", "s3://"+bucket+"/testfile1.txt")

	result := icmd.RunCmd(
		cmd,
		icmd.WithEnv(
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
		),
	)

	result.Assert(t, icmd.Success)

	assert(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "ls s3://test-list-single-s3-object/testfile1.txt" (1)`),
	}, false)

	assert(t, result.Stdout(), map[int]compareFunc{
		// 0: suffix("317 testfile1.txt"),
		0: match(`\s+(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*testfile1.txt`),
		1: equals(""),
	}, true)
}

func TestListSingleWildcardS3Object(t *testing.T) {
	const (
		bucket = "test-list-wildcard-s3-object"
		dbname = bucket
	)

	s3client, s5cmd, cleanup := setup(t, dbname)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "testfile3.txt", "this is also a file content somehow")

	cmd := s5cmd("ls", "s3://"+bucket+"/*.txt")

	result := icmd.RunCmd(
		cmd,
		icmd.WithEnv(
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
		),
	)

	result.Assert(t, icmd.Success)

	assert(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "ls s3://test-list-wildcard-s3-object/*.txt" (3)`),
	}, false)

	assert(t, result.Stdout(), map[int]compareFunc{
		0: suffix("317 testfile1.txt"),
		1: suffix("322 testfile2.txt"),
		2: suffix("330 testfile3.txt"),
		3: equals(""),
	}, true)
}

func TestListMultipleWildcardS3Object(t *testing.T) {
	const (
		bucket = "test-list-wildcard-s3-object"
		dbname = bucket
	)

	s3client, s5cmd, cleanup := setup(t, dbname)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "/a/testfile1.txt", "content")
	putFile(t, s3client, bucket, "/a/testfile2.txt", "content")
	putFile(t, s3client, bucket, "/b/testfile3.txt", "content")
	putFile(t, s3client, bucket, "/b/testfile4.txt", "content")
	putFile(t, s3client, bucket, "/c/testfile5.gz", "content")
	putFile(t, s3client, bucket, "/c/testfile6.txt.gz", "content")
	putFile(t, s3client, bucket, "/d/foo/bar/file7.txt", "content")
	putFile(t, s3client, bucket, "/d/foo/bar/testfile8.txt", "content")
	putFile(t, s3client, bucket, "/e/txt/testfile9.txt.gz", "content")
	putFile(t, s3client, bucket, "/f/txt/testfile10.txt", "content")

	const pattern = "/*/testfile*.txt"
	cmd := s5cmd("ls", "s3://"+bucket+pattern)

	result := icmd.RunCmd(
		cmd,
		icmd.WithEnv(
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
		),
	)

	result.Assert(t, icmd.Success)

	assert(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "ls s3://test-list-wildcard-s3-object/*/testfile*.txt" (6)`),
	}, false)

	assert(t, result.Stdout(), map[int]compareFunc{
		0: suffix("304 a/testfile1.txt"),
		1: suffix("304 a/testfile2.txt"),
		2: suffix("304 b/testfile3.txt"),
		3: suffix("304 b/testfile4.txt"),
		4: suffix("312 d/foo/bar/testfile8.txt"),
		5: suffix("309 f/txt/testfile10.txt"),
		6: equals(""),
	}, true)
}
