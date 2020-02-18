package e2e

import (
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestDashFFromStdin(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file.txt", "content")

	input := strings.NewReader(
		strings.Join([]string{
			"ls s3://" + bucket,
			"! echo naber",
		}, "\n"),
	)
	cmd := s5cmd("-f", "-")
	result := icmd.RunCmd(cmd, icmd.WithStdin(input))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
		1: match(`# Exiting with code 0`),
		2: match(`# Stats: S3 1 \d+ ops/sec`),
		3: match(`# Stats: Shell 1 \d+ ops/sec$`),
		4: match(`# Stats: Total 2 \d+ ops/sec \d+\.\d+ms$`),
		5: suffix(`# Using 256 workers`),
		6: suffix(` +OK "! echo naber"`),
		7: suffix(` +OK "ls s3://test-dash-f-from-stdin"`),
	}, trimMatch(dateRe), sortInput(true))

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix("file.txt"),
		2: equals("naber"),
	}, sortInput(true))
}

func TestDashFFromFile(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file.txt", "content")

	file := fs.NewFile(t, "prefix", fs.WithContent("ls s3://"+bucket+"\n! echo naber"))
	defer file.Remove()

	cmd := s5cmd("-f", file.Path())
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
		1: match(`# Exiting with code 0`),
		2: match(`.* Stats: S3 1 \d+ ops/sec`),
		3: match(`.* Stats: Shell 1 \d+ ops/sec$`),
		4: match(`.* Stats: Total 2 \d+ ops/sec \d+\.\d+ms$`),
		5: suffix(`# Using 256 workers`),
		6: suffix(` +OK "! echo naber"`),
		7: suffix(` +OK "ls s3://test-dash-f-from-file"`),
	}, trimMatch(dateRe), sortInput(true))

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix("file.txt"),
		2: equals("naber"),
	}, sortInput(true))
}

func TestDashFWildcardCountGreaterEqualThanWorkerCount(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file.txt", "content")

	content := []string{
		"cp s3://" + bucket + "/f*.txt .",
		"cp s3://" + bucket + "/f*.txt .",
		"cp s3://" + bucket + "/f*.txt .",
	}
	file := fs.NewFile(t, "prefix", fs.WithContent(strings.Join(content, "\n")))
	defer file.Remove()

	// worker count < len(wildcards)
	cmd := s5cmd("-numworkers", "2", "-f", file.Path())
	cmd.Timeout = time.Second
	result := icmd.RunCmd(cmd)

	// FIXME(ig): This is a bug (see #12). If wildcard expansion operations are
	// greater than the number of workers, queues are blocked and we got a
	// timeout.
	//
	// After the issue is resolved, change the assertion to success.
	result.Assert(t, icmd.Expected{Timeout: true})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`# Using 2 workers`),
	}, sortInput(true))

	assertLines(t, result.Stdout(), map[int]compareFunc{})
}
