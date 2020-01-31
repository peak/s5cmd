package e2e

import (
	"strings"
	"testing"

	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestDashFFromStdin(t *testing.T) {
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
		4: match(`Stats: Total 2 \d+ ops/sec \d+\.\d+ms$`),
		5: suffix(`# Using 256 workers`),
		6: match(`\+OK "! echo naber"`),
		7: match(`\+OK "ls s3://test-dash-f-from-stdin"`),
	}, sortInput(true))

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("naber"),
		1: suffix("file.txt"),
	})
}

func TestDashFFromFile(t *testing.T) {
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
		2: match(`# Stats: S3 1 \d+ ops/sec`),
		3: match(`# Stats: Shell 1 \d+ ops/sec$`),
		4: match(`Stats: Total 2 \d+ ops/sec \d+\.\d+ms$`),
		5: suffix(`# Using 256 workers`),
		6: match(`\+OK "! echo naber"`),
		7: match(`\+OK "ls s3://test-dash-f-from-file"`),
	}, sortInput(true))

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("naber"),
		1: suffix("file.txt"),
	})
}
