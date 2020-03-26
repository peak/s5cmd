package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestRunFromStdin(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file1.txt", "content")
	putFile(t, s3client, bucket, "file2.txt", "content")

	input := strings.NewReader(
		strings.Join([]string{
			fmt.Sprintf("ls s3://%v/file1.txt", bucket),
			" # this is a comment",
			fmt.Sprintf("ls s3://%v/file2.txt # this is an inline comment", bucket),
		}, "\n"),
	)
	cmd := s5cmd("run")
	result := icmd.RunCmd(cmd, icmd.WithStdin(input))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix("file1.txt"),
		2: suffix("file2.txt"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})
}

func TestRunFromStdinWithErrors(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	input := strings.NewReader(
		strings.Join([]string{
			"ls s3:/",
			fmt.Sprintf("cp s3://%v/nonexistentobject .", bucket),
		}, "\n"),
	)
	cmd := s5cmd("run")
	result := icmd.RunCmd(cmd, icmd.WithStdin(input))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
		1: contains(`ERROR "cp s3://%v/nonexistentobject nonexistentobject": NoSuchKey: status code: 404`, bucket),
		2: equals(`ERROR "ls s3:/": given object not found`),
	}, sortInput(true))
}

func TestRunFromStdinJSON(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file1.txt", "content")
	putFile(t, s3client, bucket, "file2.txt", "content")

	input := strings.NewReader(
		strings.Join([]string{
			fmt.Sprintf("ls s3://%v/file1.txt", bucket),
			fmt.Sprintf("ls s3://%v/file2.txt", bucket),
		}, "\n"),
	)
	cmd := s5cmd("--json", "run")
	result := icmd.RunCmd(cmd, icmd.WithStdin(input))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: prefix(`{"key":"s3://%v/file1.txt",`, bucket),
		2: prefix(`{"key":"s3://%v/file2.txt",`, bucket),
	}, sortInput(true), jsonCheck(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})
}

func TestRunFromFile(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file1.txt", "content")
	putFile(t, s3client, bucket, "file2.txt", "content")

	filecontent := strings.Join([]string{
		fmt.Sprintf("ls s3://%v/file1.txt", bucket),
		fmt.Sprintf("ls s3://%v/file2.txt", bucket),
	}, "\n")

	file := fs.NewFile(t, "prefix", fs.WithContent(filecontent))
	defer file.Remove()

	cmd := s5cmd("run", file.Path())
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix("file1.txt"),
		2: suffix("file2.txt"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})
}

func TestRunFromFileJSON(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file1.txt", "content")
	putFile(t, s3client, bucket, "file2.txt", "content")

	filecontent := strings.Join([]string{
		fmt.Sprintf("ls s3://%v/file1.txt", bucket),
		fmt.Sprintf("ls s3://%v/file2.txt", bucket),
	}, "\n")

	file := fs.NewFile(t, "prefix", fs.WithContent(filecontent))
	defer file.Remove()

	cmd := s5cmd("--json", "run", file.Path())
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: prefix(`{"key":"s3://%v/file1.txt",`, bucket),
		2: prefix(`{"key":"s3://%v/file2.txt",`, bucket),
	}, sortInput(true), jsonCheck(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})
}

func TestRunWildcardCountGreaterEqualThanWorkerCount(t *testing.T) {
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
	cmd := s5cmd("--numworkers", "2", "run", file.Path())
	cmd.Timeout = time.Second
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: equals(`cp s3://%v/file.txt`, bucket),
		2: equals(`cp s3://%v/file.txt`, bucket),
		3: equals(`cp s3://%v/file.txt`, bucket),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})
}
