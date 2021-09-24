package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
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
		0: suffix("file1.txt"),
		1: suffix("file2.txt"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})
}

func TestRunFromStdinWithErrors(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	input := strings.NewReader(
		strings.Join([]string{
			"ls s3/", // windows does not allow directory to contain substring `:/`
			fmt.Sprintf("cp s3://%v/nonexistentobject .", bucket),
		}, "\n"),
	)
	cmd := s5cmd("run")
	result := icmd.RunCmd(cmd, icmd.WithStdin(input))

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stdout(), map[int]compareFunc{})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: contains(`ERROR "cp s3://%v/nonexistentobject nonexistentobject": NoSuchKey: status code: 404`, bucket),
		1: equals(`ERROR "ls s3/": given object not found`),
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
		0: prefix(`{"key":"s3://%v/file1.txt",`, bucket),
		1: prefix(`{"key":"s3://%v/file2.txt",`, bucket),
	}, sortInput(true), jsonCheck(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})
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
		0: suffix("file1.txt"),
		1: suffix("file2.txt"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})
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
		0: prefix(`{"key":"s3://%v/file1.txt",`, bucket),
		1: prefix(`{"key":"s3://%v/file2.txt",`, bucket),
	}, sortInput(true), jsonCheck(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})
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
		0: equals(`cp s3://%v/file.txt file.txt`, bucket),
		1: equals(`cp s3://%v/file.txt file.txt`, bucket),
		2: equals(`cp s3://%v/file.txt file.txt`, bucket),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})
}

func TestRunSpecialCharactersInPrefix(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)
	sourceFileName := `special-chars_!@#$%^&_()_+{[_%5Cäè| __;'_,_._-中文 =/_!@#$%^&_()_+{[_%5Cäè| __;'_,_._-中文 =image.jpg`
	targetFilePath := `./image.jpg`

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, sourceFileName, "content")

	content := []string{
		`cp "s3://` + bucket + `/` + sourceFileName + `" ` + targetFilePath,
	}
	file := fs.NewFile(t, "prefix", fs.WithContent(strings.Join(content, "\n")))
	defer file.Remove()

	cmd := s5cmd("run", file.Path())
	cmd.Timeout = time.Second
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp s3://%v/%v %v`, bucket, sourceFileName, targetFilePath),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})
}

func TestRunDryRun(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	files := [...]string{"cpfile.txt", "mvfile.txt", "rmfile.txt"}
	for _, f := range files {
		putFile(t, s3client, bucket, f, "content")
	}

	filecontent := []string{
		fmt.Sprintf("cp s3://%v/%s s3://%v/cp_%s", bucket, files[0], bucket, files[0]),
		fmt.Sprintf("mv s3://%v/%s s3://%v/mv_%s", bucket, files[1], bucket, files[1]),
		fmt.Sprintf("rm s3://%v/%s", bucket, files[2]),
	}

	file := fs.NewFile(t, "prefix", fs.WithContent(strings.Join(filecontent, "\n")))
	defer file.Remove()

	cmd := s5cmd("--dry-run", "run", file.Path())
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(filecontent[0]),
		1: equals(filecontent[1]),
		2: equals(filecontent[2]),
	}, sortInput(true))

	// ensure no side effect for copy operation
	err := ensureS3Object(s3client, bucket, "cp_"+files[0], "content")
	assertError(t, err, errS3NoSuchKey)

	// ensure no side effect for move operation
	assert.Assert(t, ensureS3Object(s3client, bucket, files[1], "content"))

	err = ensureS3Object(s3client, bucket, "mv_"+files[1], "content")
	assertError(t, err, errS3NoSuchKey)

	// ensure no side effect for remove operation
	assert.Assert(t, ensureS3Object(s3client, bucket, files[2], "content"))
}

func TestRunFixDataRace_Issue301(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file1.txt", "content")

	// The fix is applicable to the run command, where there should be at least
	// 2 of the same command given as input.
	input := strings.NewReader(
		strings.Join([]string{
			fmt.Sprintf("ls s3://%v/file1.txt", bucket),
			fmt.Sprintf("ls s3://%v/file1.txt", bucket),
		}, "\n"),
	)
	cmd := s5cmd("run")
	result := icmd.RunCmd(cmd, icmd.WithStdin(input))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("file1.txt"),
		1: suffix("file1.txt"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})
}
