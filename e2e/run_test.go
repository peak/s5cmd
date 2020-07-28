package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/peak/s5cmd/storage"

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

	result.Assert(t, icmd.Success)

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

func TestRunFromFileWithMultipleSourceAndDestinationRegions(t *testing.T) {
	t.Parallel()

	const numOfRegions = 4

	var buckets [numOfRegions]string
	for i := range buckets {
		buckets[i] = randomString(30)
	}

	endpoint, workdir, cleanup := server(t, "bolt")
	defer cleanup()

	s5cmd := s5cmd(workdir, endpoint)

	regions := [numOfRegions]string{"us-east-2", "eu-east-1", "eu-central-2", "us-west-1"}
	clients := [5]*s3.S3{}
	for i, r := range regions {
		clients[i] = s3client(t, storage.S3Options{
			Endpoint:    endpoint,
			Region:      r,
			NoVerifySSL: true,
		})
	}
	for i := 0; i < numOfRegions; i++ {
		createBucket(t, clients[i], buckets[i])

		filename := fmt.Sprintf("file%d.txt", i)
		putFile(t, clients[i], buckets[i], filename, "content")
	}

	filecontent := strings.Join([]string{
		fmt.Sprintf("ls s3://%v/file0.txt", buckets[0]),
		fmt.Sprintf("cp s3://%v/file1.txt s3://%v/", buckets[1], buckets[2]),
		fmt.Sprintf("mv s3://%v/file2.txt s3://%v/", buckets[2], buckets[3]),
	}, "\n")

	file := fs.NewFile(t, "prefix", fs.WithContent(filecontent))
	defer file.Remove()

	cmd := s5cmd("run", "--source-region=us-east-2", file.Path())
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("file0.txt"),
		1: equals("cp s3://%v/file1.txt s3://%v/file1.txt", buckets[1], buckets[2]),
		2: equals("mv s3://%v/file2.txt s3://%v/file2.txt", buckets[2], buckets[3]),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})
}
