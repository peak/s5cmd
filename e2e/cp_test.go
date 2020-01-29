package e2e

import (
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestCopySingleS3ObjectToLocal(t *testing.T) {
	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a file content"
	)

	putFile(t, s3client, bucket, filename, content)

	cmd := s5cmd("cp", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "cp s3://` + bucket + `/testfile1.txt ./testfile1.txt"`),
		1: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`# Downloading testfile1.txt...`),
		1: equals(""),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, content, fs.WithMode(0644)))
	assert.Assert(t, fs.Equal(cmd.Dir, expected))
}

func TestCopyMultipleFlatS3ObjectsToLocal(t *testing.T) {
	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen it's name. btw, gz?",
		"another_test_file.txt":  "yet another txt file. yatf.",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("cp", "s3://"+bucket+"/*", ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "cp s3://` + bucket + `/* ./" (4)`),
		1: suffix(` # All workers idle, finishing up...`),
		2: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`# Downloading another_test_file.txt...`),
		2: suffix(`# Downloading filename-with-hypen.gz...`),
		3: suffix(`# Downloading readme.md...`),
		4: suffix(`# Downloading testfile1.txt...`),
		5: contains(` + "cp s3://test-copy-multiple-flat-s-3-objects-to-local/another_test_file.txt ./another_test_file.txt`),
		6: contains(` + "cp s3://test-copy-multiple-flat-s-3-objects-to-local/filename-with-hypen.gz ./filename-with-hypen.gz"`),
		7: contains(` + "cp s3://test-copy-multiple-flat-s-3-objects-to-local/readme.md ./readme.md"`),
		8: contains(` + "cp s3://test-copy-multiple-flat-s-3-objects-to-local/testfile1.txt ./testfile1.txt"`),
	}, sortInput(true))

	// assert local filesystem
	var expectedFiles []fs.PathOp
	for filename, content := range filesToContent {
		pathop := fs.WithFile(filename, content, fs.WithMode(0644))
		expectedFiles = append(expectedFiles, pathop)
	}

	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))
}
