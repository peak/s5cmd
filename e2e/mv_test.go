package e2e

import (
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestMoveSingleS3ObjectToLocal(t *testing.T) {
	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a file content"
	)

	putFile(t, s3client, bucket, filename, content)

	cmd := s5cmd("mv", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "mv s3://%v/testfile1.txt ./testfile1.txt"`, bucket),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`# Downloading testfile1.txt...`),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, content, fs.WithMode(0644)))
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// TODO: assert s3 if the file is there
}
func TestMoveMultipleFlatS3ObjectsToLocal(t *testing.T) {
	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("mv", "s3://"+bucket+"/*", ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "mv s3://%v/* ./" (4)`, bucket),
		1: suffix(` # All workers idle, finishing up...`),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`# Downloading another_test_file.txt...`),
		2: suffix(`# Downloading filename-with-hypen.gz...`),
		3: suffix(`# Downloading readme.md...`),
		4: suffix(`# Downloading testfile1.txt...`),
		5: contains(` + "mv s3://%v/another_test_file.txt ./another_test_file.txt`, bucket),
		6: contains(` + "mv s3://%v/filename-with-hypen.gz ./filename-with-hypen.gz"`, bucket),
		7: contains(` + "mv s3://%v/readme.md ./readme.md"`, bucket),
		8: contains(` + "mv s3://%v/testfile1.txt ./testfile1.txt"`, bucket),
	}, sortInput(true))

	// assert local filesystem
	var expectedFiles []fs.PathOp
	for filename, content := range filesToContent {
		pathop := fs.WithFile(filename, content, fs.WithMode(0644))
		expectedFiles = append(expectedFiles, pathop)
	}

	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// TODO: assert s3
}

func TestMoveSingleFileToS3(t *testing.T) {
	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a test file"
	)

	file := fs.NewFile(t, filename, fs.WithContent(content))
	defer file.Remove()

	fpath := file.Path()
	fname := filepath.Base(file.Path())

	cmd := s5cmd("mv", fpath, "s3://"+bucket+"/")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "mv %v s3://%v/%v"`, fpath, bucket, fname),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(` # Uploading %v... (%v bytes)`, fname, len(content)),
	})

	// TODO(ig): assert filesystem
	// TODO(ig): assert s3 object
}

func TestMoveMultipleFilesToS3(t *testing.T)     {}
func TestMoveSingleS3ObjectToS3(t *testing.T)    {}
func TestMoveMultipleS3ObjectsToS3(t *testing.T) {}
func TestMoveSingleFileToLocal(t *testing.T)     {}
func TestMoveMultipleFilesToLocal(t *testing.T)  {}
