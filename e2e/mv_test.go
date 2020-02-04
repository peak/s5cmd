package e2e

import (
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestMoveSingleS3ObjectToLocal(t *testing.T) {
	t.Parallel()

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

	// assert s3 object
	err := ensureS3Object(s3client, bucket, filename, content)
	assertError(t, err, errS3NoSuchKey)
}

func TestMoveMultipleFlatS3ObjectsToLocal(t *testing.T) {
	t.Parallel()

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

	// assert s3 objects
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

func TestMoveSingleFileToS3(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const content = "this is a test file"

	file := fs.NewFile(t, "", fs.WithContent(content))
	defer file.Remove()

	fpath := file.Path()
	filename := filepath.Base(file.Path())

	cmd := s5cmd("mv", fpath, "s3://"+bucket+"/")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "mv %v s3://%v/%v"`, fpath, bucket, filename),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(` # Uploading %v... (%v bytes)`, filename, len(content)),
	})

	// expect no files on filesystem
	expected := fs.Expected(t)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 object
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

func TestMoveMultipleFilesToS3(t *testing.T) {
	t.Parallel()

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

	var files []fs.PathOp
	for filename, content := range filesToContent {
		op := fs.WithFile(filename, content)
		files = append(files, op)
	}

	workdir := fs.NewDir(t, bucket, files...)
	defer workdir.Remove()

	cmd := s5cmd("mv", workdir.Path()+"/*", "s3://"+bucket+"/")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "mv %v/* s3://%v" (4)`, workdir.Path(), bucket),
		1: suffix(` # All workers idle, finishing up...`),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: contains(` # Uploading another_test_file.txt...`),
		2: contains(` # Uploading filename-with-hypen.gz...`),
		3: contains(` # Uploading readme.md...`),
		4: contains(` # Uploading testfile1.txt...`),
		5: contains(` + "mv %v/another_test_file.txt s3://%v/another_test_file.txt"`, workdir.Path(), bucket),
		6: contains(` + "mv %v/filename-with-hypen.gz s3://%v/filename-with-hypen.gz"`, workdir.Path(), bucket),
		7: contains(` + "mv %v/readme.md s3://%v/readme.md`, workdir.Path(), bucket),
		8: contains(` + "mv %v/testfile1.txt s3://%v/testfile1.txt"`, workdir.Path(), bucket),
	}, sortInput(true))

	// expect no files on filesystem
	expected := fs.Expected(t)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestMoveSingleS3ObjectToS3(t *testing.T) {
	t.Skip("TODO: skipped because gofakes3 fails on bucket-to-bucket copy operation")
}

func TestMoveSingleS3ObjectIntoAnotherBucket(t *testing.T) {
	t.Skip("TODO: skipped because gofakes3 fails on bucket-to-bucket copy operation")
}

func TestMoveMultipleS3ObjectsToS3(t *testing.T) {
	t.Skip("TODO: skipped because gofakes3 fails on bucket-to-bucket copy operation")
}

func TestMoveSingleFileToLocal(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename    = "testfile1.txt"
		newFilename = "testfile1-copy.txt"
		content     = "this is a test file"
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	cmd := s5cmd("mv", filename, newFilename)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "mv %v %v"`, filename, newFilename),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(newFilename, content))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestMoveMultipleFilesToLocal(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
	}

	var files []fs.PathOp
	for filename, content := range filesToContent {
		op := fs.WithFile(filename, content)
		files = append(files, op)
	}

	workdir := fs.NewDir(t, t.Name(), files...)
	defer workdir.Remove()

	cmd := s5cmd("mv", "*.txt", "another-directory/")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "mv *.txt another-directory/" (2)`),
		1: suffix(` # All workers idle, finishing up...`),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(` + "mv another_test_file.txt another-directory//another_test_file.txt"`),
		2: suffix(` + "mv testfile1.txt another-directory//testfile1.txt"`),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(
		t,
		fs.WithMode(0700),
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithFile("filename-with-hypen.gz", "file has hypen in its name"),
		fs.WithDir("another-directory",
			fs.WithMode(0755),
			fs.WithFile("testfile1.txt", "this is a test file 1"),
			fs.WithFile("another_test_file.txt", "yet another txt file. yatf."),
		),
	)

	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}
