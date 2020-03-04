package e2e

import (
	"fmt"
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

	// TODO(os): When we implement command smth like 'move',
	// 			 this assertion needs to be updated.
	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`download s3://%v/%v`, bucket, filename),
		1: equals(""),
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

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`download s3://%v/another_test_file.txt`, bucket),
		2: suffix(`download s3://%v/filename-with-hypen.gz`, bucket),
		3: suffix(`download s3://%v/readme.md`, bucket),
		4: suffix(`download s3://%v/testfile1.txt`, bucket),
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

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`upload %v`, filename),
		1: equals(""),
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

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`upload another_test_file.txt`),
		2: suffix(`upload filename-with-hypen.gz`),
		3: suffix(`upload readme.md`),
		4: suffix(`upload testfile1.txt`),
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
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a file content"
	)

	src := fmt.Sprintf("s3://%v/%v", bucket, filename)
	dst := fmt.Sprintf("s3://%v/dst/%v", bucket, filename)

	putFile(t, s3client, bucket, filename, content)

	cmd := s5cmd("mv", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`copy s3://%v/testfile1.txt`, bucket),
		1: equals(""),
	})

	// expect no s3 source object
	err := ensureS3Object(s3client, bucket, filename, content)
	assertError(t, err, errS3NoSuchKey)

	// assert s3 destination object
	assert.Assert(t, ensureS3Object(s3client, bucket, "dst/"+filename, content))
}

func TestMoveSingleS3ObjectIntoAnotherBucket(t *testing.T) {
	t.Parallel()

	srcbucket := s3BucketFromTestName(t)
	dstbucket := "copy-" + s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, srcbucket)
	createBucket(t, s3client, dstbucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a file content"
	)

	putFile(t, s3client, srcbucket, filename, content)

	src := fmt.Sprintf("s3://%v/%v", srcbucket, filename)
	dst := fmt.Sprintf("s3://%v/%v", dstbucket, filename)

	cmd := s5cmd("mv", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`copy s3://%v/testfile1.txt`, srcbucket),
		1: equals(""),
	})

	// expect no s3 source object
	err := ensureS3Object(s3client, srcbucket, filename, content)
	assertError(t, err, errS3NoSuchKey)

	// assert s3 destination object
	assert.Assert(t, ensureS3Object(s3client, dstbucket, filename, content))
}

func TestMoveMultipleS3ObjectsToS3(t *testing.T) {
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

	src := fmt.Sprintf("s3://%v/*", bucket)
	dst := fmt.Sprintf("s3://%v/dst/", bucket)

	cmd := s5cmd("mv", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: contains(""),
		1: suffix("copy s3://%v/another_test_file.txt", bucket),
		2: suffix("copy s3://%v/filename-with-hypen.gz", bucket),
		3: suffix("copy s3://%v/readme.md", bucket),
		4: suffix("copy s3://%v/testfile1.txt", bucket),
	}, sortInput(true))

	// expect no s3 source objects
	for srcfile, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, srcfile, content)
		assertError(t, err, errS3NoSuchKey)
	}

	// assert s3 destination objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, "dst/"+filename, content))
	}
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

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("copy testfile1.txt"),
		1: equals(""),
	})

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
		0: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix("copy another_test_file.txt"),
		2: suffix("copy testfile1.txt"),
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
