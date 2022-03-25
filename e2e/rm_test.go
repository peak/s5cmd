package e2e

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

// rm s3://bucket/object
func TestRemoveSingleS3Object(t *testing.T) {
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

	cmd := s5cmd("rm", "s3://"+bucket+"/"+filename)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm s3://%v/testfile1.txt", bucket),
	})

	// assert s3 object
	err := ensureS3Object(s3client, bucket, filename, content)
	assertError(t, err, errS3NoSuchKey)
}

// --json rm s3://bucket/object
func TestRemoveSingleS3ObjectJSON(t *testing.T) {
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

	cmd := s5cmd("--json", "rm", "s3://"+bucket+"/"+filename)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/%v"
			}
		`, bucket, filename),
	})

	// assert s3 object
	err := ensureS3Object(s3client, bucket, filename, content)
	assertError(t, err, errS3NoSuchKey)
}

// rm s3://bucket/*
func TestRemoveMultipleS3Objects(t *testing.T) {
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

	cmd := s5cmd("rm", "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/another_test_file.txt`, bucket),
		1: equals(`rm s3://%v/filename-with-hypen.gz`, bucket),
		2: equals(`rm s3://%v/readme.md`, bucket),
		3: equals(`rm s3://%v/testfile1.txt`, bucket),
	}, sortInput(true))

	// assert s3 objects
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// --json rm s3://bucket/*
func TestRemoveMultipleS3ObjectsJSON(t *testing.T) {
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

	cmd := s5cmd("--json", "rm", "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/another_test_file.txt"
			}
		`, bucket),
		1: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/filename-with-hypen.gz"
			}
		`, bucket),
		2: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/readme.md"
			}
		`, bucket),
		3: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/testfile1.txt"
			}
		`, bucket),
	}, sortInput(true))

	// assert s3 objects
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}

}

// rm s3://bucket/* (removes 10k objects)
func TestRemoveTenThousandS3Objects(t *testing.T) {
	t.Parallel()

	// flaky test, skip it
	t.Skip()

	bucket := s3BucketFromTestName(t)

	// ten thousand s3 objects are created for this test. by default, s3 backend is
	// bolt but we need speed for this test, hence use in-memory storage.
	s3client, s5cmd, cleanup := setup(t, withS3Backend("mem"))
	defer cleanup()

	createBucket(t, s3client, bucket)

	const filecount = 10000

	filenameFunc := func(i int) string { return fmt.Sprintf("file_%06d", i) }
	contentFunc := func(i int) string { return fmt.Sprintf("file body %06d", i) }

	for i := 0; i < filecount; i++ {
		filename := filenameFunc(i)
		content := contentFunc(i)
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("rm", "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	expected := make(map[int]compareFunc)
	for i := 0; i < filecount; i++ {
		expected[i] = contains(`rm s3://%v/file_%06d`, bucket, i)
	}

	assertLines(t, result.Stdout(), expected, sortInput(true))

	// assert s3 objects
	for i := 0; i < filecount; i++ {
		filename := filenameFunc(i)
		content := contentFunc(i)

		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// rm s3://bucket/prefix
func TestRemoveS3PrefixWithoutSlash(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const prefix = "prefix"
	src := fmt.Sprintf("s3://%v/%v", bucket, prefix)

	cmd := s5cmd("rm", src)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm s3://%v/%v", bucket, prefix),
	})
}

// rm file
func TestRemoveSingleLocalFile(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename = "testfile1.txt"
		content  = "this is a test file"
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	cmd := s5cmd("rm", filename)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm %v`, filename),
	})

	// assert local filesystem
	expected := fs.Expected(t)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// rm *.ext
func TestRemoveMultipleLocalFilesShouldNotFail(t *testing.T) {
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

	cmd := s5cmd("rm", "*.txt")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm another_test_file.txt"),
		1: equals("rm testfile1.txt"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	// assert local filesystem
	expectedFiles := []fs.PathOp{
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithFile("filename-with-hypen.gz", "file has hypen in its name"),
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// rm dir/
func TestRemoveLocalDirectory(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"testdir",
			fs.WithFile("file1.txt", "this is the first test file"),
			fs.WithFile("file2.txt", "this is the second test file"),
			fs.WithFile("readme.md", "this is a readme file"),
		),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	cmd := s5cmd("rm", "testdir")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm testdir/file1.txt"),
		1: equals("rm testdir/file2.txt"),
		2: equals("rm testdir/readme.md"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	// expected empty dir
	expected := fs.Expected(t, fs.WithDir("testdir"))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// rm prefix*
func TestRemoveLocalDirectoryWithGlob(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("Files in Windows cannot contain glob(*) characters")
	}

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"abc*",
			fs.WithFile("file1.txt", "this is the first test file"),
			fs.WithFile("file2.txt", "this is the second test file"),
		),
		fs.WithDir(
			"abcd",
			fs.WithFile("file1.txt", "this is the first test file"),
		),
		fs.WithDir(
			"abcde",
			fs.WithFile("file1.txt", "this is the first test file"),
			fs.WithFile("file2.txt", "this is the second test file"),
		),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	cmd := s5cmd("rm", "abc*")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm abc*/file1.txt"),
		1: equals("rm abc*/file2.txt"),
		2: equals("rm abcd/file1.txt"),
		3: equals("rm abcde/file1.txt"),
		4: equals("rm abcde/file2.txt"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	// expected 3 empty dirs
	expected := fs.Expected(
		t,
		fs.WithDir("abc*"),
		fs.WithDir("abcd"),
		fs.WithDir("abcde"),
	)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// rm dir/ file file2
func TestVariadicMultipleLocalFilesWithDirectory(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"testdir",
			fs.WithFile("readme.md", "this is a readme file"),
		),
		fs.WithFile("file1.txt", "this is the first test file"),
		fs.WithFile("file2.txt", "this is the second test file"),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	cmd := s5cmd("rm", "testdir", "file1.txt", "file2.txt")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm file1.txt"),
		1: equals("rm file2.txt"),
		2: equals("rm testdir/readme.md"),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	// expected empty dir
	expected := fs.Expected(t, fs.WithDir("testdir"))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// rm s3://bucket/object s3://bucket/object2
func TestVariadicRemoveS3Objects(t *testing.T) {
	t.Parallel()

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"file1.txt": "file1 content",
		"file2.txt": "file2 content",
		"file3.txt": "file3 content",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	// file4.txt is non-existent. s5cmd sends DeleteObjects request but S3
	// doesn't report whether if the given object is exists, hence reported as
	// deleted. We want to keep this behaviour.
	cmd := s5cmd(
		"rm",
		"s3://"+bucket+"/file1.txt",
		"s3://"+bucket+"/file2.txt",
		"s3://"+bucket+"/file3.txt",
		"s3://"+bucket+"/file4.txt",
	)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/file1.txt`, bucket),
		1: equals(`rm s3://%v/file2.txt`, bucket),
		2: equals(`rm s3://%v/file3.txt`, bucket),
		3: equals(`rm s3://%v/file4.txt`, bucket),
	}, sortInput(true))

	// assert s3 objects
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// rm s3://bucket/prefix/* s3://bucket/object
func TestVariadicRemoveS3ObjectsWithWildcard(t *testing.T) {
	t.Parallel()

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testdir1/file1.txt":     "file1 content",
		"testdir1/file2.txt":     "file2 content",
		"testdir1/dir/file3.txt": "file23content",
		"file4.txt":              "file4 content",
		"testdir2/file5.txt":     "file5 content",
		"testdir2/file6.txt":     "file6 content",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd(
		"rm",
		"s3://"+bucket+"/testdir1/*",
		"s3://"+bucket+"/testdir2/*",
		"s3://"+bucket+"/file4.txt",
	)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/file4.txt`, bucket),
		1: equals(`rm s3://%v/testdir1/dir/file3.txt`, bucket),
		2: equals(`rm s3://%v/testdir1/file1.txt`, bucket),
		3: equals(`rm s3://%v/testdir1/file2.txt`, bucket),
		4: equals(`rm s3://%v/testdir2/file5.txt`, bucket),
		5: equals(`rm s3://%v/testdir2/file6.txt`, bucket),
	}, sortInput(true))

	// assert s3 objects
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// rm file s3://bucket/object
func TestRemoveMultipleMixedObjects(t *testing.T) {
	t.Parallel()

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const bucket = "bucket"
	createBucket(t, s3client, bucket)

	const (
		filename = "file.txt"
		content  = "this is a test file"

		objectname = "object.txt"
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	putFile(t, s3client, bucket, objectname, content)

	remoteSource := fmt.Sprintf("s3://%v/%v", bucket, objectname)

	cmd := s5cmd("rm", filename, remoteSource)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "rm %v %v": arguments cannot have both local and remote sources`, filename, remoteSource),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, content))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3 object
	assert.Assert(t, ensureS3Object(s3client, bucket, objectname, content))
}

// --dry-run rm s3://bucket/*
func TestRemoveMultipleS3ObjectsDryRun(t *testing.T) {
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

	cmd := s5cmd("--dry-run", "rm", "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/another_test_file.txt`, bucket),
		1: equals(`rm s3://%v/filename-with-hypen.gz`, bucket),
		2: equals(`rm s3://%v/readme.md`, bucket),
		3: equals(`rm s3://%v/testfile1.txt`, bucket),
	}, sortInput(true))

	// assert s3 objects were not removed
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestRemoveS3ObjectRawFlag(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"file*.txt":  "this is a test file 1",
		"file*1.txt": "this is a test file 2",
		"file*.py":   "test file 1 python version",
		"file*.c":    "test file 1 c version.",
	}

	nonDeletedFiles := map[string]string{
		"file*1.txt": "this is a test file 2",
		"file*.py":   "test file 1 python version",
		"file*.c":    "test file 1 c version.",
	}

	deletedFiles := map[string]string{
		"file*.txt": "this is a test file 1",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("rm", "-raw", "s3://"+bucket+"/file*.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/file*.txt`, bucket),
	})

	// ensure files which is not supposed to be deleted.
	for filename, content := range nonDeletedFiles {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}

	// add no such key
	for filename, content := range deletedFiles {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

func TestRemoveS3ObjectsPrefixRawFlag(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"abc*/file.txt":  "this is a test file 1",
		"abc*/file1.txt": "this is a test file 2",
		"abc*/file.py":   "test file 1 python version",
		"abc*/file.c":    "test file 1 c version.",
		"abcd/file.txt":  "this is a test file with different prefix",
	}

	nonDeletedFiles := map[string]string{
		"abc*/file1.txt": "this is a test file 2",
		"abc*/file.py":   "test file 1 python version",
		"abc*/file.c":    "test file 1 c version.",
		"abcd/file.txt":  "this is a test file with different prefix",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("rm", "-raw", "s3://"+bucket+"/abc*/file.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/abc*/file.txt`, bucket),
	})

	// ensure files which is not supposed to be deleted.
	for filename, content := range nonDeletedFiles {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestRemoveS3PrefixRawFlag(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"abc*/file.txt":  "this is a test file 1",
		"abc*/file1.txt": "this is a test file 2",
		"abc*/file.py":   "test file 1 python version",
		"abc*/file.c":    "test file 1 c version.",
		"abcd/file.txt":  "this is a test file with different prefix",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("rm", "-raw", "s3://"+bucket+"/abc*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/abc*`, bucket), // It prints but does not delete.
	})

	// all of the files should be in S3
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

// rm --exclude "*.txt" s3://bucket/*
func TestRemoveMultipleS3ObjectsWithExcludeFilter(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)
	const excludePattern = "*.txt"

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
		"a/file.txt":             "this is a txt file",
		"b/main.txt":             "this is the second txt file",
		"a/file.py":              "this is a python file with prefix a",
	}

	expectedFiles := map[string]string{
		"testfile1.txt":         "this is a test file 1",
		"another_test_file.txt": "yet another txt file. yatf.",
		"a/file.txt":            "this is a txt file",
		"b/main.txt":            "this is the second txt file",
	}

	nonExpectedFiles := map[string]string{
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"a/file.py":              "this is a python file with prefix a",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("rm", "--exclude", excludePattern, "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/a/file.py`, bucket),
		1: equals(`rm s3://%v/filename-with-hypen.gz`, bucket),
		2: equals(`rm s3://%v/readme.md`, bucket),
	}, sortInput(true))

	// assert s3 objects were not removed
	for filename, content := range expectedFiles {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}

	// assert s3 objects should be removed
	for filename, content := range nonExpectedFiles {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// rm --exclude "*.txt" "*.gz" s3://bucket/*
func TestRemoveMultipleS3ObjectsWithExcludeFilters(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		excludePattern1 = "*.txt"
		excludePattern2 = "*.gz"
	)

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
		"a/file.txt":             "this is a txt file",
		"b/main.txt":             "this is the second txt file",
		"a/file.py":              "this is a python file with prefix a",
	}

	expectedFiles := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"another_test_file.txt":  "yet another txt file. yatf.",
		"a/file.txt":             "this is a txt file",
		"b/main.txt":             "this is the second txt file",
		"filename-with-hypen.gz": "file has hypen in its name",
	}

	nonExpectedFiles := map[string]string{
		"readme.md": "this is a readme file",
		"a/file.py": "this is a python file with prefix a",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("rm", "--exclude", excludePattern1, "--exclude", excludePattern2, "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/a/file.py`, bucket),
		1: equals(`rm s3://%v/readme.md`, bucket),
	}, sortInput(true))

	// assert s3 objects were not removed
	for filename, content := range expectedFiles {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}

	// assert s3 objects should be removed
	for filename, content := range nonExpectedFiles {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// rm --exclude "" s3://bucket/*
func TestRemoveS3ObjectsWithEmptyExcludeFilter(t *testing.T) {
	t.Parallel()

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const excludePattern = ""

	filesToContent := map[string]string{
		"testdir1/file1.txt":     "file1 content",
		"testdir1/file2.txt":     "file2 content",
		"testdir1/dir/file3.txt": "file23content",
		"file4.txt":              "file4 content",
		"testdir2/file5.txt":     "file5 content",
		"testdir2/file6.txt":     "file6 content",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("rm", "--exclude", excludePattern, "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm s3://%v/file4.txt`, bucket),
		1: equals(`rm s3://%v/testdir1/dir/file3.txt`, bucket),
		2: equals(`rm s3://%v/testdir1/file1.txt`, bucket),
		3: equals(`rm s3://%v/testdir1/file2.txt`, bucket),
		4: equals(`rm s3://%v/testdir2/file5.txt`, bucket),
		5: equals(`rm s3://%v/testdir2/file6.txt`, bucket),
	}, sortInput(true))

	// assert s3 objects not in s3 bucket.
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// rm --exclude "*.txt" dir
// rm --exclude "*.txt" dir/
// rm --exclude "*.txt" dir/*
func TestRemoveLocalDirectoryWithExcludeFilter(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name            string
		directoryPrefix string
	}{
		{
			name:            "folder without /",
			directoryPrefix: "",
		},
		{
			name:            "folder with /",
			directoryPrefix: "/",
		},
		{
			name:            "folder with / and glob *",
			directoryPrefix: "/*",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, s5cmd, cleanup := setup(t)
			defer cleanup()

			folderLayout := []fs.PathOp{
				fs.WithDir(
					"testdir",
					fs.WithFile("file1.txt", "this is the first test file"),
					fs.WithFile("file2.txt", "this is the second test file"),
					fs.WithFile("readme.md", "this is a readme file"),
				),
			}

			const excludePattern = "*.txt"

			workdir := fs.NewDir(t, t.Name(), folderLayout...)
			defer workdir.Remove()

			srcpath := workdir.Path() + tc.directoryPrefix
			srcpath = filepath.ToSlash(srcpath)

			cmd := s5cmd("rm", "--exclude", excludePattern, srcpath)
			result := icmd.RunCmd(cmd, withWorkingDir(workdir))

			result.Assert(t, icmd.Success)

			assertLines(t, result.Stdout(), map[int]compareFunc{
				0: equals("rm %v/testdir/readme.md", filepath.ToSlash(workdir.Path())),
			}, sortInput(true))

			assertLines(t, result.Stderr(), map[int]compareFunc{})

			expectedFileSystem := []fs.PathOp{
				fs.WithDir(
					"testdir",
					fs.WithFile("file1.txt", "this is the first test file"),
					fs.WithFile("file2.txt", "this is the second test file"),
				),
			}

			// assert local filesystem
			expected := fs.Expected(t, expectedFileSystem...)
			assert.Assert(t, fs.Equal(workdir.Path(), expected))
		})
	}
}

// rm --exclude "*.txt" --exclude "main*" .
func TestRemoveLocalFilesWithExcludeFilters(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"testdir",
			fs.WithFile("file1.txt", "this is the first test file"),
			fs.WithFile("file2.txt", "this is the second test file"),
			fs.WithFile("readme.md", "this is a readme file"),
		),
		fs.WithFile("main.py", "python file"),
		fs.WithFile("engine.js", "js file"),
		fs.WithFile("main.c", "c file"),
	}

	const (
		excludePattern1 = "*.txt"
		excludePattern2 = "main*"
	)

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	srcpath := workdir.Path()
	srcpath = filepath.ToSlash(srcpath)

	cmd := s5cmd("rm", "--exclude", excludePattern1, "--exclude", excludePattern2, srcpath)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm %v/engine.js", filepath.ToSlash(workdir.Path())),
		1: equals("rm %v/testdir/readme.md", filepath.ToSlash(workdir.Path())),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	expectedFileSystem := []fs.PathOp{
		fs.WithDir(
			"testdir",
			fs.WithFile("file1.txt", "this is the first test file"),
			fs.WithFile("file2.txt", "this is the second test file"),
		),
		fs.WithFile("main.py", "python file"),
		fs.WithFile("main.c", "c file"),
	}

	// assert local filesystem
	expected := fs.Expected(t, expectedFileSystem...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// rm --exclude "abc*" dir/*.txt
func TestRemoveLocalFilesWithPrefixandExcludeFilters(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"testdir",
			fs.WithFile("testfile1.txt", "this is the first test file"),
			fs.WithFile("testfile2.txt", "this is the second test file"),
			fs.WithFile("abc.txt", "this is the first test file"),
			fs.WithFile("readme.md", "this is a readme file"),
		),
		fs.WithFile("main.py", "python file"),
		fs.WithFile("engine.js", "js file"),
		fs.WithFile("abc.txt", "file with abc suffix"),
	}

	const (
		excludePattern = "abc*"
	)

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	srcpath := workdir.Join("testdir")
	srcpath = fmt.Sprintf("%v/*.txt", srcpath)
	srcpath = filepath.ToSlash(srcpath)

	cmd := s5cmd("rm", "--exclude", excludePattern, srcpath)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm %v/testdir/testfile1.txt", filepath.ToSlash(workdir.Path())),
		1: equals("rm %v/testdir/testfile2.txt", filepath.ToSlash(workdir.Path())),
	}, sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	expectedFileSystem := []fs.PathOp{
		fs.WithDir(
			"testdir",
			fs.WithFile("abc.txt", "this is the first test file"),
			fs.WithFile("readme.md", "this is a readme file"),
		),
		fs.WithFile("main.py", "python file"),
		fs.WithFile("engine.js", "js file"),
		fs.WithFile("abc.txt", "file with abc suffix"),
	}

	// assert local filesystem
	expected := fs.Expected(t, expectedFileSystem...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// rm --raw nonexistentfile
func TestRemovetNonexistingLocalFile(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("rm", "nonexistentfile")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stdout(), map[int]compareFunc{})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "rm nonexistentfile": no object found`),
	}, strictLineCheck(true))
}
