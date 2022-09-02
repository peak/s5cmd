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

// mv s3://bucket/key dir/
func TestMoveSingleS3ObjectToLocal(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a file content"
	)

	putFile(t, s3client, bucket, filename, content)

	cmd := s5cmd("mv", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`mv s3://%v/%v %v`, bucket, filename, filename),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, content, fs.WithMode(0644)))
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 object
	err := ensureS3Object(s3client, bucket, filename, content)
	assertError(t, err, errS3NoSuchKey)
}

// mv s3://bucket/key dir/
func TestMoveMultipleS3ObjectsToLocal(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
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
		0: equals(`mv s3://%v/another_test_file.txt another_test_file.txt`, bucket),
		1: equals(`mv s3://%v/filename-with-hypen.gz filename-with-hypen.gz`, bucket),
		2: equals(`mv s3://%v/readme.md readme.md`, bucket),
		3: equals(`mv s3://%v/testfile1.txt testfile1.txt`, bucket),
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

// mv file s3://bucket
func TestMoveSingleFileToS3(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const content = "this is a test file"

	file := fs.NewFile(t, "", fs.WithContent(content))
	defer file.Remove()

	fpath := filepath.ToSlash(file.Path())
	filename := filepath.Base(file.Path())

	dst := fmt.Sprintf("s3://%v/", bucket)
	cmd := s5cmd("mv", fpath, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`mv %v %v%v`, fpath, dst, filename),
	})

	// expect no files on filesystem
	expected := fs.Expected(t)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 object
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

// mv dir/* s3://bucket
func TestMoveMultipleFilesToS3(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
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

	src := filepath.ToSlash(workdir.Path())

	dst := fmt.Sprintf("s3://%v/", bucket)
	cmd := s5cmd("mv", src+"/*", dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`mv %v/another_test_file.txt %vanother_test_file.txt`, src, dst),
		1: equals(`mv %v/filename-with-hypen.gz %vfilename-with-hypen.gz`, src, dst),
		2: equals(`mv %v/readme.md %vreadme.md`, src, dst),
		3: equals(`mv %v/testfile1.txt %vtestfile1.txt`, src, dst),
	}, sortInput(true))

	// expect no files on filesystem
	expected := fs.Expected(t)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

// mv s3://bucket/object s3://bucket2/object
func TestMoveSingleS3ObjectToS3(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
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
		0: equals(`mv %v %v`, src, dst),
	})

	// expect no s3 source object
	err := ensureS3Object(s3client, bucket, filename, content)
	assertError(t, err, errS3NoSuchKey)

	// assert s3 destination object
	assert.Assert(t, ensureS3Object(s3client, bucket, "dst/"+filename, content))
}

// mv s3://bucket/object s3://bucket2/object
func TestMoveSingleS3ObjectIntoAnotherBucket(t *testing.T) {
	t.Parallel()

	srcbucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "copy")

	s3client, s5cmd := setup(t)

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
		0: equals(`mv %v %v`, src, dst),
	})

	// expect no s3 source object
	err := ensureS3Object(s3client, srcbucket, filename, content)
	assertError(t, err, errS3NoSuchKey)

	// assert s3 destination object
	assert.Assert(t, ensureS3Object(s3client, dstbucket, filename, content))
}

// mv s3://bucket/* s3://bucket2/prefix/
func TestMoveMultipleS3ObjectsToS3(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
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
		0: equals("mv s3://%v/another_test_file.txt %vanother_test_file.txt", bucket, dst),
		1: equals("mv s3://%v/filename-with-hypen.gz %vfilename-with-hypen.gz", bucket, dst),
		2: equals("mv s3://%v/readme.md %vreadme.md", bucket, dst),
		3: equals("mv s3://%v/testfile1.txt %vtestfile1.txt", bucket, dst),
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

// --dry-run mv s3://bucket/* s3://bucket2/prefix/
func TestMoveMultipleS3ObjectsToS3DryRun(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
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

	cmd := s5cmd("--dry-run", "mv", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("mv s3://%v/another_test_file.txt %vanother_test_file.txt", bucket, dst),
		1: equals("mv s3://%v/filename-with-hypen.gz %vfilename-with-hypen.gz", bucket, dst),
		2: equals("mv s3://%v/readme.md %vreadme.md", bucket, dst),
		3: equals("mv s3://%v/testfile1.txt %vtestfile1.txt", bucket, dst),
	}, sortInput(true))

	// expect no change on s3 source objects
	for srcfile, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, srcfile, content))
	}

	// assert no objects were copied to s3 destination
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, "dst/"+filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// mv --raw file s3://bucket/
func TestMoveLocalObjectToS3WithRawFlag(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}

	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	objectsToMove := []fs.PathOp{
		fs.WithFile("a*.txt", "content"),
	}

	otherObjects := []fs.PathOp{
		fs.WithDir(
			"a*b",
			fs.WithFile("file.txt", "content"),
		),

		fs.WithFile("abc.txt", "content"),
	}

	folderLayout := append(objectsToMove, otherObjects...)

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	srcpath := filepath.ToSlash(workdir.Join("a*.txt"))
	dstpath := fmt.Sprintf("s3://%v", bucket)

	cmd := s5cmd("mv", "--raw", srcpath, dstpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("mv %v %v/a*.txt", srcpath, dstpath),
	}, sortInput(true))

	expectedObjects := []string{"a*.txt"}
	for _, obj := range expectedObjects {
		err := ensureS3Object(s3client, bucket, obj, "content")
		if err != nil {
			t.Fatalf("Object %s is not in S3\n", obj)
		}
	}

	nonExpectedObjects := []string{"a*b/file.txt", "abc.txt"}
	for _, obj := range nonExpectedObjects {
		err := ensureS3Object(s3client, bucket, obj, "content")
		assertError(t, err, errS3NoSuchKey)
	}

	// assert local filesystem
	expected := fs.Expected(t, otherObjects...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}
