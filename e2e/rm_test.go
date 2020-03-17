package e2e

import (
	"fmt"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

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

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("rm s3://%v/testfile1.txt", bucket),
		1: equals(""),
	})

	// assert s3 object
	err := ensureS3Object(s3client, bucket, filename, content)
	assertError(t, err, errS3NoSuchKey)
}

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

	cmd := s5cmd("-json", "rm", "s3://"+bucket+"/"+filename)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/%v"
			}
		`, bucket, filename),
		1: equals(""),
	})

	// assert s3 object
	err := ensureS3Object(s3client, bucket, filename, content)
	assertError(t, err, errS3NoSuchKey)
}

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

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: equals(`rm s3://%v/another_test_file.txt`, bucket),
		2: equals(`rm s3://%v/filename-with-hypen.gz`, bucket),
		3: equals(`rm s3://%v/readme.md`, bucket),
		4: equals(`rm s3://%v/testfile1.txt`, bucket),
	}, sortInput(true))

	// assert s3 objects
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

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

	cmd := s5cmd("-json", "rm", "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/another_test_file.txt"
			}
		`, bucket),
		2: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/filename-with-hypen.gz"
			}
		`, bucket),
		3: json(`
			{
				"operation": "rm",
				"success": true,
				"source": "s3://%v/readme.md"
			}
		`, bucket),
		4: json(`
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

func TestRemoveTenThousandS3Objects(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	// ten thousand s3 objects are created for this test. by default, s3 backend is
	// bolt but we need speed for this test, hence use in-memory storage.
	s3client, s5cmd, cleanup := setup(t, withS3Backend("mem"))
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		filecount = 10_000
		content   = "file body"
	)

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

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})

	expected := make(map[int]compareFunc)
	expected[0] = equals("")
	for i := 0; i < filecount; i++ {
		expected[i+1] = contains(`rm s3://%v/file_%06d`, bucket, i)
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

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm %v`, filename),
		1: equals(""),
	})

	// assert local filesystem
	expected := fs.Expected(t)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

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

	result.Assert(t, icmd.Expected{ExitCode: 0})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: equals("rm another_test_file.txt"),
		2: equals("rm testfile1.txt"),
	}, strictLineCheck(true), sortInput(true))

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})

	// assert local filesystem
	expectedFiles := []fs.PathOp{
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithFile("filename-with-hypen.gz", "file has hypen in its name"),
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// TODO(ig): re-open once we support variadic args
/*
func TestBatchRemove(t *testing.T) {
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

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(""),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: equals(`rm s3://%v/file1.txt`, bucket),
		2: equals(`rm s3://%v/file2.txt`, bucket),
		3: equals(`rm s3://%v/file3.txt`, bucket),
		4: equals(`rm s3://%v/file4.txt`, bucket),
	}, sortInput(true))

	// assert s3 objects
	for filename, content := range filesToContent {
		err := ensureS3Object(s3client, bucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}
*/
