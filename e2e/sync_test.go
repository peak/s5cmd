package e2e

import (
	"fmt"
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

// sync folder/ folder2/
func TestSyncLocalToLocalNotPermitted(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		sourceFolder = "source"
		destFolder   = "dest"
	)
	sourceWorkDir := fs.NewDir(t, sourceFolder)
	destWorkDir := fs.NewDir(t, destFolder)

	srcpath := filepath.ToSlash(sourceWorkDir.Path())
	destpath := filepath.ToSlash(destWorkDir.Path())

	cmd := s5cmd("sync", srcpath, destpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "sync %s %s": local->local sync operations are not permitted`, srcpath, destpath),
	})
}

// sync source.go s3://buckey
func TestSyncLocalFileToS3NotPermitted(t *testing.T) {
	t.Parallel()

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename = "source.go"
		bucket   = "bucket"
	)

	createBucket(t, s3client, bucket)

	sourceWorkDir := fs.NewFile(t, filename)
	srcpath := filepath.ToSlash(sourceWorkDir.Path())
	dstpath := fmt.Sprintf("s3://%s/", bucket)

	cmd := s5cmd("sync", srcpath, dstpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "sync %s %s": local source must be a directory`, srcpath, dstpath),
	})
}

// sync s3://bucket/source.go .
func TestSyncSingleS3ObjectToLocalNotPermitted(t *testing.T) {
	t.Parallel()

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename = "source.go"
		bucket   = "bucket"
		content  = "content"
	)

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, content)

	srcpath := fmt.Sprintf("s3://%s/%s", bucket, filename)

	cmd := s5cmd("sync", srcpath, ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "sync %s .": remote source %q must be a bucket or a prefix`, srcpath, srcpath),
	})
}

/* // sync folder/ s3://bucket
func TestSyncLocalFolderToS3EmptyBucket(t *testing.T) {
	t.Parallel()
	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file 1"),
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithDir(
			"a",
			fs.WithFile("another_test_file.txt", "yet another txt file. yatf."),
		),
		fs.WithDir(
			"b",
			fs.WithFile("filename-with-hypen.gz", "file has hypen in its name"),
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`upload %va/another_test_file.txt %va/another_test_file.txt`, src, dst),
		1: equals(`upload %vb/filename-with-hypen.gz %vb/filename-with-hypen.gz`, src, dst),
		2: equals(`upload %vreadme.md %vreadme.md`, src, dst),
		3: equals(`upload %vtestfile1.txt %vtestfile1.txt`, src, dst),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	expectedS3Content := map[string]string{
		"testfile1.txt":            "this is a test file 1",
		"readme.md":                "this is a readme file",
		"b/filename-with-hypen.gz": "file has hypen in its name",
		"a/another_test_file.txt":  "yet another txt file. yatf.",
	}

	// assert s3
	for key, content := range expectedS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync  s3://bucket/* .
func TestSyncS3BucketToLocal(t *testing.T) {
	t.Parallel()
	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	S3Content := map[string]string{
		"testfile1.txt":            "this is a test file 1",
		"readme.md":                "this is a readme file",
		"b/filename-with-hypen.gz": "file has hypen in its name",
		"a/another_test_file.txt":  "yet another txt file. yatf.",
	}

	for filename, content := range S3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file 1"),
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithDir(
			"a",
			fs.WithFile("another_test_file.txt", "yet another txt file. yatf."),
		),
		fs.WithDir(
			"b",
			fs.WithFile("filename-with-hypen.gz", "file has hypen in its name"),
		),
	}

	src := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", src+"*", ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`download %va/another_test_file.txt a/another_test_file.txt`, src),
		1: equals(`download %vb/filename-with-hypen.gz b/filename-with-hypen.gz`, src),
		2: equals(`download %vreadme.md readme.md`, src),
		3: equals(`download %vtestfile1.txt testfile1.txt`, src),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3
	for key, content := range S3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
} */

// sync  s3://bucket/* .
func TestSyncS3BucketToLocalWithDelete(t *testing.T) {
	t.Parallel()
	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	S3Content := map[string]string{
		"testfile1.txt": "this is a test file 1",
		"readme.md":     "this is a readme file",
	}

	for filename, content := range S3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file 1"),
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithDir(
			"dir",
			fs.WithFile("main.py", "python file"),
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)
	src := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", "--delete", "--size-only", src+"*", dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`delete %vdir/main.py`, dst),
	}, sortInput(true))

	expectedFolderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file 1"),
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithDir(
			"dir",
		),
	}

	// assert local filesystem
	expected := fs.Expected(t, expectedFolderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range S3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

/* // sync folder/ s3://bucket
func TestSyncLocalFolderToS3BucketSameObjectsSameModTime(t *testing.T) {
	t.Parallel()
	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	S3Content := map[string]string{
		"testfile1.txt":            "this is a test file 1",
		"readme.md":                "this is a readme file",
		"b/filename-with-hypen.gz": "file has hypen in its name",
		"a/another_test_file.txt":  "yet another txt file. yatf.",
	}

	for filename, content := range S3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file 1"),
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithDir(
			"a",
			fs.WithFile("another_test_file.txt", "yet another txt file. yatf."),
		),
		fs.WithDir(
			"b",
			fs.WithFile("filename-with-hypen.gz", "file has hypen in its name"),
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %va/another_test_file.txt %va/another_test_file.txt": object is newer or same age`, src, dst),
		1: equals(`DEBUG "sync %vb/filename-with-hypen.gz %vb/filename-with-hypen.gz": object is newer or same age`, src, dst),
		2: equals(`DEBUG "sync %vreadme.md %vreadme.md": object is newer or same age`, src, dst),
		3: equals(`DEBUG "sync %vtestfile1.txt %vtestfile1.txt": object is newer or same age`, src, dst),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range S3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}
*/
