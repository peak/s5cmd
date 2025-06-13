package e2e

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

// sync -n s3://bucket/object file
func TestSyncFailForNonsharedFlagsFromCopyCommand(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)
	const (
		filename = "source.go"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, "content")

	srcpath := fmt.Sprintf("s3://%s/%s", bucket, filename)

	cmd := s5cmd("sync", "-n", srcpath, ".")
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Expected{ExitCode: 1})

	// urfave.Cli prints the help text and error message to stdout
	// if given flags in not present in command options.
	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("Incorrect Usage: flag provided but not defined: -n"),
	}, strictLineCheck(false))
}

// sync folder/ folder2/
func TestSyncLocalToLocal(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	sourceWorkDir := fs.NewDir(t, "source")
	destWorkDir := fs.NewDir(t, "dest")

	srcpath := filepath.ToSlash(sourceWorkDir.Path())
	destpath := filepath.ToSlash(destWorkDir.Path())

	cmd := s5cmd("sync", srcpath, destpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "sync %s %s": local->local copy operations are not permitted`, srcpath, destpath),
	})
}

// sync s3://bucket/source.go .
func TestSyncSingleS3ObjectToLocalTwice(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	const (
		filename = "source.go"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, "content")

	srcpath := fmt.Sprintf("s3://%s/%s", bucket, filename)

	cmd := s5cmd("sync", srcpath, ".")
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v %v`, srcpath, filename),
	})

	// rerunning same command should not download object, empty result expected
	result = icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)
}

// sync s3://bucket/dir/source.go .
func TestSyncSinglePrefixedS3ObjectToCurrentDirectory(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	const (
		dirname  = "dir"
		filename = "source.go"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, fmt.Sprintf("%s/%s", dirname, filename), "content")

	srcpath := fmt.Sprintf("s3://%s/%s/%s", bucket, dirname, filename)

	cmd := s5cmd("sync", srcpath, ".")
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v %v`, srcpath, filename),
	})

	// rerunning same command should not download object, empty result expected
	result = icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)
	assertLines(t, result.Stdout(), map[int]compareFunc{})
}

// sync s3://bucket/prefix/source.go dir/
func TestSyncPrefixedSingleS3ObjectToLocalDirectory(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	const (
		dirname  = "dir"
		filename = "source.go"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, fmt.Sprintf("%s/%s", dirname, filename), "content")

	srcpath := fmt.Sprintf("s3://%s/%s/%s", bucket, dirname, filename)
	dstpath := "folder"

	cmd := s5cmd("sync", srcpath, fmt.Sprintf("%v/", dstpath))
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v %v/%v`, srcpath, dstpath, filename),
	})

	// rerunning same command should not download object, empty result expected
	result = icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)
	assertLines(t, result.Stdout(), map[int]compareFunc{})
}

// sync s3://bucket/source.go dir/
func TestSyncSingleS3ObjectToLocalDirectory(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	const (
		filename = "source.go"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, "content")

	srcpath := fmt.Sprintf("s3://%s/%s", bucket, filename)
	dstpath := "folder"

	cmd := s5cmd("sync", srcpath, fmt.Sprintf("%v/", dstpath))
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v %v/%v`, srcpath, dstpath, filename),
	})

	// rerunning same command should not download object, empty result expected
	result = icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)
	assertLines(t, result.Stdout(), map[int]compareFunc{})
}

// sync file s3://bucket
func TestSyncLocalFileToS3Twice(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	const (
		filename = "testfile1.txt"
		content  = "this is the content"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// the file to be uploaded is modified
	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	dstpath := fmt.Sprintf("s3://%v", bucket)

	cmd := s5cmd("sync", filename, dstpath)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v %v/%v`, filename, dstpath, filename),
	})

	// rerunning same command should not upload files, empty result expected
	result = icmd.RunCmd(cmd, withWorkingDir(workdir))
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{})
}

// sync file s3://bucket/prefix/
func TestSyncLocalFileToS3Prefix(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	const (
		filename = "testfile1.txt"
		content  = "this is the content"
		dirname  = "dir"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	dstpath := fmt.Sprintf("s3://%v/%v", bucket, dirname)

	cmd := s5cmd("sync", filename, fmt.Sprintf("%v/", dstpath))
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v %v/%v`, filename, dstpath, filename),
	})

	// rerunning same command should not upload files, empty result expected
	result = icmd.RunCmd(cmd, withWorkingDir(workdir))
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{})
}

// sync dir/file s3://bucket
func TestSyncLocalFileInDirectoryToS3(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	const (
		dirname  = "dir"
		filename = "testfile1.txt"
		content  = "this is the content"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	workdir := fs.NewDir(t, t.Name(), fs.WithDir(dirname, fs.WithFile(filename, content)))
	defer workdir.Remove()

	srcpath := fmt.Sprintf("%v/%v", dirname, filename)
	dstpath := fmt.Sprintf("s3://%v", bucket)

	cmd := s5cmd("sync", srcpath, dstpath)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/%v %v/%v`, dirname, filename, dstpath, filename),
	})

	// rerunning same command should not upload files, empty result expected
	result = icmd.RunCmd(cmd, withWorkingDir(workdir))
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{})
}

// sync dir/file s3://bucket/prefix/
func TestSyncLocalFileInDirectoryToS3Prefix(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	const (
		dirname  = "dir"
		filename = "testfile1.txt"
		content  = "this is the content"
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	workdir := fs.NewDir(t, t.Name(), fs.WithDir(dirname, fs.WithFile(filename, content)))
	defer workdir.Remove()

	srcpath := fmt.Sprintf("%v/%v", dirname, filename)
	dstpath := fmt.Sprintf("s3://%v/%v", bucket, dirname)

	cmd := s5cmd("sync", srcpath, fmt.Sprintf("%v/", dstpath))
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/%v %v/%v`, dirname, filename, dstpath, filename),
	})

	// rerunning same command should not upload files, empty result expected
	result = icmd.RunCmd(cmd, withWorkingDir(workdir))
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{})
}

// sync --raw object* s3://bucket/prefix/
func TestCopyLocalFilestoS3WithRawFlag(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}

	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	files := []fs.PathOp{
		fs.WithFile("file*.txt", "content"),
		fs.WithFile("file*1.txt", "content"),
		fs.WithFile("file*file.txt", "content"),
		fs.WithFile("file*2.txt", "content"),
	}

	expectedFiles := []string{"file*.txt"}
	nonExpectedFiles := []string{"file*1.txt", "file*file.txt", "file*2.txt"}

	// the file to be uploaded is modified
	workdir := fs.NewDir(t, t.Name(), files...)
	defer workdir.Remove()

	dstpath := fmt.Sprintf("s3://%v/prefix/", bucket)

	cmd := s5cmd("sync", "--raw", "file*.txt", dstpath)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp file*.txt %vfile*.txt`, dstpath),
	})

	result = icmd.RunCmd(cmd, withWorkingDir(workdir))

	// second run should not upload files, empty result expected
	result.Assert(t, icmd.Success)
	assertLines(t, result.Stdout(), map[int]compareFunc{})

	for _, obj := range expectedFiles {
		err := ensureS3Object(s3client, bucket, "prefix/"+obj, "content")
		if err != nil {
			t.Fatalf("%s is not exist in s3\n", obj)
		}
	}

	for _, obj := range nonExpectedFiles {
		err := ensureS3Object(s3client, bucket, "prefix/"+obj, "content")
		assertError(t, err, errS3NoSuchKey)
	}
}

// sync folder/ s3://bucket
func TestSyncLocalFolderToS3EmptyBucket(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile.txt", "S: this is a test file"),
		fs.WithFile("readme.md", "S: this is a readme file"),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "S: yet another txt file"),
		),
		fs.WithDir("b",
			fs.WithFile("filename-with-hypen.gz", "S: file has hyphen in its name"),
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
		0: equals(`cp %va/another_test_file.txt %va/another_test_file.txt`, src, dst),
		1: equals(`cp %vb/filename-with-hypen.gz %vb/filename-with-hypen.gz`, src, dst),
		2: equals(`cp %vreadme.md %vreadme.md`, src, dst),
		3: equals(`cp %vtestfile.txt %vtestfile.txt`, src, dst),
	}, sortInput(true))

	// there should be no error, since "no object found" error for destination is ignored
	assertLines(t, result.Stderr(), map[int]compareFunc{})

	// assert local filesystem
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	expectedS3Content := map[string]string{
		"testfile.txt":             "S: this is a test file",
		"readme.md":                "S: this is a readme file",
		"b/filename-with-hypen.gz": "S: file has hyphen in its name",
		"a/another_test_file.txt":  "S: yet another txt file",
	}

	// assert s3
	for key, content := range expectedS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// cp parent/*/name.txt s3://bucket/newfolder
func TestSyncMultipleFilesWithWildcardedDirectoryToS3Bucket(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithDir("parent", fs.WithDir(
			"child1",
			fs.WithFile("name.txt", "A file in parent/child1/"),
		),
			fs.WithDir(
				"child2",
				fs.WithFile("name.txt", "A file in parent/child2/"),
			),
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	dstpath := fmt.Sprintf("s3://%v/newfolder/", bucket)
	srcpath := workdir.Path()
	srcpath = filepath.ToSlash(srcpath)
	defer workdir.Remove()

	cmd := s5cmd("sync", srcpath+"/parent/*/name.txt", dstpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)
	rs := result.Stdout()
	assertLines(t, rs, map[int]compareFunc{
		0: equals(`cp %v/parent/child1/name.txt %vchild1/name.txt`, srcpath, dstpath),
		1: equals(`cp %v/parent/child2/name.txt %vchild2/name.txt`, srcpath, dstpath),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
	expectedS3Content := map[string]string{
		"newfolder/child1/name.txt": "A file in parent/child1/",
		"newfolder/child2/name.txt": "A file in parent/child2/",
	}

	// assert s3
	for filename, content := range expectedS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

// sync  s3://bucket/* folder/
func TestSyncS3BucketToEmptyFolder(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	s3Content := map[string]string{
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
		"abc/def/test.py":         "S: file in nested folders",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	workdir := fs.NewDir(t, "somedir")
	defer workdir.Remove()

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%v/*", bucketPath)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/a/another_test_file.txt %va/another_test_file.txt`, bucketPath, dst),
		1: equals(`cp %v/abc/def/test.py %vabc/def/test.py`, bucketPath, dst),
		2: equals(`cp %v/readme.md %vreadme.md`, bucketPath, dst),
		3: equals(`cp %v/testfile.txt %vtestfile.txt`, bucketPath, dst),
	}, sortInput(true))

	expectedFolderLayout := []fs.PathOp{
		fs.WithFile("testfile.txt", "S: this is a test file"),
		fs.WithFile("readme.md", "S: this is a readme file"),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "S: yet another txt file"),
		),
		fs.WithDir("abc",
			fs.WithDir("def",
				fs.WithFile("test.py", "S: file in nested folders"),
			),
		),
	}

	// assert local filesystem
	expected := fs.Expected(t, expectedFolderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync  s3://bucket/* s3://destbucket/prefix/
func TestSyncS3BucketToEmptyS3Bucket(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	const (
		prefix = "prefix"
	)
	createBucket(t, s3client, bucket)
	createBucket(t, s3client, dstbucket)

	s3Content := map[string]string{
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
		"abc/def/test.py":         "S: file in nested folders",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%v/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/%v/", dstbucket, prefix)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/a/another_test_file.txt %va/another_test_file.txt`, bucketPath, dst),
		1: equals(`cp %v/abc/def/test.py %vabc/def/test.py`, bucketPath, dst),
		2: equals(`cp %v/readme.md %vreadme.md`, bucketPath, dst),
		3: equals(`cp %v/testfile.txt %vtestfile.txt`, bucketPath, dst),
	}, sortInput(true))

	// assert  s3 objects in source bucket.
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}

	// assert s3 objects in dest bucket
	for key, content := range s3Content {
		key = fmt.Sprintf("%s/%s", prefix, key) // add the prefix
		assert.Assert(t, ensureS3Object(s3client, dstbucket, key, content))
	}
}

// sync folder/ s3://bucket (source older, same objects)
func TestSyncLocalFolderToS3BucketSameObjectsSourceOlder(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// local files are 1 minute older than the remotes
	timestamp := fs.WithTimestamps(
		now.Add(-time.Minute), // access time
		now.Add(-time.Minute), // mod time
	)

	folderLayout := []fs.PathOp{
		fs.WithFile("main.py", "S: this is a python file", timestamp),
		fs.WithFile("testfile.txt", "S: this is a test file", timestamp),
		fs.WithFile("readme.md", "S: this is a readme file", timestamp),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "S: yet another txt file", timestamp),
			timestamp,
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	s3Content := map[string]string{
		"main.py":                 "D: this is a python file",
		"testfile.txt":            "D: this is a test file",
		"readme.md":               "D: this is a readme file",
		"a/another_test_file.txt": "D: yet another txt file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %va/another_test_file.txt %va/another_test_file.txt": object is newer or same age and object size matches`, src, dst),
		1: equals(`DEBUG "sync %vmain.py %vmain.py": object is newer or same age and object size matches`, src, dst),
		2: equals(`DEBUG "sync %vreadme.md %vreadme.md": object is newer or same age and object size matches`, src, dst),
		3: equals(`DEBUG "sync %vtestfile.txt %vtestfile.txt": object is newer or same age and object size matches`, src, dst),
	}, sortInput(true))

	// expected folder structure
	expectedFiles := []fs.PathOp{
		fs.WithFile("main.py", "S: this is a python file"),
		fs.WithFile("testfile.txt", "S: this is a test file"),
		fs.WithFile("readme.md", "S: this is a readme file"),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "S: yet another txt file"),
		),
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync folder/ s3://bucket (source newer)
func TestSyncLocalFolderToS3BucketSourceNewer(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// local files are 1 minute newer than the remotes
	timestamp := fs.WithTimestamps(
		now.Add(time.Minute),
		now.Add(time.Minute),
	)

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile.txt", "S: this is an updated test file", timestamp),
		fs.WithFile("readme.md", "S: this is an updated readme file", timestamp),
		fs.WithDir("dir",
			fs.WithFile("main.py", "S: updated python file", timestamp),
			timestamp,
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	s3Content := map[string]string{
		"testfile.txt": "D: this is a test file ",
		"readme.md":    "D: this is a readme file",
		"dir/main.py":  "D: python file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("--log", "debug", "sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %vdir/main.py %vdir/main.py`, src, dst),
		1: equals(`cp %vreadme.md %vreadme.md`, src, dst),
		2: equals(`cp %vtestfile.txt %vtestfile.txt`, src, dst),
	}, sortInput(true))

	// expected folder structure, without the timestamps.
	expectedFiles := []fs.PathOp{
		fs.WithFile("testfile.txt", "S: this is an updated test file"),
		fs.WithFile("readme.md", "S: this is an updated readme file"),
		fs.WithDir("dir",
			fs.WithFile("main.py", "S: updated python file"),
		),
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// same as local source
	expectedS3Content := map[string]string{
		"testfile.txt": "S: this is an updated test file",
		"readme.md":    "S: this is an updated readme file",
		"dir/main.py":  "S: updated python file",
	}

	// assert s3
	for key, content := range expectedS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync s3://bucket/* folder/ (same objects, source older, destination newer)
func TestSyncS3BucketToLocalFolderSameObjectsSourceOlder(t *testing.T) {
	t.Parallel()

	newer := time.Now().Add(time.Minute)

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// local files are 1 minute newer than the remote ones
	timestamp := fs.WithTimestamps(
		newer,
		newer,
	)

	folderLayout := []fs.PathOp{
		fs.WithFile("main.py", "D: this is a python file", timestamp),
		fs.WithFile("testfile.txt", "D: this is a test file", timestamp),
		fs.WithFile("readme.md", "D: this is a readme file", timestamp),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "D: yet another txt file", timestamp),
			timestamp,
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	s3Content := map[string]string{
		"main.py":                 "S: this is a python file",
		"testfile.txt":            "S: this is a test file",   // content different from local
		"readme.md":               "S: this is a readme file", // content different from local
		"a/another_test_file.txt": "S: yet another txt file",  // content different from local
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %v/a/another_test_file.txt %va/another_test_file.txt": object is newer or same age and object size matches`, bucketPath, dst),
		1: equals(`DEBUG "sync %v/main.py %vmain.py": object is newer or same age and object size matches`, bucketPath, dst),
		2: equals(`DEBUG "sync %v/readme.md %vreadme.md": object is newer or same age and object size matches`, bucketPath, dst),
		3: equals(`DEBUG "sync %v/testfile.txt %vtestfile.txt": object is newer or same age and object size matches`, bucketPath, dst),
	}, sortInput(true))

	// expected folder structure without the timestamp.
	expectedFiles := []fs.PathOp{
		fs.WithFile("main.py", "D: this is a python file"),
		fs.WithFile("testfile.txt", "D: this is a test file"),
		fs.WithFile("readme.md", "D: this is a readme file"),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "D: yet another txt file"),
		),
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync s3://bucket/* folder/ (same objects, source newer)
func TestSyncS3BucketToLocalFolderSameObjectsSourceNewer(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// local files are 1 minute older, that makes remote files newer than them.
	timestamp := fs.WithTimestamps(
		now.Add(-time.Minute),
		now.Add(-time.Minute),
	)

	folderLayout := []fs.PathOp{
		fs.WithFile("main.py", "D: this is a python file", timestamp),
		fs.WithFile("testfile.txt", "D: this is a test file", timestamp),
		fs.WithFile("readme.md", "D: this is a readme file", timestamp),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "D: yet another txt file", timestamp),
			timestamp,
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	s3Content := map[string]string{
		"main.py":                 "S: this is a python file",
		"testfile.txt":            "S: this is an updated test file",
		"readme.md":               "S: this is an updated readme file",
		"a/another_test_file.txt": "S: yet another updated txt file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		1: equals(`cp %v/main.py %vmain.py`, bucketPath, dst),
		0: equals(`cp %v/a/another_test_file.txt %va/another_test_file.txt`, bucketPath, dst),
		2: equals(`cp %v/readme.md %vreadme.md`, bucketPath, dst),
		3: equals(`cp %v/testfile.txt %vtestfile.txt`, bucketPath, dst),
	}, sortInput(true))

	// expected folder structure without the timestamp.
	expectedFiles := []fs.PathOp{
		fs.WithFile("main.py", "S: this is a python file"),
		fs.WithFile("testfile.txt", "S: this is an updated test file"),
		fs.WithFile("readme.md", "S: this is an updated readme file"),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "S: yet another updated txt file"),
		),
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync s3://bucket/* s3://destbucket/ (source newer, same objects, different content, same sizes)
func TestSyncS3BucketToS3BucketSameSizesSourceNewer(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	createBucket(t, s3client, bucket)
	createBucket(t, s3client, dstbucket)

	sourceS3Content := map[string]string{
		"main.py":                 "S: this is a python file",
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
	}

	// the file sizes are same, with different contents.
	destS3Content := map[string]string{
		"main.py":                 "D: this is a python file",
		"testfile.txt":            "D: this is a test file",
		"readme.md":               "D: this is a readme file",
		"a/another_test_file.txt": "D: yet another txt file",
	}

	// make destination files 1 minute older
	timeSource.Advance(-time.Minute)
	for filename, content := range destS3Content {
		putFile(t, s3client, dstbucket, filename, content)
	}

	timeSource.Advance(time.Minute)
	for filename, content := range sourceS3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/", dstbucket)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/a/another_test_file.txt %va/another_test_file.txt`, bucketPath, dst),
		1: equals(`cp %v/main.py %vmain.py`, bucketPath, dst),
		2: equals(`cp %v/readme.md %vreadme.md`, bucketPath, dst),
		3: equals(`cp %v/testfile.txt %vtestfile.txt`, bucketPath, dst),
	}, sortInput(true))

	// assert s3 objects in source
	for key, content := range sourceS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}

	// assert s3 objects in destination (should be same as source)
	for key, content := range sourceS3Content {
		assert.Assert(t, ensureS3Object(s3client, dstbucket, key, content))
	}
}

// sync s3://bucket/* s3://destbucket/ (source older, same objects, different content, same sizes)
func TestSyncS3BucketToS3BucketSameSizesSourceOlder(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	createBucket(t, s3client, bucket)
	createBucket(t, s3client, dstbucket)

	sourceS3Content := map[string]string{
		"main.py":                 "S: this is a python file",
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
	}

	// the file sizes are same, content different.
	destS3Content := map[string]string{
		"main.py":                 "D: this is a python file",
		"testfile.txt":            "D: this is a test file",
		"readme.md":               "D: this is a readme file",
		"a/another_test_file.txt": "D: yet another txt file",
	}

	// make source files 1 minute older
	timeSource.Advance(-time.Minute)
	for filename, content := range sourceS3Content {
		putFile(t, s3client, bucket, filename, content)
	}
	timeSource.Advance(time.Minute)

	for filename, content := range destS3Content {
		putFile(t, s3client, dstbucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/", dstbucket)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %v/a/another_test_file.txt %va/another_test_file.txt": object is newer or same age and object size matches`, bucketPath, dst),
		1: equals(`DEBUG "sync %v/main.py %vmain.py": object is newer or same age and object size matches`, bucketPath, dst),
		2: equals(`DEBUG "sync %v/readme.md %vreadme.md": object is newer or same age and object size matches`, bucketPath, dst),
		3: equals(`DEBUG "sync %v/testfile.txt %vtestfile.txt": object is newer or same age and object size matches`, bucketPath, dst),
	}, sortInput(true))

	// assert s3 objects in source
	for key, content := range sourceS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}

	// assert s3 objects in destination (should not change).
	for key, content := range destS3Content {
		assert.Assert(t, ensureS3Object(s3client, dstbucket, key, content))
	}
}

// sync --size-only s3://bucket/* folder/
func TestSyncS3BucketToLocalFolderSameObjectsSizeOnly(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithFile("test.py", "D: this is a python file"),
		fs.WithFile("testfile.txt", "D: this is a test file"),
		fs.WithFile("readme.md", "D: this is a readme file"),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "D: yet another txt file"),
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	s3Content := map[string]string{
		"test.py":                 "S: this is an updated python file", // content different from local, different size
		"testfile.txt":            "S: this is a test file",            // content different from local, same size
		"readme.md":               "S: this is a readme file",          // content different from local, same size
		"a/another_test_file.txt": "S: yet another txt file",           // content different from local, same size
		"abc/def/main.py":         "S: python file",                    // local does not have it.
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", "--size-only", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %v/a/another_test_file.txt %va/another_test_file.txt": object size matches`, bucketPath, dst),
		1: equals(`DEBUG "sync %v/readme.md %vreadme.md": object size matches`, bucketPath, dst),
		2: equals(`DEBUG "sync %v/testfile.txt %vtestfile.txt": object size matches`, bucketPath, dst),
		3: equals(`cp %v/abc/def/main.py %vabc/def/main.py`, bucketPath, dst),
		4: equals(`cp %v/test.py %vtest.py`, bucketPath, dst),
	}, sortInput(true))

	expectedFolderLayout := []fs.PathOp{
		fs.WithFile("test.py", "S: this is an updated python file"),
		fs.WithFile("testfile.txt", "D: this is a test file"),
		fs.WithFile("readme.md", "D: this is a readme file"),
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "D: yet another txt file"),
		),
		fs.WithDir("abc",
			fs.WithDir("def",
				fs.WithFile("main.py", "S: python file"),
			),
		),
	}

	// expected folder structure without the timestamp.
	expected := fs.Expected(t, expectedFolderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync s3://bucket/* s3://destbucket/ (same objects, same size, same content, different or same storage class)
func TestSyncS3BucketToS3BucketIsStorageClassChanging(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	srcbucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	createBucket(t, s3client, srcbucket)
	createBucket(t, s3client, dstbucket)

	storageClassesAndFile := []struct {
		srcStorageClass string
		dstStorageClass string
		filename        string
		content         string
	}{
		{"STANDARD", "STANDARD", "testfile1.txt", "this is a test file"},
		{"STANDARD", "GLACIER", "testfile2.txt", "this is a test file"},
		{"GLACIER", "STANDARD", "testfile3.txt", "this is a test file"},
		{"GLACIER", "GLACIER", "testfile4.txt", "this is a test file"},
	}

	for _, sc := range storageClassesAndFile {

		putObject := s3.PutObjectInput{
			Bucket:       &srcbucket,
			Key:          &sc.filename,
			Body:         strings.NewReader(sc.content),
			StorageClass: &sc.srcStorageClass,
		}

		_, err := s3client.PutObject(&putObject)
		if err != nil {
			t.Fatalf("failed to put object in %v: %v", sc.srcStorageClass, err)
		}

		putObject = s3.PutObjectInput{
			Bucket:       &dstbucket,
			Key:          &sc.filename,
			Body:         strings.NewReader(sc.content),
			StorageClass: aws.String(sc.dstStorageClass),
		}

		_, err = s3client.PutObject(&putObject)
		if err != nil {
			t.Fatalf("failed to put object in %v: %v", sc.dstStorageClass, err)
		}

	}

	bucketPath := fmt.Sprintf("s3://%v", srcbucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/", dstbucket)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	// there will be no stdout, since there are no changes
	result.Assert(t, icmd.Success)
	assertLines(t, result.Stdout(), map[int]compareFunc{})

	// assert s3 objects in source
	for _, sc := range storageClassesAndFile {
		assert.Assert(t, ensureS3Object(s3client, srcbucket, sc.filename, sc.content, ensureStorageClass(sc.srcStorageClass)))
	}

	// assert s3 objects in destination
	for _, sc := range storageClassesAndFile {
		assert.Assert(t, ensureS3Object(s3client, dstbucket, sc.filename, sc.content, ensureStorageClass(sc.dstStorageClass)))
	}
}

// sync dir s3://destbucket/ (same objects, same size, same content, different or same storage class)
func TestSyncLocalFolderToS3BucketIsStorageClassChanging(t *testing.T) {
	t.Parallel()
	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	timestamp := fs.WithTimestamps(
		now.Add(-time.Minute),
		now.Add(-time.Minute),
	)

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file", timestamp),
		fs.WithFile("testfile2.txt", "this is a test file", timestamp),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	storageClassesAndFile := []struct {
		storageClass string
		filename     string
		content      string
	}{
		{"STANDARD", "testfile1.txt", "this is a test file"},
		{"GLACIER", "testfile2.txt", "this is a test file"},
	}

	for _, sc := range storageClassesAndFile {

		putObject := s3.PutObjectInput{
			Bucket:       &bucket,
			Key:          &sc.filename,
			Body:         strings.NewReader(sc.content),
			StorageClass: aws.String(sc.storageClass),
		}

		_, err := s3client.PutObject(&putObject)
		if err != nil {
			t.Fatalf("failed to put object in %v: %v", sc.storageClass, err)
		}
	}

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	// there will be no stdout
	result.Assert(t, icmd.Success)
	assertLines(t, result.Stdout(), map[int]compareFunc{})

	expectedFiles := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file"),
		fs.WithFile("testfile2.txt", "this is a test file"),
	}

	// expected folder structure without the timestamp.
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3 objects in destination
	for _, sc := range storageClassesAndFile {
		assert.Assert(t, ensureS3Object(s3client, bucket, sc.filename, sc.content, ensureStorageClass(sc.storageClass)))
	}
}

// sync s3://srcbucket/ dir (same objects, same size, same content, different or same storage class)
func TestSyncS3BucketToLocalFolderIsStorageClassChanging(t *testing.T) {
	t.Parallel()
	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	// local files are 1 minute newer than the remotes
	timestamp := fs.WithTimestamps(
		now.Add(time.Minute),
		now.Add(time.Minute),
	)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	storageClassesAndFile := []struct {
		storageClass string
		filename     string
		content      string
	}{
		{"STANDARD", "testfile1.txt", "this is a test file"},
		{"GLACIER", "testfile2.txt", "this is a test file"},
	}

	for _, sc := range storageClassesAndFile {

		putObject := s3.PutObjectInput{
			Bucket:       &bucket,
			Key:          &sc.filename,
			Body:         strings.NewReader(sc.content),
			StorageClass: aws.String(sc.storageClass),
		}

		_, err := s3client.PutObject(&putObject)
		if err != nil {
			t.Fatalf("failed to put object in %v: %v", sc.storageClass, err)
		}
	}

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file", timestamp),
		fs.WithFile("testfile2.txt", "this is a test file", timestamp),
	}

	// put objects in local folder

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	// there will be no stdout
	result.Assert(t, icmd.Success)
	assertLines(t, result.Stdout(), map[int]compareFunc{})

	expectedFiles := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file"),
		fs.WithFile("testfile2.txt", "this is a test file"),
	}

	// expected folder structure without the timestamp.
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3 objects in destination
	for _, sc := range storageClassesAndFile {
		assert.Assert(t, ensureS3Object(s3client, bucket, sc.filename, sc.content, ensureStorageClass(sc.storageClass)))
	}
}

// sync s3://srcbucket/* s3://dstbucket/ (same objects, different size, different content, different or same storage class)
func TestSyncS3BucketToS3BucketIsStorageClassChangingWithDifferentSizeAndContent(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	srcbucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	createBucket(t, s3client, srcbucket)
	createBucket(t, s3client, dstbucket)

	storageClassesAndFile := []struct {
		srcStorageClass string
		dstStorageClass string
		filename        string
		srcContent      string
		dstContent      string
	}{
		{"STANDARD", "STANDARD", "testfile1.txt", "this is an updated test file", "this is a test file"},
		{"STANDARD", "GLACIER", "testfile2.txt", "this is an updated test file", "this is a test file"},
		{"GLACIER", "STANDARD", "testfile3.txt", "this is an updated test file", "this is a test file"},
		{"GLACIER", "GLACIER", "testfile4.txt", "this is an updated test file", "this is a test file"},
	}

	for _, sc := range storageClassesAndFile {

		putFile(t, s3client, srcbucket, sc.filename, sc.srcContent, putStorageClass(sc.srcStorageClass))

		putObject := s3.PutObjectInput{
			Bucket:       &dstbucket,
			Key:          &sc.filename,
			Body:         strings.NewReader(sc.dstContent),
			StorageClass: aws.String(sc.dstStorageClass),
		}

		_, err := s3client.PutObject(&putObject)
		if err != nil {
			t.Fatalf("failed to put object in %v: %v", sc.dstStorageClass, err)
		}
	}

	bucketPath := fmt.Sprintf("s3://%v", srcbucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/", dstbucket)

	cmd := s5cmd("sync", src, dst)

	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/testfile1.txt %vtestfile1.txt`, bucketPath, dst),
		1: equals(`cp %v/testfile2.txt %vtestfile2.txt`, bucketPath, dst),
	}, sortInput(true))

	// assert s3 objects in source
	for _, sc := range storageClassesAndFile {
		assert.Assert(t, ensureS3Object(s3client, srcbucket, sc.filename, sc.srcContent, ensureStorageClass(sc.srcStorageClass)))
	}

	// assert s3 objects in destination (file1 and file2 should be updated and file3 and file4 should be same as before)
	assert.Assert(t, ensureS3Object(s3client, dstbucket, "testfile1.txt", "this is an updated test file"), ensureStorageClass("STANDARD"))
	assert.Assert(t, ensureS3Object(s3client, dstbucket, "testfile2.txt", "this is an updated test file", ensureStorageClass("STANDARD")))
	assert.Assert(t, ensureS3Object(s3client, dstbucket, "testfile3.txt", "this is a test file", ensureStorageClass("STANDARD")))
	assert.Assert(t, ensureS3Object(s3client, dstbucket, "testfile4.txt", "this is a test file", ensureStorageClass("GLACIER")))
}

// sync dir s3://destbucket/ (same objects, different size, different content, different or same storage class)
func TestSyncLocalFolderToS3BucketIsStorageClassChangingWithDifferentSizeAndContent(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is an updated test file"),
		fs.WithFile("testfile2.txt", "this is an updated test file"),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	storageClassesAndFile := []struct {
		storageClass string
		filename     string
		content      string
	}{
		{"STANDARD", "testfile1.txt", "this is a test file"},
		{"GLACIER", "testfile2.txt", "this is a test file"},
	}

	for _, sc := range storageClassesAndFile {
		putFile(t, s3client, bucket, sc.filename, sc.content, putStorageClass(sc.storageClass))
	}

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %vtestfile1.txt %vtestfile1.txt`, src, dst),
		1: equals(`cp %vtestfile2.txt %vtestfile2.txt`, src, dst),
	}, sortInput(true))

	// expected folder structure without the timestamp.
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3 objects in destination
	assert.Assert(t, ensureS3Object(s3client, bucket, "testfile1.txt", "this is an updated test file"), ensureStorageClass("STANDARD"))
	assert.Assert(t, ensureS3Object(s3client, bucket, "testfile2.txt", "this is an updated test file"), ensureStorageClass("STANDARD"))
}

// sync s3://destbucket/ dir (same objects, different size, different content, different or same storage class)
func TestSyncS3BucketToLocalFolderIsStorageClassChangingWithDifferentSizeAndContent(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	storageClassesAndFile := []struct {
		storageClass string
		filename     string
		content      string
	}{
		{"STANDARD", "testfile1.txt", "this is an updated test file"},
		{"GLACIER", "testfile2.txt", "this is an updated test file"},
	}

	for _, sc := range storageClassesAndFile {
		putFile(t, s3client, bucket, sc.filename, sc.content, putStorageClass(sc.storageClass))
	}

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is a test file"),
		fs.WithFile("testfile2.txt", "this is a test file"),
	}

	// put objects in local folder
	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	cmd := s5cmd("sync", src, dst)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	// testfile1.txt should be updated and testfile2.txt shouldn't be updated because it is in glacier.
	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/testfile1.txt %vtestfile1.txt`, bucketPath, dst),
	}, sortInput(true))

	expectedFolderLayout := []fs.PathOp{
		fs.WithFile("testfile1.txt", "this is an updated test file"),
		fs.WithFile("testfile2.txt", "this is a test file"),
	}

	// expected folder structure without the timestamp.
	expected := fs.Expected(t, expectedFolderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// sync --delete s3://bucket/* s3://destbucket/ (storage class test)
func TestSyncS3BucketToS3BucketWithDeleteStorageClass(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	srcbucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	createBucket(t, s3client, srcbucket)
	createBucket(t, s3client, dstbucket)

	dstStorageClassesAndFile := []struct {
		storageClass string
		filename     string
		content      string
	}{
		{"STANDARD", "testfile1.txt", "this is a test file"},
		{"GLACIER", "testfile2.txt", "this is a test file"},
	}

	for _, sc := range dstStorageClassesAndFile {
		putFile(t, s3client, dstbucket, sc.filename, sc.content, putStorageClass(sc.storageClass))
	}

	bucketPath := fmt.Sprintf("s3://%v", srcbucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/", dstbucket)

	cmd := s5cmd("sync", "--delete", src, dst)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm %vtestfile1.txt`, dst),
		1: equals(`rm %vtestfile2.txt`, dst),
	}, sortInput(true))

	// assert s3 objects in destination
	for _, sc := range dstStorageClassesAndFile {
		err := ensureS3Object(s3client, dstbucket, sc.filename, sc.content, ensureStorageClass(sc.storageClass))
		assertError(t, err, errS3NoSuchKey)
	}
}

// sync --delete dir s3://destbucket/ (storage class test)
func TestSyncLocalFolderToS3BucketWithDeleteStorageClass(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	storageClassesAndFile := []struct {
		storageClass string
		filename     string
		content      string
	}{
		{"STANDARD", "testfile1.txt", "this is a test file"},
		{"GLACIER", "testfile2.txt", "this is a test file"},
	}

	for _, sc := range storageClassesAndFile {
		putFile(t, s3client, bucket, sc.filename, sc.content, putStorageClass(sc.storageClass))
	}

	workdir := fs.NewDir(t, "somedir")
	defer workdir.Remove()

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", "--delete", src, dst)

	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`rm %vtestfile1.txt`, dst),
		1: equals(`rm %vtestfile2.txt`, dst),
	}, sortInput(true))

	// assert s3 objects in destination
	for _, sc := range storageClassesAndFile {
		err := ensureS3Object(s3client, bucket, sc.filename, sc.content, ensureStorageClass(sc.storageClass))
		assertError(t, err, errS3NoSuchKey)
	}
}

// sync --size-only folder/ s3://bucket/
func TestSyncLocalFolderToS3BucketSameObjectsSizeOnly(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithFile("test.py", "S: this is a python file"),    // remote has it, different content, size same
		fs.WithFile("testfile.txt", "S: this is a test file"), // remote has it, but with different contents/size.
		fs.WithFile("readme.md", "S: this is a readme file"),  // remote has it, same object.
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "S: yet another txt file"), // remote has it, different content, same size.
		),
		fs.WithDir("abc",
			fs.WithDir("def",
				fs.WithFile("main.py", "S: python file"), // remote does not have it
			),
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	s3Content := map[string]string{
		"test.py":                 "D: this is a python file",
		"testfile.txt":            "D: this is an updated test file",
		"readme.md":               "D: this is a readme file",
		"a/another_test_file.txt": "D: yet another txt file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%s/", bucket)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", "--size-only", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %va/another_test_file.txt %va/another_test_file.txt": object size matches`, src, dst),
		1: equals(`DEBUG "sync %vreadme.md %vreadme.md": object size matches`, src, dst),
		2: equals(`DEBUG "sync %vtest.py %vtest.py": object size matches`, src, dst),
		3: equals(`cp %vabc/def/main.py %vabc/def/main.py`, src, dst),
		4: equals(`cp %vtestfile.txt %vtestfile.txt`, src, dst),
	}, sortInput(true))

	// expected folder structure without the timestamp.
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	expectedS3Content := map[string]string{
		"test.py":                 "D: this is a python file",
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "D: this is a readme file",
		"a/another_test_file.txt": "D: yet another txt file",
		"abc/def/main.py":         "S: python file",
	}

	// assert s3
	for key, content := range expectedS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync --size-only s3://bucket/* s3://destbucket/
func TestSyncS3BucketToS3BucketSizeOnly(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")
	createBucket(t, s3client, bucket)
	createBucket(t, s3client, dstbucket)

	sourceS3Content := map[string]string{
		"main.py":                 "S: this is an updated python file",
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
	}

	destS3Content := map[string]string{
		"main.py":                 "D: this is a python file", // file size is smaller than source.
		"testfile.txt":            "D: this is a test file",
		"readme.md":               "D: this is a readme file",
		"a/another_test_file.txt": "D: yet another txt file",
	}

	// make source files older in bucket.
	// timestamps should be ignored with --size-only flag
	timeSource.Advance(-time.Minute)
	for filename, content := range destS3Content {
		putFile(t, s3client, dstbucket, filename, content)
	}
	timeSource.Advance(time.Minute)

	for filename, content := range sourceS3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/", dstbucket)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", "--size-only", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %v/a/another_test_file.txt %va/another_test_file.txt": object size matches`, bucketPath, dst),
		1: equals(`DEBUG "sync %v/readme.md %vreadme.md": object size matches`, bucketPath, dst),
		2: equals(`DEBUG "sync %v/testfile.txt %vtestfile.txt": object size matches`, bucketPath, dst),
		3: equals(`cp %v/main.py %vmain.py`, bucketPath, dst),
	}, sortInput(true))

	// assert s3 objects in source
	for key, content := range sourceS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}

	expectedDestS3Content := map[string]string{
		"main.py":                 "S: this is an updated python file", // same as source.
		"testfile.txt":            "D: this is a test file",
		"readme.md":               "D: this is a readme file",
		"a/another_test_file.txt": "D: yet another txt file",
	}

	// assert s3 objects in destination
	for key, content := range expectedDestS3Content {
		assert.Assert(t, ensureS3Object(s3client, dstbucket, key, content))
	}
}

// sync --delete s3://bucket/* .
func TestSyncS3BucketToLocalWithDelete(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	s3Content := map[string]string{
		"contributing.md": "S: this is a readme file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	folderLayout := []fs.PathOp{
		fs.WithFile("testfile.txt", "D: this is a test file"),
		fs.WithFile("readme.md", "D: this is a readme file"),
		fs.WithDir("dir",
			fs.WithFile("main.py", "D: python file"),
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	src := fmt.Sprintf("s3://%v/", bucket)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	cmd := s5cmd("sync", "--delete", src+"*", dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %vcontributing.md %vcontributing.md`, src, dst),
		1: equals(`rm %vdir/main.py`, dst),
		2: equals(`rm %vreadme.md`, dst),
		3: equals(`rm %vtestfile.txt`, dst),
	}, sortInput(true))

	expectedFolderLayout := []fs.PathOp{
		fs.WithDir("dir"),
		fs.WithFile("contributing.md", "S: this is a readme file"),
	}

	// assert local filesystem
	expected := fs.Expected(t, expectedFolderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync --delete s3://bucket/* .
func TestSyncS3BucketToEmptyLocalWithDelete(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	s3Content := map[string]string{
		"contributing.md": "S: this is a readme file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	workdir := fs.NewDir(t, "somedir")
	defer workdir.Remove()

	src := fmt.Sprintf("s3://%v/", bucket)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	cmd := s5cmd("sync", "--delete", src+"*", dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)
	stdout := result.Stdout()
	assertLines(t, stdout, map[int]compareFunc{
		0: equals(`cp %vcontributing.md %vcontributing.md`, src, dst),
	}, sortInput(true))

	expectedFolderLayout := []fs.PathOp{
		fs.WithFile("contributing.md", "S: this is a readme file"),
	}

	// assert local filesystem
	expected := fs.Expected(t, expectedFolderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync --delete folder/ s3://bucket/*
func TestSyncLocalToS3BucketWithDelete(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// ensure source is older.
	folderLayout := []fs.PathOp{
		fs.WithFile("contributing.md", "S: this is a readme file", fs.WithTimestamps(now.Add(-time.Minute), now.Add(-time.Minute))),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	s3Content := map[string]string{
		"readme.md":    "D: this is a readme file",
		"dir/main.py":  "D: this is a python file",
		"testfile.txt": "D: this is a test file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", "--delete", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %vcontributing.md %vcontributing.md`, src, dst),
		1: equals(`rm %vdir/main.py`, dst),
		2: equals(`rm %vreadme.md`, dst),
		3: equals(`rm %vtestfile.txt`, dst),
	}, sortInput(true))

	// assert local filesystem
	expectedFiles := []fs.PathOp{
		fs.WithFile("contributing.md", "S: this is a readme file"),
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	expectedS3Content := map[string]string{
		"contributing.md": "S: this is a readme file",
	}

	// assert s3 objects
	for key, content := range expectedS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}

	// assert s3 objects should be deleted.
	for key, content := range s3Content {
		err := ensureS3Object(s3client, bucket, key, content)
		if err == nil {
			t.Errorf("File %v is not deleted from remote : %v\n", key, err)
		}
	}
}

// sync --delete folder/ s3://bucket/*
func TestSyncLocalToEmptyS3BucketWithDelete(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithFile("contributing.md", "S: this is a readme file", fs.WithTimestamps(now, now)),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", "--delete", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %vcontributing.md %vcontributing.md`, src, dst),
	}, sortInput(true))

	// assert local filesystem
	expectedFiles := []fs.PathOp{
		fs.WithFile("contributing.md", "S: this is a readme file"),
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	expectedS3Content := map[string]string{
		"contributing.md": "S: this is a readme file",
	}

	// assert s3 objects
	for key, content := range expectedS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync --delete s3://bucket/* s3://destbucket/
func TestSyncS3BucketToS3BucketWithDelete(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")
	createBucket(t, s3client, bucket)
	createBucket(t, s3client, dstbucket)

	sourceS3Content := map[string]string{
		"readme.md":    "S: this is a readme file",
		"dir/main.py":  "S: this is a python file",
		"testfile.txt": "S: this is a test file",
	}

	destS3Content := map[string]string{
		"main.md":      "D: this is a readme file",
		"dir/test.py":  "D: this is a python file",
		"testfile.txt": "D: this is an updated test file", // different size from source
		"Makefile":     "D: this is a makefile",
	}

	for filename, content := range sourceS3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	for filename, content := range destS3Content {
		putFile(t, s3client, dstbucket, filename, content)
	}

	src := fmt.Sprintf("s3://%v/", bucket)
	dst := fmt.Sprintf("s3://%v/", dstbucket)

	cmd := s5cmd("sync", "--delete", "--size-only", src+"*", dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %vdir/main.py %vdir/main.py`, src, dst),
		1: equals(`cp %vreadme.md %vreadme.md`, src, dst),
		2: equals(`cp %vtestfile.txt %vtestfile.txt`, src, dst),
		3: equals(`rm %vMakefile`, dst),
		4: equals(`rm %vdir/test.py`, dst),
		5: equals(`rm %vmain.md`, dst),
	}, sortInput(true))

	expectedDestS3Content := map[string]string{
		"testfile.txt": "S: this is a test file", // same as source bucket.
		"readme.md":    "S: this is a readme file",
		"dir/main.py":  "S: this is a python file",
	}

	nonExpectedDestS3Content := map[string]string{
		"dir/test.py": "S: this is a python file",
		"main.md":     "D: this is a readme file",
		"Makefile":    "S: this is a makefile",
	}

	// assert s3 objects in source.
	for key, content := range sourceS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}

	// assert s3 objects in destination. (should be)
	for key, content := range expectedDestS3Content {
		assert.Assert(t, ensureS3Object(s3client, dstbucket, key, content))
	}

	// assert s3 objects should be deleted.
	for key, content := range nonExpectedDestS3Content {
		err := ensureS3Object(s3client, dstbucket, key, content)
		if err == nil {
			t.Errorf("File %v is not deleted in remote : %v\n", key, err)
		}
	}
}

// sync s3://bucket/*.txt folder/
func TestSyncS3toLocalWithWildcard(t *testing.T) {
	t.Parallel()
	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// make local (destination) older.
	timestamp := fs.WithTimestamps(
		now.Add(-time.Minute), // access time
		now.Add(-time.Minute), // mod time
	)

	// even though test.py exists in the source, since '*.txt' wildcard
	// used, test.py will not be in the source, because all of the source
	// files will be with extension '*.txt' therefore test.py will be deleted.
	folderLayout := []fs.PathOp{
		fs.WithFile("test.py", "D: this is a python file", timestamp),
		fs.WithFile("test.txt", "D: this is a test file", timestamp),
	}

	s3Content := map[string]string{
		"test.txt":          "S: this is an updated test file",
		"readme.md":         "S: this is a readme file",
		"main.py":           "S: py file",
		"subfolder/sub.txt": "S: yet another txt",
		"test.py":           "S: this is a python file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	src := fmt.Sprintf("s3://%v/", bucket)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	cmd := s5cmd("--log", "debug", "sync", "--delete", src+"*.txt", dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		1: equals(`cp %vtest.txt %vtest.txt`, src, dst),
		0: equals(`cp %vsubfolder/sub.txt %vsubfolder/sub.txt`, src, dst),
		2: equals(`rm %vtest.py`, dst),
	}, sortInput(true))

	expectedLayout := []fs.PathOp{
		fs.WithFile("test.txt", "S: this is an updated test file"),
		fs.WithDir("subfolder",
			fs.WithFile("sub.txt", "S: yet another txt"),
		),
	}

	expected := fs.Expected(t, expectedLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// sync --delete s3://bucket/* .
func TestSyncS3BucketToLocalWithDeleteFlag(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	s3Content := map[string]string{
		"test.txt": "S: this is a test file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	workdir := fs.NewDir(t,
		"somedir",
		fs.WithFile("readme.md", "D: this is a readme file"),
		fs.WithDir(
			"subdir",
			fs.WithFile("main.py", "D: this is a python file")),
	)
	defer workdir.Remove()

	src := fmt.Sprintf("s3://%v/", bucket)
	dst := fmt.Sprintf("%v/", workdir.Path())
	dst = filepath.ToSlash(dst)

	cmd := s5cmd("--log", "debug", "sync", "--delete", src+"*", dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %vtest.txt %vtest.txt`, src, dst),
		1: equals(`rm %vreadme.md`, dst),
		2: equals(`rm %vsubdir/main.py`, dst),
	}, sortInput(true))

	expectedLayout := []fs.PathOp{
		fs.WithFile("test.txt", "S: this is a test file"),
		fs.WithDir("subdir"),
	}

	expected := fs.Expected(t, expectedLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// sync dir/ s3://bucket (symlink)
func TestSyncLocalFilesWithSymlinksToS3Bucket(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	fileContent := "CAFEBABE"
	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("file1.txt", fileContent),
			fs.WithFile("file2.txt", fileContent),
		),
		fs.WithDir("b"),
		fs.WithSymlink("b/link1", "a/file1.txt"),
		fs.WithSymlink("b/link2", "a/file2.txt"),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	src := fmt.Sprintf("%v/b", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/b/link1 %vb/link1`, filepath.ToSlash(workdir.Path()), dst),
		1: equals(`cp %v/b/link2 %vb/link2`, filepath.ToSlash(workdir.Path()), dst),
	}, sortInput(true))
}

// sync --no-follow-symlinks * s3://bucket/prefix/
func TestSyncLocalFilesWithNoFollowSymlinksToS3Bucket(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	fileContent := "CAFEBABE"
	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("file1.txt", fileContent),
			fs.WithFile("file2.txt", fileContent),
		),
		fs.WithDir("b"),
		fs.WithSymlink("b/link1", "a/file1.txt"),
		fs.WithSymlink("b/link2", "a/file2.txt"),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	src := fmt.Sprintf("%v/b", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("sync", "--no-follow-symlinks", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	// do not follow symlinks in directory b (empty result)
	assertLines(t, result.Stdout(), map[int]compareFunc{})
}

// sync --exclude pattern s3://bucket/* s3://anotherbucket/prefix/
func TestSyncS3ObjectsIntoAnotherBucketWithExcludeFilters(t *testing.T) {
	t.Parallel()

	srcbucket := s3BucketFromTestNameWithPrefix(t, "src")
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	s3client, s5cmd := setup(t)

	createBucket(t, s3client, srcbucket)
	createBucket(t, s3client, dstbucket)

	srcFiles := []string{
		"file_already_exists_in_destination.txt",
		"file_not_exists_in_destination.txt",
		"main.py",
		"main.js",
		"readme.md",
		"main.pdf",
		"main/file.txt",
	}

	dstFiles := []string{
		"prefix/file_already_exists_in_destination.txt",
	}

	expectedFiles := []string{
		"prefix/file_not_exists_in_destination.txt",
		"prefix/file_already_exists_in_destination.txt",
	}

	excludedFiles := []string{
		"main.py",
		"main.js",
		"main.pdf",
		"main/file.txt",
		"readme.md",
	}

	const (
		content         = "this is a file content"
		excludePattern1 = "main*"
		excludePattern2 = "*.md"
	)

	for _, filename := range srcFiles {
		putFile(t, s3client, srcbucket, filename, content)
	}

	for _, filename := range dstFiles {
		putFile(t, s3client, dstbucket, filename, content)
	}

	src := fmt.Sprintf("s3://%v/*", srcbucket)
	dst := fmt.Sprintf("s3://%v/prefix/", dstbucket)

	cmd := s5cmd("sync", "--exclude", excludePattern1, "--exclude", excludePattern2, src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp s3://%s/file_not_exists_in_destination.txt s3://%s/prefix/file_not_exists_in_destination.txt`, srcbucket, dstbucket),
	}, sortInput(true))

	// assert s3 source objects
	for _, filename := range srcFiles {
		assert.Assert(t, ensureS3Object(s3client, srcbucket, filename, content))
	}

	// assert s3 destination objects
	for _, filename := range expectedFiles {
		assert.Assert(t, ensureS3Object(s3client, dstbucket, filename, content))
	}

	// assert s3 destination objects which should not be in bucket.
	for _, filename := range excludedFiles {
		err := ensureS3Object(s3client, dstbucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// sync --exclude "*.gz" dir s3://bucket/
// sync --exclude "*.gz" dir/ s3://bucket/
// sync --exclude "*.gz" dir/* s3://bucket/
func TestSyncLocalDirectoryToS3WithExcludeFilter(t *testing.T) {
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

			bucket := s3BucketFromTestName(t)

			s3client, s5cmd := setup(t)

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

			const excludePattern = "*.gz"

			src := fmt.Sprintf("%v/", workdir.Path())
			src = src + tc.directoryPrefix
			dst := fmt.Sprintf("s3://%v/prefix/", bucket)

			src = filepath.ToSlash(src)
			cmd := s5cmd("sync", "--exclude", excludePattern, src, dst)
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)

			// assert local filesystem
			expected := fs.Expected(t, folderLayout...)
			assert.Assert(t, fs.Equal(workdir.Path(), expected))

			expectedS3Content := map[string]string{
				"prefix/testfile1.txt":           "this is a test file 1",
				"prefix/readme.md":               "this is a readme file",
				"prefix/a/another_test_file.txt": "yet another txt file. yatf.",
			}

			nonExpectedS3Content := map[string]string{
				"prefix/b/filename-with-hypen.gz": "file has hypen in its name",
			}

			// assert objects should be in S3
			for key, content := range expectedS3Content {
				assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
			}

			// assert objects should not be in S3.
			for key, content := range nonExpectedS3Content {
				err := ensureS3Object(s3client, bucket, key, content)
				assertError(t, err, errS3NoSuchKey)
			}
		})
	}
}

// sync --delete somedir s3://bucket/ (removes 10k objects)
func TestIssue435(t *testing.T) {
	t.Parallel()

	// Skip this as it takes too long to complete with GCS.
	skipTestIfGCS(t, "takes too long to complete")

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t, withS3Backend("mem"))

	createBucket(t, s3client, bucket)

	// empty folder
	folderLayout := []fs.PathOp{}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	const filecount = 10_000

	filenameFunc := func(i int) string { return fmt.Sprintf("file_%06d", i) }
	contentFunc := func(i int) string { return fmt.Sprintf("file body %06d", i) }

	for i := 0; i < filecount; i++ {
		filename := filenameFunc(i)
		content := contentFunc(i)
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%v/", bucket)

	cmd := s5cmd("--log", "debug", "sync", "--delete", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	expected := make(map[int]compareFunc)
	for i := 0; i < filecount; i++ {
		expected[i] = contains("rm s3://%v/file_%06d", bucket, i)
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

// sync s3://bucket/* s3://bucket/ (dest bucket is empty)
func TestSyncS3BucketToEmptyS3BucketWithExitOnErrorFlag(t *testing.T) {
	t.Parallel()
	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	const (
		prefix = "prefix"
	)
	createBucket(t, s3client, bucket)
	createBucket(t, s3client, dstbucket)

	s3Content := map[string]string{
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
		"abc/def/test.py":         "S: file in nested folders",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%v/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/%v/", dstbucket, prefix)

	cmd := s5cmd("sync", "--exit-on-error", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp %v/a/another_test_file.txt %va/another_test_file.txt`, bucketPath, dst),
		1: equals(`cp %v/abc/def/test.py %vabc/def/test.py`, bucketPath, dst),
		2: equals(`cp %v/readme.md %vreadme.md`, bucketPath, dst),
		3: equals(`cp %v/testfile.txt %vtestfile.txt`, bucketPath, dst),
	}, sortInput(true))

	// assert  s3 objects in source bucket.
	for key, content := range s3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}

	// assert s3 objects in dest bucket
	for key, content := range s3Content {
		key = fmt.Sprintf("%s/%s", prefix, key) // add the prefix
		assert.Assert(t, ensureS3Object(s3client, dstbucket, key, content))
	}
}

// sync --exit-on-error s3://bucket/* s3://NotExistingBucket/ (dest bucket doesn't exist)
func TestSyncExitOnErrorS3BucketToS3BucketThatDoesNotExist(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	destbucket := "NotExistingBucket"

	createBucket(t, s3client, bucket)

	s3Content := map[string]string{
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
		"abc/def/test.py":         "S: file in nested folders",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("s3://%v/*", bucket)
	dst := fmt.Sprintf("s3://%v/", destbucket)

	cmd := s5cmd("sync", "--exit-on-error", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: contains(`status code: 404`),
	})
}

// sync s3://bucket/* s3://NotExistingBucket/ (dest bucket doesn't exist)
func TestSyncS3BucketToS3BucketThatDoesNotExist(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	destbucket := "NotExistingBucket"

	createBucket(t, s3client, bucket)

	s3Content := map[string]string{
		"testfile.txt":            "S: this is a test file",
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
		"abc/def/test.py":         "S: file in nested folders",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("s3://%v/*", bucket)
	dst := fmt.Sprintf("s3://%v/", destbucket)

	cmd := s5cmd("sync", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: contains(`status code: 404`),
	})
}

// If source path contains a special file it should not be synced
func TestSyncSocketDestinationEmpty(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}

	t.Parallel()

	s3client, s5cmd := setup(t)
	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	workdir := fs.NewDir(t, t.Name())
	defer workdir.Remove()

	sockaddr := workdir.Join("/s5cmd.sock")
	ln, err := net.Listen("unix", sockaddr)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ln.Close()
		os.Remove(sockaddr)
	})

	cmd := s5cmd("sync", ".", "s3://"+bucket+"/")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	// assert error message
	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: contains(`is not a regular file`),
	})

	// assert logs are empty (no sync)
	assertLines(t, result.Stdout(), nil)

	// assert exit code
	result.Assert(t, icmd.Expected{ExitCode: 1})
}

// sync --include pattern s3://bucket/* s3://anotherbucket/prefix/
func TestSyncS3ObjectsIntoAnotherBucketWithIncludeFilters(t *testing.T) {
	t.Parallel()

	srcbucket := s3BucketFromTestNameWithPrefix(t, "src")
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")

	s3client, s5cmd := setup(t)

	createBucket(t, s3client, srcbucket)
	createBucket(t, s3client, dstbucket)

	srcFiles := []string{
		"file_already_exists_in_destination.txt",
		"file_not_exists_in_destination.txt",
		"main.py",
		"main.js",
		"readme.md",
		"main.pdf",
		"main/file.txt",
	}

	dstFiles := []string{
		"prefix/file_already_exists_in_destination.txt",
	}

	excludedFiles := []string{
		"prefix/file_not_exists_in_destination.txt",
	}

	includedFiles := []string{
		"main.js",
		"main.pdf",
		"main.py",
		"main/file.txt",
		"readme.md",
	}

	const (
		content         = "this is a file content"
		includePattern1 = "main*"
		includePattern2 = "*.md"
	)

	for _, filename := range srcFiles {
		putFile(t, s3client, srcbucket, filename, content)
	}

	for _, filename := range dstFiles {
		putFile(t, s3client, dstbucket, filename, content)
	}

	src := fmt.Sprintf("s3://%v/*", srcbucket)
	dst := fmt.Sprintf("s3://%v/prefix/", dstbucket)

	cmd := s5cmd("sync", "--include", includePattern1, "--include", includePattern2, src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`cp s3://%s/%s s3://%s/prefix/%s`, srcbucket, includedFiles[0], dstbucket, includedFiles[0]),
		1: equals(`cp s3://%s/%s s3://%s/prefix/%s`, srcbucket, includedFiles[1], dstbucket, includedFiles[1]),
		2: equals(`cp s3://%s/%s s3://%s/prefix/%s`, srcbucket, includedFiles[2], dstbucket, includedFiles[2]),
		3: equals(`cp s3://%s/%s s3://%s/prefix/%s`, srcbucket, includedFiles[3], dstbucket, includedFiles[3]),
		4: equals(`cp s3://%s/%s s3://%s/prefix/%s`, srcbucket, includedFiles[4], dstbucket, includedFiles[4]),
	}, sortInput(true))

	// assert s3 source objects
	for _, filename := range srcFiles {
		assert.Assert(t, ensureS3Object(s3client, srcbucket, filename, content))
	}

	// assert s3 destination objects
	for _, filename := range includedFiles {
		assert.Assert(t, ensureS3Object(s3client, dstbucket, "prefix/"+filename, content))
	}

	// assert s3 destination objects which should not be in bucket.
	for _, filename := range excludedFiles {
		err := ensureS3Object(s3client, dstbucket, filename, content)
		assertError(t, err, errS3NoSuchKey)
	}
}

// sync --hash-only folder/ s3://bucket/
func TestSyncLocalFolderToS3BucketSameObjectsHashOnly(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithFile("test.py", "S: this is a python file"),     // will be uploaded, different content (hash), size same
		fs.WithFile("testfile.txt", "SD: this is a test file"), // will not be uploaded, same content (hash) and size.
		fs.WithDir("a",
			fs.WithFile("another_test_file.txt", "S: yet another txt file"), // will be uploaded, different content (hash), same size.
		),
		fs.WithDir("abc",
			fs.WithDir("def",
				fs.WithFile("main.py", "S: python file"), // will be uploaded, remote does not have it
			),
		),
	}

	workdir := fs.NewDir(t, "somedir", folderLayout...)
	defer workdir.Remove()

	s3Content := map[string]string{
		"test.py":                 "D: this is a python file",
		"testfile.txt":            "SD: this is a test file",
		"a/another_test_file.txt": "D: yet another txt file",
	}

	for filename, content := range s3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("%v/", workdir.Path())
	src = filepath.ToSlash(src)
	dst := fmt.Sprintf("s3://%s/", bucket)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", "--hash-only", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %vtestfile.txt %vtestfile.txt": object ETag matches`, src, dst),
		1: equals(`cp %va/another_test_file.txt %va/another_test_file.txt`, src, dst),
		2: equals(`cp %vabc/def/main.py %vabc/def/main.py`, src, dst),
		3: equals(`cp %vtest.py %vtest.py`, src, dst),
	}, sortInput(true))

	// expected folder structure without the timestamp.
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	expectedS3Content := map[string]string{
		"test.py":                 "S: this is a python file",
		"testfile.txt":            "SD: this is a test file",
		"a/another_test_file.txt": "S: yet another txt file",
		"abc/def/main.py":         "S: python file",
	}

	// assert s3
	for key, content := range expectedS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}
}

// sync --hash-only s3://bucket/* s3://destbucket/
func TestSyncS3BucketToS3BucketHashOnly(t *testing.T) {
	t.Parallel()

	now := time.Now()
	timeSource := newFixedTimeSource(now)
	s3client, s5cmd := setup(t, withTimeSource(timeSource))

	bucket := s3BucketFromTestName(t)
	dstbucket := s3BucketFromTestNameWithPrefix(t, "dst")
	createBucket(t, s3client, bucket)
	createBucket(t, s3client, dstbucket)

	sourceS3Content := map[string]string{
		"main.py":                 "S: this is an updated python file", // destination does not have it
		"testfile.txt":            "SD: this is a test file",           // will not be uploaded, destination has the file with the same hash/size
		"readme.md":               "S: this is a readme file",          // will be uploaded, destination has file with different hash, but same size
		"a/another_test_file.txt": "S: yet another txt file",           // will be uploaded, destination has file with different hash, but same size
	}

	destS3Content := map[string]string{
		"testfile.txt":            "SD: this is a test file",
		"readme.md":               "D: this is a readme file",
		"a/another_test_file.txt": "D: yet another txt file",
	}

	// make source files older in bucket.
	// timestamps should be ignored with --hash-only flag
	timeSource.Advance(-time.Minute)
	for filename, content := range destS3Content {
		putFile(t, s3client, dstbucket, filename, content)
	}
	timeSource.Advance(time.Minute)

	for filename, content := range sourceS3Content {
		putFile(t, s3client, bucket, filename, content)
	}

	bucketPath := fmt.Sprintf("s3://%v", bucket)
	src := fmt.Sprintf("%s/*", bucketPath)
	dst := fmt.Sprintf("s3://%v/", dstbucket)

	// log debug
	cmd := s5cmd("--log", "debug", "sync", "--hash-only", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "sync %v/testfile.txt %vtestfile.txt": object ETag matches`, bucketPath, dst),
		1: equals(`cp %v/a/another_test_file.txt %va/another_test_file.txt`, bucketPath, dst),
		2: equals(`cp %v/main.py %vmain.py`, bucketPath, dst),
		3: equals(`cp %v/readme.md %vreadme.md`, bucketPath, dst),
	}, sortInput(true))

	// assert s3 objects in source
	for key, content := range sourceS3Content {
		assert.Assert(t, ensureS3Object(s3client, bucket, key, content))
	}

	expectedDestS3Content := map[string]string{
		"main.py":                 "S: this is an updated python file",
		"testfile.txt":            "SD: this is a test file", // same as source
		"readme.md":               "S: this is a readme file",
		"a/another_test_file.txt": "S: yet another txt file",
	}

	// assert s3 objects in destination
	for key, content := range expectedDestS3Content {
		assert.Assert(t, ensureS3Object(s3client, dstbucket, key, content))
	}
}
