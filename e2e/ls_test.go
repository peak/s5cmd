package e2e

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

// ls
func TestListBuckets(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	// alphabetically unordered list of buckets
	bucketPrefix := s3BucketFromTestName(t)
	createBucket(t, s3client, bucketPrefix+"-1")
	createBucket(t, s3client, bucketPrefix+"-2")
	createBucket(t, s3client, bucketPrefix+"-4")
	createBucket(t, s3client, bucketPrefix+"-3")

	cmd := s5cmd("ls")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	// find the first created bucket
	startIdx := strings.Index(result.Stdout(), fmt.Sprintf("s3://%v-1", bucketPrefix))
	got := result.Stdout()[startIdx:]

	// expect ordered list
	assertLines(t, got, map[int]compareFunc{
		0: suffix("s3://%v-1", bucketPrefix),
		1: suffix("s3://%v-2", bucketPrefix),
		2: suffix("s3://%v-3", bucketPrefix),
		3: suffix("s3://%v-4", bucketPrefix),
	}, strictLineCheck(false))
}

// -json ls bucket
func TestListBucketsJSON(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	// alphabetically unordered list of buckets
	bucketPrefix := s3BucketFromTestName(t)
	createBucket(t, s3client, bucketPrefix+"-1")
	createBucket(t, s3client, bucketPrefix+"-2")
	createBucket(t, s3client, bucketPrefix+"-4")
	createBucket(t, s3client, bucketPrefix+"-3")

	cmd := s5cmd("--json", "ls")
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	stdout := result.Stdout()
	// find the first created bucket
	startIdx := strings.Index(stdout, fmt.Sprintf(`"name":"%v-1"`, bucketPrefix))
	// find the start of the line
	startOfLine := strings.LastIndex(stdout[:startIdx], "{")
	beginningOfOutput := stdout[startOfLine:]

	// expect ordered list
	assertLines(t, beginningOfOutput, map[int]compareFunc{
		0: suffix(`"name":"%v-1"}`, bucketPrefix),
		1: suffix(`"name":"%v-2"}`, bucketPrefix),
		2: suffix(`"name":"%v-3"}`, bucketPrefix),
		3: suffix(`"name":"%v-4"}`, bucketPrefix),
	}, jsonCheck(true), strictLineCheck(false))
}

// ls bucket/object
func TestListSingleS3Object(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// create 2 files, expect 1.
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")

	cmd := s5cmd("ls", "s3://"+bucket+"/testfile1.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("22 testfile1.txt"),
	})
}

// -json ls bucket/object
func TestListSingleS3ObjectJSON(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// create 2 files, expect 1.
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")

	cmd := s5cmd("--json", "ls", "s3://"+bucket+"/testfile1.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: prefix(`{"key":"s3://%v/testfile1.txt",`, bucket),
	}, jsonCheck(true))
}

// ls bucket/*.ext
func TestListSingleWildcardS3Object(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "testfile3.txt", "this is also a file content somehow")

	cmd := s5cmd("ls", "s3://"+bucket+"/*.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("22 testfile1.txt"),
		1: suffix("27 testfile2.txt"),
		2: suffix("35 testfile3.txt"),
	}, alignment(true))
}

func TestListWildcardS3ObjectWithNewLineInName(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t)

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "normal.txt", "this is a file content")
	putFile(t, s3client, bucket, "another.txt", "this is another file content")
	putFile(t, s3client, bucket, "newli\ne.txt", "this is yet another file content")
	putFile(t, s3client, bucket, "nap.txt", "this, too, is a file content")

	cmd := s5cmd("ls", "s3://"+bucket+"/n*.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("28 nap.txt"),
		1: suffix("32 newli"),
		2: equals("e.txt"),
		3: suffix("22 normal.txt"),
	})
}

// ls -s bucket/object
func TestListS3ObjectsWithDashS(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")

	cmd := s5cmd("ls", "-s", "s3://"+bucket+"/testfile1.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	// TODO: test if full form of storage class is displayed (it can be done when and if gofakes3 supports storage classes)
}

// ls bucket/*/object*.ext
func TestListMultipleWildcardS3Object(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "a/testfile1.txt", "content")
	putFile(t, s3client, bucket, "a/testfile2.txt", "content")
	putFile(t, s3client, bucket, "b/testfile3.txt", "content")
	putFile(t, s3client, bucket, "b/testfile4.txt", "content")
	putFile(t, s3client, bucket, "c/testfile5.gz", "content")
	putFile(t, s3client, bucket, "c/testfile6.txt.gz", "content")
	putFile(t, s3client, bucket, "d/foo/bar/file7.txt", "content")
	putFile(t, s3client, bucket, "d/foo/bar/testfile8.txt", "content")
	putFile(t, s3client, bucket, "e/txt/testfile9.txt.gz", "content")
	putFile(t, s3client, bucket, "f/txt/testfile10.txt", "content")

	const pattern = "/*/testfile*.txt"
	cmd := s5cmd("ls", "s3://"+bucket+pattern)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("7 a/testfile1.txt"),
		1: suffix("7 a/testfile2.txt"),
		2: suffix("7 b/testfile3.txt"),
		3: suffix("7 b/testfile4.txt"),
		4: suffix("7 d/foo/bar/testfile8.txt"),
		5: suffix("7 f/txt/testfile10.txt"),
	}, alignment(true))
}

// ls bucket/prefix/object*.ext
func TestListMultipleWildcardS3ObjectWithPrefix(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "a/testfile1.txt", "content")
	putFile(t, s3client, bucket, "a/testfile2.txt", "content")
	putFile(t, s3client, bucket, "a/testfile3.txt", "content")
	putFile(t, s3client, bucket, "b/testfile4.txt", "content")
	putFile(t, s3client, bucket, "c/testfile5.gz", "content")
	putFile(t, s3client, bucket, "c/testfile6.txt.gz", "content")
	putFile(t, s3client, bucket, "d/foo/bar/file7.txt", "content")
	putFile(t, s3client, bucket, "d/foo/bar/testfile8.txt", "content")
	putFile(t, s3client, bucket, "e/txt/testfile9.txt.gz", "content")
	putFile(t, s3client, bucket, "f/txt/testfile10.txt", "content")

	const pattern = "/a/testfile*.txt"
	cmd := s5cmd("ls", "s3://"+bucket+pattern)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("7 testfile1.txt"),
		1: suffix("7 testfile2.txt"),
		2: suffix("7 testfile3.txt"),
	}, alignment(true))
}

// ls bucket
func TestListS3ObjectsAndFolders(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "content")
	putFile(t, s3client, bucket, "report.gz", "content")
	putFile(t, s3client, bucket, "a/testfile2.txt", "content")
	putFile(t, s3client, bucket, "b/testfile3.txt", "content")
	putFile(t, s3client, bucket, "b/testfile4.txt", "content")
	putFile(t, s3client, bucket, "c/testfile5.gz", "content")
	putFile(t, s3client, bucket, "d/foo/bar/file7.txt", "content")
	putFile(t, s3client, bucket, "d/foo/bar/testfile8.txt", "content")
	putFile(t, s3client, bucket, "e/txt/testfile9.txt.gz", "content")
	putFile(t, s3client, bucket, "f/txt/testfile10.txt", "content")

	cmd := s5cmd("ls", "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("DIR a/"),
		1: suffix("DIR b/"),
		2: suffix("DIR c/"),
		3: suffix("DIR d/"),
		4: suffix("DIR e/"),
		5: suffix("DIR f/"),
		6: suffix("7 report.gz"),
		7: suffix("7 testfile1.txt"),
	}, alignment(true))
}

func TestListS3ObjectsAndFoldersWithTheirFullpath(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "content")
	putFile(t, s3client, bucket, "report.gz", "content")
	putFile(t, s3client, bucket, "a/testfile2.txt", "content")
	putFile(t, s3client, bucket, "b/testfile3.txt", "content")
	putFile(t, s3client, bucket, "b/testfile4.txt", "content")
	putFile(t, s3client, bucket, "c/testfile5.gz", "content")
	putFile(t, s3client, bucket, "d/foo/bar/file7.txt", "content")
	putFile(t, s3client, bucket, "d/foo/bar/testfile8.txt", "content")
	putFile(t, s3client, bucket, "e/txt/testfile9.txt.gz", "content")
	putFile(t, s3client, bucket, "f/txt/testfile10.txt", "content")

	cmd := s5cmd("ls", "--show-fullpath", "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	// assert lexical order
	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(fmt.Sprintf("s3://%v/a/testfile2.txt", bucket)),
		1: equals(fmt.Sprintf("s3://%v/b/testfile3.txt", bucket)),
		2: equals(fmt.Sprintf("s3://%v/b/testfile4.txt", bucket)),
		3: equals(fmt.Sprintf("s3://%v/c/testfile5.gz", bucket)),
		4: equals(fmt.Sprintf("s3://%v/d/foo/bar/file7.txt", bucket)),
		5: equals(fmt.Sprintf("s3://%v/d/foo/bar/testfile8.txt", bucket)),
		6: equals(fmt.Sprintf("s3://%v/e/txt/testfile9.txt.gz", bucket)),
		7: equals(fmt.Sprintf("s3://%v/f/txt/testfile10.txt", bucket)),
		8: equals(fmt.Sprintf("s3://%v/report.gz", bucket)),
		9: equals(fmt.Sprintf("s3://%v/testfile1.txt", bucket)),
	}, alignment(true))
}

// ls bucket/prefix
func TestListS3ObjectsAndFoldersWithPrefix(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "content")
	putFile(t, s3client, bucket, "report.gz", "content")
	putFile(t, s3client, bucket, "a/testfile2.txt", "content")
	putFile(t, s3client, bucket, "t/testfile3.txt", "content")

	// search with prefix t
	cmd := s5cmd("ls", "s3://"+bucket+"/t")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix("DIR t/"),
		1: suffix("7 testfile1.txt"),
	}, alignment(true))
}

// ls bucket/*/object*.ext
func TestListNonexistingS3ObjectInGivenPrefix(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const pattern = "/*/testfile*.txt"
	cmd := s5cmd("ls", "s3://"+bucket+pattern)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stdout(), map[int]compareFunc{})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "ls s3://%v/*/testfile*.txt": no object found`, bucket),
	}, strictLineCheck(false))
}

// ls bucket/object (nonexistent)
func TestListNonexistingS3Object(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("ls", "s3://"+bucket+"/nosuchobject")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stdout(), map[int]compareFunc{})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "ls s3://%v/nosuchobject": no object found`, bucket),
	}, strictLineCheck(false))
}

// ls -e bucket
func TestListS3ObjectsWithDashE(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	putFile(t, s3client, bucket, "testfile1.txt", strings.Repeat("this is a file content", 10000))
	putFile(t, s3client, bucket, "testfile2.txt", strings.Repeat("this is also a file content", 10000))

	cmd := s5cmd("ls", "-e", "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(`^ \w+ \d+ testfile1.txt$`),
		1: match(`^ \w+ \d+ testfile2.txt$`),
	}, trimMatch(dateRe), alignment(true))
}

// ls -H bucket
func TestListS3ObjectsWithDashH(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	putFile(t, s3client, bucket, "testfile1.txt", strings.Repeat("this is a file content", 10000))
	putFile(t, s3client, bucket, "testfile2.txt", strings.Repeat("this is also a file content", 10000))

	cmd := s5cmd("ls", "-H", "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(`^ 214.8K testfile1.txt$`),
		1: match(`^ 263.7K testfile2.txt$`),
	}, trimMatch(dateRe), alignment(true))
}

// ls --exclude "*.txt" s3://bucket/*
func TestListS3ObjectsWithExcludeFilter(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	const (
		content        = "content"
		excludePattern = "*.txt"
	)

	filenames := []string{
		"file.txt",
		"file.py",
		"hasan.txt",
		"a/try.txt",
		"a/try.py",
		"a/file.c",
		"file2.txt",
		"file2.txt.extension", // this should not be excluded.
		"newli\ne",
		"newli\ne.txt",
	}

	s3client, s5cmd := setup(t)

	createBucket(t, s3client, bucket)

	for _, filename := range filenames {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("ls", "--exclude", excludePattern, "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(`a/file.c`),
		1: match(`a/try.py`),
		2: match(`file.py`),
		3: match(`file2.txt.extension`),
		4: match("newli"),
		5: match("e"),
	}, trimMatch(dateRe), alignment(false))
}

// ls --exclude ".txt" --exclude ".py" s3://bucket
func TestListS3ObjectsWithExcludeFilters(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	const (
		content         = "content"
		excludePattern1 = "*.txt"
		excludePattern2 = "*.py"
	)

	filenames := []string{
		"file.txt",
		"file.py",
		"hasan.txt",
		"a/try.txt",
		"a/try.py",
		"a/file.c",
		"file2.txt",
	}

	s3client, s5cmd := setup(t)

	createBucket(t, s3client, bucket)

	for _, filename := range filenames {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("ls", "--exclude", excludePattern1, "--exclude", excludePattern2, "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(`DIR a/`),
	}, trimMatch(dateRe), alignment(true))
}

// ls --exclude "" s3://bucket
func TestListS3ObjectsWithEmptyExcludeFilter(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	const content = "content"
	const excludePattern = ""
	filenames := []string{
		"file.txt",
		"file.py",
		"a/try.txt",
		"a/try.py",
		"a/file.c",
		"file2.txt",
	}

	s3client, s5cmd := setup(t)

	createBucket(t, s3client, bucket)

	for _, filename := range filenames {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("ls", "--exclude", excludePattern, "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(`DIR a/`),
		1: match(`file.py`),
		2: match("file.txt"),
		3: match("file2.txt"),
	}, trimMatch(dateRe), alignment(true))
}

// ls --exclude "some-dir/some-dir/file.txt" s3://bucket/*
func TestListNestedPrefixedS3ObjectsWithExcludeFilter(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	const content = "content"
	const excludePattern = "some-dir/some-dir/file.txt"
	filenames := []string{
		"file.txt",
		"some-dir/file.txt",
		"some-dir/some-dir/file.txt",
		"some-dir/some-dir/some-dir/file.txt",
	}

	s3client, s5cmd := setup(t)

	createBucket(t, s3client, bucket)

	for _, filename := range filenames {
		putFile(t, s3client, bucket, filename, content)
	}

	srcpath := fmt.Sprintf("s3://%v/*", bucket)
	cmd := s5cmd("ls", "--exclude", excludePattern, srcpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(`file.txt`),
		1: match("some-dir/file.txt"),
		2: match("some-dir/some-dir/some-dir/file.txt"),
	}, trimMatch(dateRe), alignment(true))
}

// ls --exclude "main*" directory
// ls --exclude "main*" directory/
// ls --exclude "main*" directory/*
func TestListLocalFilesWithExcludeFilter(t *testing.T) {
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

			_, s5cmd := setup(t)

			const excludePattern = "main*"
			folderLayout := []fs.PathOp{
				fs.WithDir(
					"main",
					fs.WithFile("try.txt", "this is a txt file"),
				),
				fs.WithFile("file1.txt", "this is the first test file"),
				fs.WithFile("main.py", "this is a python file"),
				fs.WithFile("main.c", "this is a c file"),
				fs.WithFile("main.txt", "this is a txt file"),
				fs.WithFile("main2.txt", "this is a txt file"),
				fs.WithFile("readme.md", "this is a readme file"),
			}

			workdir := fs.NewDir(t, t.Name(), folderLayout...)
			defer workdir.Remove()
			srcpath := workdir.Path()
			srcpath = srcpath + tc.directoryPrefix
			srcpath = filepath.ToSlash(srcpath)

			cmd := s5cmd("ls", "--exclude", excludePattern, srcpath)
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)

			assertLines(t, result.Stdout(), map[int]compareFunc{
				0: match("file1.txt"),
				1: match("readme.md"),
			}, trimMatch(dateRe), alignment(true))
		})
	}
}

// ls --exclude "main*" --exclude ".txt" directory/
func TestListLocalFilesWithExcludeFilters(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	const (
		excludePattern1 = "main*"
		excludePattern2 = "*.txt"
	)

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"main",
			fs.WithFile("try.txt", "this is a txt file"),
		),
		fs.WithFile("file1.txt", "this is the first test file"),
		fs.WithFile("main.py", "this is a python file"),
		fs.WithFile("main.c", "this is a c file"),
		fs.WithFile("main.txt", "this is a txt file"),
		fs.WithFile("main2.txt", "this is a txt file"),
		fs.WithFile("readme.md", "this is a readme file"),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()
	srcpath := workdir.Path()
	srcpath = filepath.ToSlash(srcpath)

	cmd := s5cmd("ls", "--exclude", excludePattern1, "--exclude", excludePattern2, srcpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match("readme.md"),
	}, trimMatch(dateRe), alignment(true))

}

// ls --exclude "main*" directory/*.txt
func TestListLocalFilesWithPrefixAndExcludeFilter(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	const (
		excludePattern1 = "main*"
		prefix          = "*.txt"
	)

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"main",
			fs.WithFile("try.txt", "this is a txt file"),
		),
		fs.WithFile("file1.txt", "this is the first test file"),
		fs.WithFile("main.py", "this is a python file"),
		fs.WithFile("main.c", "this is a c file"),
		fs.WithFile("main.txt", "this is a txt file"),
		fs.WithFile("main2.txt", "this is a txt file"),
		fs.WithFile("readme.md", "this is a readme file"),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()
	srcpath := workdir.Join(prefix)
	srcpath = filepath.ToSlash(srcpath)

	cmd := s5cmd("ls", "--exclude", excludePattern1, srcpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match("file1.txt"),
	}, trimMatch(dateRe), alignment(true))
}

// ls --exclude "some-dir/some-dir/file.txt" directory
func TestListNestedLocalFolders(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	const (
		excludePattern1 = "some-dir/some-dir/file.txt"
	)

	folderLayout := []fs.PathOp{
		fs.WithFile("file.txt", "content"),
		fs.WithDir("some-dir",
			fs.WithDir("some-dir",
				fs.WithDir("some-dir",
					fs.WithFile("file.txt", "content"),
				),
				fs.WithFile("file.txt", "this is a txt file"),
			),
			fs.WithFile("file.txt", "this is a txt file"),
		),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()
	srcpath := workdir.Path()
	srcpath = filepath.ToSlash(srcpath)

	cmd := s5cmd("ls", "--exclude", excludePattern1, srcpath)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(filepath.ToSlash("file.txt")),
		1: match(filepath.ToSlash("file.txt")),
		2: match(filepath.ToSlash("file.txt")),
	}, trimMatch(dateRe), alignment(true))
}
