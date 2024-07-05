package e2e

import (
	"fmt"
	"strings"
	"testing"

	"gotest.tools/v3/icmd"
)

func TestDiskUsageSingleS3Object(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// create 2 files, expect 1.
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")

	cmd := s5cmd("du", "s3://"+bucket+"/testfile1.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`22 bytes in 1 objects: s3://%v/testfile1.txt`, bucket),
	})
}

func TestDiskUsageSingleS3ObjectJSON(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)
	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	// create 2 files, expect 1.
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")

	cmd := s5cmd("--json", "du", "s3://"+bucket+"/testfile1.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: json(`
			{
				"source": "s3://%v/testfile1.txt",
				"count":1,
				"size":22
			}
		`, bucket),
	})
}

func TestDiskUsageMultipleS3Objects(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)
	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "foo/testfile3.txt", "this is also a file content")

	cmd := s5cmd("du", "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`49 bytes in 2 objects: s3://%v`, bucket),
	})
}

func TestDiskUsageWildcard(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "foo/testfile3.txt", "this is also a file content somehow")
	putFile(t, s3client, bucket, "bar/testfile3.gz", "this is also a file content somehow")

	cmd := s5cmd("du", "s3://"+bucket+"/*.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`84 bytes in 3 objects: s3://%v/*.txt`, bucket),
	})
}

func TestDiskUsageS3ObjectsAndFolders(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "content")
	putFile(t, s3client, bucket, "report.gz", "content")
	putFile(t, s3client, bucket, "/a/testfile2.txt", "content")
	putFile(t, s3client, bucket, "/b/testfile3.txt", "content")
	putFile(t, s3client, bucket, "/b/testfile4.txt", "content")
	putFile(t, s3client, bucket, "/c/testfile5.gz", "content")
	putFile(t, s3client, bucket, "/d/foo/bar/file7.txt", "content")
	putFile(t, s3client, bucket, "/d/foo/bar/testfile8.txt", "content")
	putFile(t, s3client, bucket, "/e/txt/testfile9.txt.gz", "content")
	putFile(t, s3client, bucket, "/f/txt/testfile10.txt", "content")

	cmd := s5cmd("du", "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`14 bytes in 2 objects: s3://%v`, bucket),
	})
}

func TestDiskUsageWildcardS3ObjectsWithDashH(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	putFile(t, s3client, bucket, "testfile1.txt", strings.Repeat("this is a file content", 10000))
	putFile(t, s3client, bucket, "testfile2.txt", strings.Repeat("this is also a file content", 1000))
	putFile(t, s3client, bucket, "foo/testfile3.txt", strings.Repeat("this is also a file content", 1000))

	cmd := s5cmd("du", "-H", "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`241.2K bytes in 2 objects: s3://%v`, bucket),
	})
}

func TestDiskUsageMissingObject(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("du", "s3://"+bucket+"/non-existent-file")
	result := icmd.RunCmd(cmd)

	// s5cmd returns 0 if given object is not found
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`0 bytes in 0 objects: s3://%v/non-existent-file`, bucket),
	})
}

// du --exclude "main*" s3://bucket/*.txt
func TestDiskUsageWildcardWithExcludeFilter(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t)

	const excludePattern = "main*"

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "main.txt", "this is also a file content")
	putFile(t, s3client, bucket, "main2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "main.py", "this is a python file")
	putFile(t, s3client, bucket, "foo/testfile3.txt", "this is also a file content somehow")
	putFile(t, s3client, bucket, "bar/testfile3.gz", "this is also a file content somehow")

	cmd := s5cmd("du", "--exclude", excludePattern, "s3://"+bucket+"/*.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`84 bytes in 3 objects: s3://%v/*.txt`, bucket),
	})
}

// du --exclude "main*" --exclude "*.gz" s3://bucket/*
func TestDiskUsageWildcardWithExcludeFilters(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t)

	const (
		excludePattern1 = "main*"
		excludePattern2 = "*.gz"
	)

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "main.txt", "this is also a file content")
	putFile(t, s3client, bucket, "main2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "main.py", "this is a python file")
	putFile(t, s3client, bucket, "foo/testfile3.txt", "this is also a file content somehow")
	putFile(t, s3client, bucket, "bar/testfile3.gz", "this is also a file content somehow")

	cmd := s5cmd("du", "--exclude", excludePattern1, "--exclude", excludePattern2, "s3://"+bucket+"/*")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`84 bytes in 3 objects: s3://%v/*`, bucket),
	})
}

func TestDiskUsageByVersionIDAndAllVersions(t *testing.T) {
	skipTestIfGCS(t, "versioning is not supported in GCS")

	t.Parallel()

	bucket := s3BucketFromTestName(t)

	// versioninng is only supported with in memory backend!
	s3client, s5cmd := setup(t, withS3Backend("mem"))

	const filename = "testfile.txt"

	var (
		contents = []string{
			"This is first content",
			"Second content it is, and it is a bit longer!!!",
		}
		sizes = []int{len(contents[0]), len(contents[1])}
	)

	// create a bucket and Enable versioning
	createBucket(t, s3client, bucket)
	setBucketVersioning(t, s3client, bucket, "Enabled")

	// upload two versions of the file with same key
	putFile(t, s3client, bucket, filename, contents[0])
	putFile(t, s3client, bucket, filename, contents[1])

	//  get disk usage
	cmd := s5cmd("du", "s3://"+bucket+"/"+filename)
	result := icmd.RunCmd(cmd)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(fmt.Sprintf("%d bytes in %d objects: s3://%v/%v", sizes[1], 1, bucket, filename)),
	})

	// we expect to see disk usage of 2 versions of objects
	cmd = s5cmd("du", "--all-versions", "s3://"+bucket+"/"+filename)
	result = icmd.RunCmd(cmd)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(fmt.Sprintf("%d bytes in %d objects: s3://%v/%v", sizes[0]+sizes[1], 2, bucket, filename)),
	})

	// now we will list and parse their version IDs
	cmd = s5cmd("ls", "--all-versions", "s3://"+bucket+"/"+filename)
	result = icmd.RunCmd(cmd)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: contains("%v", filename),
		1: contains("%v", filename),
	})

	versionIDs := make([]string, 0)
	for _, row := range strings.Split(result.Stdout(), "\n") {
		if row != "" {
			arr := strings.Split(row, " ")
			versionIDs = append(versionIDs, arr[len(arr)-1])
		}
	}

	for i, version := range versionIDs {
		cmd = s5cmd("du", "--version-id", version,
			fmt.Sprintf("s3://%v/%v", bucket, filename))
		result = icmd.RunCmd(cmd)
		assertLines(t, result.Stdout(), map[int]compareFunc{
			0: equals(fmt.Sprintf("%d bytes in %d objects: s3://%v/%v", sizes[i], 1, bucket, filename)),
		})
	}
}

func TestDiskUsageEmptyBucket(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("du", "s3://"+bucket)
	result := icmd.RunCmd(cmd)


	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`0 bytes in 0 objects: s3://%v`, bucket),
	})
}
