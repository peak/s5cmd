package e2e

import (
	"fmt"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/icmd"
)

// head (without anything -> error)
func TestHead(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("head")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: match(`ERROR "head": target should be remote object or bucket`),
	})
}

// head bucket
func TestHeadBucket(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("head", fmt.Sprintf("s3://%v", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`{"bucket":"s3://%v"}`, bucket),
	}, jsonCheck(true), strictLineCheck(false))
}

// head bucket (non-existent)
func TestHeadBucketNonExistent(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("head", "s3://non-existent-bucket")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: contains(`ERROR "head s3://non-existent-bucket": NotFound: Not Found status code: 404`),
	})
}

// --json head bucket
func TestHeadBucketJSON(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("--json", "head", fmt.Sprintf("s3://%v", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`{"bucket":"s3://%v"}`, bucket),
	}, jsonCheck(true), strictLineCheck(false))
}

// --json head bucket (non-existent)
func TestHeadBucketJSONNonExistent(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("--json", "head", "s3://non-existent-bucket")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
	assert.Equal(t, strings.Contains(result.Stderr(), `"error":"NotFound: Not Found status code: 404`), true)
}

// head object
func TestHeadObject(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "myfile.txt", "content")

	cmd := s5cmd("head", fmt.Sprintf("s3://%v/myfile.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	expectedOutput := fmt.Sprintf(`{"key":"s3://%v/myfile.txt","last_modified":"[0-9-]+T[0-9:.]+Z","size":\d+,"storage_class":"STANDARD","etag":"[a-f0-9]+","metadata":\{\}}`, bucket)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, jsonCheck(true), strictLineCheck(false))
}

// head object (non-existent)
func TestHeadObjectNonExistent(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("head", fmt.Sprintf("s3://%v/non-existent-file.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: contains(`non-existent-file.txt not found`),
	})
}

// --json head object
func TestHeadObjectJSON(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file.txt", "content")

	cmd := s5cmd("--json", "head", fmt.Sprintf("s3://%v/file.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	expectedOutput := fmt.Sprintf(`{"key":"s3://%v/file.txt","last_modified":"[0-9-]+T[0-9:.]+Z","size":\d+,"storage_class":"STANDARD","etag":"[a-f0-9]+","metadata":\{\}}`, bucket)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, jsonCheck(true), strictLineCheck(false))
}

// --json head object (non-existent)
func TestHeadObjectJSONNonExistent(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("--json", "head", fmt.Sprintf("s3://%v/non-existent-file.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: contains(`non-existent-file.txt not found`),
	}, jsonCheck(true), strictLineCheck(false))
}

// head --raw objectWithGlobChar
// head --raw s3://bucket/file*.txt
func TestHeadObjectRaw(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file*.txt", "content")

	cmd := s5cmd("head", "--raw", fmt.Sprintf("s3://%v/file*.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	expectedOutput := fmt.Sprintf(`{"key":"s3://%v/file\*.txt","last_modified":"[0-9-]+T[0-9:.]+Z","size":\d+,"storage_class":"STANDARD","etag":"[a-f0-9]+","metadata":\{\}}`, bucket)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, jsonCheck(true), strictLineCheck(false))
}

// head object s3://bucket/file*.txt
func TestHeadObjectWildcardWithoutRawFlag(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	putFile(t, s3client, bucket, "file.txt", "content")

	cmd := s5cmd("head", fmt.Sprintf("s3://%v/file*.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: contains(`can not contain glob characters`),
	})
}

// head --version-id object s3://bucket/file.txt
func TestHeadObjectWithVersionID(t *testing.T) {
	skipTestIfGCS(t, "versioning is not supported in GCS")

	t.Parallel()

	bucket := s3BucketFromTestName(t)

	// versioninng is only supported with in memory backend!
	s3client, s5cmd := setup(t, withS3Backend("mem"))

	const filename = "testfile.txt"

	contents := []string{
		"This is first content",
		"Second content it is, and it is a bit longer!!!",
	}

	// create a bucket and Enable versioning
	createBucket(t, s3client, bucket)
	setBucketVersioning(t, s3client, bucket, "Enabled")

	// upload two versions of the file with same key
	putFile(t, s3client, bucket, filename, contents[0])
	putFile(t, s3client, bucket, filename, contents[1])

	// now we will list and parse their version IDs
	cmd := s5cmd("ls", "--all-versions", "s3://"+bucket+"/"+filename)
	result := icmd.RunCmd(cmd)

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
		cmd = s5cmd("head", "--version-id", version,
			fmt.Sprintf("s3://%v/%v", bucket, filename))

		result = icmd.RunCmd(cmd)
		result.Assert(t, icmd.Success)

		expectedOutput := fmt.Sprintf(`{"key":"s3://%v/testfile.txt","last_modified":"[0-9-]+T[0-9:.]+Z","size":%d,"storage_class":"STANDARD","etag":"[a-f0-9]+","metadata":\{\}}`, bucket, len(contents[i]))

		assertLines(t, result.Stdout(), map[int]compareFunc{
			0: match(expectedOutput),
		}, jsonCheck(true), strictLineCheck(false))
	}
}

// head s3://bucket/file.txt (file.txt has metadata)
func TestHeadObjectWithMetadata(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	metadata := make(map[string]*string)
	value := "value1"
	value2 := "value2"
	metadata["key1"] = &value
	metadata["key2"] = &value2

	putFile(t, s3client, bucket, "file.txt", "content", putArbitraryMetadata(metadata))

	cmd := s5cmd("head", fmt.Sprintf("s3://%v/file.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	expectedOutput := fmt.Sprintf(`{"key":"s3://%v/file.txt","last_modified":"[0-9-]+T[0-9:.]+Z","size":\d+,"storage_class":"STANDARD","etag":"[a-f0-9]+","metadata":{(?:"key1":"value1","key2":"value2"|"key2":"value2","key1":"value1")}}`, bucket)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, jsonCheck(true), strictLineCheck(false))
}

// head --json object s3://bucket/file.txt (metadata)
func TestHeadObjectJSONMetadata(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	metadata := make(map[string]*string)
	value := "value1"

	metadata["key1"] = &value

	putFile(t, s3client, bucket, "file.txt", "content", putArbitraryMetadata(metadata))

	cmd := s5cmd("--json", "head", fmt.Sprintf("s3://%v/file.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	expectedOutput := fmt.Sprintf(`{"key":"s3://%v/file.txt","last_modified":"[0-9-]+T[0-9:.]+Z","size":\d+,"storage_class":"STANDARD","etag":"[a-f0-9]+","metadata":{"key1":"value1"}}`, bucket)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, jsonCheck(true), strictLineCheck(false))
}
