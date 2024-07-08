package e2e

import (
	"fmt"
	"strings"
	"testing"

	"gotest.tools/v3/icmd"
)

// head (without anything -> error)

func TestHead(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("head")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
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
		0: suffix(fmt.Sprintf("s3://%v", bucket)),
	})
}

// head bucket (non-existent)

func TestHeadBucketNonExistent(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("head", "s3://non-existent-bucket")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
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
		0: suffix(`{"name":"s3://%v"}`, bucket),
	}, jsonCheck(true), strictLineCheck(false))
}

// --json head bucket (non-existent)

func TestHeadBucketJSONNonExistent(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("--json", "head", "s3://non-existent-bucket")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
}

// head bucket --humanize

func TestHeadBucketHumanize(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("head", "--humanize", fmt.Sprintf("s3://%v", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(fmt.Sprintf("s3://%v", bucket)),
	})
}

// head bucket --etag
func TestHeadBucketEtag(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("head", "--etag", fmt.Sprintf("s3://%v", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(fmt.Sprintf("s3://%v", bucket)),
	})
}

// head bucket --storage-class

func TestHeadBucketStorageClass(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("head", "--storage-class", fmt.Sprintf("s3://%v", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(fmt.Sprintf("s3://%v", bucket)),
	})
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

	storageClassPattern := `(?:STANDARD|)`
	sizePattern := `\d+`
	s3urlPattern := `myfile.txt`

	expectedOutput := fmt.Sprintf(
		`%s\s+%s\s+%s`,
		storageClassPattern,
		sizePattern,
		s3urlPattern,
	)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, trimMatch(dateRe), alignment(true))
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

	etagPattern := `\"[^\"]+\"`
	lastModifiedPattern := `\"[^\"]+\"`
	typePattern := `\"[^\"]+\"`
	sizePattern := `\d+`
	storageClassPattern := `\"[^\"]+\"`

	expectedOutput := fmt.Sprintf(
		`{"key":"s3://%v/file.txt","etag":%s,"last_modified":%s,"type":%s,"size":%s,"storage_class":%s,"metadata":\{\}}`,
		bucket,
		etagPattern,
		lastModifiedPattern,
		typePattern,
		sizePattern,
		storageClassPattern,
	)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, jsonCheck(true), strictLineCheck(false))
}

// --json head object (non-existent)

func TestHeadObjectJSONNonExistent(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("--json", "head", "s3://non-existent-bucket/non-existent-file.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
}

// head object --humanize

func TestHeadObjectHumanize(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	largeFileContent := make([]byte, 1024*1024*50) // 50MB
	for i := 0; i < len(largeFileContent); i++ {
		largeFileContent[i] = 'a'
	}
	putFile(t, s3client, bucket, "file.txt", string(largeFileContent))

	cmd := s5cmd("head", "--humanize", fmt.Sprintf("s3://%v/file.txt", bucket))
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
	storageClassPattern := `(?:STANDARD|)`
	sizePattern := `(?:\d+(\.\d+)?[KMGTP]?B?)`
	s3urlPattern := `file.txt`

	expectedOutput := fmt.Sprintf(
		`%s\s+%s\s+%s\s+%s`,
		datePattern,
		storageClassPattern,
		sizePattern,
		s3urlPattern,
	)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, alignment(true))
}

// head object --etag

func TestHeadObjectEtag(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "file.txt", "content")

	cmd := s5cmd("head", "--etag", fmt.Sprintf("s3://%v/file.txt", bucket))
	result := icmd.RunCmd(cmd)
	result.Assert(t, icmd.Success)

	datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
	storageClassPattern := `(?:STANDARD|)`
	sizePattern := `\d+`
	s3urlPattern := `file.txt`
	etagPattern := `[a-f0-9]+`

	expectedOutput := fmt.Sprintf(
		`%s\s+%s\s+%s\s+%s\s+%s`,
		datePattern,
		storageClassPattern,
		etagPattern,
		sizePattern,
		s3urlPattern,
	)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, alignment(true))
}

// head object --show-fullpath

func TestHeadObjectShowFullpath(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	putFile(t, s3client, bucket, "file.txt", "content")

	cmd := s5cmd("head", "--show-fullpath", fmt.Sprintf("s3://%v/file.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	s3urlPattern := fmt.Sprintf(`s3://%s/file.txt`, bucket)
	expectedOutput := s3urlPattern

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	})
}

func TestHeadObjectShowFullpathNonExistent(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("head", "--show-fullpath", "s3://non-existent-bucket/non-existent-file.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
}

// head object --humanize --etag

func TestHeadObjectHumanizeEtag(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	largeFileContent := make([]byte, 1024*1024*50) // 50MB
	for i := 0; i < len(largeFileContent); i++ {
		largeFileContent[i] = 'a'
	}
	putFile(t, s3client, bucket, "file.txt", string(largeFileContent))

	cmd := s5cmd("head", "--humanize", "--etag", fmt.Sprintf("s3://%v/file.txt", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
	storageClassPattern := `(?:STANDARD|)`
	sizePattern := `(?:\d+(\.\d+)?[KMGTP]?B?)`
	s3urlPattern := `file.txt`
	etagPattern := `[a-f0-9]+`

	expectedOutput := fmt.Sprintf(
		`%s\s+%s\s+%s\s+%s\s+%s`,
		datePattern,
		storageClassPattern,
		etagPattern,
		sizePattern,
		s3urlPattern,
	)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, alignment(true))
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

	datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
	storageClassPattern := `(?:STANDARD|)`
	sizePattern := `\d+`
	s3urlPattern := `file\*.txt`

	expectedOutput := fmt.Sprintf(
		`%s\s+%s\s+%s\s+%s`,
		datePattern,
		storageClassPattern,
		sizePattern,
		s3urlPattern,
	)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, alignment(true))
}

// head object s3://bucket/file*.txt

func TestHeadObjectWildcardWithoutRawFlag(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("head", "s3://bucket/file*.txt")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
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

	etagPattern := `\"[^\"]+\"`
	lastModifiedPattern := `\"[^\"]+\"`
	typePattern := `\"[^\"]+\"`
	sizePattern := `\d+`
	storageClassPattern := `\"[^\"]+\"`
	metadataPattern := `\"key1\":\"value1\"`

	expectedOutput := fmt.Sprintf(
		`{"key":"s3://%v/file.txt","etag":%s,"last_modified":%s,"type":%s,"size":%s,"storage_class":%s,"metadata":{%s}}`,
		bucket,
		etagPattern,
		lastModifiedPattern,
		typePattern,
		sizePattern,
		storageClassPattern,
		metadataPattern,
	)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: match(expectedOutput),
	}, jsonCheck(true), strictLineCheck(false))
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

		datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
		storageClassPattern := `(?:STANDARD|)`
		sizePattern := len(contents[i])
		s3urlPattern := filename

		expectedOutput := fmt.Sprintf(
			`%s\s+%s\s+%d\s+%s`,
			datePattern,
			storageClassPattern,
			sizePattern,
			s3urlPattern,
		)

		assertLines(t, result.Stdout(), map[int]compareFunc{
			0: match(expectedOutput),
		}, alignment(true))

	}
}
