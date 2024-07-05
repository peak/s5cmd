package e2e

import (
	"fmt"
	"regexp"
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

	result.Assert(t, icmd.Expected{ExitCode: 0, Out: bucket})
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

	result.Assert(t, icmd.Expected{ExitCode: 0, Out: fmt.Sprintf(`{"created_at":"%v","name":"%v"}`, "0001-01-01T00:00:00Z", bucket)})
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

	result.Assert(t, icmd.Expected{ExitCode: 0, Out: bucket})
}

// head bucket --etag
func TestHeadBucketEtag(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("head", "--etag", fmt.Sprintf("s3://%v", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 0, Out: bucket})
}

// head bucket --storage-class

func TestHeadBucketStorageClass(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	cmd := s5cmd("head", "--storage-class", fmt.Sprintf("s3://%v", bucket))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 0, Out: bucket})
}

// head object

func TestHeadObjectOutput(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "myfile.txt", "content")

	cmd := s5cmd("head", fmt.Sprintf("s3://%v/myfile.txt", bucket))
	result := icmd.RunCmd(cmd)

	datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
	storageClassPattern := `(?:STANDART|)`
	sizePattern := `\d+`
	s3urlPattern := `myfile.txt`

	expectedOutput := fmt.Sprintf(
		`%s\s+%s\s+%s\s+%s`,
		datePattern,
		storageClassPattern,
		sizePattern,
		s3urlPattern,
	)

	match, err := regexp.MatchString(expectedOutput, result.Combined())
	if err != nil {
		t.Fatalf("regex match failed: %v", err)
	}

	if !match {
		t.Errorf("expected output to match:\n%s\nbut got:\n%s", expectedOutput, result.Combined())
	}

	result.Assert(t, icmd.Expected{ExitCode: 0})
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

	etagPattern := `\"[^\"]+\"`
	lastModifiedPattern := `\"[^\"]+\"`
	typePattern := `\"[^\"]+\"`
	sizePattern := `\d+`
	storageClassPattern := `\"[^\"]+\"`

	expectedOutput := fmt.Sprintf(
		`{"key":"s3://%v/file.txt","etag":%s,"last_modified":%s,"type":%s,"size":%s,"storage_class":%s}`,
		bucket,
		etagPattern,
		lastModifiedPattern,
		typePattern,
		sizePattern,
		storageClassPattern,
	)

	match, _ := regexp.MatchString(expectedOutput, result.Combined())

	if !match {
		t.Errorf("expected output to match:\n%s\nbut got:\n%s", expectedOutput, result.Combined())
	}

	result.Assert(t, icmd.Expected{ExitCode: 0})
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

	datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
	storageClassPattern := `(?:STANDART|)`
	sizePattern := `(?:\d+(\.\d+)?[KMGTP]?B?)`
	s3urlPattern := `file.txt`

	expectedOutput := fmt.Sprintf(
		`%s\s+%s\s+%s\s+%s`,
		datePattern,
		storageClassPattern,
		sizePattern,
		s3urlPattern,
	)

	match, _ := regexp.MatchString(expectedOutput, result.Combined())

	if !match {
		t.Errorf("expected output to match:\n%s\nbut got:\n%s", expectedOutput, result.Combined())
	}

	result.Assert(t, icmd.Expected{ExitCode: 0})

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

	datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
	storageClassPattern := `(?:STANDART|)`
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

	match, _ := regexp.MatchString(expectedOutput, result.Combined())

	if !match {
		t.Errorf("expected output to match:\n%s\nbut got:\n%s", expectedOutput, result.Combined())
	}

	result.Assert(t, icmd.Expected{ExitCode: 0})
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

	s3urlPattern := fmt.Sprintf(`s3://%s/file.txt`, bucket)

	expectedOutput := s3urlPattern

	match, _ := regexp.MatchString(expectedOutput, result.Combined())

	if !match {
		t.Errorf("expected output to match:\n%s\nbut got:\n%s", expectedOutput, result.Combined())
	}

	result.Assert(t, icmd.Expected{ExitCode: 0})
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

	datePattern := `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`
	storageClassPattern := `(?:STANDART|)`
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

	match, _ := regexp.MatchString(expectedOutput, result.Combined())

	if !match {
		t.Errorf("expected output to match:\n%s\nbut got:\n%s", expectedOutput, result.Combined())
	}

	result.Assert(t, icmd.Expected{ExitCode: 0})
}
