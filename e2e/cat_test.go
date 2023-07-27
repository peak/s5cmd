package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/icmd"
)

func TestCatS3Object(t *testing.T) {
	t.Parallel()

	const (
		filename = "file.txt"
	)
	contents, expected := getSequentialFileContent()

	testcases := []struct {
		name      string
		cmd       []string
		expected  map[int]compareFunc
		assertOps []assertOp
	}{
		{
			name: "cat remote object",
			cmd: []string{
				"cat",
			},
			expected: expected,
		},
		{
			name: "cat remote object with json flag",
			cmd: []string{
				"--json",
				"cat",
			},
			expected: expected,
			assertOps: []assertOp{
				jsonCheck(true),
			},
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s3client, s5cmd := setup(t)

			bucket := s3BucketFromTestName(t)

			createBucket(t, s3client, bucket)
			putFile(t, s3client, bucket, filename, contents)

			src := fmt.Sprintf("s3://%v/%v", bucket, filename)
			tc.cmd = append(tc.cmd, src)

			cmd := s5cmd(tc.cmd...)
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)

			assertLines(t, result.Stdout(), tc.expected)
		})
	}

}

func TestCatS3ObjectFail(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		src       string
		name      string
		cmd       []string
		expected  map[int]compareFunc
		assertOps []assertOp
	}{
		{
			src:  "s3://%v/prefix/file.txt",
			name: "cat non existent remote object",
			cmd: []string{
				"cat",
			},
			expected: map[int]compareFunc{
				0: match(`ERROR "cat s3://(.*)/prefix/file\.txt": NoSuchKey:`),
			},
		},
		{
			src:  "s3://%v/prefix/file.txt",
			name: "cat non existent remote object with json flag",
			cmd: []string{
				"--json",
				"cat",
			},
			expected: map[int]compareFunc{
				0: match(`{"operation":"cat","command":"cat s3:\/\/(.*)\/prefix\/file\.txt","error":"NoSuchKey:`),
			},
			assertOps: []assertOp{
				jsonCheck(true),
			},
		},
		{
			src:  "s3://%v/prefix/file.txt/*",
			name: "cat remote object with glob",
			cmd: []string{
				"--json",
				"cat",
			},
			expected: map[int]compareFunc{
				0: match(`{"operation":"cat","command":"cat s3:\/\/(.+)?\/prefix\/file\.txt\/\*","error":"remote source \\"s3:\/\/(.*)\/prefix\/file\.txt\/\*\\" can not contain glob characters"}`),
			},
			assertOps: []assertOp{
				jsonCheck(true),
			},
		},
		{
			src:  "s3://%v/prefix/",
			name: "cat bucket",
			cmd: []string{
				"cat",
			},
			expected: map[int]compareFunc{
				0: match(`ERROR "cat s3://(.+)?": remote source must be an object`),
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s3client, s5cmd := setup(t)

			bucket := s3BucketFromTestName(t)
			createBucket(t, s3client, bucket)

			tc.cmd = append(tc.cmd, fmt.Sprintf(tc.src, bucket))
			cmd := s5cmd(tc.cmd...)

			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Expected{ExitCode: 1})
			assertLines(t, result.Stderr(), tc.expected, tc.assertOps...)
		})
	}
}

func TestCatLocalFileFail(t *testing.T) {
	t.Parallel()

	const (
		filename = "file.txt"
	)

	testcases := []struct {
		name     string
		cmd      []string
		expected map[int]compareFunc
	}{
		{
			name: "cat local file",
			cmd: []string{
				"cat",
				filename,
			},
			expected: map[int]compareFunc{
				0: contains(`ERROR "cat file.txt": source must be a remote object`),
			},
		},
		{
			name: "cat local file with json flag",
			cmd: []string{
				"--json",
				"cat",
				filename,
			},
			expected: map[int]compareFunc{
				0: contains(`{"operation":"cat","command":"cat file.txt","error":"source must be a remote object"}`),
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, s5cmd := setup(t)

			cmd := s5cmd(tc.cmd...)
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Expected{ExitCode: 1})

			assertLines(t, result.Stderr(), tc.expected)
		})
	}
}

// getSequentialFileContent creates a string with 64666688 in size (~61.670 MB)
func getSequentialFileContent() (string, map[int]compareFunc) {
	sb := strings.Builder{}
	expectedLines := make(map[int]compareFunc)

	for i := 0; i < 50000; i++ {
		line := fmt.Sprintf(`{ "line": "%d", "id": "i%d", data: "some event %d" }`, i, i, i)
		sb.WriteString(line)
		sb.WriteString("\n")

		expectedLines[i] = equals(line)
	}

	return sb.String(), expectedLines
}

func TestCatByVersionID(t *testing.T) {
	skipTestIfGCS(t, "versioning is not supported in GCS")

	t.Parallel()

	bucket := s3BucketFromTestName(t)

	// versioninng is only supported with in memory backend!
	s3client, s5cmd := setup(t, withS3Backend("mem"))

	const filename = "testfile.txt"

	var contents = []string{
		"This is first content",
		"Second content it is, and it is a bit longer!!!",
	}

	// create a bucket and Enable versioning
	createBucket(t, s3client, bucket)
	setBucketVersioning(t, s3client, bucket, "Enabled")

	// upload two versions of the file with same key
	putFile(t, s3client, bucket, filename, contents[0])
	putFile(t, s3client, bucket, filename, contents[1])

	//  get content of the latest
	cmd := s5cmd("cat", "s3://"+bucket+"/"+filename)
	result := icmd.RunCmd(cmd)

	assert.Assert(t, result.Stdout() == contents[1])

	if diff := cmp.Diff(contents[1], result.Stdout()); diff != "" {
		t.Errorf("(-want +got):\n%v", diff)
	}

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
		cmd = s5cmd("cat", "--version-id", version,
			fmt.Sprintf("s3://%v/%v", bucket, filename))
		result = icmd.RunCmd(cmd)

		if diff := cmp.Diff(contents[i], result.Stdout()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
	}
}
