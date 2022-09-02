package e2e

import (
	"fmt"
	"strings"
	"testing"

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
				0: match(`{"operation":"cat","command":"cat s3:\/\/(.*)\/prefix\/file\.txt\/\*","error":"remote source \\"s3:\/\/(.*)\/prefix\/file\.txt\/\*\\" can not contain glob characters"}`),
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
				0: match(`ERROR "cat s3://(.*)": remote source must be an object`),
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
