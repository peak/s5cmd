package e2e

import (
	"fmt"
	"strings"
	"testing"

	"gotest.tools/v3/icmd"
)

func TestCatS3Object(t *testing.T) {
	const (
		bucket   = "bucket"
		filename = "file.txt"
	)

	src := fmt.Sprintf("s3://%v/%v", bucket, filename)
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
				src,
			},
			expected: expected,
		},
		{
			name: "cat remote object with json flag",
			cmd: []string{
				"--json",
				"cat",
				src,
			},
			expected: expected,
			assertOps: []assertOp{
				jsonCheck(true),
			},
		},
		{
			// generated file is ~61.670 MB, run with lower part size to assert sequential writes
			name: "cat remote object with part-size set to 1MB",
			cmd: []string{
				"cat",
				"-part-size",
				"1",
				src,
			},
			expected: expected,
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s3client, s5cmd, cleanup := setup(t)
			defer cleanup()

			createBucket(t, s3client, bucket)
			putFile(t, s3client, bucket, filename, contents)

			cmd := s5cmd(tc.cmd...)
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)

			assertLines(t, result.Stdout(), tc.expected)
		})
	}

}

func TestCatS3ObjectFail(t *testing.T) {
	const (
		bucket   = "bucket"
		filename = "file.txt"
	)

	bucketSrc := fmt.Sprintf("s3://%v", bucket)
	prefixSrc := fmt.Sprintf("%v/prefix", bucketSrc)
	src := fmt.Sprintf("%s/%v", prefixSrc, filename)

	testcases := []struct {
		name      string
		cmd       []string
		expected  map[int]compareFunc
		assertOps []assertOp
	}{
		{
			name: "cat non existent remote object",
			cmd: []string{
				"cat",
				src,
			},
			expected: map[int]compareFunc{
				0: contains(`ERROR "cat s3://bucket/prefix/file.txt": NoSuchKey: status code: 404`),
				1: equals(""),
			},
		},
		{
			name: "cat non existent remote object with json flag",
			cmd: []string{
				"--json",
				"cat",
				src,
			},
			expected: map[int]compareFunc{
				0: contains(`{"operation":"cat","command":"cat s3://bucket/prefix/file.txt","error":"NoSuchKey: status code: 404,`),
				1: equals(""),
			},
			assertOps: []assertOp{
				jsonCheck(true),
			},
		},
		{
			name: "cat remote object with glob",
			cmd: []string{
				"--json",
				"cat",
				src + "/*",
			},
			expected: map[int]compareFunc{
				0: equals(`{"operation":"cat","command":"cat s3://bucket/prefix/file.txt/*","error":"remote source \"s3://bucket/prefix/file.txt/*\" can not contain glob characters"}`),
				1: equals(""),
			},
			assertOps: []assertOp{
				jsonCheck(true),
			},
		},
		{
			name: "cat bucket",
			cmd: []string{
				"cat",
				bucketSrc,
			},
			expected: map[int]compareFunc{
				0: contains(`ERROR "cat s3://bucket": remote source must an object`),
				1: equals(""),
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s3client, s5cmd, cleanup := setup(t)
			defer cleanup()

			createBucket(t, s3client, bucket)

			cmd := s5cmd(tc.cmd...)
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Expected{ExitCode: 1})
			assertLines(t, result.Stderr(), tc.expected, tc.assertOps...)
		})
	}
}

func TestCatLocalFileFail(t *testing.T) {
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
				1: equals(""),
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
				1: equals(""),
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, s5cmd, cleanup := setup(t)
			defer cleanup()

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

	for i := 1; i <= 1000000; i++ {
		line := fmt.Sprintf(`{ "line": "%d", "id": "i%d", data: "some event %d" }`, i, i, i)
		sb.WriteString(line)
		sb.WriteString("\n")

		expectedLines[i-1] = equals(line)
		expectedLines[i] = equals("")
	}

	return sb.String(), expectedLines
}
