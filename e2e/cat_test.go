package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/icmd"
)

const (
	kb int64 = 1024
	mb       = kb * kb
)

func TestCatS3Object(t *testing.T) {
	t.Parallel()

	const (
		filename = "file.txt"
	)
	contents, expected := getSequentialFileContent(4 * mb)

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
		{
			name: "cat remote object with lower part size and higher concurrency",
			cmd: []string{
				"cat",
				"-p",
				"1",
				"-c",
				"2",
			},
			expected: expected,
		},
		{
			name: "cat remote object with json flag lower part size and higher concurrency",
			cmd: []string{
				"--json",
				"cat",
				"-p",
				"1",
				"-c",
				"2",
			}, expected: expected,
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
				0: match(`ERROR "cat s3://(.*)/prefix/file\.txt":(.*) not found`),
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
				0: match(`{"operation":"cat","command":"cat s3:\/\/(.*)\/prefix\/file\.txt","error":"(.*) not found`),
			},
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

func TestCatInEmptyBucket(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	t.Run("EmptyBucket", func(t *testing.T) {
		cmd := s5cmd("cat", fmt.Sprintf("s3://%v", bucket))
		result := icmd.RunCmd(cmd)

		result.Assert(t, icmd.Expected{ExitCode: 0})
		assertLines(t, result.Stdout(), nil)
	})

	t.Run("PrefixInEmptyBucket", func(t *testing.T) {
		cmd := s5cmd("cat", fmt.Sprintf("s3://%v/", bucket))
		result := icmd.RunCmd(cmd)

		result.Assert(t, icmd.Expected{ExitCode: 0})
		assertLines(t, result.Stdout(), nil)
	})

	t.Run("WildcardInEmptyBucket", func(t *testing.T) {
		cmd := s5cmd("cat", fmt.Sprintf("s3://%v/*", bucket))
		result := icmd.RunCmd(cmd)

		result.Assert(t, icmd.Expected{ExitCode: 1})
		assertLines(t, result.Stderr(), map[int]compareFunc{
			0: contains(fmt.Sprintf(`ERROR "cat s3://%v/*": no object found`, bucket)),
		})
	})
}

// getSequentialFileContent creates a string with size bytes in size.
func getSequentialFileContent(size int64) (string, map[int]compareFunc) {
	sb := strings.Builder{}
	expectedLines := make(map[int]compareFunc)
	totalBytesWritten := int64(0)
	for i := 0; totalBytesWritten < size; i++ {
		line := fmt.Sprintf(`{ "line": "%d", "id": "i%d", data: "some event %d" }`, i, i, i)
		sb.WriteString(line)
		sb.WriteString("\n")
		totalBytesWritten += int64(len(line))
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

	version := "1"

	// wildcard and prefix fail cases
	cmd = s5cmd("cat", "--version-id", version, "s3://"+bucket+"/")
	result = icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "cat --version-id=%v s3://%v/": wildcard/prefix operations are disabled with --version-id flag`, version, bucket),
	}, strictLineCheck(false))

	cmd = s5cmd("cat", "--version-id", version, "s3://"+bucket+"/folder/")
	result = icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "cat --version-id=%v s3://%v/folder/": wildcard/prefix operations are disabled with --version-id flag`, version, bucket),
	}, strictLineCheck(false))

	cmd = s5cmd("cat", "--version-id", version, "s3://"+bucket+"/*")
	result = icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})
	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "cat --version-id=%v s3://%v/*": wildcard/prefix operations are disabled with --version-id flag`, version, bucket),
	}, strictLineCheck(false))
}

func TestCatPrefix(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)
	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	testCases := []struct {
		files    []string
		prefix   string
		expected string
	}{
		{files: []string{"file1.txt", "file2.txt"}, prefix: "", expected: "content0content1"},
		{files: []string{"dir/file3.txt", "dir/file4.txt"}, prefix: "", expected: "content0content1"},
		{files: nil, prefix: "dir/", expected: "content2content3"},
		{files: []string{"dir/nesteddir/file5.txt"}, prefix: "dir/", expected: "content2content3"},
		{files: nil, prefix: "dir/nesteddir/", expected: "content4"},
	}

	offset := 0
	for _, tc := range testCases {
		if tc.files != nil {
			var concatenatedContent strings.Builder
			for idx, file := range tc.files {
				content := fmt.Sprintf("content%d", idx+offset)
				putFile(t, s3client, bucket, file, content)
				concatenatedContent.WriteString(content)
			}
			offset += len(tc.files)
		}
		verifyCatCommand(t, s5cmd, bucket, tc.expected, tc.prefix)
	}
}

func TestCatWildcard(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)
	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	files := []struct {
		key     string
		content string
	}{
		{"foo1.txt", "content0"},
		{"foo2.txt", "content1"},
		{"bar1.txt", "content2"},
		{"foolder/foo3.txt", "content3"},
		{"log-file-2024-01.txt", "content4"},
		{"log-file-2024-02.txt", "content5"},
		{"log-file-2023-01.txt", "content6"},
		{"log-file-2022-01.txt", "content7"},
	}

	for _, file := range files {
		putFile(t, s3client, bucket, file.key, file.content)
	}

	testCases := []struct {
		prefix   string
		expected string
	}{
		{"foo*", "content0content1content3"},
		{"log-file-2024-*", "content4content5"},
		{"log-file-*", "content7content6content4content5"},
	}

	for _, tc := range testCases {
		verifyCatCommand(t, s5cmd, bucket, tc.expected, tc.prefix)
	}
}

func TestPrefixWildcardFail(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)
	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	testCases := []struct {
		prefix string
	}{
		{"foo*"},
		{"foolder/"},
	}

	for _, tc := range testCases {
		cmd := s5cmd("cat", fmt.Sprintf("s3://%v/%v", bucket, tc.prefix))
		result := icmd.RunCmd(cmd)

		result.Assert(t, icmd.Expected{ExitCode: 1})
		assertLines(t, result.Stderr(), map[int]compareFunc{
			0: equals(`ERROR "cat s3://%v/%v": no object found`, bucket, tc.prefix),
		}, strictLineCheck(false))
	}

	for _, tc := range testCases {
		cmd := s5cmd("--json", "cat", fmt.Sprintf("s3://%v/%v", bucket, tc.prefix))
		result := icmd.RunCmd(cmd)

		result.Assert(t, icmd.Expected{ExitCode: 1})
		assertLines(t, result.Stderr(), map[int]compareFunc{
			0: equals(`{"operation":"cat","command":"cat s3://%v/%v","error":"no object found"}`, bucket, tc.prefix),
		}, strictLineCheck(false))
	}

}

func verifyCatCommand(t *testing.T, s5cmd func(...string) icmd.Cmd, bucket, expectedContent, prefix string) {
	cmd := s5cmd("cat", fmt.Sprintf("s3://%v/%v", bucket, prefix))
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)
	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(expectedContent),
	}, alignment(true))
}
