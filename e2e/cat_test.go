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

	t.Run("without prefix", func(t *testing.T) {
		t.Parallel()

		cmd := s5cmd("cat", fmt.Sprintf("s3://%v", bucket))
		result := icmd.RunCmd(cmd)

		result.Assert(t, icmd.Expected{ExitCode: 0})
		assertLines(t, result.Stdout(), nil)
	})

	t.Run("with prefix", func(t *testing.T) {
		t.Parallel()

		cmd := s5cmd("cat", fmt.Sprintf("s3://%v/", bucket))
		result := icmd.RunCmd(cmd)

		result.Assert(t, icmd.Expected{ExitCode: 0})
		assertLines(t, result.Stdout(), nil)
	})

	t.Run("with wildcard", func(t *testing.T) {
		t.Parallel()

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

	testCases := []struct {
		filesAndContents map[string]string
		prefix           string
		expected         string
	}{
		{
			filesAndContents: map[string]string{
				"file1.txt": "content0",
				"file2.txt": "content1",
			},
			expected: "content0content1",
		},
		{
			filesAndContents: map[string]string{
				"file1.txt":     "content0",
				"file2.txt":     "content1",
				"dir/file3.txt": "content2",
				"dir/file4.txt": "content3",
			},
			expected: "content0content1",
		},
		{
			filesAndContents: map[string]string{
				"file1.txt":     "content0",
				"file2.txt":     "content1",
				"dir/file3.txt": "content2",
				"dir/file4.txt": "content3",
			},
			prefix:   "dir/",
			expected: "content2content3",
		},
		{
			filesAndContents: map[string]string{
				"file1.txt":               "content0",
				"file2.txt":               "content1",
				"dir/file3.txt":           "content2",
				"dir/file4.txt":           "content3",
				"dir/nesteddir/file5.txt": "content4",
			},
			prefix:   "dir/",
			expected: "content2content3",
		},
		{
			filesAndContents: map[string]string{
				"file1.txt":               "content0",
				"file2.txt":               "content1",
				"dir/file3.txt":           "content2",
				"dir/file4.txt":           "content3",
				"dir/nesteddir/file5.txt": "content4",
			},
			prefix:   "dir/nesteddir/",
			expected: "content4",
		},
	}

	for idx, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("case-%v", idx), func(t *testing.T) {
			t.Parallel()

			s3client, s5cmd := setup(t)

			bucket := s3BucketFromTestName(t)
			createBucket(t, s3client, bucket)

			for file, content := range tc.filesAndContents {
				putFile(t, s3client, bucket, file, content)
			}

			cmd := s5cmd("cat", fmt.Sprintf("s3://%v/%v", bucket, tc.prefix))
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			assertLines(t, result.Stdout(), map[int]compareFunc{
				0: equals(tc.expected),
			}, alignment(true))
		})
	}
}

func TestCatWildcard(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)
	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	fileAndContent := map[string]string{
		"foo1.txt":             "content0",
		"foo2.txt":             "content1",
		"bar1.txt":             "content2",
		"foolder/foo3.txt":     "content3",
		"log-file-2024-01.txt": "content4",
		"log-file-2024-02.txt": "content5",
		"log-file-2023-01.txt": "content6",
		"log-file-2022-01.txt": "content7",
	}

	for file, content := range fileAndContent {
		putFile(t, s3client, bucket, file, content)
	}

	testCases := []struct {
		name       string
		expression string
		expected   string
	}{
		{
			name:       "wildcard matching with both file and folder",
			expression: "foo*",
			expected:   "content0content1content3",
		},
		{
			name:       "log files 2024",
			expression: "log-file-2024-*",
			expected:   "content4content5",
		},
		{
			name:       "all log files",
			expression: "log-file-*",
			expected:   "content7content6content4content5",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run("", func(t *testing.T) {
			t.Parallel()

			cmd := s5cmd("cat", fmt.Sprintf("s3://%v/%v", bucket, tc.expression))
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			assertLines(t, result.Stdout(), map[int]compareFunc{
				0: equals(tc.expected),
			}, alignment(true))
		})
	}
}

func TestPrefixWildcardFail(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		expression string
	}{
		{
			name:       "wildcard",
			expression: "foo*",
		},
		{
			name:       "prefix",
			expression: "foolder/",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Run("default", func(t *testing.T) {
				t.Parallel()

				s3client, s5cmd := setup(t)

				bucket := s3BucketFromTestName(t)
				createBucket(t, s3client, bucket)

				cmd := s5cmd("cat", fmt.Sprintf("s3://%v/%v", bucket, tc.expression))
				result := icmd.RunCmd(cmd)

				result.Assert(t, icmd.Expected{ExitCode: 1})
				assertLines(t, result.Stderr(), map[int]compareFunc{
					0: equals(`ERROR "cat s3://%v/%v": no object found`, bucket, tc.expression),
				}, strictLineCheck(false))
			})
			t.Run("json", func(t *testing.T) {
				t.Parallel()
				s3client, s5cmd := setup(t)

				bucket := s3BucketFromTestName(t)
				createBucket(t, s3client, bucket)

				cmd := s5cmd("--json", "cat", fmt.Sprintf("s3://%v/%v", bucket, tc.expression))
				result := icmd.RunCmd(cmd)

				result.Assert(t, icmd.Expected{ExitCode: 1})
				assertLines(t, result.Stderr(), map[int]compareFunc{
					0: equals(`{"operation":"cat","command":"cat s3://%v/%v","error":"no object found"}`, bucket, tc.expression),
				}, strictLineCheck(false))
			})
		})
	}
}
