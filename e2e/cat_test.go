package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestCatS3Object(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
				0: contains(`ERROR "cat s3://bucket": remote source must be an object`),
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

	for i := 0; i < 50000; i++ {
		line := fmt.Sprintf(`{ "line": "%d", "id": "i%d", data: "some event %d" }`, i, i, i)
		sb.WriteString(line)
		sb.WriteString("\n")

		expectedLines[i] = equals(line)
	}

	return sb.String(), expectedLines
}

func TestCatByVersionID(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	// versioninng is only supported with in memory backend!
	s3client, s5cmd, cleanup := setup(t, withS3Backend("mem"))
	defer cleanup()

	const (
		filename = "testfile.txt"
	)
	var (
		contents = []string{
			"Sen\nSen esirliğim ve hürriyetimsin,",
			"Sen büyük, güzel ve muzaffer\nve ulaşıldıkça ulaşılmaz olan hasretimsin...",
		}
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename+"1", contents[0]), fs.WithFile(filename+"2", contents[1]))
	defer workdir.Remove()

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
