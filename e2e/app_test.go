package e2e

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestAppRetryCount(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name             string
		retry            int
		expectedError    error
		expectedExitCode int
	}{
		{
			name:             "retry_count_negative",
			retry:            -1,
			expectedError:    fmt.Errorf(`ERROR retry count cannot be a negative value`),
			expectedExitCode: 1,
		},
		{
			name:             "retry_count_zero",
			retry:            0,
			expectedError:    nil,
			expectedExitCode: 0,
		},
		{
			name:             "retry_count_positive",
			retry:            20,
			expectedError:    nil,
			expectedExitCode: 0,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, s5cmd, cleanup := setup(t)
			defer cleanup()

			cmd := s5cmd("-r", strconv.Itoa(tc.retry))
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Expected{ExitCode: tc.expectedExitCode})

			if tc.expectedError == nil {
				if result.Stderr() != "" {
					t.Fatalf("expected no error, got: %q", result.Stderr())
				}
				return
			}

			if result.Stderr() == "" {
				t.Fatalf("expected error %q, got none", tc.expectedError)
			}
			assertLines(t, result.Stderr(), map[int]compareFunc{
				0: equals("%v", tc.expectedError),
			})
		})
	}
}

// Checks if the stats are written in necessary conditions.
// 1. Print with every log level when there is an operation
// 2. Do not print when used with help & version commands.
func TestAppDashStat(t *testing.T) {
	t.Parallel()

	const (
		bucket                  = "bucket"
		fileContent             = "this is a file content"
		src                     = "file1.txt"
		expectedOutputIfPrinted = "Operation\tTotal\tError\tSuccess\t"
	)

	var testcases = []struct {
		command         string
		isPrintExpected bool
	}{
		{
			command:         fmt.Sprintf("--stat --log %v cp s3://bucket/%v .", "trace", src),
			isPrintExpected: true,
		},
		{
			command:         fmt.Sprintf("--stat --log %v cp s3://bucket/%v .", "debug", src),
			isPrintExpected: true,
		},
		{
			command:         fmt.Sprintf("--stat --log %v cp s3://bucket/%v .", "info", src),
			isPrintExpected: true,
		},
		{
			command:         fmt.Sprintf("--stat --log %v cp s3://bucket/%v .", "error", src),
			isPrintExpected: true,
		},
		// if level is an empty string, it ignores log levels and uses default.
		{
			command:         "--stat help",
			isPrintExpected: false,
		},
		{
			command:         "--stat version",
			isPrintExpected: false,
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.command, func(t *testing.T) {
			t.Parallel()
			s3client, s5cmd, cleanup := setup(t)
			defer cleanup()

			createBucket(t, s3client, bucket)
			putFile(t, s3client, bucket, src, fileContent)
			cmd := s5cmd(strings.Fields(tc.command)...)

			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			out := result.Stdout()
			assert.Assert(t, tc.isPrintExpected == strings.Contains(out, expectedOutputIfPrinted))
		})
	}
}

func TestAppProxy(t *testing.T) {

	testcases := []struct {
		name string
		flag string
	}{
		{
			name: "without no-verify-ssl flag",
			flag: "",
		},
		{
			name: "with no-verify-ssl flag",
			flag: "--no-verify-ssl",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			const expectedReqs = 1

			proxy := httpProxy{}
			pxyUrl, cleanup := setupProxy(&proxy)
			defer cleanup()

			os.Setenv("http_proxy", pxyUrl)

			_, s5cmd, cleanup := setup(t, withProxy())
			defer cleanup()

			var cmd icmd.Cmd
			if tc.flag != "" {
				cmd = s5cmd(tc.flag, "ls")
			} else {
				cmd = s5cmd("ls")
			}

			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			assert.Assert(t, proxy.isSuccessful(expectedReqs))
		})
	}
}

func TestAppUnknownCommand(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("unknown-command")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "unknown-command": command not found`),
	})
}

func TestUsageError(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("--recursive", "ls")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stdout(), map[int]compareFunc{})
	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals("Incorrect Usage: flag provided but not defined: -recursive"),
		1: equals("See 's5cmd --help' for usage"),
	})
}

func TestInvalidLoglevel(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("--log", "notexist", "ls")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`Incorrect Usage: invalid value "notexist" for flag -log: allowed values: [trace, debug, info, error]`),
		1: equals("See 's5cmd --help' for usage"),
	})
}

func TestCompletionFlag(t *testing.T) {
	flag := "--generate-bash-completion"
	bucket := s3BucketFromTestName(t)

	testcases := []struct {
		name          string
		precedingArgs []string
		arg           string
		remoteFiles   []string
		expected      []string
		shell         string
	}{
		{
			name:          "cp complete empty string",
			precedingArgs: []string{"cp"},
			arg:           "",
			expected:      []string{},
			shell:         "/bin/bash",
		},
		{
			name:          "cp complete bucket names",
			precedingArgs: []string{"cp"},
			arg:           "s3://",
			expected:      []string{"s3://" + bucket + "/"},
			shell:         "/bin/pwsh",
		},
		{
			name:          "cp complete bucket keys pwsh",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/",
			remoteFiles: []string{
				"file0.txt",
				"file1.txt",
				"filedir/child.txt",
				"dir/child.txt",
			},
			expected: keysToS3URL("s3://", bucket,
				"file0.txt",
				"file1.txt",
				"filedir/",
				"dir/",
			),
			shell: "/bin/pwsh",
		}, {
			name:          "cp complete keys with colon",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/co:lo",
			remoteFiles: []string{
				"co:lon:in:key",
				"co:lonized",
			},
			expected: append(
				keysToS3URL("s3://", bucket, "co:lon:in:key", "co:lonized"),
				"lon:in:key", "lonized"),
			shell: "/bin/bash",
		}, {
			name:          "cp complete keys with asterisk",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/as*",
			remoteFiles: []string{
				"as*terisk",
				"as*oburiks",
			},
			expected: keysToS3URL("s3://", bucket, "as*terisk", "as*oburiks"),
			shell:    "/bin/pwsh",
		},
		/* Question marks are thought to be wildcard so they cannot be properly handled yet
		{
			name:          "cp complete keys with question mark",
			precedingArgs: []string{"cp", "--raw"},
			arg:           "s3://" + bucket + "/qu?",
			remoteFiles: []string{
				"qu?estion",
				"qu?vestion",
			},
			expected: keysToS3URL("s3://", bucket,
				"qu?estion", "qu?vestion"),
			shell: "/bin/pwsh",
		},
		*/
		{
			name:          "cp complete keys with backslash",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/back\\",
			remoteFiles: []string{
				`back\slash`,
				`backback`,
			},
			expected: keysToS3URL("s3://", bucket,
				`back\slash`),
			shell: "/bin/pwsh",
		},
	}

	workdir := fs.NewDir(t, "completionTest",
		fs.WithFiles(
			map[string]string{
				"dif":   "content",
				"root1": "content",
				"root2": "content",
			},
		),
		fs.WithDir("dir", fs.WithFiles(
			map[string]string{
				"root1": "content",
				"root2": "content",
			},
		)),
	)

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			os.Setenv("SHELL", tc.shell)

			s3client, s5cmd, cleanup := setup(t)
			defer cleanup()

			// prepare remote bucket content
			createBucket(t, s3client, bucket)

			for _, f := range tc.remoteFiles {
				putFile(t, s3client, bucket, f, "content")
			}

			cmd := s5cmd(append(tc.precedingArgs, tc.arg, flag)...)
			result := icmd.RunCmd(cmd, withWorkingDir(workdir), withEnv("SHELL", tc.shell))
			fmt.Println(tc.name, "§", os.Getenv("SHELL"), "$", result.Stdout(), "$")

			assertLines(t, result.Stdout(), expectedSliceToEqualsMap(tc.expected, true), sortInput(true))
		})
	}
}
