package e2e

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
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

// Checks if the stats are written at the end of each log level output.
func TestAppDashStat(t *testing.T) {
	t.Parallel()

	const (
		bucket      = "bucket"
		fileContent = "this is a file content"
		dst         = "."
		src         = "file1.txt"
	)

	testcases := []struct {
		name  string
		level string
	}{
		{
			name:  "--stat --log trace cp s3://bucket/object .",
			level: "trace",
		},
		{
			name:  "--stat --log debug cp s3://bucket/object .",
			level: "debug",
		},
		{
			name:  "--stat --log info cp s3://bucket/object .",
			level: "info",
		},
		{
			name:  "--stat --log error cp s3://bucket/object .",
			level: "error",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s3client, s5cmd, cleanup := setup(t)
			defer cleanup()

			createBucket(t, s3client, bucket)

			putFile(t, s3client, bucket, src, fileContent)

			srcPath := fmt.Sprintf("s3://%v/%v", bucket, src)
			cmd := s5cmd("--stat", "--log", tc.level, "cp", srcPath, dst)
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)

			out := result.Stdout()
			tsv := fmt.Sprintf("%s\t%s\t%s\t%s\t", "Operation", "Total", "Error", "Success")

			assert.Assert(t, strings.Contains(out, tsv))

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
			httpProxy, cleanupProxy := proxyFake()
			defer cleanupProxy()
			os.Setenv("http_proxy", httpProxy)
			_, s5cmd, cleanup := setup(t, withFakeProxy())
			defer cleanup()
			var cmd icmd.Cmd
			if tc.flag != "" {
				cmd = s5cmd(tc.flag, "--log", "trace", "ls")
			} else {
				cmd = s5cmd("--log", "trace", "ls")
			}
			result := icmd.RunCmd(cmd)
			result.Assert(t, icmd.Success)
		})
	}
	assert.Assert(t, successfulRequests(len(testcases)))
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
