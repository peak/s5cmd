package e2e

import (
	"fmt"
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
