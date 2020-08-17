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
	// t.Parallel()

	testcases := []struct {
		name             string
		retry            int
		expectedError    error
		expectedExitCode int
	}{
		{
			name:             "retry_count_negative",
			retry:            -1,
			expectedError:    fmt.Errorf(`ERROR " ": retry count cannot be a negative value`),
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

func TestAppDashStat(t *testing.T) {
	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("--stat", "ls")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 0})

	out := result.Stdout()

	assert.Assert(t, strings.Contains(out, "Operation"))
	assert.Assert(t, strings.Contains(out, "Total"))
	assert.Assert(t, strings.Contains(out, "Error"))
}
