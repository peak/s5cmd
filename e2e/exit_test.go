package e2e

import (
	"strconv"
	"testing"

	"gotest.tools/v3/icmd"
)

func TestExit(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	for _, code := range []int{0, 1, 2, 127} {
		cmd := s5cmd("exit", strconv.Itoa(code))
		result := icmd.RunCmd(cmd)

		result.Assert(t, icmd.Expected{ExitCode: code})

		assertLines(t, result.Stderr(), map[int]compareFunc{
			0: suffix(` +OK "exit %v"`, code),
		})

		assertLines(t, result.Stdout(), map[int]compareFunc{})
	}
}
