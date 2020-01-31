package e2e

import (
	"testing"

	"gotest.tools/v3/icmd"
)

func TestBangRunEcho(t *testing.T) {
	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("!", "echo 'foo'")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` +OK "! echo 'foo'"`),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("'foo'"),
	})
}

func TestBangCommandNotFound(t *testing.T) {
	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("!", "there-is-no-command-like-this")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 127})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(` -ERR "! there-is-no-command-like-this": exec: "there-is-no-command-like-this": executable file not found in $PATH`),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{})
}
