package e2e

import (
	"testing"

	"gotest.tools/v3/icmd"
)

func TestBangRunEcho(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

func TestSuccessfulBangBang(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("!", "echo", "foo", "&&", "!", "echo SUCCESS", "||", "!", "echo FAIL")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 0})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "! echo foo"`),
		1: suffix(`+OK "! echo SUCCESS"`),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`foo`),
		1: suffix(`SUCCESS`),
	})
}

func TestFailedBangBang(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("mv", "s3://nosuchbucket/nosuchkey", ".", "&&", "!", "echo SUCCESS", "||", "!", "echo FAIL")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 127})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: match(` -ERR "mv s3://nosuchbucket/nosuchkey ./nosuchkey": NoSuchBucket: The specified bucket does not exist status code: 404`),
		1: suffix(`+OK "! echo FAIL"`),
	})

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(` # Downloading nosuchkey...`),
		1: suffix(`FAIL`),
	})
}
