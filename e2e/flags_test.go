package e2e

import (
	"testing"

	"gotest.tools/v3/icmd"
)

func TestFlagVersion(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	cmd := s5cmd("-version")
	result := icmd.RunCmd(cmd)

	// make sure that -version flag works as expected:
	// https://github.com/peak/s5cmd/issues/70#issuecomment-592218542
	result.Assert(t, icmd.Success)
}
