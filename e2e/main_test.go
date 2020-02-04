package e2e

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	cleanup := goBuildS5cmd()
	code := m.Run()
	cleanup()
	os.Exit(code)
}
