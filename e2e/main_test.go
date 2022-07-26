package e2e

import (
	"flag"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if !flag.Parsed() {
		flag.Parse()
	}

	cleanup := goBuildS5cmd()
	code := m.Run()
	cleanup()
	os.Exit(code)
}
