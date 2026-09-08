//go:build windows

package e2e

// setTestUmask is a no-op on Windows, which has no umask.
func setTestUmask() {}
