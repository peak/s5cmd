//go:build !windows

package e2e

import "syscall"

// setTestUmask fixes the umask for the test process and for the s5cmd
// binaries it spawns. Several tests assert on the permissions of directories
// s5cmd creates, and those are created with os.ModePerm, so the resulting
// mode depends entirely on the umask the suite happens to inherit: 0755
// under the common 022, but 0775 under 002. Pin it so the assertions mean
// the same thing on every machine.
func setTestUmask() {
	syscall.Umask(0o022)
}
