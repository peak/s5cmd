package main

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// TestSIGINTExitCode verifies that sending SIGINT to s5cmd results in
// exit code 130 (the POSIX standard: 128 + signal number 2).
//
// The test builds the binary, starts it with a long-running command,
// sends SIGINT after a short delay, and checks the exit code.
func TestSIGINTExitCode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess test in short mode")
	}

	// Build the binary into a temp dir.
	tmpDir := t.TempDir()
	bin := tmpDir + "/s5cmd"
	if err := exec.Command("go", "build", "-o", bin, ".").Run(); err != nil {
		t.Fatalf("failed to build s5cmd: %v", err)
	}

	// Use --no-sign-request so credential lookup is skipped entirely, then
	// point at a black-hole address.  The binary will block on the TCP dial
	// for as long as we need.  Setting AWS_DEFAULT_REGION avoids the
	// region-discovery HTTP call so the binary goes straight to dialling
	// the black-hole endpoint.
	cmd := exec.Command(bin,
		"--no-sign-request",
		"--endpoint-url", "http://10.255.255.1:19999", // routable but unreachable
		"--retry-count", "10",
		"ls", "s3://bucket/",
	)
	cmd.Env = append(os.Environ(), "AWS_DEFAULT_REGION=us-east-1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start s5cmd: %v", err)
	}

	// Give the process time to start and block on the network dial.
	// Use 3 s to remain reliable even under heavy test-suite parallelism.
	time.Sleep(3 * time.Second)

	// Send SIGINT.
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("failed to send SIGINT: %v", err)
	}

	err := cmd.Wait()
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError after SIGINT, got: %T %v", err, err)
	}

	got := exitErr.ExitCode()
	t.Logf("process state: %v", exitErr.ProcessState)
	if got != 130 {
		t.Errorf("expected exit code 130 after SIGINT, got %d", got)
	}
}
