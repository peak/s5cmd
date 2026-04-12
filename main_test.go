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

	// Use a high retry count + endpoint that delays so the command stays
	// alive long enough to receive SIGINT.  --endpoint-url pointing at a
	// closed port will cause the SDK to retry, keeping the process running.
	cmd := exec.Command(bin,
		"--endpoint-url", "http://10.255.255.1:19999", // routable but unreachable
		"--retry-count", "10",
		"ls", "s3://bucket/",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start s5cmd: %v", err)
	}

	// Give the process time to start and block on the network call.
	time.Sleep(500 * time.Millisecond)

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
