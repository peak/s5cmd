package log

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// captureOutputs redirects os.Stdout and os.Stderr, calls f, then returns
// whatever was written to each. The caller is responsible for draining the
// logger (e.g. calling Close) inside f before this function reads the pipes.
func captureOutputs(t *testing.T, f func()) (stdout, stderr string) {
	t.Helper()

	origStdout := os.Stdout
	origStderr := os.Stderr
	defer func() {
		os.Stdout = origStdout
		os.Stderr = origStderr
	}()

	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	os.Stdout = wOut
	os.Stderr = wErr

	f()

	wOut.Close()
	wErr.Close()

	var bufOut, bufErr bytes.Buffer
	if _, err := bufOut.ReadFrom(rOut); err != nil {
		t.Fatal(err)
	}
	if _, err := bufErr.ReadFrom(rErr); err != nil {
		t.Fatal(err)
	}
	return bufOut.String(), bufErr.String()
}

// reinitOutputCh creates a fresh outputCh so tests don't share state.
func reinitOutputCh() {
	outputCh = make(chan output, 10000)
}

// TestDebugWritesToStderr verifies that Debug messages go to stderr, not stdout.
func TestDebugWritesToStderr(t *testing.T) {
	reinitOutputCh()
	Init("debug", false)

	stdout, stderr := captureOutputs(t, func() {
		Debug(DebugMessage{Err: "hello debug"})
		Close()
	})

	if stdout != "" {
		t.Errorf("Debug wrote to stdout, want stderr only; stdout=%q", stdout)
	}
	if !strings.Contains(stderr, "hello debug") {
		t.Errorf("stderr does not contain expected message; stderr=%q", stderr)
	}
}

// TestTraceWritesToStderr verifies that Trace messages go to stderr, not stdout.
func TestTraceWritesToStderr(t *testing.T) {
	reinitOutputCh()
	Init("trace", false)

	stdout, stderr := captureOutputs(t, func() {
		Trace(TraceMessage{Message: "hello trace"})
		Close()
	})

	if stdout != "" {
		t.Errorf("Trace wrote to stdout, want stderr only; stdout=%q", stdout)
	}
	if !strings.Contains(stderr, "hello trace") {
		t.Errorf("stderr does not contain expected message; stderr=%q", stderr)
	}
}

// TestInfoWritesToStdout verifies that Info messages continue to go to stdout.
func TestInfoWritesToStdout(t *testing.T) {
	reinitOutputCh()
	Init("info", false)

	stdout, _ := captureOutputs(t, func() {
		Info(ErrorMessage{Err: "hello info"})
		Close()
	})

	if !strings.Contains(stdout, "hello info") {
		t.Errorf("Info did not write to stdout; stdout=%q", stdout)
	}
}
