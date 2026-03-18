package log

import (
	"os"
	"strings"
	"testing"
)

// testMessage implements Message interface for testing.
type testMessage struct {
	msg string
}

func (t testMessage) String() string { return t.msg }
func (t testMessage) JSON() string   { return `{"msg":"` + t.msg + `"}` }

func TestInitWithLogFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "s5cmd-log-test-*.log")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Re-create the outputCh since it may have been closed by a previous test.
	outputCh = make(chan output, 10000)

	Init("info", false, WithLogFile(tmpPath))
	Info(testMessage{msg: "hello from test"})
	Close()

	data, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	if !strings.Contains(string(data), "hello from test") {
		t.Fatalf("expected log file to contain 'hello from test', got: %s", string(data))
	}
}

func TestCloseMultipleTimes(t *testing.T) {
	// Re-create the outputCh since it may have been closed by a previous test.
	outputCh = make(chan output, 10000)

	Init("info", false)

	// First close should work normally.
	Close()
	// Second close should not panic (protected by sync.Once).
	Close()
}
