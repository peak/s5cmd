package log

import (
	"bytes"
	"fmt"
	"github.com/peak/s5cmd/log/stat"
	"io"
	"os"
	"strings"
	"testing"
)

func TestStat(t *testing.T) {
	levels := []string{
		"trace",
		"debug",
		"info",
		"error",
	}
	for _, l := range levels {
		testStatHelper(l, t)
	}
	// testStatHelper method closes and remakes the outputCh
	// but this creates a useless channel at the end too
	// so we need to close it at the end.
	Close()
}

func testStatHelper(level string, t *testing.T) {
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	Init(level, true)
	op := "op"

	var s, e int64 = 1, 0
	Stat(stat.Stats{{Operation: op, Success: s, Error: e}})

	// Close closes the output channel so that the current test level can have its output.
	Close()
	// To be able to test the remaining tests, we should create new channel for them
	outputCh = make(chan output, 10000)
	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()
	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC
	out = strings.TrimSpace(out)
	if out != fmt.Sprintf("{\"operation\":\"%s\",\"success\":%v,\"error\":%v}", op, s, e) {
		t.Errorf("Stat does not print in %v level!\n$%v$", level, out)
	}
}
