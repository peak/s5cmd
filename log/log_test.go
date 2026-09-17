package log

import "testing"

func TestCloseMultipleTimes(t *testing.T) {
	Init("info", false)

	Close()
	// Second Close must not panic with "close of closed channel".
	Close()
}
