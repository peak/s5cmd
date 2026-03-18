package progressbar

import (
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestCommandProgress_Finish(t *testing.T) {
	t.Parallel()
	cp := New()
	cp.Start()
	cp.Finish()
	assert.Equal(t, true, cp.progressbar.IsFinished())
}

func TestCommandProgress_IncrementCompletedObjects(t *testing.T) {
	t.Parallel()
	cp := New()
	cp.IncrementCompletedObjects()
	assert.Equal(t, int64(1), cp.completedObjects)
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "1/0"))
}

func TestCommandProgress_IncrementTotalObjects(t *testing.T) {
	t.Parallel()
	cp := New()
	cp.Start()
	cp.IncrementTotalObjects()
	assert.Equal(t, int64(1), cp.totalObjects)
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "0/1"))
}

func TestCommandProgress_AddCompletedBytes(t *testing.T) {
	t.Parallel()
	cp := New()
	cp.Start()
	bytes := int64(101)
	cp.AddCompletedBytes(bytes)
	assert.Equal(t, bytes, cp.progressbar.Current())
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "101 B"))
}

func TestCommandProgress_AddTotalBytes(t *testing.T) {
	t.Parallel()
	cp := New()
	cp.Start()
	bytes := int64(102)
	cp.AddTotalBytes(bytes)
	assert.Equal(t, bytes, cp.progressbar.Total())
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "102 B"))
}

func TestFormatBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{999, "999 B"},
		{1000, "1.0 kB"},
		{1500, "1.5 kB"},
		{1000000, "1.0 MB"},
		{1000000000, "1.0 GB"},
	}

	for _, tc := range tests {
		got := formatBytes(tc.input)
		assert.Equal(t, tc.want, got)
	}
}

func TestFormatDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input time.Duration
		want  string
	}{
		{5 * time.Second, "5s"},
		{65 * time.Second, "1m5s"},
		{3661 * time.Second, "1h1m1s"},
		{0, "0s"},
	}

	for _, tc := range tests {
		got := formatDuration(tc.input)
		assert.Equal(t, tc.want, got)
	}
}
