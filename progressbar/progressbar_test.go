package progressbar

import (
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestCommandProgress_Finish(t *testing.T) {
	t.Parallel()
	cp := NewCommandProgressBar()
	cp.Start()
	cp.Finish()
	assert.Equal(t, true, cp.progressbar.IsFinished())
}

func TestCommandProgress_IncrementCompletedObjects(t *testing.T) {
	t.Parallel()
	cp := NewCommandProgressBar()
	cp.IncrementCompletedObjects()
	assert.Equal(t, int64(1), cp.completedObjects)
	// Verify that the progress bar's task status has been updated
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "1/0"))
}

func TestCommandProgress_IncrementTotalObjects(t *testing.T) {
	t.Parallel()
	cp := NewCommandProgressBar()
	cp.Start()
	cp.IncrementTotalObjects()
	assert.Equal(t, int64(1), cp.totalObjects)
	// Verify that the progress bar's task status has been updated
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "0/1"))
}

func TestCommandProgress_AddCompletedBytes(t *testing.T) {
	t.Parallel()
	cp := NewCommandProgressBar()
	cp.Start()
	bytes := int64(101)
	cp.AddCompletedBytes(bytes)
	assert.Equal(t, int64(101), cp.completedBytes)
	// Verify that the progress bar's completed bytes have been updated
	assert.Equal(t, int64(bytes), cp.progressbar.Current())
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "101 B"))
}

func TestCommandProgress_AddTotalBytes(t *testing.T) {
	t.Parallel()
	cp := NewCommandProgressBar()
	cp.Start()
	bytes := int64(102)
	cp.AddTotalBytes(bytes)
	assert.Equal(t, int64(102), cp.totalBytes)
	// Verify that the progress bar's total bytes have been updated
	assert.Equal(t, bytes, cp.progressbar.Total())
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "102 B"))
}
