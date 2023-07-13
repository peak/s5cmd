package progress

import (
	"reflect"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestCommandProgress_InitializeProgressBar(t *testing.T) {
	t.Parallel()
	cp := &CommandProgress{}
	cp.InitializeProgressBar()
	assert.Equal(t, "*pb.ProgressBar", reflect.TypeOf(cp.progressbar).String())
}

func TestCommandProgress_Finish(t *testing.T) {
	t.Parallel()
	cp := &CommandProgress{}
	cp.InitializeProgressBar()

	cp.Finish()

	assert.Equal(t, true, cp.progressbar.IsFinished())
}

func TestCommandProgress_IncrementCompletedObjects(t *testing.T) {
	t.Parallel()
	cp := &CommandProgress{}
	cp.InitializeProgressBar()
	cp.IncrementCompletedObjects()
	assert.Equal(t, int64(1), cp.completedObjects)
	// Verify that the progress bar's task status has been updated
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "1/0 items are completed."))
}

func TestCommandProgress_IncrementTotalObjects(t *testing.T) {
	t.Parallel()
	cp := &CommandProgress{}
	cp.InitializeProgressBar()
	cp.IncrementTotalObjects()
	assert.Equal(t, int64(1), cp.totalObjects)
	// Verify that the progress bar's task status has been updated
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "0/1 items are completed."))
}

func TestCommandProgress_AddCompletedBytesInt64(t *testing.T) {
	t.Parallel()
	cp := &CommandProgress{}
	cp.InitializeProgressBar()

	bytes := int64(100)
	cp.AddCompletedBytesInt64(bytes)

	assert.Equal(t, int64(100), cp.completedBytes)
	// Verify that the progress bar's completed bytes have been updated
	assert.Equal(t, bytes, cp.progressbar.Current())
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "100 B"))

}

func TestCommandProgress_AddCompletedBytes(t *testing.T) {
	t.Parallel()
	cp := &CommandProgress{}
	cp.InitializeProgressBar()

	bytes := 101
	cp.AddCompletedBytes(bytes)

	assert.Equal(t, int64(101), cp.completedBytes)
	// Verify that the progress bar's completed bytes have been updated
	assert.Equal(t, int64(bytes), cp.progressbar.Current())
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "101 B"))
}

func TestCommandProgress_AddTotalBytes(t *testing.T) {
	t.Parallel()
	cp := &CommandProgress{}
	cp.InitializeProgressBar()

	bytes := int64(102)
	cp.AddTotalBytes(bytes)

	assert.Equal(t, int64(102), cp.totalBytes)
	// Verify that the progress bar's total bytes have been updated
	assert.Equal(t, bytes, cp.progressbar.Total())
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "102 B"))
}
