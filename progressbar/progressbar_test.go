package progressbar

import (
	"strings"
	"sync"
	"testing"

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

func TestCommandProgress_ConcurrentIncrement(t *testing.T) {
	t.Parallel()
	cp := New()

	const (
		workers    = 8
		iterations = 500
	)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				cp.IncrementTotalObjects()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				cp.IncrementCompletedObjects()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(workers*iterations), cp.totalObjects)
	assert.Equal(t, int64(workers*iterations), cp.completedObjects)
	assert.Equal(t, true, strings.Contains(cp.progressbar.String(), "4000/4000"))
}
