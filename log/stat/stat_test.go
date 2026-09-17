package stat

import (
	"errors"
	"sync"
	"testing"
)

func resetStats() {
	enabled = false
	stats = statistics{}
}

func TestStatisticsCollect(t *testing.T) {
	resetStats()
	defer resetStats()

	InitStat()

	var nilErr error
	Collect("cp", &nilErr)()
	Collect("cp", &nilErr)()
	Collect("cp", nil)() // nil *error also counts as success

	anErr := errors.New("some error")
	Collect("cp", &anErr)()

	Collect("mv", &nilErr)()

	statMap := map[string]Stat{}
	for _, s := range Statistics() {
		statMap[s.Operation] = s
	}

	cp, ok := statMap["cp"]
	if !ok {
		t.Fatal("expected 'cp' in statistics")
	}
	if cp.Success != 3 || cp.Error != 1 {
		t.Errorf("cp: expected success=3 error=1, got success=%d error=%d", cp.Success, cp.Error)
	}

	mv, ok := statMap["mv"]
	if !ok {
		t.Fatal("expected 'mv' in statistics")
	}
	if mv.Success != 1 || mv.Error != 0 {
		t.Errorf("mv: expected success=1 error=0, got success=%d error=%d", mv.Success, mv.Error)
	}
}

func TestStatisticsDisabled(t *testing.T) {
	resetStats()

	if got := Statistics(); len(got) != 0 {
		t.Errorf("expected empty stats when disabled, got %d entries", len(got))
	}
}

func TestStatisticsConcurrentCollect(t *testing.T) {
	resetStats()
	defer resetStats()

	InitStat()

	const (
		workers    = 8
		iterations = 500
	)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var nilErr error
			anErr := errors.New("some error")
			for j := 0; j < iterations; j++ {
				Collect("cp", &nilErr)()
				Collect("cp", &anErr)()
			}
		}()
	}

	// Read statistics while workers are still collecting.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = Statistics()
		}
	}()

	wg.Wait()

	result := Statistics()
	if len(result) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(result))
	}
	want := int64(workers * iterations)
	if result[0].Success != want || result[0].Error != want {
		t.Errorf("expected success=%d error=%d, got success=%d error=%d", want, want, result[0].Success, result[0].Error)
	}
}
