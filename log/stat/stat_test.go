package stat

import (
	"fmt"
	"testing"
)

func TestStatisticsCollect(t *testing.T) {
	// Reset global state.
	enabled = false
	stats = statistics{}

	InitStat()

	// Simulate successful cp operations.
	var nilErr error
	Collect("cp", &nilErr)()
	Collect("cp", &nilErr)()
	Collect("cp", nil)() // nil *error pointer also counts as success

	// Simulate a failed cp operation.
	anErr := fmt.Errorf("some error")
	Collect("cp", &anErr)()

	// Simulate a successful mv operation.
	Collect("mv", &nilErr)()

	result := Statistics()

	// Build a map for easy lookup.
	statMap := map[string]Stat{}
	for _, s := range result {
		statMap[s.Operation] = s
	}

	cpStat, ok := statMap["cp"]
	if !ok {
		t.Fatal("expected 'cp' in statistics")
	}
	if cpStat.Success != 3 {
		t.Errorf("expected cp success=3, got %d", cpStat.Success)
	}
	if cpStat.Error != 1 {
		t.Errorf("expected cp error=1, got %d", cpStat.Error)
	}

	mvStat, ok := statMap["mv"]
	if !ok {
		t.Fatal("expected 'mv' in statistics")
	}
	if mvStat.Success != 1 {
		t.Errorf("expected mv success=1, got %d", mvStat.Success)
	}
	if mvStat.Error != 0 {
		t.Errorf("expected mv error=0, got %d", mvStat.Error)
	}

	// Reset for other tests.
	enabled = false
}

func TestStatisticsDisabled(t *testing.T) {
	// Reset global state - do NOT call InitStat().
	enabled = false
	stats = statistics{}

	result := Statistics()
	if len(result) != 0 {
		t.Errorf("expected empty stats when disabled, got %d entries", len(result))
	}
}
