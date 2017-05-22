package main

import (
	"os"
	"sort"
	"testing"

	"github.com/posener/complete"
)

func TestPredictions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		predictor  complete.Predictor
		last       string
		completion []string
	}{
		{
			name:       "predict tests ok",
			predictor:  predictTest,
			completion: []string{"TestPredictions", "Example"},
		},
		{
			name:      "predict tests not found",
			predictor: predictTest,
			last:      "X",
		},
		{
			name:       "predict benchmark ok",
			predictor:  predictBenchmark,
			completion: []string{"BenchmarkFake"},
		},
		{
			name:      "predict benchmarks not found",
			predictor: predictBenchmark,
			last:      "X",
		},
		{
			name:       "predict packages ok",
			predictor:  complete.PredictFunc(predictPackages),
			completion: []string{"./"},
		},
		{
			name:      "predict packages not found",
			predictor: complete.PredictFunc(predictPackages),
			last:      "X",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := complete.Args{Last: tt.last}
			got := tt.predictor.Predict(a)
			if want := tt.completion; !equal(got, want) {
				t.Errorf("Failed %s: completion = %q, want %q", t.Name(), got, want)
			}
		})
	}
}

func BenchmarkFake(b *testing.B) {}

func Example() {
	os.Setenv("COMP_LINE", "go ru")
	main()
	// output: run

}

func equal(s1, s2 []string) bool {
	sort.Strings(s1)
	sort.Strings(s2)
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}
