#!/bin/bash
set -e

# Performance comparison script for extsort library

RESULTS_DIR="bench_results"
mkdir -p "$RESULTS_DIR"

echo "=== External Sort Performance Comparison ==="
echo "Results will be stored in the '$RESULTS_DIR' directory."
echo

echo "1. Running all throughput and memory benchmarks..."
go test -run=^$ -bench=. -benchmem -count=6 -benchtime=3s | tee "$RESULTS_DIR/all_benchmarks.txt"
echo "Benchmark results saved to $RESULTS_DIR/all_benchmarks.txt"

echo
echo "2. Generating CPU and Memory Profiles..."
echo "Using a large dataset (1,000,000) for more meaningful profiles."
go test -run=^$ -bench=BenchmarkGenericIntSortComparison/size_1000000 -cpuprofile="$RESULTS_DIR/cpu.prof" -memprofile="$RESULTS_DIR/mem.prof"
echo "Profile files generated in '$RESULTS_DIR/'"

echo
echo "3. Running Scaling Analysis..."

# Create separate files for benchstat comparison
for size in 1000 10000 100000 1000000; do
    echo "  - Running for dataset size: $size"
    go test -run=^$ -bench="^BenchmarkGenericIntSortComparison/size_$size$" -benchmem -count=6 -benchtime=1s > "$RESULTS_DIR/scaling_size_$size.txt"
done
echo "Scaling analysis results saved."

echo
echo "4. Analyzing Scaling Results with benchstat..."
# Check if benchstat is installed
if ! command -v benchstat &> /dev/null
then
    echo "WARNING: 'benchstat' could not be found. Please install it for a statistical comparison:"
    echo "go install golang.org/x/perf/cmd/benchstat@latest"
    echo
    echo "Raw scaling results:"
    grep "Benchmark" "$RESULTS_DIR"/scaling_size_*.txt
else
    benchstat "$RESULTS_DIR"/scaling_size_*.txt
fi

echo
echo "=== Analysis Complete ==="
echo "To analyze profiles, use:"
echo "go tool pprof $RESULTS_DIR/cpu.prof"
echo "go tool pprof $RESULTS_DIR/mem.prof"