package extsort

// Config holds configuration settings for external sorting operations.
// All fields have sensible defaults and can be left as zero values to use defaults.
type Config struct {
	// ChunkSize specifies the maximum number of records to store in each chunk
	// before writing to disk. Larger chunks use more memory but reduce I/O operations.
	// Default: 1,000,000 records. Must be > 1.
	ChunkSize int

	// NumWorkers controls the maximum number of goroutines used for parallel
	// chunk sorting and merging. More workers can improve CPU utilization on multi-core systems.
	// Default: 2 workers. Must be > 1.
	NumWorkers int

	// ChanBuffSize sets the buffer size for internal channels used during chunk merging.
	// Larger buffers can improve throughput but use more memory.
	// Default: 1. Must be >= 0.
	ChanBuffSize int

	// SortedChanBuffSize sets the buffer size for the output channel that delivers
	// sorted results. Larger buffers allow more decoupling between sorting and consumption.
	// Default: 1000. Must be >= 0.
	SortedChanBuffSize int

	// TempFilesDir specifies the directory for temporary files during sorting.
	// When empty (default), the library uses intelligent directory selection that
	// prefers disk-backed locations over potentially memory-backed filesystems
	// (like tmpfs on Linux). This helps prevent out-of-memory issues when sorting
	// datasets larger than available RAM.
	//
	// For production use with large datasets, it's recommended to explicitly set
	// this to a known disk-backed directory (such as "/var/tmp" on Unix systems)
	// to ensure optimal performance and avoid memory limitations. On Linux systems,
	// prefer "/var/tmp" over "/tmp" since "/tmp" may be mounted as tmpfs.
	//
	// Default: "" (intelligent selection).
	TempFilesDir string
}

// DefaultConfig returns a Config with sensible default values optimized for
// general-purpose external sorting. These defaults balance memory usage,
// I/O efficiency, and parallelism for typical workloads.
func DefaultConfig() *Config {
	return &Config{
		ChunkSize:          int(1e6), // 1M
		NumWorkers:         2,
		ChanBuffSize:       16,
		SortedChanBuffSize: 1000,
		TempFilesDir:       "",
	}
}

// mergeConfig validates and normalizes a Config by replacing zero/invalid values
// with defaults. If config is nil, returns DefaultConfig().
// This ensures all sorter instances have valid configuration values.
func mergeConfig(c *Config) *Config {
	d := DefaultConfig()
	if c == nil {
		return d
	}
	if c.ChunkSize < 1 {
		c.ChunkSize = d.ChunkSize
	}
	if c.NumWorkers < 1 {
		c.NumWorkers = d.NumWorkers
	}
	if c.ChanBuffSize < 0 {
		c.ChanBuffSize = d.ChanBuffSize
	}
	if c.SortedChanBuffSize < 0 {
		c.SortedChanBuffSize = d.SortedChanBuffSize
	}
	return c
}
