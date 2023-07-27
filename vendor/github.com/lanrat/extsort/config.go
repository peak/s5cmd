package extsort

// Config holds configuration settings for extsort
type Config struct {
	ChunkSize          int    // amount of records to store in each chunk which will be written to disk
	NumWorkers         int    // maximum number of workers to use for parallel sorting
	ChanBuffSize       int    // buffer size for merging chunks
	SortedChanBuffSize int    // buffer size for passing records to output
	TempFilesDir       string // empty for use OS default ex: /tmp
}

// DefaultConfig returns the default configuration options sued if none provided
func DefaultConfig() *Config {
	return &Config{
		ChunkSize:          int(1e6), // 1M
		NumWorkers:         2,
		ChanBuffSize:       1,
		SortedChanBuffSize: 10,
		TempFilesDir:       "",
	}
}

// mergeConfig takes a provided config and replaces any values not set with the defaults
func mergeConfig(c *Config) *Config {
	d := DefaultConfig()
	if c == nil {
		return d
	}
	if c.ChunkSize <= 1 {
		c.ChunkSize = d.ChunkSize
	}
	if c.NumWorkers <= 1 {
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
