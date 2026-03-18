// Package tempfile provides an abstraction for creating virtual temporary files
// that are mapped to sections of a single physical file on disk. This design minimizes
// file descriptor usage while supporting efficient sequential writes and concurrent reads.
package tempfile

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	// Pre-computed directory choices for performance
	diskPreferredDir string
	memoryAllowedDir string
	dirDiscoveryOnce sync.Once

	// Cached expensive operations
	cachedHomeDir string
	cachedWorkDir string
	cachedOSTemp  string
)

// GetTempDir returns the optimal temporary directory for the given preference.
// If dir is provided and non-empty, it's validated and returned if usable.
// Otherwise, returns a pre-computed optimal directory based on preferDiskBacked.
// This function is thread-safe and performs O(1) lookups after initialization.
func GetTempDir(dir string, preferDiskBacked bool) string {
	// If caller provides a specific directory, validate and use it
	if dir != "" {
		if isDirectoryUsable(dir) {
			return dir
		}
		// Fall through to use pre-computed directory if provided dir is unusable
	}

	// Ensure directories have been discovered (happens once)
	dirDiscoveryOnce.Do(discoverOptimalDirectories)

	if preferDiskBacked {
		return diskPreferredDir
	}
	return memoryAllowedDir
}

// discoverOptimalDirectories finds and caches the best directories for both preferences.
// This runs once and caches expensive operations like os.UserHomeDir() and os.Getwd().
// Called by sync.Once to ensure thread-safe initialization.
func discoverOptimalDirectories() {
	// Cache expensive operations once
	cacheExpensiveOperations()

	// Find optimal directory for disk-preferred usage
	diskPreferredDir = findBestDirectory(true)

	// Find optimal directory for memory-allowed usage
	memoryAllowedDir = findBestDirectory(false)
}

// cacheExpensiveOperations caches results of expensive OS calls like os.TempDir(),
// os.UserHomeDir(), and os.Getwd() to avoid repeated system calls during directory selection.
func cacheExpensiveOperations() {
	cachedOSTemp = os.TempDir()

	if homeDir, err := os.UserHomeDir(); err == nil {
		cachedHomeDir = homeDir
	}

	if workDir, err := os.Getwd(); err == nil {
		cachedWorkDir = workDir
	}
}

// findBestDirectory finds the best available directory for the given preference.
// It iterates through candidates in priority order and returns the first usable directory.
// Falls back to OS temp directory if no candidates are usable.
func findBestDirectory(preferDiskBacked bool) string {
	candidates := buildCandidateList(preferDiskBacked)

	for _, candidate := range candidates {
		if isDirectoryUsable(candidate) {
			return candidate
		}
	}

	// Final fallback to OS default temp dir
	return cachedOSTemp
}

// buildCandidateList returns a prioritized list of temporary directory candidates.
// When preferDiskBacked is true, disk-preferred candidates are prioritized first.
// Uses cached values for performance. The order depends on the OS and preferDiskBacked setting.
func buildCandidateList(preferDiskBacked bool) []string {
	var candidates []string

	if preferDiskBacked {
		// Add disk-preferred candidates first
		candidates = append(candidates, buildDiskPreferredCandidates()...)
	}

	// Add OS default temp directory (cached)
	candidates = append(candidates, cachedOSTemp)

	// Add additional fallbacks (using cached values)
	candidates = append(candidates, buildAdditionalFallbacks()...)

	return candidates
}

// buildDiskPreferredCandidates returns directories that are more likely to be disk-backed
// rather than memory-backed (like tmpfs). On Unix-like systems, this typically includes
// /var/tmp which is traditionally disk-backed, unlike /tmp which may be tmpfs.
func buildDiskPreferredCandidates() []string {
	var candidates []string

	switch runtime.GOOS {
	case "linux", "darwin", "freebsd", "openbsd", "netbsd", "dragonfly", "solaris":
		// Unix-like systems: /var/tmp is traditionally disk-backed
		candidates = append(candidates, "/var/tmp")

		// Additional Unix candidates
		if runtime.GOOS == "darwin" {
			// macOS specific paths
			candidates = append(candidates, "/private/var/tmp")
		}

	case "windows":
		// On Windows, temp dirs are typically disk-backed, but we can prefer
		// specific locations like the user's temp directory
		// Windows temp handling is generally fine as-is

	default:
		// For other/unknown OS, be conservative and use minimal candidates
	}

	return candidates
}

// buildAdditionalFallbacks returns additional fallback directories as last resort.
// Creates process-specific subdirectories in the user's home directory and current
// working directory. Uses cached directory values for performance.
func buildAdditionalFallbacks() []string {
	var candidates []string

	// Try user home directory with subdirectory (using cached value)
	if cachedHomeDir != "" {
		extsortTempDir := filepath.Join(cachedHomeDir, extsortTempDirName)
		candidates = append(candidates, extsortTempDir)
	}

	// Try current working directory with subdirectory (using cached value)
	if cachedWorkDir != "" {
		extsortTempDir := filepath.Join(cachedWorkDir, extsortTempDirName)
		candidates = append(candidates, extsortTempDir)
	}

	return candidates
}

// isDirectoryUsable checks if a directory exists and is a directory, or can be created.
// It returns true for non-existent directories that could potentially be created.
// We don't test writability here to avoid creating unnecessary files - the actual
// writability will be tested when we try to create the temp file.
func isDirectoryUsable(dir string) bool {
	// Check if directory exists
	stat, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			// Directory doesn't exist - we'll try to create it when needed
			return true
		}
		return false
	}

	// Check if it's actually a directory
	return stat.IsDir()
}
