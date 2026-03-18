package tempfile

import (
	"bufio"
	"io"
)

// TempWriter defines the interface for sequential writing to virtual temporary file sections.
// It provides methods for writing data, managing section boundaries, and transitioning
// to read mode. Implementations handle the underlying storage mechanism (disk or memory).
type TempWriter interface {
	// Close terminates the writer and cleans up resources.
	// This is irreversible and prevents transitioning to read mode.
	io.Closer

	// Size returns the number of virtual file sections created.
	Size() int

	// Write appends data to the current virtual file section.
	Write(p []byte) (int, error)

	// WriteString appends string data to the current virtual file section.
	WriteString(s string) (int, error)

	// Next finalizes the current section and prepares for the next one.
	// Returns the offset where the next section will begin.
	Next() (int64, error)

	// Save finalizes all sections and returns a TempReader for data access.
	// After calling Save(), the TempWriter cannot be used for further writing.
	Save() (TempReader, error)
}

// TempReader defines the interface for reading from virtual temporary file sections.
// It provides concurrent access to any section created by the corresponding TempWriter.
// Multiple readers can access different sections simultaneously for efficient merging.
type TempReader interface {
	// Close terminates the reader and cleans up resources.
	// This should be called after all reading operations are complete.
	io.Closer

	// Size returns the total number of virtual file sections available for reading.
	Size() int

	// Read returns a buffered reader for the specified virtual file section.
	// The section index i must be in the range [0, Size()-1].
	// Each call may return a new reader instance positioned at the section start.
	Read(i int) *bufio.Reader
}
