package tempfile

import (
	"bufio"
	"bytes"
	"io"
)

// MockFileWriter provides an in-memory implementation of the TempWriter interface.
// It stores all data in memory using bytes.Buffer instead of writing to disk files.
// This is useful for testing and benchmarking without filesystem I/O overhead.
type MockFileWriter struct {
	data     *bytes.Buffer
	sections []int
}

// mockFileReader provides an in-memory implementation of the TempReader interface.
// It reads data from the bytes.Buffer written by MockFileWriter, providing
// the same sectioned access pattern as the disk-based implementation.
type mockFileReader struct {
	data     *bytes.Reader
	sections []int
	readers  []*bufio.Reader
}

// Mock creates a new in-memory TempWriter with the specified initial capacity.
// The parameter n sets the initial capacity of the underlying buffer to reduce
// memory reallocations during writing. Use this for testing and benchmarking
// scenarios where disk I/O should be avoided.
func Mock(n int) *MockFileWriter {
	var m MockFileWriter
	m.data = bytes.NewBuffer(make([]byte, 0, n))
	return &m
}

// Size returns the total number of virtual file sections that have been created.
// This includes the current section being written plus all completed sections.
func (w *MockFileWriter) Size() int {
	// we add one because we only write to the sections when we are done
	return len(w.sections) + 1
}

// Close terminates the MockFileWriter and releases all memory.
// This operation is irreversible and prevents transitioning to read mode.
// Use Save() instead to transition from writing to reading.
func (w *MockFileWriter) Close() error {
	w.data.Reset()
	w.sections = nil
	w.data = nil
	return nil
}

// Write appends data to the current virtual file section in memory.
func (w *MockFileWriter) Write(p []byte) (int, error) {
	return w.data.Write(p)
}

// WriteString appends string data to the current virtual file section in memory.
func (w *MockFileWriter) WriteString(s string) (int, error) {
	return w.data.WriteString(s)
}

// Next finalizes the current virtual file section and prepares for writing the next section.
// It records the section boundary for later reading and returns the offset where the next section begins.
func (w *MockFileWriter) Next() (int64, error) {
	// save offsets
	pos := w.data.Len()
	w.sections = append(w.sections, pos)
	return int64(pos), nil
}

// Save finalizes all virtual file sections and returns a TempReader for accessing the data.
// After calling Save(), the MockFileWriter can no longer be used for writing.
// The returned TempReader allows concurrent access to any virtual file section.
func (w *MockFileWriter) Save() (TempReader, error) {
	_, err := w.Next()
	if err != nil {
		return nil, err
	}
	return newMockTempReader(w.sections, w.data.Bytes())
}

// newMockTempReader creates a TempReader from in-memory data with section boundaries.
// This allows reading back data written by MockFileWriter in the same sectioned manner.
func newMockTempReader(sections []int, data []byte) (*mockFileReader, error) {
	// create TempReader
	var r mockFileReader
	r.data = bytes.NewReader(data)
	r.sections = sections
	r.readers = make([]*bufio.Reader, len(r.sections))

	offset := 0
	for i, end := range r.sections {
		section := io.NewSectionReader(r.data, int64(offset), int64(end-offset))
		offset = end
		r.readers[i] = bufio.NewReaderSize(section, fileBufferSize)
	}

	return &r, nil
}

// Close releases memory used by the mockFileReader.
// This should be called after all reading operations are complete.
func (r *mockFileReader) Close() error {
	r.readers = nil
	r.data = nil
	return nil
}

// Size returns the number of virtual file sections available for reading.
func (r *mockFileReader) Size() int {
	return len(r.readers)
}

// Read returns a buffered reader for the specified virtual file section.
// Panics if the section index is out of range.
func (r *mockFileReader) Read(i int) *bufio.Reader {
	if i < 0 || i >= len(r.readers) {
		panic("tempfile: read request out of range")
	}
	return r.readers[i]
}
