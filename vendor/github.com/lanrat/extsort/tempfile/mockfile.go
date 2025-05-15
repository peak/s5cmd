package tempfile

import (
	"bufio"
	"bytes"
	"io"
)

// MockFileWriter allows writing to a temp file(s) that are backed by memory
type MockFileWriter struct {
	data     *bytes.Buffer
	sections []int
}

// mockFileReader allows reading from a temp file(s) that are backed by memory
type mockFileReader struct {
	data     *bytes.Reader
	sections []int
	readers  []*bufio.Reader
}

// Mock returns a new tempfileWrite backed by memory
func Mock(n int) *MockFileWriter {
	var m MockFileWriter
	m.data = bytes.NewBuffer(make([]byte, 0, n))
	return &m
}

// Size return the number of "files" saved
func (w *MockFileWriter) Size() int {
	// we add one because we only write to the sections when we are done
	return len(w.sections) + 1
}

// Close stops the tempfile from accepting new data,
// works like an abort, unrecoverable
func (w *MockFileWriter) Close() error {
	w.data.Reset()
	w.sections = nil
	w.data = nil
	return nil
}

// Write writes the byte to the temp file
func (w *MockFileWriter) Write(p []byte) (int, error) {
	return w.data.Write(p)
}

// WriteString writes the string to the temp file
func (w *MockFileWriter) WriteString(s string) (int, error) {
	return w.data.WriteString(s)
}

// Next stops writing the the current section/file and prepares the tempWriter for the next one
func (w *MockFileWriter) Next() (int64, error) {
	// save offsets
	pos := w.data.Len()
	w.sections = append(w.sections, pos)
	return int64(pos), nil
}

// Save stops allowing new writes and returns a TempReader for reading the data back
func (w *MockFileWriter) Save() (TempReader, error) {
	_, err := w.Next()
	if err != nil {
		return nil, err
	}
	return newMockTempReader(w.sections, w.data.Bytes())
}

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

// Close does nothing much on a MockTempWriter
func (r *mockFileReader) Close() error {
	r.readers = nil
	r.data = nil
	return nil
}

// Size returns the number of sections/files in the reader
func (r *mockFileReader) Size() int {
	return len(r.readers)
}

// Read returns a reader for the provided section
func (r *mockFileReader) Read(i int) *bufio.Reader {
	if i < 0 || i >= len(r.readers) {
		panic("tempfile: read request out of range")
	}
	return r.readers[i]
}
