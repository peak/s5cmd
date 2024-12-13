// Package tempfile implements a virtual temp files that can be written to (in series)
// and then read back (series/parallel) and then removed from the filesystem when done
// if multiple "tempfiles" are needed on the application layer, they are mapped to
// sections of the same real file on the filesystem
package tempfile

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// file IO buffer size for each file
const fileBufferSize = 1 << 16 // 64k

// filename prefix for files put in temp directory
var mergeFilenamePrefix = fmt.Sprintf("extsort_%d_", os.Getpid())

// FileWriter allows for creating virtual temp files for reading/writing
type FileWriter struct {
	file      *os.File
	bufWriter *bufio.Writer
	sections  []int64
}

type fileReader struct {
	file     *os.File
	sections []int64
	readers  []*bufio.Reader
}

// New creates a new tempfile in dir, if dir is empty the OS default is used
func New(dir string) (*FileWriter, error) {
	var w FileWriter
	var err error
	w.file, err = ioutil.TempFile(dir, mergeFilenamePrefix)
	if err != nil {
		return nil, err
	}
	w.bufWriter = bufio.NewWriterSize(w.file, fileBufferSize)
	w.sections = make([]int64, 0, 10)

	return &w, nil
}

// Size returns the number of virtual files created
func (w *FileWriter) Size() int {
	// we add one because we only write to the sections when we are done
	return len(w.sections) + 1
}

// Name returns the full path of the underlying file on the OS
func (w *FileWriter) Name() string {
	return w.file.Name()
}

// Close stops the tempfile from accepting new data,
// closes the file, and removes the temp file from disk
// works like an abort, unrecoverable
func (w *FileWriter) Close() error {
	err := w.file.Close()
	if err != nil {
		return err
	}
	w.sections = nil
	w.bufWriter = nil
	return os.Remove(w.file.Name())
}

// Write writes a byte to the current file section
func (w *FileWriter) Write(p []byte) (int, error) {
	return w.bufWriter.Write(p)
}

// WriteString writes s to the current file section
func (w *FileWriter) WriteString(s string) (int, error) {
	return w.bufWriter.WriteString(s)
}

// Next stops writing the the current section/file and prepares the tempWriter for the next one
func (w *FileWriter) Next() (int64, error) {
	// save offsets
	err := w.bufWriter.Flush()
	if err != nil {
		return 0, err
	}
	pos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	w.sections = append(w.sections, pos)

	return pos, nil
}

// Save stops writing new data/sections to the temp file and returns a reader for reading the data/sections back
func (w *FileWriter) Save() (TempReader, error) {
	_, err := w.Next()
	if err != nil {
		return nil, err
	}
	err = w.file.Sync()
	if err != nil {
		return nil, err
	}
	err = w.file.Close()
	if err != nil {
		return nil, err
	}
	return newTempReader(w.file.Name(), w.sections)
}

func newTempReader(filename string, sections []int64) (*fileReader, error) {
	// create TempReader
	var err error
	var r fileReader
	r.file, err = os.Open(filename)
	if err != nil {
		return nil, err
	}
	r.sections = sections
	r.readers = make([]*bufio.Reader, len(r.sections))

	offset := int64(0)
	for i, end := range r.sections {
		section := io.NewSectionReader(r.file, offset, end-offset)
		offset = end
		r.readers[i] = bufio.NewReaderSize(section, fileBufferSize)
	}

	return &r, nil
}

func (r *fileReader) Close() error {
	r.readers = nil
	err := r.file.Close()
	if err != nil {
		return err
	}
	return os.Remove(r.file.Name())
}

func (r *fileReader) Size() int {
	return len(r.readers)
}

func (r *fileReader) Read(i int) *bufio.Reader {
	if i < 0 || i >= len(r.readers) {
		panic("tempfile: read request out of range")
	}
	return r.readers[i]
}
