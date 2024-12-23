package tempfile

import (
	"bufio"
	"io"
)

// TempWriter interface defines the virtual tempfile creation/writing stage
type TempWriter interface {
	io.Closer
	Size() int
	Write(p []byte) (int, error)
	WriteString(s string) (int, error)
	Next() (int64, error)
	Save() (TempReader, error)
}

// TempReader defines the methods to allow reading the sections from tempfiles
type TempReader interface {
	io.Closer
	Size() int
	Read(i int) *bufio.Reader
}
