package storage

import (
	"io"
	"os"
)

type StdinReader interface {
	io.ReadCloser
}

type stdin struct {
	file *os.File
}

func (s *stdin) Read(p []byte) (n int, err error) {
	return s.file.Read(p)
}

func (s *stdin) Close() error {
	return nil
}

func (f *Filesystem) ReadStdin() (StdinReader, error) {
	return &stdin{file: os.Stdin}, nil
}
