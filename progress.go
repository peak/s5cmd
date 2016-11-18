package s5cmd

import "io"

type writerAtProgress struct {
	io.WriterAt
	callback writerAtCallback
}

type writerAtCallback func(int64)

func NewWriterAtProgress(w io.WriterAt, callback writerAtCallback) writerAtProgress {
	return writerAtProgress{
		w,
		callback,
	}
}

func (w writerAtProgress) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = w.WriterAt.WriteAt(p, off)
	w.callback(int64(n))
	return
}
