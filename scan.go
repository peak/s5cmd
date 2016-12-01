package main

import (
	"bufio"
	"context"
	"io"
)

type CancelableScanner struct {
	*bufio.Scanner
	data chan string
	err  chan error
	ctx  context.Context
}

func NewCancelableScanner(ctx context.Context, r io.Reader) *CancelableScanner {
	return &CancelableScanner{
		bufio.NewScanner(r),
		make(chan string),
		make(chan error),
		ctx,
	}
}

func (s *CancelableScanner) Start() *CancelableScanner {
	go func() {
		for s.Scan() {
			s.data <- s.Text()
		}
		if err := s.Err(); err != nil {
			s.err <- err
		}
		close(s.data)
		close(s.err)
	}()
	return s
}

func (s *CancelableScanner) ReadOne(from <-chan *Job, to chan<- *Job) (string, error) {
	for {
		select {
		//case to <- <-from:
		case j, ok := <-from:
			if ok {
				to <- j
			}
		case <-s.ctx.Done():
			return "", context.Canceled
		case str, ok := <-s.data:
			if !ok {
				return "", io.EOF
			}
			return str, nil
		case err, ok := <-s.err:
			if !ok {
				return "", io.EOF
			}
			return "", err
		}
	}
}
