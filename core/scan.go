package core

import (
	"bufio"
	"context"
	"io"
)

// CancelableScanner is a scanner which also listens for the context cancellations and optionally pumps data between channels
type CancelableScanner struct {
	*bufio.Scanner
	data chan string
	err  chan error
	ctx  context.Context
}

// NewCancelableScanner creates a new CancelableScanner
func NewCancelableScanner(ctx context.Context, r io.Reader) *CancelableScanner {
	return &CancelableScanner{
		bufio.NewScanner(r),
		make(chan string),
		make(chan error),
		ctx,
	}
}

// Start stats the goroutine on the CancelableScanner and returns itself
func (s *CancelableScanner) Start() *CancelableScanner {
	go func() {
		defer func() {
			close(s.data)
			close(s.err)
		}()
		for s.Scan() {
			s.data <- s.Text()
		}
		if err := s.Err(); err != nil {
			s.err <- err
		}
	}()
	return s
}

// ReadOne reads one line from the started CancelableScanner, as well as listening to ctx.Done messages and optionally/continuously pumping *Jobs from the from<- chan to the to<- chan
func (s *CancelableScanner) ReadOne(from <-chan *Job, to chan<- *Job) (string, error) {
	for {
		select {
		// case to <- <-from:
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
