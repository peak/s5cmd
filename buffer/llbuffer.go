package buffer

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"sync"
)

type FileChunk struct {
	Start   int64
	End     int64
	Content []byte
}

type OrderedBuffer struct {
	w             io.Writer
	totalExpected int64
	written       int64
	maxSize       int
	list          *list.List
	mu            *sync.Mutex
	Queued        int
}

func NewOrderedBuffer(expectedBytes int64, maxSize int, w io.Writer) *OrderedBuffer {
	return &OrderedBuffer{
		totalExpected: expectedBytes,
		written:       0,
		list:          list.New(),
		maxSize:       maxSize,
		mu:            &sync.Mutex{},
		w:             w,
		Queued:        0,
	}
}

func (ob *OrderedBuffer) WriteAt(p []byte, offset int64) (int, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	fmt.Fprintf(os.Stderr, "***** call: offset:%d, len(p):%d\n", offset, len(p))
	if ob.list.Front() == nil {
		// if the queue is empty and the chunk is writeable, push it without queueing
		if ob.written == offset {
			n, err := ob.w.Write(p)
			if err != nil {
				fmt.Fprintf(os.Stderr, "***** error: while writing to stdout. err: %v, n:%d lenContent:%d\n", err, n, len(p))
			}
			ob.written += int64(len(p))
			fmt.Fprintf(os.Stderr, "***** stdout: offset:%d, len(p):%d\n", offset, len(p))
			return len(p), nil
		}
		ob.list.PushBack(&FileChunk{
			Start:   offset,
			Content: p,
		})
		ob.Queued++
		return len(p), nil
	}
	inserted := false
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(*FileChunk)
		if !ok {
		}
		if offset < v.Start {
			ob.list.InsertBefore(&FileChunk{
				Start:   offset,
				Content: p,
			}, e)
			inserted = true
			break
		}
	}
	if !inserted {
		ob.list.PushBack(&FileChunk{
			Start:   offset,
			Content: p,
		})
	}
	ob.Queued++
	removeList := make([]*list.Element, 0)
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(*FileChunk)
		if !ok {
			fmt.Fprintf(os.Stderr, "***** error: can't cast\n")
			// NOTE: should we handle this ?
			continue
		}
		if v.Start == ob.written {
			n, err := ob.w.Write(v.Content)
			fmt.Fprintf(os.Stderr, "***** stdout: offset:%d, len(p):%d\n", v.Start, len(v.Content))
			if err != nil {
				fmt.Fprintf(os.Stderr, "***** error: while writing to stdout. err: %v n:%d, lenContent:%d\n", err, n, len(v.Content))
				return n, err
			}
			removeList = append(removeList, e)
			ob.written += int64(n)
			ob.Queued--
		} else {
			break
		}
	}
	for _, e := range removeList {
		ob.list.Remove(e)
	}
	return len(p), nil
}
