// Package buffer implements an unbounded buffer for ordering concurrent writes for
// non-seekable writers. It keeps an internal linked list that keeps the chunks in order
// and flushes buffered chunks when the smallest expected offset is available.
package buffer

import (
	"container/list"
	"io"
	"sync"
)

type fileChunk struct {
	Start   int64
	Content []byte
}

type OrderedWriterAt struct {
	w       io.Writer
	written int64
	list    *list.List
	mu      *sync.Mutex
}

func NewOrderedWriterAt(w io.Writer) *OrderedWriterAt {
	return &OrderedWriterAt{
		written: 0,
		list:    list.New(),
		mu:      &sync.Mutex{},
		w:       w,
	}
}

func (ob *OrderedWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if ob.list.Front() == nil {
		// if the queue is empty and the chunk is writeable, push it without queueing
		if ob.written == offset {
			n, err := ob.w.Write(p)
			if n != len(p) || err != nil {
				return n, err
			}
			ob.written += int64(len(p))
			return len(p), nil
		}
		b := make([]byte, len(p))
		copy(b, p)
		ob.list.PushBack(&fileChunk{
			Start:   offset,
			Content: b,
		})
		return len(p), nil
	}
	inserted := false
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, _ := e.Value.(*fileChunk)
		if offset < v.Start {
			b := make([]byte, len(p))
			copy(b, p)
			ob.list.InsertBefore(&fileChunk{
				Start:   offset,
				Content: b,
			}, e)
			inserted = true
			break
		}
	}
	if !inserted {
		b := make([]byte, len(p))
		copy(b, p)
		ob.list.PushBack(&fileChunk{
			Start:   offset,
			Content: b,
		})
	}
	removeList := make([]*list.Element, 0)
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, _ := e.Value.(*fileChunk)
		if v.Start == ob.written {
			n, err := ob.w.Write(v.Content)
			if err != nil {
				return n, err
			}
			removeList = append(removeList, e)
			ob.written += int64(n)
		} else {
			break
		}
	}
	for _, e := range removeList {
		ob.list.Remove(e)
	}
	return len(p), nil
}
