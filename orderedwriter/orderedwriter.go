// Package buffer implements an unbounded buffer for ordering concurrent writes for
// non-seekable writers. It keeps an internal linked list that keeps the chunks in order
// and flushes buffered chunks when the smallest expected offset is available.
package orderedwriter

import (
	"container/list"
	"io"
	"sync"
)

type chunk struct {
	offset int64
	value  []byte
}

type OrderedWriterAt struct {
	mu      *sync.Mutex
	list    *list.List
	w       io.Writer
	written int64
}

func New(w io.Writer) *OrderedWriterAt {
	return &OrderedWriterAt{
		mu:      &sync.Mutex{},
		list:    list.New(),
		w:       w,
		written: 0,
	}
}

func (w *OrderedWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.list.Front() == nil {
		// if the queue is empty and the chunk is writeable, push it without queueing
		if w.written == offset {
			n, err := w.w.Write(p)
			if err != nil {
				return n, err
			}
			w.written += int64(n)
			return len(p), nil
		}
		b := make([]byte, len(p))
		copy(b, p)
		w.list.PushBack(&chunk{
			offset: offset,
			value:  b,
		})
		return len(p), nil
	}

	inserted := false
	for e := w.list.Front(); e != nil; e = e.Next() {
		v, _ := e.Value.(*chunk)
		if offset < v.offset {
			b := make([]byte, len(p))
			copy(b, p)
			w.list.InsertBefore(&chunk{
				offset: offset,
				value:  b,
			}, e)
			inserted = true
			break
		}
	}
	if !inserted {
		b := make([]byte, len(p))
		copy(b, p)
		w.list.PushBack(&chunk{
			offset: offset,
			value:  b,
		})
	}
	removeList := make([]*list.Element, 0)
	for e := w.list.Front(); e != nil; e = e.Next() {
		v, _ := e.Value.(*chunk)
		if v.offset != w.written {
			break
		}
		n, err := w.w.Write(v.value)
		if err != nil {
			return n, err
		}
		removeList = append(removeList, e)
		w.written += int64(n)

	}
	for _, e := range removeList {
		w.list.Remove(e)
	}
	return len(p), nil
}
