// Package orderedwriter implements an unbounded buffer for ordering concurrent writes for
// non-seekable writers. It keeps an internal linked list that keeps the chunks in order
// and flushes buffered chunks when the expected offset is available.
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

	// If the queue is empty and the chunk is writeable, push it without queueing.
	if w.list.Front() == nil && w.written == offset {
		n, err := w.w.Write(p)
		if err != nil {
			return n, err
		}
		w.written += int64(n)
		return len(p), nil
	}

	// Copy the chunk, buffered writers can modify
	// the slice before we consume them.
	b := make([]byte, len(p))
	copy(b, p)

	// If there are no items in the list and we can't write
	// directly push back and return early.
	if w.list.Front() == nil {
		w.list.PushBack(&chunk{
			offset: offset,
			value:  b,
		})
		return len(p), nil
	}

	// Traverse the list from the beginning and insert
	// it to the smallest index possible. That is,
	// compare the element's offset with the offset
	// that you want to buffer.
	var inserted bool
	for e := w.list.Front(); e != nil; e = e.Next() {
		v, _ := e.Value.(*chunk)
		if offset < v.offset {
			w.list.InsertBefore(&chunk{
				offset: offset,
				value:  b,
			}, e)
			inserted = true
			break
		}
	}

	// If the chunk haven't been inserted, put it at
	// the end of the buffer.
	if !inserted {
		w.list.PushBack(&chunk{
			offset: offset,
			value:  b,
		})
	}

	// If the expected offset is buffered,
	// flush the items that you can.
	var removeList []*list.Element
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

	// Remove the items that have been written.
	for _, e := range removeList {
		w.list.Remove(e)
	}

	return len(p), nil
}
