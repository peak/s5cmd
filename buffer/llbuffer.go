package buffer

import (
	"container/list"
	"io"
	"sync"
)

type FileChunk struct {
	Start   int64
	Content []byte
}

type OrderedBuffer struct {
	w             io.Writer
	totalExpected int64
	written       int64
	list          *list.List
	mu            *sync.Mutex
	Queued        int
}

func NewOrderedBuffer(expectedBytes int64, w io.Writer) *OrderedBuffer {
	return &OrderedBuffer{
		totalExpected: expectedBytes,
		written:       0,
		list:          list.New(),
		mu:            &sync.Mutex{},
		w:             w,
		Queued:        0,
	}
}

func (ob *OrderedBuffer) WriteAt(p []byte, offset int64) (int, error) {
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
		ob.list.PushBack(&FileChunk{
			Start:   offset,
			Content: b,
		})
		ob.Queued++
		return len(p), nil
	}
	inserted := false
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(*FileChunk)
		if !ok {
			// NOTE: should we handle this ?
			continue
		}
		if offset < v.Start {
			b := make([]byte, len(p))
			copy(b, p)
			ob.list.InsertBefore(&FileChunk{
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
		ob.list.PushBack(&FileChunk{
			Start:   offset,
			Content: b,
		})
	}
	ob.Queued++
	removeList := make([]*list.Element, 0)
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(*FileChunk)
		if !ok {
			// NOTE: should we handle this ?
			continue
		}
		if v.Start == ob.written {
			n, err := ob.w.Write(v.Content)
			if err != nil {
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
