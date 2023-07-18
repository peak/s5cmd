package buffer

import (
	"container/list"
	"io"
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
	if ob.list.Front() == nil {
		// if the queue is empty and the chunk is writeable, push it without queueing
		if ob.written == offset {
			ob.w.Write(p)
			ob.written += int64(len(p))

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
