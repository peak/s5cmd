package buffer

import (
	"container/list"
	"fmt"
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
	mu            sync.Mutex
}

func NewOrderedBuffer(expectedBytes int64, maxSize int, w io.Writer) *OrderedBuffer {
	return &OrderedBuffer{
		totalExpected: expectedBytes,
		written:       0,
		list:          list.New(),
		maxSize:       maxSize,
		mu:            sync.Mutex{},
		w:             w,
	}
}

func (ob *OrderedBuffer) WriteAt(p []byte, offset int64) (int, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	if ob.list.Front() == nil {
		ob.list.PushBack(&FileChunk{
			Start:   offset,
			Content: p,
		})
		// flush the remaining items
		if offset+int64(len(p)) == ob.totalExpected && ob.written+int64(len(p)) == ob.totalExpected {
			ob.w.Write(p)
			ob.list.Init()
		}

		return len(p), nil
	}
	inserted := false
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(*FileChunk)
		if !ok {
			fmt.Printf("error while casting the node\n")
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
	removeList := make([]*list.Element, 0)
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(*FileChunk)
		if !ok {
			fmt.Printf("error while casting the node\n")
		}
		if v.Start == ob.written {
			n, err := ob.w.Write(v.Content)
			if err != nil {
				fmt.Println("error while writing the next chunk to the writer")
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
