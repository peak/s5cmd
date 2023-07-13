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
	head          *list.List
	mu            sync.Mutex
}

func (ob *OrderedBuffer) NewOrderedBuffer(expectedBytes int64, maxSize int, w io.Writer) *OrderedBuffer {
	return &OrderedBuffer{
		totalExpected: expectedBytes,
		written:       0,
		head:          list.New(),
		maxSize:       maxSize,
		mu:            sync.Mutex{},
	}
}

func (ob *OrderedBuffer) WriteAt(p []byte, offset int64) (int, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	for e := ob.head.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(FileChunk)
		if !ok {
			fmt.Printf("error while casting the node")
		}
		if offset < v.Start {
			ob.head.InsertBefore(FileChunk{
				Start:   offset,
				End:     offset + int64(len(p)),
				Content: p,
			}, e)
		}
	}
	before := ob.written
	for e := ob.head.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(FileChunk)
		if !ok {
			fmt.Printf("error while casting the node")
		}
		if v.Start == ob.written {
			n, err := ob.w.Write(v.Content)
			if err != nil {
				fmt.Println("error while writing the next chunk to the writer")
				return n, err
			}
			ob.written += int64(n)
		} else {
			break
		}
	}
	return int(ob.written) - int(before), nil
}
