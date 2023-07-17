package buffer

import (
	"container/list"
	"fmt"
	"io"
	"sync"

	"github.com/peak/s5cmd/v2/log"
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

func debugMessage(s string) {
	return
	fmt.Printf("%v\n", log.DebugMessage{
		Operation: "Get",
		Command:   "cat",
		Err:       s,
	})
}

func (ob *OrderedBuffer) WriteAt(p []byte, offset int64) (int, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	debugMessage(fmt.Sprintf("WriteAt with offset: %d, len:%d", offset, len(p)))
	if ob.list.Front() == nil {
		// if the queue is empty and the chunk is writeable, push it without queueing
		if ob.written == offset {
			debugMessage(fmt.Sprintf("WriteAt bypass-write with offset: %d, len:%d", offset, len(p)))
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
			debugMessage("error while casting the node\n")
		}
		if offset < v.Start {
			debugMessage(fmt.Sprintf("\tWriteAt insert before %d,  offset: %d, len:%d", v.Start, offset, len(p)))
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
		debugMessage(fmt.Sprintf("\tWriteAt push back offset: %d, len:%d", offset, len(p)))
	}
	ob.Queued++
	debugMessage(fmt.Sprintf("\tWriteAt Total Queued after writes: %d", ob.Queued))
	removeList := make([]*list.Element, 0)
	for e := ob.list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(*FileChunk)
		if !ok {
			debugMessage("error while casting the node\n")
		}
		if v.Start == ob.written {
			debugMessage(fmt.Sprintf("\tWriteAt read offset %d, len:%d", v.Start, len(v.Content)))
			n, err := ob.w.Write(v.Content)
			if err != nil {
				debugMessage("error while writing the next chunk to the writer")
				return n, err
			}
			removeList = append(removeList, e)
			ob.written += int64(n)
			ob.Queued--
		} else {
			break
		}
	}
	debugMessage(fmt.Sprintf("\tWriteAt Total Queued after reads: %d", ob.Queued))
	for _, e := range removeList {
		ob.list.Remove(e)
	}
	return len(p), nil
}
