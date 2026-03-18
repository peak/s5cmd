// Package queue provides a generic priority queue implementation optimized for external sorting.
// It uses Go's container/heap package internally to maintain heap properties efficiently.
// The priority queue supports any type E with a user-provided comparison function.
package queue

// Implementation is based on the Go standard library example:
// https://golang.org/pkg/container/heap/#example__priorityQueue

import (
	"container/heap"
	"fmt"
)

// item is a container for holding values with a priority in the queue
type item[E any] struct {
	value E
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// innerPriorityQueue implements heap.Interface and holds Items
type innerPriorityQueue[E any] struct {
	items       []*item[E]
	compareFunc func(E, E) int
}

// PriorityQueue is a generic priority queue that maintains elements in sorted order
// according to a user-provided comparison function. It provides efficient O(log n)
// insertion and removal of the minimum/maximum element. This implementation is
// specifically optimized for the external sorting use case where elements need
// to be efficiently merged from multiple sorted streams.
type PriorityQueue[E any] struct {
	ipq innerPriorityQueue[E]
}

// NewPriorityQueue creates a new priority queue with the given comparison function.
// The cmpFunc should return a negative integer if the first argument should have higher priority
// (appear earlier) than the second argument, zero if they are equal, and a positive integer
// if the first should appear later. For ascending order, use cmp.Compare(a, b).
// The queue starts empty and elements can be added with Push().
func NewPriorityQueue[E any](cmpFunc func(E, E) int) *PriorityQueue[E] {
	var pq PriorityQueue[E]
	pq.ipq.items = make([]*(item[E]), 0)
	pq.ipq.compareFunc = cmpFunc
	heap.Init(&pq.ipq)
	return &pq
}

// Len returns the current number of elements in the priority queue.
// This operation is O(1).
func (pq *PriorityQueue[E]) Len() int {
	return pq.ipq.Len()
}

// Push adds a new element to the priority queue, maintaining heap properties.
// The element will be positioned according to the comparison function provided
// during queue creation. This operation is O(log n).
func (pq *PriorityQueue[E]) Push(x E) {
	var i item[E]
	i.value = x
	heap.Push(&pq.ipq, i)
	heap.Fix(&pq.ipq, i.index)
}

// Pop removes and returns the highest priority element from the queue.
// The returned element is the one that would be returned by Peek().
// This operation is O(log n). Panics if the queue is empty.
func (pq *PriorityQueue[E]) Pop() E {
	item := heap.Pop(&pq.ipq).(*item[E])
	return item.value
}

// Peek returns the highest priority element without removing it from the queue.
// This allows inspection of the next element that would be returned by Pop().
// This operation is O(1). Panics if the queue is empty.
func (pq *PriorityQueue[E]) Peek() E {
	return pq.ipq.items[0].value
}

// PeekUpdate must be called after modifying the value returned by Peek() in-place.
// This re-establishes the heap property when the priority of the top element changes.
// This is more efficient than Pop() followed by Push() when updating the top element.
// This operation is O(log n).
func (pq *PriorityQueue[E]) PeekUpdate() {
	heap.Fix(&pq.ipq, 0)
}

// Print outputs the current contents of the priority queue to stdout.
// Note that elements are printed in heap order, not priority order.
// This method is primarily intended for debugging purposes.
func (pq *PriorityQueue[E]) Print() {
	fmt.Print("[")
	for i := range pq.ipq.items {
		fmt.Print(pq.ipq.items[i].value, ", ")

	}
	fmt.Println("]")
}

func (pq *innerPriorityQueue[E]) Len() int {
	return len(pq.items)
}

func (pq *innerPriorityQueue[E]) Less(i, j int) bool {
	// TODO make full use of compareFunc returning an int
	return pq.compareFunc(pq.items[i].value, pq.items[j].value) < 0
}

func (pq *innerPriorityQueue[E]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *innerPriorityQueue[E]) Push(x any) {
	n := len(pq.items)
	i := x.(item[E])
	i.index = n
	pq.items = append(pq.items, &i)
}

func (pq *innerPriorityQueue[E]) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}
