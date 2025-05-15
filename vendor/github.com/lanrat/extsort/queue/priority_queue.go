// Package queue provided a generic priority queue implantation based on the internal heap
package queue

// Priority queue based on
// https://golang.org/pkg/container/heap/#example__priorityQueue

import (
	"container/heap"
	"fmt"
)

// item is a container for holding values with a priority in the queue
type item struct {
	value interface{}
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// innerPriorityQueue implements heap.Interface and holds Items
type innerPriorityQueue struct {
	items    []*item
	lessFunc func(interface{}, interface{}) bool
}

// PriorityQueue implemented using a heap
type PriorityQueue struct {
	ipq innerPriorityQueue
}

// NewPriorityQueue creates a new heap based PriorityQueue using cmpFunc as the comparison function
func NewPriorityQueue(cmpFunc func(interface{}, interface{}) bool) *PriorityQueue {
	var pq PriorityQueue
	pq.ipq.items = make([]*item, 0)
	pq.ipq.lessFunc = cmpFunc
	heap.Init(&pq.ipq)
	return &pq
}

// Len returns the number of items in the queue
func (pq *PriorityQueue) Len() int {
	return pq.ipq.Len()
}

// Push adds x to the queue
func (pq *PriorityQueue) Push(x interface{}) {
	var i item
	i.value = x
	heap.Push(&pq.ipq, i)
	heap.Fix(&pq.ipq, i.index)
}

// Pop removes and returns the next item in the queue
func (pq *PriorityQueue) Pop() interface{} {
	item := heap.Pop(&pq.ipq).(*item)
	return item.value
}

// Peek returns the next item in the queue without removing it
func (pq *PriorityQueue) Peek() interface{} {
	return pq.ipq.items[0].value
}

// PeekUpdate reorders the backing heap with the new values
func (pq *PriorityQueue) PeekUpdate() {
	heap.Fix(&pq.ipq, 0)
}

// Print prints the current ordered queue
func (pq *PriorityQueue) Print() {
	fmt.Print("[")
	for i := range pq.ipq.items {
		fmt.Print(pq.ipq.items[i].value, ", ")

	}
	fmt.Println("]")
}

func (pq *innerPriorityQueue) Len() int {
	return len(pq.items)
}

func (pq *innerPriorityQueue) Less(i, j int) bool {
	return pq.lessFunc(pq.items[i].value, pq.items[j].value)
}

func (pq *innerPriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *innerPriorityQueue) Push(x interface{}) {
	n := len(pq.items)
	i := x.(item)
	i.index = n
	pq.items = append(pq.items, &i)
}

func (pq *innerPriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}
