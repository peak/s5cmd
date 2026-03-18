package extsort

import (
	"bytes"
	"cmp"
	"encoding/gob"
	"sync"
)

// OrderedSorter provides external sorting for types that implement cmp.Ordered.
// It embeds GenericSorter and adds optimized byte serialization using gob encoding
// with a sync.Pool for buffer reuse to reduce allocations.
type OrderedSorter[T cmp.Ordered] struct {
	GenericSorter[T]
	bufferPool sync.Pool
}

// newOrderedSorter creates a new OrderedSorter with an initialized buffer pool
// for efficient gob encoding/decoding operations.
func newOrderedSorter[T cmp.Ordered]() *OrderedSorter[T] {
	s := &OrderedSorter[T]{
		bufferPool: sync.Pool{
			New: func() any {
				return &bytes.Buffer{}
			},
		},
	}
	return s
}

// fromBytesOrdered deserializes a byte slice back to the original type T
// using gob decoding. It reuses buffers from the pool for efficiency.
// Returns an error if decoding fails.
func (s *OrderedSorter[T]) fromBytesOrdered(d []byte) (T, error) {
	var v T
	buf := s.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Write(d)
	defer s.bufferPool.Put(buf)

	dec := gob.NewDecoder(buf)
	err := dec.Decode(&v)
	return v, err
}

// toBytesOrdered serializes a value of type T to bytes using gob encoding.
// It reuses buffers from the pool and returns a copy of the serialized data.
// Returns an error if encoding fails.
func (s *OrderedSorter[T]) toBytesOrdered(d T) ([]byte, error) {
	buf := s.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer s.bufferPool.Put(buf)

	enc := gob.NewEncoder(buf)
	err := enc.Encode(d)
	if err != nil {
		return nil, err
	}

	// Need to copy the bytes since we're returning the buffer to the pool
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// Ordered performs external sorting on a channel of cmp.Ordered types.
// It returns the sorter instance, output channel with sorted results, and error channel.
// Uses gob encoding for serialization and the < operator for comparison.
func Ordered[T cmp.Ordered](input <-chan T, config *Config) (*OrderedSorter[T], <-chan T, <-chan error) {
	orderedSorter := newOrderedSorter[T]()
	s, output, errChan := Generic(input, orderedSorter.fromBytesOrdered, orderedSorter.toBytesOrdered, cmp.Compare, config)
	if s == nil {
		return nil, output, errChan
	}
	orderedSorter.GenericSorter = *s
	return orderedSorter, output, errChan
}

// OrderedMock performs external sorting with a mock implementation that limits
// the number of items to sort (useful for testing). Takes the same parameters as
// Ordered plus n which limits the number of items processed.
func OrderedMock[T cmp.Ordered](input <-chan T, config *Config, n int) (*OrderedSorter[T], <-chan T, <-chan error) {
	orderedSorter := newOrderedSorter[T]()
	s, output, errChan := MockGeneric(input, orderedSorter.fromBytesOrdered, orderedSorter.toBytesOrdered, cmp.Compare, config, n)
	if s == nil {
		return nil, output, errChan
	}
	orderedSorter.GenericSorter = *s
	return orderedSorter, output, errChan
}
