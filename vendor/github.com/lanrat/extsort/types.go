package extsort

import "context"

// Sorter is the interface that all extsort sorter implementations must satisfy.
// It provides a single Sort method that performs the complete external sorting operation
// within the provided context, allowing for cancellation and timeout control.
type Sorter interface {
	// Sort performs the complete external sorting operation.
	// It reads from the input channel, sorts the data using temporary storage,
	// and delivers results to the output channel. The operation can be cancelled
	// or timed out using the provided context.
	Sort(context.Context)
}

// FromBytesGeneric is a function type for deserializing bytes back to type E.
// It's used during the merge phase to reconstruct items from temporary storage.
// The function should be the inverse of the corresponding ToBytesGeneric function.
// It returns an error for any deserialization failures, which will be wrapped
// in a DeserializationError by the external sorter.
type FromBytesGeneric[E any] func([]byte) (E, error)

// ToBytesGeneric is a function type for serializing type E to bytes.
// It's used during chunk saving to store items in temporary files.
// The function should produce deterministic output that can be read back
// by the corresponding FromBytesGeneric function. It returns an error for any
// serialization failures, which will be wrapped in a SerializationError by the external sorter.
type ToBytesGeneric[E any] func(E) ([]byte, error)

// CompareGeneric is a function type for comparing two items of type E.
// It must implement a strict weak ordering: reflexivity, antisymmetry, and transitivity.
// Returns a negative integer if a should be ordered before b, zero if they are equal,
// and a positive integer if a should be ordered after b in the final sorted output.
// The function must be consistent and must handle any errors by panicking.
// This follows the same semantics as cmp.Compare and can be implemented using cmp.Compare[T] for ordered types.
type CompareGeneric[E any] func(a, b E) int
