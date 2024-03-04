package extsort

import "context"

// SortType defines the interface required by the extsort library to be able to sort the items
type SortType interface {
	ToBytes() []byte // ToBytes used for marshaling with gob
}

// FromBytes unmarshal bytes from gob to create a SortType when reading back the sorted items
type FromBytes func([]byte) SortType

// CompareLessFunc compares two SortType items and returns true if a is less than b
type CompareLessFunc func(a, b SortType) bool

// Sorter is the interface that all extsort sorters must satisfy
type Sorter interface {
	Sort(context.Context)
}
