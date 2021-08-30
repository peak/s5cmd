package sortedslice

import "github.com/peak/s5cmd/storage"

// Slice implements the sort.Sort interface
type Slice []*storage.Object

func (o Slice) Len() int {
	return len(o)
}

func (o Slice) Less(i, j int) bool {
	return o[i].URL.Relative() < o[j].URL.Relative()
}

func (o Slice) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}
