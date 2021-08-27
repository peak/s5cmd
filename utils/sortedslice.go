package utils

import "github.com/peak/s5cmd/storage"

type SortedObjectSlice []*storage.Object

func (o SortedObjectSlice) Len() int {
	return len(o)
}

func (o SortedObjectSlice) Less(i, j int) bool {
	return o[i].URL.ObjectPath() < o[j].URL.ObjectPath()
}

func (o SortedObjectSlice) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}
