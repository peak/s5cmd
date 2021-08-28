package sortedslice

import "github.com/peak/s5cmd/storage"

type Slice []*storage.Object

func (o Slice) Len() int {
	return len(o)
}

func (o Slice) Less(i, j int) bool {
	return o[i].URL.ObjectPath() < o[j].URL.ObjectPath()
}

func (o Slice) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}
