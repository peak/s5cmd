# extsort

[![PkgGoDev](https://pkg.go.dev/badge/github.com/lanrat/extsort)](https://pkg.go.dev/github.com/lanrat/extsort)

[![Go Report Card](https://goreportcard.com/badge/github.com/lanrat/extsort)](https://goreportcard.com/report/github.com/lanrat/extsort)

An [external sorting](https://en.wikipedia.org/wiki/External_sorting) library for golang (i.e. on disk sorting) on an arbitrarily channel, even if the generated content doesn't all fit in memory at once. Once sorted, it returns a new channel returning data in sorted order.

In order to remain efficient for all implementations, extsort doesn't handle serialization, but leaves that to the user by operating on types that implement the [`SortType.ToBytes`](https://pkg.go.dev/github.com/lanrat/extsort#SortType) and [`FromBytes`](https://pkg.go.dev/github.com/lanrat/extsort#FromBytes) interfaces.

extsort also has a `Strings()` interface which does not require the overhead of converting everything to/from bytes and is faster for string types.

extsort is not a [stable sort](https://en.wikipedia.org/wiki/Sorting_algorithm#Stability).

## Example

```go
package main

import (
    "encoding/binary"
    "fmt"
    "math/rand"

    "github.com/lanrat/extsort"
)

var count = int(1e7) // 10M

type sortInt struct {
    i int64
}

func (s sortInt) ToBytes() []byte {
    buf := make([]byte, binary.MaxVarintLen64)
    binary.PutVarint(buf, s.i)
    return buf
}

func sortIntFromBytes(b []byte) extsort.SortType {
    i, _ := binary.Varint(b)
    return sortInt{i: i}
}

func compareSortIntLess(a, b extsort.SortType) bool {
    return a.(sortInt).i < b.(sortInt).i
}

func main() {
    // create an input channel with unsorted data
    inputChan := make(chan extsort.SortType)
    go func() {
        for i := 0; i < count; i++ {
            inputChan <- sortInt{i: rand.Int63()}
        }
        close(inputChan)
    }()

    // create the sorter and start sorting
    sorter, outputChan, errChan := extsort.New(inputChan, sortIntFromBytes, compareSortIntLess, nil)
    sorter.Sort(context.Background())

    // print output sorted data
    for data := range outputChan {
        fmt.Printf("%d\n", data.(sortInt).i)
    }
    if err := <-errChan; err != nil {
        fmt.Printf("err: %s", err.Error())
    }
}
```

## [extsort/diff](https://pkg.go.dev/github.com/lanrat/extsort/diff)

The diff sub-package is a self-contained package that assists with diffing of channels of sorted data. It can be used with the extsort methods or on its own

## TODO

* parallelize merging after sorting
