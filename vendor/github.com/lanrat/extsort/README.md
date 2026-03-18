# extsort

[![PkgGoDev](https://pkg.go.dev/badge/github.com/lanrat/extsort)](https://pkg.go.dev/github.com/lanrat/extsort)
[![Go Report Card](https://goreportcard.com/badge/github.com/lanrat/extsort)](https://goreportcard.com/report/github.com/lanrat/extsort)

A high-performance [external sorting](https://en.wikipedia.org/wiki/External_sorting) library for Go that enables sorting of arbitrarily large datasets that don't fit entirely in memory. The library operates on channels and uses temporary disk storage to handle datasets larger than available RAM.

## Features

- **Memory Efficient**: Sorts datasets of any size using configurable memory limits
- **High Performance**: Optimized for throughput with parallel sorting and merging
- **Generic Support**: Modern Go generics for type-safe operations
- **Legacy Compatible**: Maintains backward compatibility with interface-based API
- **Cross-Platform**: Works on Unix, Linux, macOS, and Windows
- **Channel-Based**: Integrates seamlessly with Go's concurrency patterns

## Installation

```bash
go get github.com/lanrat/extsort
```

## Quick Start

### Generic API (Recommended)

The modern generic API provides type safety and improved performance:

```go
import (
    "context"
    "fmt"
    "math/rand"

    "github.com/lanrat/extsort"
)

func main() {
    // Create input channel with unsorted integers
    inputChan := make(chan int, 100)
    go func() {
        defer close(inputChan)
        for i := 0; i < 1000000; i++ {
            inputChan <- rand.Int()
        }
    }()

    // Sort using the generic API
    sorter, outputChan, errChan := extsort.Ordered(inputChan, nil)

    // Start sorting in background
    go sorter.Sort(context.Background())

    // Process sorted results
    for value := range outputChan {
        fmt.Println(value)
    }

    // Check for errors
    if err := <-errChan; err != nil {
        panic(err)
    }
}
```

### String Sorting

For string data, use the optimized string sorter:

```go
import (
    "context"
    "fmt"

    "github.com/lanrat/extsort"
)

func main() {
    words := []string{"zebra", "apple", "banana", "cherry"}

    inputChan := make(chan string, len(words))
    for _, word := range words {
        inputChan <- word
    }
    close(inputChan)

    sorter, outputChan, errChan := extsort.Strings(inputChan, nil)
    go sorter.Sort(context.Background())

    fmt.Println("Sorted words:")
    for word := range outputChan {
        fmt.Println(word)
    }

    if err := <-errChan; err != nil {
        panic(err)
    }
}
```

### Custom Types with Generic API

```go
import (
    "bytes"
    "context"
    "encoding/gob"
    "fmt"

    "github.com/lanrat/extsort"
)

type Person struct {
    Name string
    Age  int
}

func personToBytes(p Person) ([]byte, error) {
    var buf bytes.Buffer
    enc := gob.NewEncoder(&buf)
    err := enc.Encode(p)
    return buf.Bytes(), err
}

func personFromBytes(data []byte) (Person, error) {
    var p Person
    buf := bytes.NewReader(data)
    dec := gob.NewDecoder(buf)
    err := dec.Decode(&p)
    return p, err
}

func comparePersonsByAge(a, b Person) int {
    // Sort by age
    if a.Age != b.Age {
        if a.Age < b.Age {
            return -1
        }
        return 1
    }
    return 0
}

func main() {
    people := []Person{
        {"Alice", 30},
        {"Bob", 25},
        {"Charlie", 35},
    }

    inputChan := make(chan Person, len(people))
    for _, person := range people {
        inputChan <- person
    }
    close(inputChan)

    sorter, outputChan, errChan := extsort.Generic(
        inputChan,
        personFromBytes,
        personToBytes,
        comparePersonsByAge,
        nil,
    )

    go sorter.Sort(context.Background())

    fmt.Println("People sorted by age:")
    for person := range outputChan {
        fmt.Printf("%s (age %d)\n", person.Name, person.Age)
    }

    if err := <-errChan; err != nil {
        panic(err)
    }
}
```

## Configuration

Customize sorting behavior with the Config struct:

```go
config := &extsort.Config{
    ChunkSize:          500000,  // Records per chunk (default: 1M)
    NumWorkers:         4,       // Parallel sorting/merging workers (default: 2)
    ChanBuffSize:       10,      // Channel buffer size (default: 1)
    SortedChanBuffSize: 1000,    // Output channel buffer (default: 1000)
    TempFilesDir:       "/var/tmp",  // Temporary files directory (default: intelligent selection)
}

sorter, outputChan, errChan := extsort.Ordered(inputChan, config)
```

### Temporary Directory Selection

When `TempFilesDir` is empty (default), the library intelligently selects temporary directories that prefer disk-backed locations over potentially memory-backed filesystems. On Linux systems where `/tmp` may be mounted as tmpfs (memory-backed), this helps prevent out-of-memory issues when sorting datasets larger than available RAM.

**For production use with large datasets, it's recommended to explicitly set `TempFilesDir` to a known disk-backed directory** (such as `/var/tmp` on Unix systems) to ensure optimal performance and avoid memory limitations.

## Legacy Interface-Based API

The library maintains backward compatibility with the original interface-based API:

```go
import (
    "context"
    "encoding/binary"
    "fmt"
    "math/rand"

    "github.com/lanrat/extsort"
)

type sortInt struct {
    value int64
}

func (s sortInt) ToBytes() []byte {
    buf := make([]byte, 8)
    binary.LittleEndian.PutUint64(buf, uint64(s.value))
    return buf
}

func sortIntFromBytes(data []byte) extsort.SortType {
    value := int64(binary.LittleEndian.Uint64(data))
    return sortInt{value: value}
}

func compareSortInt(a, b extsort.SortType) bool {
    return a.(sortInt).value < b.(sortInt).value
}

func main() {
    inputChan := make(chan extsort.SortType, 100)
    go func() {
        defer close(inputChan)
        for i := 0; i < 100000; i++ {
            inputChan <- sortInt{value: rand.Int63()}
        }
    }()

    sorter, outputChan, errChan := extsort.New(
        inputChan,
        sortIntFromBytes,
        compareSortInt,
        nil,
    )

    go sorter.Sort(context.Background())

    for item := range outputChan {
        fmt.Println(item.(sortInt).value)
    }

    if err := <-errChan; err != nil {
        panic(err)
    }
}
```

## Diff Sub-Package

The `diff` sub-package provides functionality for comparing two sorted data streams and identifying differences. It's particularly useful for comparing large datasets efficiently.

### Basic String Diff

```go
import (
    "context"
    "fmt"

    "github.com/lanrat/extsort/diff"
)

func main() {
    // Create two sorted string channels
    streamA := make(chan string, 5)
    streamB := make(chan string, 5)

    // Populate stream A
    go func() {
        defer close(streamA)
        for _, item := range []string{"apple", "banana", "cherry", "elderberry"} {
            streamA <- item
        }
    }()

    // Populate stream B
    go func() {
        defer close(streamB)
        for _, item := range []string{"banana", "cherry", "date", "fig"} {
            streamB <- item
        }
    }()

    // Create error channels
    errA := make(chan error, 1)
    errB := make(chan error, 1)
    close(errA)
    close(errB)

    // Process differences
    result, err := diff.Strings(
        context.Background(),
        streamA, streamB,
        errA, errB,
        func(delta diff.Delta, item string) error {
            switch delta {
            case diff.OLD:
                fmt.Printf("Only in A: %s\n", item)
            case diff.NEW:
                fmt.Printf("Only in B: %s\n", item)
            }
            return nil
        },
    )

    if err != nil {
        panic(err)
    }

    fmt.Printf("Summary: %d items only in A, %d items only in B, %d common items\n",
        result.ExtraA, result.ExtraB, result.Common)
}
```

### Generic Diff for Custom Types

```go
import (
    "context"
    "fmt"

    "github.com/lanrat/extsort/diff"
)

func main() {
    // Create channels with integer data
    streamA := make(chan int, 5)
    streamB := make(chan int, 5)
    errA := make(chan error, 1)
    errB := make(chan error, 1)

    // Populate streams
    go func() {
        defer close(streamA)
        defer close(errA)
        for _, num := range []int{1, 3, 5, 7, 9} {
            streamA <- num
        }
    }()

    go func() {
        defer close(streamB)
        defer close(errB)
        for _, num := range []int{2, 4, 5, 6, 8} {
            streamB <- num
        }
    }()

    // Compare using generic diff
    compareFunc := func(a, b int) int {
        if a < b {
            return -1
        }
        if a > b {
            return 1
        }
        return 0
    }

    resultFunc := func(delta diff.Delta, item int) error {
        symbol := map[diff.Delta]string{diff.OLD: "<", diff.NEW: ">"}[delta]
        fmt.Printf("%s %d\n", symbol, item)
        return nil
    }

    result, err := diff.Generic(
        context.Background(),
        streamA, streamB,
        errA, errB,
        compareFunc,
        resultFunc,
    )

    if err != nil {
        panic(err)
    }

    fmt.Printf("Differences found: %d\n", result.ExtraA+result.ExtraB)
}
```

### Parallel Diff Processing

```go
import (
    "context"
    "fmt"
    "sync"

    "github.com/lanrat/extsort/diff"
)

func main() {
    streamA := make(chan string, 100)
    streamB := make(chan string, 100)
    errA := make(chan error, 1)
    errB := make(chan error, 1)

    // Populate streams with test data
    go func() {
        defer close(streamA)
        defer close(errA)
        for i := 0; i < 50; i += 2 {
            streamA <- fmt.Sprintf("item_%03d", i)
        }
    }()

    go func() {
        defer close(streamB)
        defer close(errB)
        for i := 1; i < 50; i += 2 {
            streamB <- fmt.Sprintf("item_%03d", i)
        }
    }()

    // Use channel-based result processing for parallel handling
    resultFunc, resultChan := diff.StringResultChan()

    var wg sync.WaitGroup
    wg.Add(1)

    // Process results in parallel
    go func() {
        defer wg.Done()
        for result := range resultChan {
            fmt.Printf("Difference: %s %s\n", result.D, result.S)
        }
    }()

    // Start diff operation
    go func() {
        defer close(resultChan)
        _, err := diff.Strings(
            context.Background(),
            streamA, streamB,
            errA, errB,
            resultFunc,
        )
        if err != nil {
            fmt.Printf("Diff error: %v\n", err)
        }
    }()

    wg.Wait()
    fmt.Println("Diff processing complete")
}
```

## Performance Considerations

- **Memory Usage**: Configure `ChunkSize` based on available memory (larger chunks = less I/O, more memory)
- **Parallelism**: Increase `NumWorkers` on multi-core systems
- **Temporary Storage**:
  - Explicitly set `TempFilesDir` to a known disk-backed directory for large datasets
  - On Linux, prefer `/var/tmp` over `/tmp` (which may be tmpfs/memory-backed)
  - Use fast storage (SSD recommended) for temporary files
- **Channel Buffers**: Tune buffer sizes based on your producer/consumer patterns

## Error Handling

The library uses Go's standard error handling patterns. Always check the error channel:

```go
sorter, outputChan, errChan := extsort.Ordered(inputChan, nil)
go sorter.Sort(context.Background())

for item := range outputChan {
    // Process sorted item
}

if err := <-errChan; err != nil {
    // Handle error
    log.Fatal(err)
}
```

## Limitations

- **Not Stable**: The sort is not stable (equal elements may change relative order)
- **Disk Space**: Requires temporary disk space approximately equal to input data size
- **Memory**: Minimum memory usage depends on chunk size configuration

## License

This project is licensed under the Apache License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
