package buffer

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

const (
	testRuns = 64
)

func TestShuffleWriteWithStaticChunkSize(t *testing.T) {
	t.Parallel()
	for b := 0; b < testRuns; b++ {
		t.Run(fmt.Sprintf("Run%d", b), func(t *testing.T) {
			t.Parallel()
			var result bytes.Buffer
			chunkSize := 5
			fileSize := 1000
			expected := make([]byte, fileSize)

			for i := 0; i < fileSize; i++ {
				ch := rand.Intn(255)
				expected[i] = uint8(ch)
			}

			tasks := []fileChunk{}
			// first create chunks number of tasks
			// Create tasks and put them in the array
			for i := 0; i < fileSize; i += chunkSize {
				task := fileChunk{
					Start:   int64(i),
					Content: expected[i : i+chunkSize],
				}
				tasks = append(tasks, task)
			}

			// shuffle
			for i := range tasks {
				j := rand.Intn(i + 1)
				tasks[i], tasks[j] = tasks[j], tasks[i]
			}

			buff := NewOrderedWriterAt(&result)

			for _, task := range tasks {
				buff.WriteAt(task.Content, int64(task.Start))

			}
			// Ensure all chunks have been written correctly
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
				cmp.Diff(result.Bytes(), expected)
			}
		})
	}
}

func TestShuffleWriteWithRandomChunkSize(t *testing.T) {
	t.Parallel()
	maxFileSize := 1024 * 100
	minChunkSize, maxChunkSize := 5, 1000
	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("TR%d", b), func(t *testing.T) {
			t.Parallel()
			var result bytes.Buffer
			var offset int64
			expected := []byte{}
			chunks := []fileChunk{}
			// generate chunks
			for i := 0; i <= maxFileSize; {
				chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				chunk := make([]byte, chunkSize)
				for j := 0; j < chunkSize; j++ {
					chunk[j] = uint8(rand.Intn(256))
				}
				chunks = append(chunks, fileChunk{
					Start:   offset,
					Content: chunk,
				})
				expected = append(expected, chunk...)
				offset += int64(chunkSize)
				i += chunkSize
			}
			// shuffle the chunks
			for i := range chunks {
				j := rand.Intn(i + 1)
				chunks[i], chunks[j] = chunks[j], chunks[i]
			}

			buff := NewOrderedWriterAt(&result)

			for _, task := range chunks {
				buff.WriteAt(task.Content, int64(task.Start))
			}
			// Ensure all chunks have been written correctly
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
				cmp.Diff(result.Bytes(), expected)
			}
		})
	}

}

func TestShuffleConcurrentWriteWithRandomChunkSize(t *testing.T) {
	t.Parallel()
	maxFileSize := 1024
	minChunkSize, maxChunkSize := 5, 100
	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("Run%d", b), func(t *testing.T) {
			t.Parallel()
			var result bytes.Buffer
			var offset int64
			expected := []byte{}
			chunks := []fileChunk{}
			// generate chunks
			for i := 0; i <= maxFileSize; {
				chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				chunk := make([]byte, chunkSize)
				for j := 0; j < chunkSize; j++ {
					chunk[j] = uint8(rand.Intn(256))
				}
				chunks = append(chunks, fileChunk{
					Start:   offset,
					Content: chunk,
				})
				expected = append(expected, chunk...)
				offset += int64(chunkSize)
				i += chunkSize
			}
			// shuffle the chunks
			for i := range chunks {
				j := rand.Intn(i + 1)
				chunks[i], chunks[j] = chunks[j], chunks[i]
			}

			buff := NewOrderedWriterAt(&result)

			chunkChan := make(chan fileChunk, 1)

			var wg sync.WaitGroup
			workerCount := 5 + rand.Intn(20) // 5-25 workers
			for i := 0; i < workerCount; i++ {
				wg.Add(1)
				go func(ch chan fileChunk) {
					defer wg.Done()
					for task := range ch {
						buff.WriteAt(task.Content, int64(task.Start))
					}
				}(chunkChan)
			}

			for _, chunk := range chunks {
				chunkChan <- chunk
			}
			close(chunkChan)
			wg.Wait()

			// Ensure all chunks have been written correctly
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
				cmp.Diff(result.Bytes(), expected)
			}
		})
	}
}

// when you use io.Copy, the slice it gives you changes in time.
// You should copy the slice when a OrderedWriterAt.WriteAt
// call occurs and don't count on the given slice's Content.

// We will simulate the aws-sdk-go's download manager algorithm.
// See: https://github.com/aws/aws-sdk-go/blob/main/service/s3/s3manager/download.go

// dlchunk represents a single chunk of data to write by the worker routine.
// This structure also implements an io.SectionReader style interface for
// io.WriterAt, effectively making it an io.SectionWriter (which does not
// exist).
// NOTE: Content is added for testing, it's not present in AWS-SDK-GO
type dlchunk struct {
	w       io.WriterAt
	Start   int64
	size    int64
	cur     int64
	Content []byte
}

// Write wraps io.WriterAt for the dlchunk, writing from the dlchunk's Start
// position to its end (or EOF).
//
// If a range is specified on the dlchunk the size will be ignored when writing.
// as the total size may not of be known ahead of time.
func (c *dlchunk) Write(p []byte) (n int, err error) {
	if c.cur >= c.size {
		return 0, io.EOF
	}

	n, err = c.w.WriteAt(p, c.Start+c.cur)
	c.cur += int64(n)

	return
}
func TestBufferWithChangingSlice(t *testing.T) {
	t.Parallel()
	maxFileSize := 1024 * 1024
	testRuns := 1
	minChunkSize, maxChunkSize := 5, 100
	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("Run%d", b), func(t *testing.T) {
			t.Parallel()
			var expectedFileSize int64
			expected := []byte{}
			chunks := []dlchunk{}

			// generate chunks
			for i := 0; i <= maxFileSize; {
				chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				chunk := make([]byte, chunkSize)
				for j := 0; j < chunkSize; j++ {
					chunk[j] = uint8(rand.Intn(256))
				}
				// we use the expectedFileSize to track the offset
				chunks = append(chunks, dlchunk{
					Start:   int64(expectedFileSize),
					size:    int64(chunkSize),
					Content: chunk,
				})
				expected = append(expected, chunk...)
				expectedFileSize += int64(chunkSize)
				i += chunkSize
			}
			// shuffle the chunks
			for i := range chunks {
				j := rand.Intn(i + 1)
				chunks[i], chunks[j] = chunks[j], chunks[i]
			}

			result := bytes.NewBuffer(make([]byte, 0, expectedFileSize))
			buff := NewOrderedWriterAt(result)

			// we set the writerAt after we initialize the buff, since
			// the buff can't be initialized until the expectedFileSize
			// is known.
			for i := 0; i < len(chunks); i++ {
				chunks[i].w = buff
			}
			chunkChan := make(chan dlchunk, 1)

			var wg sync.WaitGroup
			var blockingTask dlchunk
			workerCount := 5 + rand.Intn(20) // 5-25 workers
			for i := 0; i < workerCount; i++ {
				wg.Add(1)
				go func(ch chan dlchunk) {
					defer wg.Done()
					for task := range ch {
						if task.Start == 0 {
							blockingTask = task
							continue
						}
						r := bytes.NewReader(task.Content)
						io.Copy(&task, r)
					}
				}(chunkChan)
			}

			for _, chunk := range chunks {
				chunkChan <- chunk
			}
			close(chunkChan)
			wg.Wait()
			// block all chunks except the first one
			r := bytes.NewReader(blockingTask.Content)
			io.Copy(&blockingTask, r)
			// Ensure all chunks have been written correctly
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
				cmp.Diff(result.Bytes(), expected)
			}
		})
	}
}

func BenchmarkBufferWithChangingSlice(bench *testing.B) {
	maxFileSize := 1024 * 1024
	minChunkSize, maxChunkSize := 5, 100
	for b := 0; b < bench.N; b++ {
		var expectedFileSize int64
		expected := []byte{}
		chunks := []dlchunk{}

		// generate chunks
		for i := 0; i <= maxFileSize; {
			chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
			chunk := make([]byte, chunkSize)
			for j := 0; j < chunkSize; j++ {
				chunk[j] = uint8(rand.Intn(256))
			}
			chunks = append(chunks, dlchunk{
				Start:   int64(expectedFileSize),
				size:    int64(chunkSize),
				Content: chunk,
			})
			expected = append(expected, chunk...)
			expectedFileSize += int64(chunkSize)
			i += chunkSize
		}
		// shuffle the chunks
		for i := range chunks {
			j := rand.Intn(i + 1)
			chunks[i], chunks[j] = chunks[j], chunks[i]
		}
		result := bytes.NewBuffer(make([]byte, 0, expectedFileSize))
		buff := NewOrderedWriterAt(result)

		// we set the writerAt after we initialize the buff, since
		// the buff can't be initialized until the expectedFileSize
		// is known.
		for i := 0; i < len(chunks); i++ {
			chunks[i].w = buff
		}
		chunkChan := make(chan dlchunk, 1)

		var wg sync.WaitGroup
		var blockingTask dlchunk
		workerCount := 5 + rand.Intn(20) // 5-25 workers
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func(ch chan dlchunk) {
				defer wg.Done()
				for task := range ch {
					if task.Start == 0 {
						blockingTask = task
						continue
					}
					r := bytes.NewReader(task.Content)
					io.Copy(&task, r)
				}
			}(chunkChan)
		}

		for _, chunk := range chunks {
			chunkChan <- chunk
		}
		close(chunkChan)
		wg.Wait()
		// block all chunks except the first one
		r := bytes.NewReader(blockingTask.Content)
		io.Copy(&blockingTask, r)
		if !bytes.Equal(result.Bytes(), expected) {
			bench.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
			cmp.Diff(result.Bytes(), expected)
		}
	}
}
