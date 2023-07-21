package buffer_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/peak/s5cmd/v2/buffer"
)

type FileChunk struct {
	start   int
	end     int
	content []byte
}

func TestShuffleWriteWithStaticChunkSize(t *testing.T) {
	t.Parallel()
	testRuns := 64
	for b := 0; b < testRuns; b++ {
		t.Run(fmt.Sprintf("Run%d", b), func(t *testing.T) {
			var result bytes.Buffer
			chunkSize := 5
			fileSize := 1000
			expected := make([]byte, fileSize)
			var expectedFileSize int64

			for i := 0; i < fileSize; i++ {
				ch := rand.Intn(255)
				expected[i] = uint8(ch)
			}

			tasks := []FileChunk{}
			// first create chunks number of tasks
			// Create tasks and put them in the array
			for i := 0; i < fileSize; i += chunkSize {
				task := FileChunk{
					start:   i,
					end:     i + chunkSize,
					content: expected[i : i+chunkSize],
				}
				tasks = append(tasks, task)
			}

			// shuffle
			for i := range tasks {
				j := rand.Intn(i + 1)
				tasks[i], tasks[j] = tasks[j], tasks[i]
			}

			rb := buffer.NewOrderedBuffer(expectedFileSize, &result)

			for _, task := range tasks {
				rb.WriteAt(task.content, int64(task.start))

			}
			// Ensure all chunks have been written correctly
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
				t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
			}

			// Check if there are any leftover chunks in the OrderedBuffer
			remainingChunks := rb.Queued
			if remainingChunks != 0 {
				t.Errorf("OrderedBuffer contains %d leftover chunks", remainingChunks)
			}
		})
	}
}

func TestShuffleWriteWithRandomChunkSize(t *testing.T) {
	t.Parallel()
	maxFileSize := 1024 * 100
	testRuns := 64
	minChunkSize, maxChunkSize := 5, 1000
	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("TR%d", b), func(t *testing.T) {
			var result bytes.Buffer
			var expectedFileSize int64
			expected := []byte{}
			chunks := []FileChunk{}
			// generate chunks
			for i := 0; i <= maxFileSize; {
				chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				chunk := make([]byte, chunkSize)
				for j := 0; j < chunkSize; j++ {
					chunk[j] = uint8(rand.Intn(256))
				}
				chunks = append(chunks, FileChunk{
					start:   int(expectedFileSize),
					content: chunk,
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

			// currently the max buffer size is disabled
			rb := buffer.NewOrderedBuffer(expectedFileSize, &result)

			for _, task := range chunks {
				rb.WriteAt(task.content, int64(task.start))

			}

			// Ensure all chunks have been written correctly
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
				t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
			}

			// Check if there are any leftover chunks in the OrderedBuffer
			remainingChunks := rb.Queued
			if remainingChunks != 0 {
				t.Errorf("OrderedBuffer contains %d leftover chunks", remainingChunks)
			}
		})
	}

}

func TestShuffleConcurrentWriteWithRandomChunkSize(t *testing.T) {
	t.Parallel()
	maxFileSize := 1024
	testRuns := 64
	minChunkSize, maxChunkSize := 5, 100
	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("Run%d", b), func(t *testing.T) {
			var result bytes.Buffer
			var expectedFileSize int64
			expected := []byte{}
			chunks := []FileChunk{}
			// generate chunks
			for i := 0; i <= maxFileSize; {
				chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				chunk := make([]byte, chunkSize)
				for j := 0; j < chunkSize; j++ {
					chunk[j] = uint8(rand.Intn(256))
				}
				chunks = append(chunks, FileChunk{
					start:   int(expectedFileSize),
					content: chunk,
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

			rb := buffer.NewOrderedBuffer(expectedFileSize, &result)

			chunkChan := make(chan FileChunk, 1)

			var wg sync.WaitGroup
			workerCount := 5 + rand.Intn(20) // 5-25 workers
			for i := 0; i < workerCount; i++ {
				wg.Add(1)
				go func(ch chan FileChunk) {
					defer wg.Done()
					for task := range ch {
						rb.WriteAt(task.content, int64(task.start))
					}
				}(chunkChan)
			}

			for _, chunk := range chunks {
				chunkChan <- chunk
			}
			close(chunkChan)
			wg.Wait()

			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
				t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
			}

			// Check if there are any leftover chunks in the OrderedBuffer
			remainingChunks := rb.Queued
			if remainingChunks != 0 {
				t.Errorf("OrderedBuffer contains %d leftover chunks", remainingChunks)
			}
		})
	}
}

// when you use io.Copy, the slice it gives you changes in time.
// You should copy the slice when a OrderedBuffer.WriteAt
// call occurs and don't count on the given slice's content.

// We will simulate the aws-sdk-go's download manager algorithm.
// See: https://github.com/aws/aws-sdk-go/blob/main/service/s3/s3manager/download.go

// dlchunk represents a single chunk of data to write by the worker routine.
// This structure also implements an io.SectionReader style interface for
// io.WriterAt, effectively making it an io.SectionWriter (which does not
// exist).
// NOTE: content is added for testing, it's not present in AWS-SDK-GO
type dlchunk struct {
	w       io.WriterAt
	start   int64
	size    int64
	cur     int64
	content []byte
}

// Write wraps io.WriterAt for the dlchunk, writing from the dlchunk's start
// position to its end (or EOF).
//
// If a range is specified on the dlchunk the size will be ignored when writing.
// as the total size may not of be known ahead of time.
func (c *dlchunk) Write(p []byte) (n int, err error) {
	if c.cur >= c.size {
		return 0, io.EOF
	}

	n, err = c.w.WriteAt(p, c.start+c.cur)
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
			var result bytes.Buffer
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
					start:   int64(expectedFileSize),
					size:    int64(chunkSize),
					content: chunk,
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

			rb := buffer.NewOrderedBuffer(expectedFileSize, &result)

			// we set the writerAt after we initialize the buffer, since
			// the buffer can't be initialized until the expectedFileSize
			// is known.
			for i := 0; i < len(chunks); i++ {
				chunks[i].w = rb
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
						if task.start == 0 {
							blockingTask = task
							continue
						}
						r := bytes.NewReader(task.content)
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
			r := bytes.NewReader(blockingTask.content)
			io.Copy(&blockingTask, r)
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
				t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
			}

			// Check if there are any leftover chunks in the OrderedBuffer
			remainingChunks := rb.Queued
			if remainingChunks != 0 {
				t.Errorf("OrderedBuffer contains %d leftover chunks", remainingChunks)
			}
		})
	}
}
