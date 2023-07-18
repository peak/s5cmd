package buffer_test

import (
	"bytes"
	"fmt"
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
	// static chunk size
	gbs := 1000
	testRuns := 64
	for b := 0; b < testRuns; b++ {
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

		rb := buffer.NewOrderedBuffer(expectedFileSize, gbs, &result)

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
			rb := buffer.NewOrderedBuffer(expectedFileSize, -1, &result)

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
	maxFileSize := 1024
	testRuns := 64
	minChunkSize, maxChunkSize := 5, 100
	for b := 0; b < testRuns; b++ {
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

		rb := buffer.NewOrderedBuffer(expectedFileSize, -1, &result)

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
	}
}

// when you use io.Copy, the slice it gives you
// changes in time. You should copy the slice when
// a call occurs and don't count on the given slice's content.
func TestBufferWithChangingSlice(t *testing.T) {
	maxFileSize := 1024 * 1024
	testRuns := 1
	minChunkSize, maxChunkSize := 5, 100
	for b := 0; b < testRuns; b++ {
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

		rb := buffer.NewOrderedBuffer(expectedFileSize, -1, &result)

		chunkChan := make(chan FileChunk, 1)

		var wg sync.WaitGroup
		var blockingTask FileChunk
		workerCount := 5 + rand.Intn(20) // 5-25 workers
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func(ch chan FileChunk) {
				defer wg.Done()
				for task := range ch {
					if task.start == 0 {
						blockingTask = task
						continue
					}
					rb.WriteAt(task.content, int64(task.start))
				}
			}(chunkChan)
		}

		for _, chunk := range chunks {
			chunkChan <- chunk
		}
		close(chunkChan)
		wg.Wait()
		// block all chunks except the first one
		rb.WriteAt(blockingTask.content, int64(blockingTask.start))

		if !bytes.Equal(result.Bytes(), expected) {
			t.Errorf("Length comparison: Got: %d bytes, expected: %d bytes\n", len(result.Bytes()), len(expected))
			t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
		}

		// Check if there are any leftover chunks in the OrderedBuffer
		remainingChunks := rb.Queued
		if remainingChunks != 0 {
			t.Errorf("OrderedBuffer contains %d leftover chunks", remainingChunks)
		}
	}
}
