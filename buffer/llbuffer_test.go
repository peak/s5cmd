package buffer_test

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/peak/s5cmd/v2/buffer"
	"gotest.tools/v3/assert"
)

func makeChunk(n, size int) []byte {
	chunk := make([]byte, size)
	for i := 0; i < size; i++ {
		chunk[i] = byte(n)
	}
	return chunk
}

// Allocate gbs, fill whole, write seq, expect ordered seq
func TestSequentialWrite(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	rb := buffer.NewOrderedBuffer(int64(workerCount*offset), gbs, &result)

	// nolap
	for i := 0; i < workerCount; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, chunk...)
	}

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

	// fill buffer at worst case
	chunks := workerCount * (workerCount + 1) / 2
	result.Reset()
	rb = buffer.NewOrderedBuffer(int64(chunks*offset), gbs, &result)
	expected = make([]byte, 0)
	// nolap
	for i := 0; i < chunks; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, chunk...)
	}

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}
}

// Allocate gbs, fill whole, write random, expect ordered seq
func TestRandomWrite(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	rb := buffer.NewOrderedBuffer(int64(workerCount*offset), gbs, &result)

	// make a shuffled array - nolap
	j := 0
	for _, i := range rand.Perm(workerCount) {
		off := int64(i * offset)
		rb.WriteAt(makeChunk(i, offset), off)
		expected = append(expected, makeChunk(j, offset)...)
		j++
	}

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}
	// fill buffer at worst case
	chunks := workerCount*(workerCount+1)/2 + workerCount
	result.Reset()
	rb = buffer.NewOrderedBuffer(int64(chunks*offset), gbs, &result)
	expected = make([]byte, 0)
	// make a shuffled array - nolap
	j = 0
	for _, i := range rand.Perm(chunks) {
		off := int64(i * offset)
		rb.WriteAt(makeChunk(i, offset), off)
		expected = append(expected, makeChunk(j, offset)...)
		j++
	}

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}
}

// Allocate gbs, fill except the head,read nothing
// after 50 ms, put head, expect seq full read.
func TestBlockingHeadSequentialWrite(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	rb := buffer.NewOrderedBuffer(int64(workerCount*offset), gbs, &result)

	// all except first chunk
	for i := 1; i < workerCount; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, chunk...)
	}

	assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")

	// give the missing chunk, write at offset 0
	rb.WriteAt(makeChunk(0, offset), int64(0))
	// add the chunk to the head of the expected value
	expected = append(makeChunk(0, offset), expected...)
	// expect all items to be completed and read from the channel

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

	// fill the buffer
	chunks := workerCount * (workerCount + 1) / 2
	result.Reset()
	rb = buffer.NewOrderedBuffer(int64(chunks*offset), gbs, &result)
	expected = make([]byte, 0)

	// nolap
	for i := 1; i < chunks; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, chunk...)
	}

	// give the missing chunk, write at offset 0
	assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")

	rb.WriteAt(makeChunk(0, offset), int64(0))
	expected = append(makeChunk(0, offset), expected...)
	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

}

// Allocate gbs, fill at random except the head,read nothing
// After the missing piece, expect full seq read
func TestBlockingHeadRandomWrite(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	rb := buffer.NewOrderedBuffer(int64(workerCount*offset), gbs, &result)

	// all except first chunk

	j := 1
	for _, i := range rand.Perm(workerCount) {
		if i == 0 {
			continue
		}
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, makeChunk(j, offset)...)
		j++
	}

	assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")

	// give the missing chunk, write at offset 0
	rb.WriteAt(makeChunk(0, offset), int64(0))
	// add the chunk to the head of the expected value
	expected = append(makeChunk(0, offset), expected...)
	// expect all items to be completed and read from the channel

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

	// fill the buffer
	chunks := workerCount * (workerCount + 1) / 2
	result.Reset()
	rb = buffer.NewOrderedBuffer(int64(chunks*offset), gbs, &result)
	expected = make([]byte, 0)

	j = 1
	for _, i := range rand.Perm(chunks) {
		if i == 0 {
			continue
		}
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, makeChunk(j, offset)...)
		j++
	}

	// give the missing chunk, write at offset 0
	assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")

	rb.WriteAt(makeChunk(0, offset), int64(0))
	expected = append(makeChunk(0, offset), expected...)
	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}
}

// Allocate gbs, fill with gaps, read piece by piece
// at the end, expect all pieces sequentially.
func TestBlockingGapsSequentially(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	rb := buffer.NewOrderedBuffer(int64(workerCount*offset), gbs, &result)

	// build expected value
	for i := 0; i < workerCount; i++ {
		for j := 0; j < offset; j++ {
			expected = append(expected, byte(i))
		}
	}
	// you have 5 workers, so 5 chunks expected at the buffer, 0 index
	// is the head chunk
	// fill 1,2
	for i := 1; i <= 2; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	// assert no read, no item, no end
	assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")
	// fill 3,4
	for i := 3; i <= 4; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	// give the missing chunk, write at offset 0
	assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")

	rb.WriteAt(makeChunk(0, offset), int64(0))
	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

	chunks := workerCount*(workerCount+1)/2 + workerCount
	result.Reset()
	rb = buffer.NewOrderedBuffer(int64(chunks*offset), gbs, &result)
	expected = make([]byte, 0)
	// you have chunks number of workers, so the expected chunks will fill the buffer
	// in worst case.

	// strategy:
	// fill the buffer with random chunks width random ranges except the head.
	// expect all to be not pushed to channel
	// fill the head chunk, expect all to arrive in order

	// chunks = 20 in this case, for the sake of the brewity of the test indexes are
	// assigned manually

	indexes := [][]int{
		{1, 5},
		{8, 10},
		{11, 14},
		{6, 7},
		{15, 19},
	}
	// build expected value
	for i := 0; i < chunks; i++ {
		for j := 0; j < offset; j++ {
			expected = append(expected, byte(i))
		}
	}

	// calculate the expected before
	// expect no reads
	for _, rg := range indexes {
		start, end := rg[0], rg[1]

		for i := start; i <= end; i++ {
			off := int64(i * offset)
			chunk := makeChunk(i, offset)
			rb.WriteAt(chunk, off)
		}
		assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")
	}

	rb.WriteAt(makeChunk(0, offset), int64(0))

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

}
func TestBlockingGapsAtRandom(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	rb := buffer.NewOrderedBuffer(int64(workerCount*offset), gbs, &result)

	// build expected value
	for i := 0; i < workerCount; i++ {
		for j := 0; j < offset; j++ {
			expected = append(expected, byte(i))
		}
	}
	// randomness at this part is simply reversing the orders
	// 5 workers, 2 + 1 partitions,NOTE: 1,2 has one other permutation
	// other than itself: 2,1. So we reverse the fills.
	// you have 5 workers, so 5 chunks expected at the buffer, 0 index
	// is the head chunk
	// fill 1,2
	for i := 4; i >= 3; i-- {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	// assert no read, no item, no end
	assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")
	// fill 3,4
	for i := 2; i >= 1; i-- {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	// give the missing chunk, write at offset 0
	assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")

	rb.WriteAt(makeChunk(0, offset), int64(0))

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

	chunks := workerCount*(workerCount+1)/2 + workerCount
	result.Reset()
	rb = buffer.NewOrderedBuffer(int64(chunks*offset), gbs, &result)
	expected = make([]byte, 0)
	// you have chunks number of workers, so the expected chunks will fill the buffer
	// in worst case.

	// strategy:
	// fill the buffer with random chunks width random ranges except the head.
	// expect all to be not pushed to channel
	// fill the head chunk, expect all to arrive in order

	// chunks = 20 in this case, for the sake of the brewity of the test indexes are
	// assigned manually

	indexes := [][]int{
		{1, 5},
		{8, 10},
		{11, 14},
		{6, 7},
		{15, 19},
	}
	// build expected value
	for i := 0; i < chunks; i++ {
		for j := 0; j < offset; j++ {
			expected = append(expected, byte(i))
		}
	}

	// calculate the expected before
	// expect no reads
	for _, rg := range indexes {
		start, width := rg[0], rg[1]-rg[0]

		for _, r := range rand.Perm(width + 1) {
			i := r + start
			off := int64(i * offset)
			chunk := makeChunk(i, offset)
			rb.WriteAt(chunk, off)
		}
		assert.Equal(t, len(result.Bytes()), 0, "writer must be empty")
	}

	rb.WriteAt(makeChunk(0, offset), int64(0))

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}
}

// alloc gbs, start writer, expect after all
// writers, the items are written in order
func TestConcurrentWrite(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	chunks := 1024 * 512
	rb := buffer.NewOrderedBuffer(int64(chunks*offset), gbs, &result)

	// first create chunks number of tasks
	tasks := make(chan WriteTask, workerCount)

	var wg sync.WaitGroup
	// spin up a worker pool
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		// simulate task
		go func(ch chan WriteTask) {
			defer wg.Done()
			for task := range ch {
				chunk := makeChunk(task.i, offset)
				rb.WriteAt(chunk, task.offset)
			}
		}(tasks)
	}
	for i := 0; i < chunks; i++ {
		s := make([]byte, offset)
		for j := 0; j < offset; j++ {
			// it will overflow but won't affect the end result, since makeChunk
			// also overflows while filling.
			s[j] = byte(i)
		}
		expected = append(expected, s...)
		task := WriteTask{
			i:      i,
			offset: int64(i * offset),
			sleep:  time.Duration(rand.Intn(2)+1) * time.Millisecond, // every task sleeps between 1 ms and 50ms
		}
		tasks <- task
	}
	close(tasks)
	wg.Wait()

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
		t.Errorf("Got chunks: %d, expected chunks: %d, diff(got - expected): %d", len(result.Bytes())/offset, len(expected)/offset, (len(result.Bytes())-len(expected))/offset)
		got := result.Bytes()
		min := chunks
		if min < len(got)/offset {
			min = len(got) / offset
		}
		for i := 0; i < (min-1)*offset; i += offset {
			gotChunk := got[i : i+offset]
			expectedChunk := expected[i : i+offset]
			chunkIndex := i / offset
			if !bytes.Equal(gotChunk, expectedChunk) {
				t.Errorf("Diff at chunkIndex: %d\n", chunkIndex)
			}
		}
	}

}

// WriteTask is used for creating mock tasks that complete in different times.
type WriteTask struct {
	i      int
	offset int64
	size   int64
	sleep  time.Duration
}

type FileChunk struct {
	start   int
	end     int
	content []byte
}

func TestShuffleWrite(t *testing.T) {
	// This test runs like a benchmark
	gbs := 1000
	testRuns := 1
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

// allocate gbs, test the last chunk with size
// less than the offset
func TestTinyChunk(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	rb := buffer.NewOrderedBuffer(int64((workerCount-1)*offset)+int64(offset/2), gbs, &result)

	// nolap
	for i := 0; i < workerCount-1; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, chunk...)
	}

	// make the last chunk tiny
	miniChunk := makeChunk(workerCount, offset/2)
	off := int64(offset * (workerCount - 1))

	expected = append(expected, miniChunk...)
	rb.WriteAt(miniChunk, off)

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

	// fill buffer at worst case
	chunks := workerCount*(workerCount+1)/2 + workerCount
	result.Reset()
	rb = buffer.NewOrderedBuffer(int64((chunks-1)*offset)+int64(offset/2), gbs, &result)
	expected = make([]byte, 0)
	// nolap
	for i := 0; i < chunks-1; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, chunk...)
	}
	miniChunk = makeChunk(chunks, offset/2)
	off = int64(offset * (chunks - 1))

	expected = append(expected, miniChunk...)
	rb.WriteAt(miniChunk, off)

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}

}

func TestHalfFillWithTinyChunk(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := (workerCount * (workerCount + 1) / 2) + workerCount
	var result bytes.Buffer
	expected := make([]byte, 0)
	rb := buffer.NewOrderedBuffer(int64(workerCount*2*offset), gbs, &result)
	// nolap
	for i := 0; i < (workerCount*2)-1; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
		expected = append(expected, chunk...)
	}

	// make the last chunk tiny
	miniChunk := makeChunk(workerCount, offset/2)
	off := int64(offset * ((workerCount * 2) - 1))

	expected = append(expected, miniChunk...)
	rb.WriteAt(miniChunk, off)

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("Got: %v, expected: %v\n", result.Bytes(), expected)
	}
}
