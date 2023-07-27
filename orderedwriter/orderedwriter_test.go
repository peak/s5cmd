package orderedwriter

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"gotest.tools/v3/assert"
)

const (
	testRuns = 64
)

func randomBytes(n int) []byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]

	}
	return b
}

func TestShuffleWriteWithStaticChunkSize(t *testing.T) {
	t.Parallel()

	// only for this test
	const (
		chunkSize = 5
		fileSize  = 1000
	)

	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("Run%d", b), func(t *testing.T) {
			t.Parallel()
			var result bytes.Buffer

			// we know the filesize ahead so make it buffered
			expected := randomBytes(fileSize)
			chunks := []chunk{}

			// first create chunks number of tasks
			// Create tasks and put them in the array
			for i := 0; i < fileSize; i += chunkSize {
				task := chunk{
					offset: int64(i),
					value:  expected[i : i+chunkSize],
				}
				chunks = append(chunks, task)
			}

			rand.Shuffle(len(chunks), func(i, j int) {
				chunks[i], chunks[j] = chunks[j], chunks[i]
			})

			buf := New(&result)

			for _, chunk := range chunks {
				buf.WriteAt(chunk.value, int64(chunk.offset))

			}

			// Ensure all chunks have been written correctly
			assert.DeepEqual(t, result.Bytes(), expected)
		})
	}
}

func TestShuffleWriteWithRandomChunkSize(t *testing.T) {
	t.Parallel()

	const (
		maxFileSize                = 1024 * 100
		minChunkSize, maxChunkSize = 5, 1000
	)

	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("TR%d", b), func(t *testing.T) {
			t.Parallel()

			var (
				result   bytes.Buffer
				offset   int64
				expected []byte
				chunks   []chunk
			)

			// generate chunks
			for i := 0; i <= maxFileSize; {
				chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				bytechunk := randomBytes(chunkSize)

				chunks = append(chunks, chunk{
					offset: offset,
					value:  bytechunk,
				})
				expected = append(expected, bytechunk...)
				offset += int64(chunkSize)
				i += chunkSize
			}

			rand.Shuffle(len(chunks), func(i, j int) {
				chunks[i], chunks[j] = chunks[j], chunks[i]
			})

			buf := New(&result)

			for _, chunk := range chunks {
				buf.WriteAt(chunk.value, int64(chunk.offset))
			}

			// Ensure all chunks have been written correctly
			assert.DeepEqual(t, result.Bytes(), expected)
		})
	}

}

func TestShuffleConcurrentWriteWithRandomChunkSize(t *testing.T) {
	t.Parallel()

	const (
		maxFileSize                = 1024 * 100
		minChunkSize, maxChunkSize = 5, 1000
	)

	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("Run%d", b), func(t *testing.T) {
			t.Parallel()

			var (
				result   bytes.Buffer
				expected []byte
				chunks   []chunk
			)

			// generate chunks
			for i := 0; i <= maxFileSize; {
				chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				bytechunk := randomBytes(chunkSize)

				chunks = append(chunks, chunk{
					offset: int64(i),
					value:  bytechunk,
				})

				expected = append(expected, bytechunk...)
				i += chunkSize
			}

			rand.Shuffle(len(chunks), func(i, j int) {
				chunks[i], chunks[j] = chunks[j], chunks[i]
			})

			buf := New(&result)

			chunkch := make(chan chunk)

			var wg sync.WaitGroup
			workerCount := 5 + rand.Intn(20) // 5-25 workers
			for i := 0; i < workerCount; i++ {
				wg.Add(1)
				go func(ch chan chunk) {
					defer wg.Done()
					for task := range ch {
						buf.WriteAt(task.value, int64(task.offset))
					}
				}(chunkch)
			}

			for _, chunk := range chunks {
				chunkch <- chunk
			}

			close(chunkch)
			wg.Wait()

			// Ensure all chunks have been written correctly
			assert.DeepEqual(t, result.Bytes(), expected)
		})
	}
}

// when you use io.Copy, the slice it gives you changes in time.
// You should copy the slice when a OrderedWriterAt.WriteAt
// call occurs and don't count on the given slice's value.

// We will simulate the aws-sdk-go's download manager algorithm.
// See: https://github.com/aws/aws-sdk-go/blob/main/service/s3/s3manager/download.go

// dlchunk represents a single chunk of data to write by the worker routine.
// This structure also implements an io.SectionReader style interface for
// io.WriterAt, effectively making it an io.SectionWriter (which does not
// exist).
// NOTE: value is added for testing, it's not present in AWS-SDK-GO
type dlchunk struct {
	w      io.WriterAt
	offset int64
	size   int64
	cur    int64
	value  []byte
}

// Write wraps io.WriterAt for the dlchunk, writing from the dlchunk's offset
// position to its end (or EOF).
//
// If a range is specified on the dlchunk the size will be ignored when writing.
// as the total size may not of be known ahead of time.
func (c *dlchunk) Write(p []byte) (int, error) {
	if c.cur >= c.size {
		return 0, io.EOF
	}

	n, err := c.w.WriteAt(p, c.offset+c.cur)
	c.cur += int64(n)

	return n, err
}

func TestBufferWithChangingSlice(t *testing.T) {
	t.Parallel()

	const (
		maxFileSize                = 1024 * 1024
		testRuns                   = 1
		minChunkSize, maxChunkSize = 5, 100
	)

	for b := 0; b < testRuns; b++ {
		b := b
		t.Run(fmt.Sprintf("Run%d", b), func(t *testing.T) {
			t.Parallel()

			var (
				expectedFileSize int64
				expected         []byte
				chunks           []dlchunk
			)

			// generate chunks
			for i := 0; i <= maxFileSize; {
				chunkSize := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				bytechunk := randomBytes(chunkSize)

				// we use the expectedFileSize to track the offset
				chunks = append(chunks, dlchunk{
					offset: int64(expectedFileSize),
					size:   int64(chunkSize),
					value:  bytechunk,
				})

				expected = append(expected, bytechunk...)
				expectedFileSize += int64(chunkSize)
				i += chunkSize
			}

			rand.Shuffle(len(chunks), func(i, j int) {
				chunks[i], chunks[j] = chunks[j], chunks[i]
			})

			result := bytes.NewBuffer(make([]byte, 0, expectedFileSize))
			buf := New(result)

			// we set the writerAt after we initialize the buf, since
			// the buf can't be initialized until the expectedFileSize
			// is known.
			for i := 0; i < len(chunks); i++ {
				chunks[i].w = buf
			}

			var (
				wg           sync.WaitGroup
				blockingTask dlchunk
			)

			chunkch := make(chan dlchunk)
			workerCount := 5 + rand.Intn(20) // 5-25 workers

			for i := 0; i < workerCount; i++ {
				wg.Add(1)
				go func(ch chan dlchunk) {
					defer wg.Done()
					for task := range ch {
						// the test is to block all chunks then
						// releasing them, the first bytechunk is blocked
						if task.offset == 0 {
							blockingTask = task
							continue
						}

						r := bytes.NewReader(task.value)
						io.Copy(&task, r)
					}
				}(chunkch)
			}

			for _, bytechunk := range chunks {
				chunkch <- bytechunk
			}

			close(chunkch)
			wg.Wait()

			// write the blocking bytechunk
			r := bytes.NewReader(blockingTask.value)
			io.Copy(&blockingTask, r)

			// Ensure all chunks have been written correctly
			assert.DeepEqual(t, result.Bytes(), expected)
		})
	}
}
