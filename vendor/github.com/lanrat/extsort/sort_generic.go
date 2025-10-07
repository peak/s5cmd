// Package extsort implements an unstable external sort for all the records in a chan or iterator
package extsort

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"slices"
	"sync"

	"github.com/lanrat/extsort/queue"
	"github.com/lanrat/extsort/tempfile"

	"golang.org/x/sync/errgroup"
)

// genericChunk represents a collection of any data that can be sorted.
// It holds data in memory before being sorted using slices.SortFunc.
type genericChunk[E any] struct {
	data []E
}

// getChunk retrieves a chunk from the pool and initializes it
func (s *GenericSorter[E]) getChunk() *genericChunk[E] {
	c := s.pools.chunkPool.Get().(*genericChunk[E])

	// Get a slice pointer from the pool
	slicePtr := s.pools.slicePool.Get().(*[]E)
	*slicePtr = (*slicePtr)[:0] // Reset length but keep capacity

	c.data = *slicePtr
	return c
}

// putChunk returns a chunk to the pool for reuse
func (s *GenericSorter[E]) putChunk(c *genericChunk[E]) {
	if c != nil && c.data != nil {
		// Return the slice to the pool
		data := c.data
		c.data = nil // Clear reference before putting
		s.pools.slicePool.Put(&data)

		// Return the chunk to the pool
		s.pools.chunkPool.Put(c)
	}
}

// memoryPools holds sync.Pool instances for memory reuse
type memoryPools struct {
	chunkPool     sync.Pool // *chunk objects
	slicePool     sync.Pool // []any slices
	byteSlicePool sync.Pool // []byte slices for serialization
	scratchPool   sync.Pool // scratch buffers for binary encoding
}

// GenericSorter implements external sorting for any type E using a divide-and-conquer approach.
// It reads input from a channel, splits data into chunks that fit in memory, sorts each chunk,
// saves them to temporary files, then merges all chunks back into a sorted output stream.
// The sorter uses configurable parallelism for both sorting and merging phases,
// and employs memory pools to reduce garbage collection pressure during operation.
type GenericSorter[E any] struct {
	config         Config
	buildSortCtx   context.Context
	saveCtx        context.Context
	mergeErrChan   chan error
	tempWriter     tempfile.TempWriter
	tempReader     tempfile.TempReader
	input          <-chan E
	chunkChan      chan *genericChunk[E]
	saveChunkChan  chan *genericChunk[E]
	mergeChunkChan chan E
	compareFunc    CompareGeneric[E]
	fromBytes      FromBytesGeneric[E]
	toBytes        ToBytesGeneric[E]
	pools          *memoryPools
	singleChunk    *genericChunk[E] // Holds the single chunk for optimization
}

// newSorter creates a new GenericSorter instance with the given configuration.
// This internal constructor initializes all channels, applies configuration defaults,
// and sets up memory pools for efficient resource reuse during sorting operations.
func newSorter[E any](input <-chan E, fromBytes FromBytesGeneric[E], toBytes ToBytesGeneric[E], compareFunc CompareGeneric[E], config *Config) *GenericSorter[E] {
	config = mergeConfig(config)
	s := &GenericSorter[E]{
		input:          input,
		compareFunc:    compareFunc,
		fromBytes:      fromBytes,
		toBytes:        toBytes,
		config:         *config,
		chunkChan:      make(chan *genericChunk[E], config.ChanBuffSize),
		saveChunkChan:  make(chan *genericChunk[E], config.NumWorkers*2), // Buffer for workers to avoid deadlock
		mergeChunkChan: make(chan E, config.SortedChanBuffSize),
		mergeErrChan:   make(chan error, 1),
	}
	s.pools = s.initMemoryPools()
	return s
}

// initMemoryPools initializes sync.Pool instances for efficient memory reuse during sorting.
// Creates pools for chunks, slices, byte slices, and scratch buffers to reduce GC pressure
// and improve performance during high-frequency allocation/deallocation cycles.
func (s *GenericSorter[E]) initMemoryPools() *memoryPools {
	pools := &memoryPools{}

	// Pool for chunk objects
	pools.chunkPool = sync.Pool{
		New: func() any {
			return &genericChunk[E]{}
		},
	}

	// Pool for slices - store pointers to slices
	pools.slicePool = sync.Pool{
		New: func() any {
			slice := make([]E, 0, s.config.ChunkSize)
			return &slice
		},
	}

	// Pool for byte slices (for serialization) - store pointers to slices
	pools.byteSlicePool = sync.Pool{
		New: func() any {
			slice := make([]byte, 0, 1024) // Start with 1KB capacity
			return &slice
		},
	}

	// Pool for scratch buffers (for binary encoding) - store pointers to slices
	pools.scratchPool = sync.Pool{
		New: func() any {
			slice := make([]byte, binary.MaxVarintLen64)
			return &slice
		},
	}

	return pools
}

// Generic creates a new external sorter for any type E and returns the sorter instance,
// output channel with sorted results, and error channel.
//
// Parameters:
//   - input: Channel providing the data to be sorted
//   - fromBytes: Function to deserialize E from bytes when reading from disk
//   - toBytes: Function to serialize E to bytes when writing to disk
//   - compareFunc: Comparison function that returns negative/zero/positive for less/equal/greater
//   - config: Configuration options (nil uses defaults)
//
// The sorting process:
//  1. Reads data from input channel into memory chunks
//  2. Sorts each chunk in parallel using the provided compareFunc
//  3. Saves sorted chunks to temporary files using toBytes serialization
//  4. Merges all chunks back into sorted order using fromBytes deserialization
//
// Call Sort() on the returned sorter to begin the sorting process.
// Results are delivered via the output channel, errors via the error channel.
// On error or interruption, temporary files may remain in config.TempFilesDir.
func Generic[E any](input <-chan E, fromBytes FromBytesGeneric[E], toBytes ToBytesGeneric[E], compareFunc CompareGeneric[E], config *Config) (*GenericSorter[E], <-chan E, <-chan error) {
	var err error
	s := newSorter(input, fromBytes, toBytes, compareFunc, config)
	s.tempWriter, err = tempfile.New(s.config.TempFilesDir, true)
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return nil, s.mergeChunkChan, s.mergeErrChan
	}
	return s, s.mergeChunkChan, s.mergeErrChan
}

// MockGeneric creates an external sorter that uses in-memory storage instead of disk files.
// This is primarily useful for testing and benchmarking without filesystem I/O overhead.
// The parameter n specifies the initial capacity of the in-memory buffer.
// All other behavior is identical to Generic().
func MockGeneric[E any](input <-chan E, fromBytes FromBytesGeneric[E], toBytes ToBytesGeneric[E], compareFunc CompareGeneric[E], config *Config, n int) (*GenericSorter[E], <-chan E, <-chan error) {
	s := newSorter(input, fromBytes, toBytes, compareFunc, config)
	s.tempWriter = tempfile.Mock(n)
	return s, s.mergeChunkChan, s.mergeErrChan
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
// this blocks while sorting chunks and unblocks when merging
// NOTE: the context passed to Sort must outlive Sort() returning.
// Merge uses the same context and runs in a goroutine after Sort returns().
// for example, if calling sort in an errGroup, you must pass the group's parent context into sort.
func (s *GenericSorter[E]) Sort(ctx context.Context) {
	var buildSortErrGroup, saveErrGroup *errgroup.Group
	buildSortErrGroup, s.buildSortCtx = errgroup.WithContext(ctx)
	saveErrGroup, s.saveCtx = errgroup.WithContext(ctx)

	//start creating chunks
	buildSortErrGroup.Go(s.buildChunks)

	// sort chunks
	for i := 0; i < s.config.NumWorkers; i++ {
		buildSortErrGroup.Go(s.sortChunks)
	}

	// Start the save worker that will handle single-chunk optimization
	saveErrGroup.Go(s.saveChunksOptimized)

	err := buildSortErrGroup.Wait()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return
	}

	// Close saveChunkChan to signal end of chunks
	close(s.saveChunkChan)

	// Wait for save worker to complete
	err = saveErrGroup.Wait()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return
	}

	// Check if single chunk optimization was used
	if s.singleChunk != nil {
		// Single chunk case - output directly
		go s.outputSingleChunk(ctx)
		return
	}

	// Multiple chunks: read chunks and merge
	// if this errors, it is returned in the errorChan
	go s.mergeNChunks(ctx)
}

// buildChunks reads data from the input chan to builds chunks and pushes them to chunkChan
func (s *GenericSorter[E]) buildChunks() error {
	defer close(s.chunkChan) // if this is not called on error, causes a deadlock

	for {
		c := s.getChunk()
		for i := 0; i < s.config.ChunkSize; i++ {
			select {
			case rec, ok := <-s.input:
				if !ok {
					break
				}
				c.data = append(c.data, rec)
			case <-s.buildSortCtx.Done():
				s.putChunk(c) // Return unused chunk to pool
				return s.buildSortCtx.Err()
			}
		}
		if len(c.data) == 0 {
			// the chunk is empty, return it to pool
			s.putChunk(c)
			break
		}

		select {
		// chunk is now full
		case s.chunkChan <- c:
		case <-s.buildSortCtx.Done():
			s.putChunk(c) // Return unused chunk to pool
			return s.buildSortCtx.Err()
		}
	}

	return nil
}

// sortChunks is a worker for sorting the data stored in a chunk prior to save
func (s *GenericSorter[E]) sortChunks() error {
	for {
		select {
		case b, more := <-s.chunkChan:
			if more {
				// Create channels to communicate completion and errors
				sortDone := make(chan error, 1)

				// Run sort in a separate goroutine
				go func() {
					defer func() {
						// Recover from panics in comparison function
						if r := recover(); r != nil {
							sortDone <- NewComparisonError(r, "sortChunks")
						} else {
							sortDone <- nil // Success
						}
					}()
					slices.SortFunc(b.data, s.compareFunc)
				}()

				// Wait for either sort completion or context cancellation
				select {
				case sortErr := <-sortDone:
					if sortErr != nil {
						// Sort failed due to panic
						s.putChunk(b) // Return chunk to pool
						return sortErr
					}
					// Sort completed successfully, proceed to save
					select {
					case s.saveChunkChan <- b:
					case <-s.buildSortCtx.Done():
						return s.buildSortCtx.Err()
					}
				case <-s.buildSortCtx.Done():
					// Context cancelled while sorting - abandon this chunk
					return s.buildSortCtx.Err()
				}
			} else {
				return nil
			}
		case <-s.buildSortCtx.Done():
			return s.buildSortCtx.Err()
		}
	}
}

// outputSingleChunk handles the single-chunk optimization by directly outputting
// the sorted chunk without any disk I/O. This provides significant performance
// benefits for small datasets that fit entirely in memory.
func (s *GenericSorter[E]) outputSingleChunk(ctx context.Context) {
	defer close(s.mergeChunkChan)
	defer close(s.mergeErrChan)

	// Use the chunk collected by collectSingleChunk
	chunk := s.singleChunk
	if chunk == nil {
		// No chunk collected - this shouldn't happen but handle gracefully
		return
	}

	// Output each item in the sorted chunk directly
	for _, item := range chunk.data {
		select {
		case s.mergeChunkChan <- item:
		case <-ctx.Done():
			s.mergeErrChan <- ctx.Err()
			return
		}
	}

	// Return chunk to pool
	s.putChunk(chunk)
	s.singleChunk = nil // Clear reference
}

// saveChunksOptimized handles both single-chunk and multi-chunk cases
// For single chunk: stores it in memory to avoid disk I/O
// For multiple chunks: saves all chunks to disk normally
func (s *GenericSorter[E]) saveChunksOptimized() error {
	// Get the first chunk with context checking
	var firstChunk *genericChunk[E]
	var ok bool
	select {
	case firstChunk, ok = <-s.saveChunkChan:
		if !ok {
			// Channel closed, no chunks at all
			return nil
		}
	case <-s.saveCtx.Done():
		return s.saveCtx.Err()
	}

	// Try to get a second chunk with context checking
	var secondChunk *genericChunk[E]
	select {
	case secondChunk, ok = <-s.saveChunkChan:
		if !ok {
			// Channel closed after first chunk - single chunk optimization
			s.singleChunk = firstChunk
			return nil
		}
	case <-s.saveCtx.Done():
		s.putChunk(firstChunk) // Return to pool before exiting
		return s.saveCtx.Err()
	}

	// We have at least 2 chunks - use multi-chunk path
	// Save the first chunk
	if err := s.saveChunk(firstChunk); err != nil {
		s.putChunk(secondChunk) // Return to pool
		return err
	}

	// Save the second chunk
	if err := s.saveChunk(secondChunk); err != nil {
		return err
	}

	// Continue saving any remaining chunks with context checking
	for {
		select {
		case chunk, ok := <-s.saveChunkChan:
			if !ok {
				// Channel closed, we're done
				// Finalize the temp writer and save it for reading
				var err error
				s.tempReader, err = s.tempWriter.Save()
				return err
			}
			if err := s.saveChunk(chunk); err != nil {
				return err
			}
		case <-s.saveCtx.Done():
			return s.saveCtx.Err()
		}
	}
}

// saveChunk processes a single chunk
func (s *GenericSorter[E]) saveChunk(b *genericChunk[E]) error {
	scratchPtr := s.pools.scratchPool.Get().(*[]byte)
	scratch := *scratchPtr
	defer s.pools.scratchPool.Put(scratchPtr)

	for _, d := range b.data {
		// binary encoding for size
		raw, err := s.toBytes(d)
		if err != nil {
			s.putChunk(b) // Return chunk to pool on error
			return NewSerializationError(err, "saveChunk")
		}
		n := binary.PutUvarint(scratch, uint64(len(raw)))
		_, err = s.tempWriter.Write(scratch[:n])
		if err != nil {
			s.putChunk(b) // Return chunk to pool on error
			return NewDiskError(err, "write size header", "")
		}
		// add data
		_, err = s.tempWriter.Write(raw)
		if err != nil {
			s.putChunk(b) // Return chunk to pool on error
			return NewDiskError(err, "write data", "")
		}
	}
	_, err := s.tempWriter.Next()
	if err != nil {
		s.putChunk(b) // Return chunk to pool on error
		return NewDiskError(err, "next chunk", "")
	}
	// Successfully processed chunk, return to pool
	s.putChunk(b)
	return nil
}

// mergeNChunks runs asynchronously in the background feeding data to getNext
// sends errors to s.mergeErrorChan. Uses parallel merging for better performance.
func (s *GenericSorter[E]) mergeNChunks(ctx context.Context) {
	defer close(s.mergeChunkChan)
	defer func() {
		if s.tempReader != nil {
			err := s.tempReader.Close()
			if err != nil {
				// Try to send error, but don't panic if channel is closed
				select {
				case s.mergeErrChan <- err:
				default:
				}
			}
		}
	}()
	// Always ensure error channel is closed
	defer close(s.mergeErrChan)

	if s.tempReader == nil {
		return
	}

	numChunks := s.tempReader.Size()
	if numChunks == 0 {
		return
	}

	// For small number of chunks, use single-threaded merge
	if numChunks <= s.config.NumWorkers {
		s.mergeNChunksSingleThreaded(ctx)
		return
	}

	// Use parallel merging for many chunks
	s.mergeNChunksParallel(ctx)
}

// mergeNChunksSingleThreaded is the original single-threaded implementation
func (s *GenericSorter[E]) mergeNChunksSingleThreaded(ctx context.Context) {
	pq := queue.NewPriorityQueue(func(a, b *mergeFile[E]) int {
		return s.compareFunc(a.nextRec, b.nextRec)
	})

	for i := 0; i < s.tempReader.Size(); i++ {
		merge := &mergeFile[E]{
			fromBytes: s.fromBytes,
			reader:    s.tempReader.Read(i),
		}
		_, ok, err := merge.getNext() // start the merge by preloading the values
		if err == io.EOF || !ok {
			continue
		}
		if err != nil {
			s.mergeErrChan <- err
			return
		}
		pq.Push(merge)
	}

	for pq.Len() > 0 {
		merge := pq.Peek()
		rec, more, err := merge.getNext()
		if err != nil {
			s.mergeErrChan <- err
			return
		}
		if more {
			pq.PeekUpdate()
		} else {
			pq.Pop()
		}
		// check for err in context just in case
		select {
		case s.mergeChunkChan <- rec:
		case <-ctx.Done():
			s.mergeErrChan <- ctx.Err()
			return
		}
	}
}

// mergeNChunksParallel implements parallel k-way merging with robust cancellation
func (s *GenericSorter[E]) mergeNChunksParallel(ctx context.Context) {
	numChunks := s.tempReader.Size()
	numWorkers := s.config.NumWorkers

	// Create a cancellable context for all merge operations
	mergeCtx, mergeCancel := context.WithCancel(ctx)
	defer mergeCancel() // Ensure all goroutines stop when this function returns

	// Create intermediate channels for each worker
	intermediateChanSize := s.config.SortedChanBuffSize
	intermediateChans := make([]chan E, numWorkers)
	for i := range intermediateChans {
		intermediateChans[i] = make(chan E, intermediateChanSize)
	}

	// Error collection
	errChan := make(chan error, numWorkers+1) // +1 for final merge errors
	var mergeErr error
	var errOnce sync.Once

	// Start workers
	var wg sync.WaitGroup
	chunksPerWorker := (numChunks + numWorkers - 1) / numWorkers
	workersStarted := 0

	for i := 0; i < numWorkers; i++ {
		startChunk := i * chunksPerWorker
		endChunk := (i + 1) * chunksPerWorker
		if endChunk > numChunks {
			endChunk = numChunks
		}
		if startChunk >= numChunks {
			break
		}
		workersStarted++
		wg.Add(1)

		go func(workerIdx, start, end int) {
			defer wg.Done()
			defer close(intermediateChans[workerIdx]) // Each worker closes its own channel

			if err := s.mergeWorkerSimple(mergeCtx, start, end, intermediateChans[workerIdx]); err != nil {
				errChan <- err
				mergeCancel() // Cancel all operations on error
			}
		}(i, startChunk, endChunk)
	}

	// Start error collector with wait group for synchronization
	var errorCollectorWg sync.WaitGroup
	errorCollectorWg.Add(1)
	go func() {
		defer errorCollectorWg.Done()
		for err := range errChan {
			if err != nil {
				errOnce.Do(func() {
					mergeErr = err
					mergeCancel() // Cancel all operations on first error
				})
			}
		}
	}()

	// Start final merge in a goroutine to avoid blocking
	var finalMergeWg sync.WaitGroup
	finalMergeWg.Add(1)
	go func() {
		defer finalMergeWg.Done()
		s.finalMergeSimple(mergeCtx, intermediateChans[:workersStarted])
	}()

	// Wait for all workers to complete
	wg.Wait()

	// Wait for final merge to complete
	finalMergeWg.Wait()

	close(errChan) // Signal error collector to stop

	// Wait for error collector to finish processing all errors
	errorCollectorWg.Wait()

	// Send any collected error (now safe to read mergeErr)
	if mergeErr != nil {
		s.mergeErrChan <- mergeErr
	} else if ctx.Err() != nil {
		s.mergeErrChan <- ctx.Err()
	}
}

// mergeWorkerSimple merges a subset of chunks with proper context handling
func (s *GenericSorter[E]) mergeWorkerSimple(ctx context.Context, startChunk, endChunk int, output chan<- E) error {
	pq := queue.NewPriorityQueue(func(a, b *mergeFile[E]) int {
		return s.compareFunc(a.nextRec, b.nextRec)
	})

	// Initialize merge files for this worker's chunk range
	for i := startChunk; i < endChunk; i++ {
		merge := &mergeFile[E]{
			fromBytes: s.fromBytes,
			reader:    s.tempReader.Read(i),
		}
		_, ok, err := merge.getNext()
		if err == io.EOF || !ok {
			continue
		}
		if err != nil {
			return err
		}
		pq.Push(merge)
	}

	// Merge this worker's chunks
	for pq.Len() > 0 {
		// Check context before processing
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		merge := pq.Peek()
		rec, more, err := merge.getNext()
		if err != nil {
			return err
		}
		if more {
			pq.PeekUpdate()
		} else {
			pq.Pop()
		}

		select {
		case output <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// finalMergeSimple performs streaming merge with simpler synchronization
func (s *GenericSorter[E]) finalMergeSimple(ctx context.Context, intermediateChans []chan E) {
	pq := queue.NewPriorityQueue(func(a, b *channelMergeSource[E]) int {
		return s.compareFunc(a.nextRec, b.nextRec)
	})

	// Initialize sources
	for _, ch := range intermediateChans {
		source := &channelMergeSource[E]{ch: ch}
		if source.getNextSimple() {
			pq.Push(source)
		}
	}

	// Perform final streaming merge with proper context handling
	for pq.Len() > 0 {
		// Check if context is cancelled before each iteration
		if ctx.Err() != nil {
			return
		}

		source := pq.Peek()

		// Try to send with context cancellation support
		select {
		case s.mergeChunkChan <- source.nextRec:
			// Successfully sent, try to get next from this source
			if source.getNextSimple() {
				pq.PeekUpdate()
			} else {
				pq.Pop()
			}
		case <-ctx.Done():
			// Context cancelled, exit immediately
			return
		}
	}
}

// channelMergeSource represents a source of sorted data from a channel
type channelMergeSource[E any] struct {
	ch      <-chan E
	nextRec E
	hasNext bool
}

// getNextSimple reads from channel without context (channel close handles cancellation)
func (c *channelMergeSource[E]) getNextSimple() bool {
	rec, ok := <-c.ch
	if ok {
		c.nextRec = rec
		c.hasNext = true
		return true
	}
	c.hasNext = false
	return false
}

// mergeFile represents each sorted chunk on disk and its next value
type mergeFile[E any] struct {
	nextRec   E
	fromBytes FromBytesGeneric[E]
	reader    *bufio.Reader
}

// getNext returns the next value from the sorted chunk on disk.
// The first call will return nil while the struct is initialized.
// It handles deserialization errors by wrapping them in DeserializationError instances.
func (m *mergeFile[E]) getNext() (E, bool, error) {
	var newRecBytes []byte
	old := m.nextRec

	n, err := binary.ReadUvarint(m.reader)
	if err == nil {
		newRecBytes = make([]byte, int(n))
		_, err = io.ReadFull(m.reader, newRecBytes)
	}
	if err != nil {
		if err == io.EOF {
			return old, false, nil
		}
		return old, false, err
	}

	m.nextRec, err = m.fromBytes(newRecBytes)
	if err != nil {
		return old, true, NewDeserializationError(err, len(newRecBytes), "getNext")
	}

	return old, true, nil
}
