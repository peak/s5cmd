// Package extsort implements an unstable external sort for all the records in a chan or iterator
package extsort

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"sort"

	"github.com/lanrat/extsort/queue"
	"github.com/lanrat/extsort/tempfile"

	"golang.org/x/sync/errgroup"
)

type chunk struct {
	data []SortType
	less CompareLessFunc
}

func newChunk(size int, lessFunc CompareLessFunc) *chunk {
	c := new(chunk)
	c.less = lessFunc
	c.data = make([]SortType, 0, size)
	return c
}

func (c *chunk) Len() int {
	return len(c.data)
}

func (c *chunk) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

func (c *chunk) Less(i, j int) bool {
	return c.less(c.data[i], c.data[j])
}

// SortTypeSorter stores an input chan and feeds Sort to return a sorted chan
type SortTypeSorter struct {
	config         Config
	buildSortCtx   context.Context
	saveCtx        context.Context
	mergeErrChan   chan error
	tempWriter     tempfile.TempWriter
	tempReader     tempfile.TempReader
	input          chan SortType
	chunkChan      chan *chunk
	saveChunkChan  chan *chunk
	mergeChunkChan chan SortType
	lessFunc       CompareLessFunc
	fromBytes      FromBytes
}

func newSorter(i chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) *SortTypeSorter {
	s := new(SortTypeSorter)
	s.input = i
	s.lessFunc = lessFunc
	s.fromBytes = fromBytes
	s.config = *mergeConfig(config)
	s.chunkChan = make(chan *chunk, s.config.ChanBuffSize)
	s.saveChunkChan = make(chan *chunk, s.config.ChanBuffSize)
	s.mergeChunkChan = make(chan SortType, s.config.SortedChanBuffSize)
	s.mergeErrChan = make(chan error, 1)
	return s
}

// New returns a new Sorter instance that can be used to sort the input chan
// fromBytes is needed to unmarshal SortTypes from []byte on disk
// lessfunc is the comparator used for SortType
// config ca be nil to use the defaults, or only set the non-default values desired
// if errors or interrupted, may leave temp files behind in config.TempFilesDir
// the returned channels contain the data returned from calling Sort()
func New(i chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) (*SortTypeSorter, chan SortType, chan error) {
	var err error
	s := newSorter(i, fromBytes, lessFunc, config)
	s.tempWriter, err = tempfile.New(s.config.TempFilesDir)
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
	}
	return s, s.mergeChunkChan, s.mergeErrChan
}

// NewMock is the same as New() but is backed by memory instead of a temporary file on disk
// n is the size to initialize the backing bytes buffer too
func NewMock(i chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config, n int) (*SortTypeSorter, chan SortType, chan error) {
	s := newSorter(i, fromBytes, lessFunc, config)
	s.tempWriter = tempfile.Mock(n)
	return s, s.mergeChunkChan, s.mergeErrChan
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
// this blocks while sorting chunks and unblocks when merging
// NOTE: the context passed to Sort must outlive Sort() returning.
// Merge uses the same context and runs in a goroutine after Sort returns().
// for example, if calling sort in an errGroup, you must pass the group's parent context into sort.
func (s *SortTypeSorter) Sort(ctx context.Context) {
	var buildSortErrGroup, saveErrGroup *errgroup.Group
	buildSortErrGroup, s.buildSortCtx = errgroup.WithContext(ctx)
	saveErrGroup, s.saveCtx = errgroup.WithContext(ctx)

	//start creating chunks
	buildSortErrGroup.Go(s.buildChunks)

	// sort chunks
	for i := 0; i < s.config.NumWorkers; i++ {
		buildSortErrGroup.Go(s.sortChunks)
	}

	// save chunks
	saveErrGroup.Go(s.saveChunks)

	err := buildSortErrGroup.Wait()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return
	}

	// need to close saveChunkChan
	close(s.saveChunkChan)
	err = saveErrGroup.Wait()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return
	}

	// read chunks and merge
	// if this errors, it is returned in the errorChan
	go s.mergeNChunks(ctx)
}

// buildChunks reads data from the input chan to builds chunks and pushes them to chunkChan
func (s *SortTypeSorter) buildChunks() error {
	defer close(s.chunkChan) // if this is not called on error, causes a deadlock

	for {
		c := newChunk(s.config.ChunkSize, s.lessFunc)
		for i := 0; i < s.config.ChunkSize; i++ {
			select {
			case rec, ok := <-s.input:
				if !ok {
					break
				}
				c.data = append(c.data, rec)
			case <-s.buildSortCtx.Done():
				return s.buildSortCtx.Err()
			}
		}
		if len(c.data) == 0 {
			// the chunk is empty
			break
		}

		// chunk is now full
		s.chunkChan <- c
	}

	return nil
}

// sortChunks is a worker for sorting the data stored in a chunk prior to save
func (s *SortTypeSorter) sortChunks() error {
	for {
		select {
		case b, more := <-s.chunkChan:
			if more {
				// sort
				sort.Sort(b)
				// save
				select {
				case s.saveChunkChan <- b:
				case <-s.buildSortCtx.Done():
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

// saveChunks is a worker for saving sorted data to disk
func (s *SortTypeSorter) saveChunks() error {
	var err error
	scratch := make([]byte, binary.MaxVarintLen64)
	for {
		select {
		case b, more := <-s.saveChunkChan:
			if more {
				for _, d := range b.data {
					// binary encoding for size
					raw := d.ToBytes()
					n := binary.PutUvarint(scratch, uint64(len(raw)))
					_, err = s.tempWriter.Write(scratch[:n])
					if err != nil {
						return err
					}
					// add data
					_, err = s.tempWriter.Write(raw)
					if err != nil {
						return err
					}
				}
				_, err = s.tempWriter.Next()
				if err != nil {
					return err
				}
			} else {
				s.tempReader, err = s.tempWriter.Save()
				return err
			}
		case <-s.saveCtx.Done():
			// delete the temp file from disk (error unchecked)
			s.tempWriter.Close()
			return s.saveCtx.Err()
		}
	}
}

// mergeNChunks runs asynchronously in the background feeding data to getNext
// sends errors to s.mergeErrorChan
func (s *SortTypeSorter) mergeNChunks(ctx context.Context) {
	//populate queue with data from mergeFile list
	defer close(s.mergeChunkChan)
	// close temp file when done
	defer func() {
		err := s.tempReader.Close()
		if err != nil {
			s.mergeErrChan <- err
		}
	}()
	defer close(s.mergeErrChan)
	pq := queue.NewPriorityQueue(func(a, b interface{}) bool {
		return s.lessFunc(a.(*mergeFile).nextRec, b.(*mergeFile).nextRec)
	})

	for i := 0; i < s.tempReader.Size(); i++ {
		merge := new(mergeFile)
		merge.fromBytes = s.fromBytes
		merge.reader = s.tempReader.Read(i)
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
		merge := pq.Peek().(*mergeFile)
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
			s.mergeErrChan <- err
			return
		}
	}
}

// mergeFile represents each sorted chunk on disk and its next value
type mergeFile struct {
	nextRec   SortType
	fromBytes FromBytes
	reader    *bufio.Reader
}

// getNext returns the next value from the sorted chunk on disk
// the first call will return nil while the struct is initialized
func (m *mergeFile) getNext() (SortType, bool, error) {
	var newRecBytes []byte
	old := m.nextRec

	n, err := binary.ReadUvarint(m.reader)
	if err == nil {
		newRecBytes = make([]byte, int(n))
		_, err = io.ReadFull(m.reader, newRecBytes)
	}
	if err != nil {
		if err == io.EOF {
			m.nextRec = nil
			return old, false, nil
		}
		return nil, false, err
	}

	m.nextRec = m.fromBytes(newRecBytes)
	return old, true, nil
}
