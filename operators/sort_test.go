//go:build !race

package operators

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"iter"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/daniar-achakeev/paloo_db/io"
	"github.com/daniar-achakeev/paloo_db/utils"
)

/*
// GoStandarSortRunGeneratorParallel test just uses divides into smaller chunks and then
// applies sort in parallel

	type GoSortRunGeneratorParallel[T any, C utils.Comparator[T]] struct {
		runSize               int // maximum size of each run in bytes
		initialRunSize        int // estimated initial size of each run
		sliceBuffer           []T
		parallelism           int
		mergeFunc             MergeFunc[T, C]
		comparatorFunc        C
		getByteSize           utils.GetByteSize[T]
		serialize             utils.Serializer[T]
		tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) io.TempFileWriter[T]
	}

func NewGoSortRunGeneratorParallel[T any, C utils.Comparator[T]](

	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) io.TempFileWriter[T],
	bufferSize int,
	runSize int,
	initialRunSize int,
	parallelism int,

	) *GoSortRunGeneratorParallel[T, C] {
		runGen := &GoSortRunGeneratorParallel[T, C]{
			tempFileWriterFactory: tempFileWriterFactory,
			bufferSize:            bufferSize,
			runSize:               runSize,
			initialRunSize:        initialRunSize,
			parallelism:           parallelism,
		}
		runGen.sliceBuffer = make([]T, 0, initialRunSize)
		return runGen
	}

	func (g *GoSortRunGeneratorParallel[T, C]) GenerateRuns(input iter.Seq[T], createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error {
		if input == nil {
			return fmt.Errorf("input iterator is nil")
		}
		currentSizeBytes := 0
		currentRunIndex := 0
		for t := range input {
			byteSize := g.getByteSize.GetByteSize(t)
			addedSize := currentSizeBytes + byteSize
			if addedSize > g.runSize {
				// sort and flush
				if err := g.sortAndFlush(currentRunIndex, createTmpFile); err != nil {
					return fmt.Errorf("failed to sort and flush: %v", err)
				}
				currentSizeBytes = 0
				currentRunIndex++
				// reset the slice buffer
				g.sliceBuffer = nil
			}
			if g.sliceBuffer == nil {
				g.sliceBuffer = make([]T, 0, g.initialRunSize)
			}
			g.sliceBuffer = append(g.sliceBuffer, t)
			currentSizeBytes += byteSize
		}
		// flush the remaining items
		if len(g.sliceBuffer) > 0 {
			if err := g.sortAndFlush(currentRunIndex, createTmpFile); err != nil {
				return fmt.Errorf("failed to sort and flush remaining items: %v", err)
			}
		}
		return nil
	}

// sortAndFlush sorts the current sliceBuffer and flushes it to a temporary file using multiple goroutines.
// It divides the sliceBuffer into chunks and sorts each chunk in parallel, writing each sorted chunk to a separate temporary file.
// This approach allows for concurrent sorting and writing, improving performance on multi-core systems.

	func (g *GoSortRunGeneratorParallel[T, C]) sortAndFlush(currentRunIndex int, createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error {
		// current plan not to use errgroup
		// change in the future if needed
		var sortedSeq iter.Seq2[T, error]
		var err error
		// currently we use a wrapper around the comparator function
		cmpFunc := func(a, b T) int {
			return g.comparatorFunc.Compare(a, b)
		}
		if g.parallelism > 1 {
			var wg sync.WaitGroup
			errorsChan := make(chan error, g.parallelism)
			chunksChan := make(chan []T, g.parallelism)
			defer close(errorsChan)
			chunkSize := (len(g.sliceBuffer) + g.parallelism - 1) / g.parallelism
			for i := 0; i < g.parallelism; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					start := i * chunkSize
					if start >= len(g.sliceBuffer) {
						// no more data to process
						return
					}
					end := min((i+1)*chunkSize, len(g.sliceBuffer))
					part := g.sliceBuffer[start:end]
					// Sort the chunk
					slices.SortFunc(part, cmpFunc)
					// Send the sorted chunk to the chunks channel
					chunksChan <- part
				}(i)
			}
			wg.Wait()
			// Check for any errors
			select {
			case err := <-errorsChan:
				return err
			default:
			}
			// Close the chunks channel to signal that no more chunks will be sent
			// This is safe to do here because all goroutines have completed
			// and no more chunks will be sent
			// get all chunks from the channel
			chunks := make([]iter.Seq2[T, error], 0, g.parallelism)
			mapToRecordWithError := func(seq iter.Seq[T]) iter.Seq2[T, error] {
				return func(yield func(T, error) bool) {
					for item := range seq {
						if !yield(item, nil) {
							break
						}
					}
				}
			}
			for len(chunksChan) > 0 {
				chunk := <-chunksChan
				chunks = append(chunks, mapToRecordWithError(slices.Values(chunk)))
			}
			// Close the chunks channel
			defer close(chunksChan)
			// single threaded merge now to
			// merge all chunks into a single sorted sequence
			sortedSeq, err = g.mergeFunc(chunks, g.comparatorFunc)
			if err != nil {
				return fmt.Errorf("failed to merge sorted chunks: %v", err)
			}
		} else {
			// Sort the entire sliceBuffer
			slices.SortFunc(g.sliceBuffer, cmpFunc)
			// Create a sequence from the sorted sliceBuffer
			sortedSeq = func(yield func(T, error) bool) {
				for _, item := range g.sliceBuffer {
					if !yield(item, nil) {
						break
					}
				}
			}
		}
		tmpFile, err := createTmpFile(currentRunIndex, 0)
		if err != nil {
			return fmt.Errorf("failed to create temporary file: %v", err)
		}
		defer tmpFile.Close()
		writer := g.tempFileWriterFactory(tmpFile, g.bufferSize, g.serialize)
		if err := writer.WriteSeq(func(yield func(T) bool) {
			for r, err := range sortedSeq {
				if err != nil {
					// stop on error
					return
				}
				if !yield(r) {
					break
				}
			}
		}); err != nil {
			return fmt.Errorf("failed to write merged sequence to temporary file: %v", err)
		}
		return nil
	}
*/

/* Heap merge implementation for testing purposes.
* This implementation uses a min-heap to merge multiple sorted sequences.
* It is not optimized for production use and is intended for testing and comparison purposes only.
 */

// PullIterRecordPair is a helper struct to hold the current record and the next function of an iterator
type PullIterRecordPair[T any] struct {
	record T
	next   func() (T, error, bool)
	stop   func()
}

// internal comparator for PullIterRecordPair
type PullIterRecordPairComparator[T any, C utils.Comparator[T]] struct {
	c C
}

func NewPullIterRecordPairComparator[T any, C utils.Comparator[T]](c C) PullIterRecordPairComparator[T, C] {
	return PullIterRecordPairComparator[T, C]{c: c}
}

func (c PullIterRecordPairComparator[T, C]) Compare(a, b PullIterRecordPair[T]) int {
	return c.c.Compare(a.record, b.record)
}

// MergeHeapFunc has the type of MergeFunc that uses a min-heap to merge sorted sequences.
// It takes a slice of sorted sequences and a comparator function, and returns a single merged sequence.
// It returns an error if any of the input sequences yield an error.
func MergeHeapFunc[T any, C utils.Comparator[T]](sequences []iter.Seq2[T, error], comparatorFunc C) (iter.Seq2[T, error], error) {
	// create heap
	heapCompare := NewPullIterRecordPairComparator(comparatorFunc)
	mergeHeap := &MergeHeap[PullIterRecordPair[T], PullIterRecordPairComparator[T, C]]{
		items:   []PullIterRecordPair[T]{},
		compare: heapCompare,
	}
	heap.Init(mergeHeap)
	// Implement merging logic here
	for _, s := range sequences {
		// pull the first item from each sequence
		next, stop := iter.Pull2(s)
		r, err, ok := next()
		if !ok {
			continue
		}
		if err != nil {
			return nil, err
		}
		pair := PullIterRecordPair[T]{record: r, next: next, stop: stop}
		heap.Push(mergeHeap, pair)
	}
	return func(yield func(T, error) bool) {
		for mergeHeap.Len() > 0 {
			// pull the smallest item from the heap
			item := heap.Pop(mergeHeap).(PullIterRecordPair[T])
			// yield the item
			if !yield(item.record, nil) {
				return
			}
			// push the next item from the same sequence
			next, stop := item.next, item.stop
			if r, err, ok := next(); ok {
				if err != nil {
					// stop the iteration on error
					// TODO: should I push error and Zero?
					stop()
					return
				}
				pair := PullIterRecordPair[T]{record: r, next: next, stop: stop}
				heap.Push(mergeHeap, pair)
			} else {
				stop()
			}
		}
	}, nil
}

// MergeHeap is a min-heap used for merging sorted sequences.
// uses standard library container/heap interface
type MergeHeap[T any, C utils.Comparator[T]] struct {
	items   []T
	compare C
}

func (h *MergeHeap[T, C]) Len() int {
	return len(h.items)
}

func (h *MergeHeap[T, C]) Less(i, j int) bool {
	return h.compare.Compare(h.items[i], h.items[j]) < 0
}

func (h *MergeHeap[T, C]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *MergeHeap[T, C]) Push(x any) {
	h.items = append(h.items, x.(T))
}

func (h *MergeHeap[T, C]) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}

func permutate[T any](slice []T) []T {
	n := len(slice)
	r := rand.New(rand.NewSource(42))
	for i := n - 1; i > 0; i-- {
		// get a random index j such that 0 <= j <= i
		if i > 1 {
			j := int(r.Int31n(int32(i - 1)))
			// swap slice[i] and slice[j]
			slice[i], slice[j] = slice[j], slice[i]
		}
	}
	return slice
}

/* END Heap merge implementation for testing purposes.
* This implementation uses a min-heap to merge multiple sorted sequences.
* It is not optimized for production use and is intended for testing and comparison purposes only.
 */

// Int32DSCmp is a deserializer, serializer and comparator for int32 type
type Int32DSCmp struct{}

func (d Int32DSCmp) Deserialize(data []byte) (int32, error) {
	return int32(binary.BigEndian.Uint32(data)), nil
}

// Serialize
func (d Int32DSCmp) Serialize(item int32, buf []byte) error {
	binary.BigEndian.PutUint32(buf, uint32(item))
	return nil
}

func (d Int32DSCmp) Compare(a, b int32) int {
	return int(a - b)
}

func (g Int32DSCmp) GetByteSize(item int32) int {
	return 4
}

func TestGoSortRunGenerator(t *testing.T) {
	elementsCount := 128 + 30 // we would have three runs each run will be sorted in parallel producing 4 files per run
	readWriteBufferSize := 64 // 12 bytes per page header 64 -12 = 52 bytes for data => 13 int32 per page
	runSizeByteMemory := 256  // 256 /4 = 64 int32 per run
	directory := t.TempDir()
	int32Slice := make([]int32, 0, elementsCount)
	for i := range elementsCount {
		int32Slice = append(int32Slice, int32(i))
	}
	int32Slice = permutate(int32Slice)
	int32DSCmp := Int32DSCmp{}
	factory := func(file *os.File, serialize utils.Serializer[int32]) io.TempFileWriter[int32] {
		return io.NewFixedSizeTempFileWriter(file, readWriteBufferSize, 4, serialize)
	}
	runGenerator := NewGoStandarSortRunGenerator(
		runSizeByteMemory, // run size
		128/4,             // initial run size
		int32DSCmp,
		int32DSCmp,
		int32DSCmp,
		factory,
	)
	createTmpFile := func(currentRunIndex int, index int) (*os.File, error) {
		fileName := fmt.Sprintf("%s/%s_run_%06d_%03d.%s", directory, "sort", currentRunIndex, index, "tmp")
		return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	}
	err := runGenerator.GenerateRuns(slices.Values(int32Slice), createTmpFile)
	if err != nil {
		t.Fatalf("failed to generate runs: %v", err)
	}
	// read back the files and verify sorting
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("failed to read directory: %v", err)
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	slices.Sort(files)
	t.Log("Generated files:", files)
	count := 0
	for _, file := range files {
		f, err := os.Open(directory + "/" + file)
		if err != nil {
			t.Fatalf("failed to open file %s: %v", file, err)
		}
		reader := io.NewFixedLenTempFileReader(f, 64, 4, int32DSCmp)
		t.Logf("Reading file: %s", file)
		previous := int32(-1)
		for r, err := range reader.All() { // just to ensure we can read all records
			if err != nil {
				t.Errorf("read failed: %v", err)
			}
			count++
			// check ordering
			if r < previous {
				t.Errorf("expected %d, but got %d", previous+1, r)
			}
			previous = r
		}
		f.Close()
	}
	if count != elementsCount {
		t.Errorf("expected to read %d records, but got %d", elementsCount, count)
	}
}

// TODO More tests  e.g. error handling and edge cases
// also different configurations e.g. parallelism, run size, buffer size etc.

func Map[T any, U any](input iter.Seq[T], mapFunc func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for item := range input {
			if !yield(mapFunc(item)) {
				break
			}
		}
	}
}

func Map2[T any, U any](input iter.Seq[T], mapFunc func(T) U) iter.Seq2[U, error] {
	return func(yield func(U, error) bool) {
		for item := range input {
			if !yield(mapFunc(item), nil) {
				break
			}
		}
	}
}

func TestKWayMergeHeap(t *testing.T) {
	k := 5
	sequences := make([]iter.Seq2[int32, error], 0)
	allCount := 0
	slice := make([]int32, 0)
	numElements := 53
	for i := range numElements {
		slice = append(slice, int32(i))
		allCount++
	}
	chunkSize := (numElements + k - 1) / k
	for i := range k {
		start := i * chunkSize
		end := min(start+chunkSize, numElements)
		if start >= end {
			continue
		}
		subSlice := slice[start:end]
		t.Log("Sequence", i, "from", start, "to", end, "size", len(subSlice), "contents:", subSlice)
		mappedIt := Map2(slices.Values(subSlice), func(v int32) int32 {
			return v
		})
		sequences = append(sequences, mappedIt)
	}
	t.Logf("Merging %d sequences", len(sequences))

	merger := MergeHeapFunc[int32, Int32DSCmp]
	comparator := Int32DSCmp{}
	mergedSeq, err := merger(sequences, comparator)
	if err != nil {
		t.Fatalf("failed to merge sequences: %v", err)
	}
	previous := int32(-1)
	count := 0
	for r, err := range mergedSeq {
		if err != nil {
			t.Errorf("failed to read merged sequence: %v", err)
			continue
		}
		if r < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		//t.Log("sorted record", r)
		previous = r
		count++
	}
	if count != allCount {
		t.Errorf("expected to read %d records, but got %d", allCount, count)
	}
}

type IntCmp struct{}

func (d IntCmp) Compare(a, b int) int {
	return a - b
}

func initTestTournamentTree() (*TournamentTree[int, IntCmp], []iter.Seq2[int, error]) {
	k := 9
	a1 := []int{3, 12, 18}
	a2 := []int{1, 16, 21, 27}
	a3 := []int{4, 7, 23, 28}
	a4 := []int{10, 11, 26, 30}
	a5 := []int{5, 13, 25}
	a6 := []int{9, 14}
	a7 := []int{2, 15, 22}
	a8 := []int{6, 17, 20}
	a9 := []int{8, 19, 24, 29}
	iterSlice := make([]iter.Seq2[int, error], 0, k)
	iterSlice = append(iterSlice, Map2(slices.Values(a1), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a2), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a3), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a4), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a5), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a6), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a7), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a8), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a9), func(v int) int { return v }))
	cmp := IntCmp{}
	tournament := NewTournamentTree(cmp, []TNode[int]{
		{value: 3, idx: 0},
		{value: 1, idx: 1},
		{value: 4, idx: 2},
		{value: 10, idx: 3},
		{value: 5, idx: 4},
		{value: 9, idx: 5},
		{value: 2, idx: 6},
		{value: 6, idx: 7},
		{value: 8, idx: 8},
	})
	return tournament, iterSlice
}

func TestTournamentTreeBuild(t *testing.T) {
	tournament, sources := initTestTournamentTree()
	iterators := make([]struct {
		next func() (int, error, bool)
		stop func()
	}, len(sources))
	for i, source := range sources {
		next, stop := iter.Pull2(source)
		iterators[i].next, iterators[i].stop = next, stop
	}
	// consume initial winners
	for _, it := range iterators {
		it.next()
	}
	// test first ten winners
	for i := range 30 {
		winner, _ := tournament.Winner()
		//t.Log("Winner: ", winner, ok, "Index:", i)
		if winner.value != i+1 {
			t.Fatalf("expected winner to be %d, but got %d", i+1, winner.value)
		}
		//t.Log(tournament.tree)
		nextVal, _, ok := iterators[winner.idx].next()
		if !ok {
			tournament.Challenge(TNode[int]{idx: -1}, winner.idx)
			continue
		}
		//t.Log(nextVal)
		tournament.Challenge(TNode[int]{value: nextVal, idx: winner.idx}, winner.idx)
	}
	for _, source := range iterators {
		source.stop()
	}
	_, ok := tournament.Winner() // should nok
	if ok {
		t.Fatalf("expected no winner, but got one")
	}
}

func TestTournamentMerge(t *testing.T) {
	k := 5
	sequences := make([]iter.Seq2[int32, error], 0)
	allCount := 0
	slice := make([]int32, 0)
	numElements := 53
	for i := range numElements {
		slice = append(slice, int32(i))
		allCount++
	}
	chunkSize := (numElements + k - 1) / k
	for i := range k {
		start := i * chunkSize
		end := min(start+chunkSize, numElements)
		if start >= end {
			continue
		}
		subSlice := slice[start:end]
		t.Log("Sequence", i, "from", start, "to", end, "size", len(subSlice), "contents:", subSlice)
		mappedIt := Map2(slices.Values(subSlice), func(v int32) int32 {
			return v
		})
		sequences = append(sequences, mappedIt)
	}
	t.Logf("Merging %d sequences", len(sequences))
	merger := MergeTournamentFunc[int32, Int32DSCmp]
	mergedSeq, err := merger(sequences, Int32DSCmp{})
	if err != nil {
		t.Fatalf("failed to merge sequences: %v", err)
	}
	previous := int32(-1)
	count := 0
	for r, err := range mergedSeq {
		if err != nil {
			t.Errorf("failed to read record: %v", err)
			continue
		}
		if r < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		//t.Log("sorted record", r)
		previous = r
		count++
	}
	if count != allCount {
		t.Errorf("expected to read %d records, but got %d", allCount, count)
	}
}

type Int64DSCmp struct{}

func (d Int64DSCmp) Deserialize(data []byte) (int64, error) {
	return int64(binary.BigEndian.Uint64(data)), nil
}

// Serialize
func (d Int64DSCmp) Serialize(item int64, buf []byte) error {
	binary.BigEndian.PutUint64(buf, uint64(item))
	return nil
}

func (d Int64DSCmp) Compare(a, b int64) int {
	return int(a - b)
}

func (g Int64DSCmp) GetByteSize(item int64) int {
	return 8
}

func TestSimpleInt64Sort(t *testing.T) {
	elementsCount := 1000
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	tmpDirectory := t.TempDir()
	prefix := "int64sort"
	suffix := "tmp"
	kWay := 5
	readBufferSize, writeBufferSize := 1024, 1024 //
	runSize := 512                                // 512 /8 = 64 int64 per run
	writeFactory := func(file *os.File, serialize utils.Serializer[int64]) io.TempFileWriter[int64] {
		return io.NewFixedSizeTempFileWriter(file, writeBufferSize, 8, serialize)
	}
	readFactory := func(file *os.File, deserialize utils.Deserializer[int64]) io.TempFileReader[int64] {
		return io.NewFixedLenTempFileReader(file, readBufferSize, 8, deserialize)
	}
	cmpDeSer := Int64DSCmp{}
	runGenerator := NewGoStandarSortRunGenerator(
		runSize,
		512/8, // initial run size
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		writeFactory,
	)
	sorter := NewSorter(
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		MergeTournamentFunc[int64, Int64DSCmp],
		runGenerator,
		readFactory,
		writeFactory,
		tmpDirectory,
		prefix,
		suffix,
		kWay,
	)
	sortedSeq, err := sorter.Sort(slices.Values(int64Slice))
	if err != nil {
		t.Fatalf("failed to sort: %v", err)
	}
	previous := int64(-1)
	count := 0
	for r, err := range sortedSeq {
		if err != nil {
			t.Errorf("read failed: %v", err)
		}
		if r < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		previous = r
		count++
	}
	if count != elementsCount {
		t.Errorf("expected to read %d records, but got %d", elementsCount, count)
	}
}

func TestSimpleInt64SortLarge(t *testing.T) {

	if strings.Contains(t.Name(), "large") {
		t.Skip("Local run test only")
	}

	elementsCount := 100_000_000
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	tests := []struct {
		name string
		heap bool
	}{
		{"Tournament", false},
		{"Heap", true},
	}
	prefix := "int64sort"
	suffix := "tmp"
	kWay := 64                                            // memory is 128 KB * 100 at least
	runSize := 1024 * 1024 * 16                           // 16 MB Buffer
	readBufferSize, writeBufferSize := 1024*512, 1024*512 // 512 KB
	writeFactory := func(file *os.File, serialize utils.Serializer[int64]) io.TempFileWriter[int64] {
		return io.NewFixedSizeTempFileWriter(file, writeBufferSize, 8, serialize)
	}
	readFactory := func(file *os.File, deserialize utils.Deserializer[int64]) io.TempFileReader[int64] {
		return io.NewFixedLenTempFileReader(file, readBufferSize, 8, deserialize)
	}
	cmpDeSer := Int64DSCmp{}
	// start tests
	for _, tt := range tests {
		t.Logf("Running test: %s", tt.name)
		t.Run(tt.name, func(t *testing.T) {
			tmpDirectory := t.TempDir()
			t.Logf("Using temp directory: %s", tmpDirectory)
			var sortedSeq iter.Seq2[int64, error]
			var err error
			if tt.heap {
				runGenerator := NewGoStandarSortRunGenerator(
					runSize,
					runSize/8, // initial run size
					cmpDeSer,
					cmpDeSer,
					cmpDeSer,
					writeFactory,
				)
				sorter := NewSorter(
					cmpDeSer,
					cmpDeSer,
					cmpDeSer,
					MergeHeapFunc[int64, Int64DSCmp],
					runGenerator,
					readFactory,
					writeFactory,
					tmpDirectory,
					prefix,
					suffix,
					kWay,
				)
				start := time.Now()
				sortedSeq, err = sorter.Sort(slices.Values(int64Slice))
				if err != nil {
					t.Fatalf("failed to sort: %v", err)
				}
				duration := time.Since(start)
				t.Logf("Sort -1 merge %s", duration)
			} else {
				runGenerator := NewGoStandarSortRunGenerator(
					runSize,
					runSize/8, // initial run size
					cmpDeSer,
					cmpDeSer,
					cmpDeSer,
					writeFactory,
				)
				sorter := NewSorter(
					cmpDeSer,
					cmpDeSer,
					cmpDeSer,
					MergeTournamentFunc[int64, Int64DSCmp],
					runGenerator,
					readFactory,
					writeFactory,
					tmpDirectory,
					prefix,
					suffix,
					kWay,
				)
				start := time.Now()
				sortedSeq, err = sorter.Sort(slices.Values(int64Slice))
				if err != nil {
					t.Fatalf("failed to sort: %v", err)
				}
				duration := time.Since(start)
				t.Logf("Sort -1 merge %s", duration)
			}
			// verify sorting
			previous := int64(-1)
			count := 0
			start := time.Now()
			for r, err := range sortedSeq {
				if err != nil {
					t.Fatalf("read failed: %v", err)
				}
				if r < previous {
					t.Fatalf("expected %d, but got %d", previous, r)
				}
				previous = r
				count++
			}
			if count != elementsCount {
				t.Fatalf("expected to read %d records, but got %d", elementsCount, count)
			}
			duration := time.Since(start)
			t.Logf("Last Merge %s", duration)
		})
	}
}
