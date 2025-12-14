// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory of this source tree.

package operators

// This file implements a generic external sorter that can sort large datasets that do not fit into memory.
// TODO: provide abstraction to run generating and merging in parallel
// the main sorter should be flexible enough to allow different strategies for run generation and merging
// e.g. multi-threaded, single-threaded, replacement-selection, radix,... etc.
// We are still using standard range iterators mostly iter.Seq and iter.Seq2 with error handling
import (
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/daniar-achakeev/paloo_db/io"
	"github.com/daniar-achakeev/paloo_db/utils"
)

// Redefine back as function alias
type MergeFunc[T any, C utils.Comparator[T]] func(sequences []iter.Seq2[T, error], cmp C) (iter.Seq2[T, error], error)

// RunGenerator is an interface for generating sorted runs from an input sequence.
type RunGenerator[T any, C utils.Comparator[T]] interface {
	GenerateRuns(input iter.Seq[T], createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error
}

// Sorter is a generic external sorter that can sort large datasets that do not fit into memory.
// main task is to orchestrate the sorting process by generating sorted runs and merging them.
// TODO: workload/resource  manager will assign memory and cpu to the sorter
// It uses a combination of in-memory sorting and external sorting techniques to achieve this.
// currently, we will implement a simple sorting with comparator on deserialized items
// TODO: implement also comparators based on serialized items to avoid deserialization overhead
// TODO: if data can fully fit into memory, we can use in-memory sorting algorithms, fall back to external sorting otherwise
// TODO: since we process data in chunks and temp files are block oriented, we could also think about parallelizing flushing to disk and reading from disk
type Sorter[T any, C utils.Comparator[T]] struct {
	comparatorFunc        C
	serialize             utils.Serializer[T]
	deserialize           utils.Deserializer[T]
	mergeFunc             MergeFunc[T, C]
	runGenerator          RunGenerator[T, C]
	tempFileReaderFactory func(file *os.File, deserialize utils.Deserializer[T]) io.TempFileReader[T]
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) io.TempFileWriter[T]
	kWayMergeSize         int // max number of files that would be merged in each round
	directoryPath         string
	filePrefix            string
	fileExtension         string
	runStr                string
	mergeStr              string
	currentMergeRound     int
}

func NewSorter[T any, C utils.Comparator[T]](
	comparatorFunc C,
	serialize utils.Serializer[T],
	deserialize utils.Deserializer[T],
	mergeFunc MergeFunc[T, C],
	runGenerator RunGenerator[T, C],
	tempFileReaderFactory func(file *os.File, deserialize utils.Deserializer[T]) io.TempFileReader[T],
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) io.TempFileWriter[T],
	directoryPath string,
	filePrefix string,
	fileExtension string,
	kWayMergeSize int,
) *Sorter[T, C] {
	return &Sorter[T, C]{
		comparatorFunc:        comparatorFunc,
		serialize:             serialize,
		deserialize:           deserialize,
		runGenerator:          runGenerator,
		mergeFunc:             mergeFunc,
		tempFileReaderFactory: tempFileReaderFactory,
		tempFileWriterFactory: tempFileWriterFactory,
		directoryPath:         directoryPath,
		filePrefix:            filePrefix,
		fileExtension:         fileExtension,
		kWayMergeSize:         kWayMergeSize,
		runStr:                "run",
		mergeStr:              "merge",
	}
}

func (s *Sorter[T, C]) Sort(input iter.Seq[T]) (iter.Seq2[T, error], error) {
	if input == nil {
		return nil, fmt.Errorf("input iterator is nil")
	}
	// Initialize the run generator
	createTmpFile := func(currentRunIndex int, index int) (*os.File, error) {
		fileName := fmt.Sprintf("%s/%s_%s_%d_%05d.%s", s.directoryPath, s.filePrefix, s.runStr, currentRunIndex, index, s.fileExtension)
		return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	}
	err := s.runGenerator.GenerateRuns(input, createTmpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to generate runs: %v", err)
	}
	// now read all files with the given prefix from the directory
	// and merge them using the merge function
	isRunStage := true
	s.currentMergeRound = 0
	files, err := s.getFilesToMerge(isRunStage, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to get files to merge: %v", err)
	}
	for len(files) > s.kWayMergeSize {
		// now chunk the files into batches of kWayMergeSize
		s.currentMergeRound++
		for i := 0; i < len(files); i += s.kWayMergeSize {
			end := min(i+s.kWayMergeSize, len(files))
			batch := files[i:end]
			mergeSeq, err := s.mergeFiles(batch)
			if err != nil {
				return nil, fmt.Errorf("failed to merge files: %v", err)
			}
			err = s.flushMergeSequence(mergeSeq, s.currentMergeRound, i)
			if err != nil {
				return nil, fmt.Errorf("failed to flush merge sequence: %v", err)
			}
		}
		s.deleteFiles(files)
		isRunStage = false
		files, err = s.getFilesToMerge(isRunStage, s.currentMergeRound)
		if err != nil {
			return nil, fmt.Errorf("failed to get files to merge: %v", err)
		}
	}
	return s.mergeFiles(files)
}

func (s *Sorter[T, C]) mergeFiles(files []string) (iter.Seq2[T, error], error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files to merge")
	}
	if len(files) == 1 {
		file, err := s.openFile(files[0])
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", files[0], err)
		}
		reader := s.tempFileReaderFactory(file, s.deserialize)
		return reader.All(), nil
	}
	slicesOfSeq := make([]iter.Seq2[T, error], 0, len(files))
	for _, fileName := range files {
		file, err := s.openFile(fileName)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", fileName, err)
		}
		reader := s.tempFileReaderFactory(file, s.deserialize)
		slicesOfSeq = append(slicesOfSeq, reader.All())
	}
	return s.mergeFunc(slicesOfSeq, s.comparatorFunc)
}

func (s *Sorter[T, C]) getFilesToMerge(isRun bool, level int) ([]string, error) {
	entries, err := os.ReadDir(s.directoryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %v", err)
	}
	var files []string
	prefixCheck := s.filePrefix + "_" + s.runStr + "_"
	if !isRun {
		prefixCheck = s.filePrefix + "_" + s.mergeStr + "_" + fmt.Sprintf("%d", level) + "_"
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefixCheck) {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

func (s *Sorter[T, C]) flushMergeSequence(mergeSeq iter.Seq2[T, error], currentMergeRound int, index int) error {
	// create a new temporary file for the merged output
	// realistically index is 6 decimal digits
	fileName := fmt.Sprintf("%s/%s_%s_%d_%06d.%s", s.directoryPath, s.filePrefix, s.mergeStr, currentMergeRound, index, s.fileExtension)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create merge output file %s: %v", fileName, err)
	}
	defer file.Close()
	// create a writer
	writer := s.tempFileWriterFactory(file, s.serialize)
	defer writer.Close()
	err = writer.WriteSeq(func(yield func(T) bool) {
		for r, err := range mergeSeq {
			if err != nil {
				// stop on error
				return
			}
			if !yield(r) {
				return
			}
		}
	})
	if err != nil {
		return fmt.Errorf("failed to write merged sequence to file %s: %v", fileName, err)
	}
	return nil
}

func (s *Sorter[T, C]) deleteFiles(files []string) error {
	// asynchronously delete files
	// FIXME: currently fire and forget
	// in the future, we can use a worker pool to limit the number of concurrent deletions
	// and also handle errors properly
	// passing the error channel back to the caller
	go func() {
		for _, file := range files {
			filePath := filepath.Join(s.directoryPath, file)
			os.Remove(filePath)
		}
	}()
	return nil
}

func (s *Sorter[T, C]) openFile(fileName string) (*os.File, error) {
	// simple open file for reading
	filePath := filepath.Join(s.directoryPath, fileName)
	return os.Open(filePath)
}

func (s *Sorter[T, C]) Close() error {
	// FIXME: Implement any necessary cleanup logic here
	// removes all files in the directory with the same prefix
	return nil
}

// GoStandarSortRunGenerator uses golang standard slices.sort
// wraps into func the comparator not so fast no inlining
type GoStandarSortRunGenerator[T any, C utils.Comparator[T]] struct {
	runSize               int // maximum size of each run in bytes
	initialRunSize        int // estimated initial size of each run
	sliceBuffer           []T //
	comparatorFunc        C
	getByteSize           utils.GetByteSize[T]
	serialize             utils.Serializer[T]
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) io.TempFileWriter[T]
}

func NewGoStandarSortRunGenerator[T any, C utils.Comparator[T]](
	runSize int,
	initialRunSize int,
	comparatorFunc C,
	getByteSize utils.GetByteSize[T],
	serialize utils.Serializer[T],
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) io.TempFileWriter[T],
) *GoStandarSortRunGenerator[T, C] {
	return &GoStandarSortRunGenerator[T, C]{
		runSize:               runSize,
		initialRunSize:        initialRunSize,
		comparatorFunc:        comparatorFunc,
		getByteSize:           getByteSize,
		serialize:             serialize,
		tempFileWriterFactory: tempFileWriterFactory,
		sliceBuffer:           make([]T, 0, initialRunSize),
	}

}

func (g *GoStandarSortRunGenerator[T, C]) GenerateRuns(input iter.Seq[T], createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error {
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
	//
	return nil
}

// sortAndFlush sorts the current sliceBuffer and writes to temp file
func (g *GoStandarSortRunGenerator[T, C]) sortAndFlush(currentRunIndex int, createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error {
	var err error
	// Sort the entire sliceBuffer
	// NOTE: FIXME currently we use a wrapper around the comparator function
	slices.SortFunc(g.sliceBuffer, g.comparatorFunc.Compare)
	tmpFile, err := createTmpFile(currentRunIndex, 0)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tmpFile.Close()
	writer := g.tempFileWriterFactory(tmpFile, g.serialize)
	if err := writer.WriteSeq(slices.Values(g.sliceBuffer)); err != nil {
		return fmt.Errorf("failed to write merged sequence to temporary file: %v", err)
	}
	return nil
}

// MergeTournamentFunc
func MergeTournamentFunc[T any, C utils.Comparator[T]](sequences []iter.Seq2[T, error], cmp C) (iter.Seq2[T, error], error) {
	// create tournament tree
	iterators := make([]struct {
		next func() (T, error, bool)
		stop func()
	}, len(sequences))
	contestents := make([]TNode[T], len(sequences))
	for i, s := range sequences {
		next, stop := iter.Pull2(s)
		iterators[i].next, iterators[i].stop = next, stop
		r, err, ok := iterators[i].next()
		if !ok {
			contestents[i] = TNode[T]{idx: -1} // sentinel
			stop()
			continue
		}
		if err != nil {
			return nil, err
		}
		contestents[i] = TNode[T]{value: r, idx: i}
	}
	tournament := NewTournamentTree(cmp, contestents)
	return func(yield func(T, error) bool) {
		for { // repeatedly pull the smallest item from the tournament tree
			winner, ok := tournament.Winner()
			if !ok { // all sources exhausted
				return
			}
			winnerValue := winner.value
			if !yield(winnerValue, nil) {
				return
			}
			nextRecord, err, ok := iterators[winner.idx].next()
			if err != nil {
				yield(*new(T), err)
				iterators[winner.idx].stop()
				return
			}
			if !ok { // source exhausted
				tournament.Challenge(TNode[T]{idx: -1}, winner.idx)
				iterators[winner.idx].stop()
				continue
			}
			tournament.Challenge(TNode[T]{value: nextRecord, idx: winner.idx}, winner.idx)
		}
	}, nil
}

// TNode: represents a node in the tournament tree used for k-way merging.
type TNode[T any] struct {
	value T   // value of the looser
	idx   int // source index of the looser
}

func (t TNode[T]) String() string {
	return fmt.Sprintf("TNode{value: %v, idx: %d}", t.value, t.idx)
}

// TournamentTree: a k-way merge algorithm using a tournament tree
type TournamentTree[T any, C utils.Comparator[T]] struct {
	tree   []TNode[T]
	cmp    C
	height int
}

func NewTournamentTree[T any, C utils.Comparator[T]](cmp C, fContestents []TNode[T]) *TournamentTree[T, C] {
	k := len(fContestents)
	height := 0
	for (1 << height) < k {
		height++
	}
	tree := make([]TNode[T], 1<<height)
	for i := 0; i < cap(tree); i++ {
		tree[i] = TNode[T]{value: utils.Zero[T](), idx: -1} // all sentinel
	}
	cand := make([]TNode[T], 0, len(fContestents))
	cand = append(cand, fContestents...)
	for h := height - 1; h >= 0; h-- {
		start := 1 << h
		winners := make([]TNode[T], 0, 1<<h)
		for i := 0; i < len(cand); i, start = i+2, start+1 {
			if i+1 >= len(cand) {
				winners = append(winners, cand[i]) // winner
				tree[start] = TNode[T]{idx: -1}    // loser
				continue
			}
			if cand[i].idx == -1 && cand[i+1].idx == -1 {
				winners = append(winners, TNode[T]{idx: -1}) // winner
				tree[start] = TNode[T]{idx: -1}              // loser
				continue
			}
			if cand[i].idx == -1 && cand[i+1].idx != -1 {
				winners = append(winners, cand[i+1]) //winner
				tree[start] = TNode[T]{idx: -1}      // loser
				continue
			}
			if cand[i].idx != -1 && cand[i+1].idx == -1 {
				winners = append(winners, cand[i]) //winner
				tree[start] = TNode[T]{idx: -1}    // loser
				continue
			}
			if cmp.Compare(cand[i].value, cand[i+1].value) < 0 {
				winners = append(winners, cand[i])
				tree[start] = cand[i+1]
			} else {
				winners = append(winners, cand[i+1])
				tree[start] = cand[i]
			}
		}
		cand = winners
	}
	tree[0] = cand[0] // // set winner
	return &TournamentTree[T, C]{tree: tree, cmp: cmp, height: height}
}

// Winner returns the current winner of the tournament
func (t *TournamentTree[T, C]) Winner() (winner TNode[T], ok bool) {
	return t.tree[0], t.tree[0].idx != -1
}

// Challenge updates the tournament tree with a new challenger
func (t *TournamentTree[T, C]) Challenge(challenger TNode[T], index int) {
	w := challenger
	pIdx := (len(t.tree) + index) / 2
	for pIdx >= 1 {
		p := t.tree[pIdx] //
		if p.idx == -1 {  // sentinel
			pIdx /= 2
			continue
		}
		if w.idx == -1 || t.cmp.Compare(w.value, p.value) >= 0 { // sentinel
			w, t.tree[pIdx] = p, w // swap
		}
		pIdx /= 2
	}
	t.tree[0] = w
}
