package desync

import (
	"os"
	"sort"
	"sync"
)

// SparseFile represents a file that is written as it is read (Copy-on-read). It is
// used as a fast cache. Any chunk read from the store to satisfy a read operation
// is written to the file.
type SparseFile struct {
	name string
	idx  Index

	loader *sparseFileLoader
}

// SparseFileHandle is used to access a sparse file. All read operations performed
// on the handle are either done on the file if the required ranges are available
// or loaded from the store and written to the file.
type SparseFileHandle struct {
	sf   *SparseFile
	file *os.File
}

func NewSparseFile(name string, idx Index, s Store) (*SparseFile, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return &SparseFile{
		name:   name,
		idx:    idx,
		loader: newSparseFileLoader(name, idx, s),
	}, nil
}

// Open returns a handle for a sparse file.
func (sf *SparseFile) Open() (*SparseFileHandle, error) {
	file, err := os.Open(sf.name)
	return &SparseFileHandle{
		sf:   sf,
		file: file,
	}, err
}

// Length returns the size of the index used for the sparse file.
func (sf *SparseFile) Length() int64 {
	return sf.idx.Length()
}

// ReadAt reads from the sparse file. All accessed ranges are first written
// to the file and then returned.
func (h *SparseFileHandle) ReadAt(b []byte, offset int64) (int, error) {
	if err := h.sf.loader.loadRange(offset, int64(len(b))); err != nil {
		return 0, err
	}
	return h.file.ReadAt(b, offset)
}

func (h *SparseFileHandle) Close() error {
	return h.file.Close()
}

type sparseIndexChunk struct {
	IndexChunk
	once sync.Once
}

// Loader for sparse files
type sparseFileLoader struct {
	name string
	done []bool
	mu   sync.RWMutex
	s    Store

	chunks []*sparseIndexChunk
}

func newSparseFileLoader(name string, idx Index, s Store) *sparseFileLoader {
	chunks := make([]*sparseIndexChunk, 0, len(idx.Chunks))
	for _, c := range idx.Chunks {
		chunks = append(chunks, &sparseIndexChunk{IndexChunk: c})
	}

	return &sparseFileLoader{
		name:   name,
		done:   make([]bool, len(idx.Chunks)),
		chunks: chunks,
		s:      s,
	}
}

// For a given byte range, returns the index of the first and last chunk needed to populate it
func (l *sparseFileLoader) indexRange(start, length int64) (int, int) {
	end := uint64(start + length - 1)
	firstChunk := sort.Search(len(l.chunks), func(i int) bool { return start < int64(l.chunks[i].Start+l.chunks[i].Size) })
	if length < 1 {
		return firstChunk, firstChunk
	}
	if firstChunk >= len(l.chunks) { // reading past the end, load the last chunk
		return len(l.chunks) - 1, len(l.chunks) - 1
	}

	// Could do another binary search to find the last, but in reality, most reads are short enough to fall
	// into one or two chunks only, so may as well use a for loop here.
	lastChunk := firstChunk
	for i := firstChunk + 1; i < len(l.chunks); i++ {
		if end < l.chunks[i].Start {
			break
		}
		lastChunk++
	}
	return firstChunk, lastChunk
}

// Loads all the chunks needed to populate the given byte range (if not already loaded)
func (l *sparseFileLoader) loadRange(start, length int64) error {
	first, last := l.indexRange(start, length)
	var chunksNeeded []int
	l.mu.RLock()
	for i := first; i <= last; i++ {
		if l.done[i] {
			continue
		}
		chunksNeeded = append(chunksNeeded, i)
	}
	l.mu.RUnlock()

	// TODO: Load the chunks concurrently
	for _, chunk := range chunksNeeded {
		if err := l.loadChunk(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (l *sparseFileLoader) loadChunk(i int) error {
	var loadErr error
	l.chunks[i].once.Do(func() {
		c, err := l.s.GetChunk(l.chunks[i].ID)
		if err != nil {
			loadErr = err
			return
		}
		b, err := c.Uncompressed()
		if err != nil {
			loadErr = err
			return
		}

		f, err := os.OpenFile(l.name, os.O_RDWR, 0666)
		if err != nil {
			loadErr = err
			return
		}
		defer f.Close()

		if _, err := f.WriteAt(b, int64(l.chunks[i].Start)); err != nil {
			loadErr = err
			return
		}

		l.mu.Lock()
		l.done[i] = true
		l.mu.Unlock()
	})
	return loadErr
}
