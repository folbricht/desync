package desync

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"

	"github.com/boljen/go-bitmap"
)

// SparseFile represents a file that is written as it is read (Copy-on-read). It is
// used as a fast cache. Any chunk read from the store to satisfy a read operation
// is written to the file.
type SparseFile struct {
	name string
	idx  Index
	opt  SparseMountOptions

	loader *sparseFileLoader
}

// SparseFileHandle is used to access a sparse file. All read operations performed
// on the handle are either done on the file if the required ranges are available
// or loaded from the store and written to the file.
type SparseFileHandle struct {
	sf   *SparseFile
	file *os.File
}

func NewSparseFile(name string, idx Index, s Store, opt SparseMountOptions) (*SparseFile, error) {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	loader := newSparseFileLoader(name, idx, s)
	sf := &SparseFile{
		name:   name,
		idx:    idx,
		loader: loader,
		opt:    opt,
	}

	// Simple check to see if the file is correct for the given index by
	// just comparing the size. If it's not, then just reset the file and
	// don't load a state.
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	sparseFileMatch := stat.Size() == idx.Length()

	// If the sparse-file looks like it's of the right size, and we have a
	// save state file, try to use those. No need to further initialize if
	// that's successful
	if sparseFileMatch && opt.StateSaveFile != "" {
		stateFile, err := os.Open(opt.StateSaveFile)
		if err == nil {
			defer stateFile.Close()

			// If we can load the state file, we have everything needed,
			// no need to initialize it.
			if err := loader.loadState(stateFile); err == nil {
				return sf, nil
			}
		}
	}

	// Create the new file at full size, that was we can skip loading null-chunks,
	// this should be a NOP if the file matches the index size already.
	if err = f.Truncate(idx.Length()); err != nil {
		return nil, err
	}

	// Try to initialize the sparse file from a prior state file if one is provided.
	// This will concurrently load all chunks marked "done" in the state file and
	// write them to the sparse file.
	if opt.StateInitFile != "" {
		initFile, err := os.Open(opt.StateInitFile)
		if err != nil {
			return nil, err
		}
		defer initFile.Close()
		if err := loader.preloadChunksFromState(initFile, opt.StateInitConcurrency); err != nil {
			return nil, err
		}
	}

	return sf, nil
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

// WriteState saves the state of file, basically which chunks were loaded
// and which ones weren't.
func (sf *SparseFile) WriteState() error {
	if sf.opt.StateSaveFile == "" {
		return nil
	}
	f, err := os.Create(sf.opt.StateSaveFile)
	if err != nil {
		return err
	}
	defer f.Close()
	return sf.loader.writeState(f)
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
	done bitmap.Bitmap
	mu   sync.RWMutex
	s    Store

	nullChunk *NullChunk
	chunks    []*sparseIndexChunk
}

func newSparseFileLoader(name string, idx Index, s Store) *sparseFileLoader {
	chunks := make([]*sparseIndexChunk, 0, len(idx.Chunks))
	for _, c := range idx.Chunks {
		chunks = append(chunks, &sparseIndexChunk{IndexChunk: c})
	}

	return &sparseFileLoader{
		name:      name,
		done:      bitmap.New(len(idx.Chunks)),
		chunks:    chunks,
		s:         s,
		nullChunk: NewNullChunk(idx.Index.ChunkSizeMax),
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
		b := l.done.Get(i)
		if b {
			continue
		}
		// The file is truncated and blank, so no need to load null chunks
		if l.chunks[i].ID == l.nullChunk.ID {
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
		l.done.Set(i, true)
		l.mu.Unlock()
	})
	return loadErr
}

// writeState saves the current internal state about which chunks have
// been loaded. It's a bitmap of the
// same length as the index, with 0 = chunk has not been loaded and
// 1 = chunk has been loaded.
func (l *sparseFileLoader) writeState(w io.Writer) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := w.Write(l.done.Data(false))
	return err
}

// loadState reads the "done" state from a reader. It's expected to be
// a list of '0' and '1' bytes where 0 means the chunk hasn't been
// written to the sparse file yet.
func (l *sparseFileLoader) loadState(r io.Reader) error {
	done, err := l.stateFromReader(r)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.done = done
	return nil
}

// Starts n goroutines to pre-load chunks that were marked as "done" in a state
// file.
func (l *sparseFileLoader) preloadChunksFromState(r io.Reader, n int) error {
	state, err := l.stateFromReader(r)
	if err != nil {
		return err
	}

	// Start the workers for parallel pre-loading
	ch := make(chan int)
	for i := 0; i < n; i++ {
		go func() {
			for chunkIdx := range ch {
				_ = l.loadChunk(chunkIdx)
			}
		}()
	}

	// Start the feeder. Iterate over the chunks and see if any of them
	// are marked done in the state. If so, load those chunks.
	go func() {
		for chunkIdx := range l.chunks {
			if state.Get(chunkIdx) {
				ch <- chunkIdx
			}
		}
		close(ch)
	}()
	return nil
}

func (l *sparseFileLoader) stateFromReader(r io.Reader) (bitmap.Bitmap, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	// Very basic check that the state file really is for the sparse
	// file and not something else.
	chunks := len(l.chunks)
	if (chunks%8 == 0 && len(b) != chunks/8) || (chunks%8 != 0 && len(b) != 1+chunks/8) {
		return nil, errors.New("sparse state file does not match the index")
	}
	return b, nil
}
