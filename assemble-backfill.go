package desync

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// seedFileState is what a seed file looked like when its chunks were
// validated. Comparing it with the file after assembly shows whether the file
// was written to while it was read.
type seedFileState struct {
	info os.FileInfo

	// recent is set when the file was modified shortly before it was
	// validated. Some filesystems keep modification times to the second or
	// worse, a later write could then leave the time unchanged.
	recent bool
}

// seedTimeResolution is the coarsest resolution of modification times among
// common filesystems, FAT's two seconds.
const seedTimeResolution = 2 * time.Second

func newSeedFileState(info os.FileInfo) seedFileState {
	return seedFileState{
		info:   info,
		recent: time.Since(info.ModTime()) < seedTimeResolution,
	}
}

// changed reports whether the file may have been written to since its state
// was recorded. A block device has no meaningful modification time, it always
// counts as changed.
func (s seedFileState) changed(name string) bool {
	if s.recent || isDevice(s.info.Mode()) {
		return true
	}
	info, err := os.Stat(name)
	if err != nil {
		return true
	}
	return !os.SameFile(info, s.info) ||
		info.Size() != s.info.Size() ||
		!info.ModTime().Equal(s.info.ModTime())
}

// chunksInTarget reports for every chunk of the index whether the file holds
// its data at the chunk's position, hashing them with n goroutines.
func chunksInTarget(name string, idx Index, n int) ([]bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	found := make([]bool, len(idx.Chunks))
	var wg sync.WaitGroup
	work := make(chan int)
	for range max(n, 1) {
		wg.Go(func() {
			buf := make([]byte, idx.Index.ChunkSizeMax)
			for i := range work {
				found[i] = chunkInPlace(f, idx.Chunks[i], buf)
			}
		})
	}
	for i := range idx.Chunks {
		work <- i
	}
	close(work)
	wg.Wait()
	return found, nil
}

// backfill checks every chunk of the assembled file and writes the ones that
// don't match from the store. It's used when a seed changed while it was read,
// whatever came from it may be damaged, including what the self-seed copied
// from there on.
func backfill(name string, idx Index, s Store, n int, stats *ExtractStats) error {
	found, err := chunksInTarget(name, idx, n)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	for i, ok := range found {
		if ok {
			continue
		}
		c := idx.Chunks[i]
		if _, _, err := (&copyFromStore{store: s, chunk: c}).Execute(f); err != nil {
			return fmt.Errorf("chunk %s at offset %d came from a seed that changed during assembly: %w", c.ID, c.Start, err)
		}
		stats.addChunksBackfilled(1)
	}
	return nil
}
