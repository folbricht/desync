package desync

import (
	"fmt"
	"os"
)

// inPlaceCopy copies a chunk from one position to another within the same file.
// It uses ReadAt/WriteAt (pread/pwrite) which are position-independent and safe
// for concurrent use on the same file handle.
type inPlaceCopy struct {
	srcOffset uint64
	srcSize   uint64
	dstOffset uint64
	dstSize   uint64

	// Cycle-breaking: the first mover in a cycle pre-reads the buffered
	// operation's source before executing its own copy.
	preBuffers []*inPlaceCopy // targets whose writeBuf to populate before own copy
	writeBuf   []byte         // non-nil → write from this buffer, skip file read
}

func (s *inPlaceCopy) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	// Step 1: Pre-read sources for cycle-broken chunks before our own copy
	// overwrites their data.
	for _, pb := range s.preBuffers {
		pb.writeBuf = make([]byte, pb.srcSize)
		if _, err := f.ReadAt(pb.writeBuf, int64(pb.srcOffset)); err != nil {
			return 0, 0, fmt.Errorf("inPlaceCopy pre-buffer read at %d: %w", pb.srcOffset, err)
		}
	}

	// Step 2: If this chunk was cycle-broken, write from the pre-read buffer.
	if s.writeBuf != nil {
		if _, err := f.WriteAt(s.writeBuf, int64(s.dstOffset)); err != nil {
			return 0, 0, fmt.Errorf("inPlaceCopy buffer write at %d: %w", s.dstOffset, err)
		}
		return s.dstSize, 0, nil
	}

	// Step 3: Normal copy — read source into a temp buffer, then write to dest.
	// Always buffer first to handle overlapping ranges safely.
	buf := make([]byte, s.srcSize)
	if _, err := f.ReadAt(buf, int64(s.srcOffset)); err != nil {
		return 0, 0, fmt.Errorf("inPlaceCopy read at %d: %w", s.srcOffset, err)
	}
	if _, err := f.WriteAt(buf, int64(s.dstOffset)); err != nil {
		return 0, 0, fmt.Errorf("inPlaceCopy write at %d: %w", s.dstOffset, err)
	}
	return s.dstSize, 0, nil
}

func (s *inPlaceCopy) String() string {
	return fmt.Sprintf("InPlace: Copy [%d:%d] to [%d:%d]",
		s.srcOffset, s.srcOffset+s.srcSize,
		s.dstOffset, s.dstOffset+s.dstSize)
}
