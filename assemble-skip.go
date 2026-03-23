package desync

import (
	"fmt"
	"os"
)

// skipInPlace skips data chunks that are already in place.
type skipInPlace struct {
	start uint64
	end   uint64
}

func (s *skipInPlace) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	return 0, 0, nil
}

func (s *skipInPlace) String() string {
	return fmt.Sprintf("InPlace: Skip [%d:%d]", s.start, s.end)
}

// inPlaceSeedSkip skips a chunk that is already in the correct position
// according to an in-place seed's index. Unlike skipInPlace (which is
// created after hashing the data), this is based on index comparison
// and carries validation info so Validate() can verify the data.
type inPlaceSeedSkip struct {
	chunk IndexChunk
	seed  Seed
	file  string
}

func (s *inPlaceSeedSkip) Execute(f *os.File) (uint64, uint64, error) {
	return 0, 0, nil
}

func (s *inPlaceSeedSkip) String() string {
	return fmt.Sprintf("InPlace: Skip [%d:%d]", s.chunk.Start, s.chunk.Start+s.chunk.Size)
}

func (s *inPlaceSeedSkip) Seed() Seed   { return s.seed }
func (s *inPlaceSeedSkip) File() string { return s.file }

func (s *inPlaceSeedSkip) Validate(file *os.File) error {
	b := make([]byte, s.chunk.Size)
	if _, err := file.ReadAt(b, int64(s.chunk.Start)); err != nil {
		return err
	}
	if Digest.Sum(b) != s.chunk.ID {
		return fmt.Errorf("in-place seed index for %s doesn't match its data", s.file)
	}
	return nil
}
