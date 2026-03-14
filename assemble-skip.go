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
