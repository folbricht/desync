package desync

import (
	"fmt"
	"os"
)

type fileSeedSource struct {
	segment SeedSegment
	seed    Seed
	srcFile string
	offset  uint64
	length  uint64
	isBlank bool
}

func (s *fileSeedSource) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	blocksize := blocksizeOfFile(f.Name())
	return s.segment.WriteInto(f, s.offset, s.length, blocksize, s.isBlank)
}

func (s *fileSeedSource) String() string {
	return fmt.Sprintf("FileSeed(%s): Copy to [%d:%d]", s.srcFile, s.offset, s.offset+s.length)
}
