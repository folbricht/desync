package desync

import (
	"fmt"
	"os"
)

type copyFromStore struct {
	store Store
	chunk IndexChunk
}

func (s *copyFromStore) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	chunk, err := s.store.GetChunk(s.chunk.ID)
	if err != nil {
		return 0, 0, err
	}
	b, err := chunk.Data()
	if err != nil {
		return 0, 0, err
	}
	if s.chunk.Size != uint64(len(b)) {
		return 0, 0, fmt.Errorf("unexpected size for chunk %s", s.chunk.ID)
	}
	_, err = f.WriteAt(b, int64(s.chunk.Start))
	return 0, 0, err
}

func (s *copyFromStore) String() string {
	return fmt.Sprintf("Store: Copy %v to [%d:%d]", s.chunk.ID.String(), s.chunk.Start, s.chunk.Start+s.chunk.Size)
}
