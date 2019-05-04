package desync

var _ Store = &TestStore{}

type TestStore struct {
	Chunks map[ChunkID][]byte

	// Override the default behavior by setting these functions
	GetChunkFunc func(ChunkID) (*Chunk, error)
	HasChunkFunc func(ChunkID) (bool, error)
}

func (s *TestStore) GetChunk(id ChunkID) (*Chunk, error) {
	if s.GetChunkFunc != nil {
		return s.GetChunkFunc(id)
	}
	b, ok := s.Chunks[id]
	if !ok {
		return nil, ChunkMissing{id}
	}
	return &Chunk{compressed: b}, nil
}

func (s *TestStore) HasChunk(id ChunkID) (bool, error) {
	if s.HasChunkFunc != nil {
		return s.HasChunkFunc(id)
	}
	_, ok := s.Chunks[id]
	return ok, nil
}

func (s *TestStore) String() string { return "TestStore" }

func (s *TestStore) Close() error { return nil }
