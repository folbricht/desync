package desync

var _ WriteStore = &TestStore{}

type TestStore struct {
	Chunks map[ChunkID][]byte

	// Override the default behavior by setting these functions
	GetChunkFunc   func(ChunkID) (*Chunk, error)
	HasChunkFunc   func(ChunkID) (bool, error)
	StoreChunkFunc func(chunk *Chunk) error
}

func (s *TestStore) GetChunk(id ChunkID) (*Chunk, error) {
	if s.GetChunkFunc != nil {
		return s.GetChunkFunc(id)
	}
	b, ok := s.Chunks[id]
	if !ok {
		return nil, ChunkMissing{id}
	}
	return NewChunk(b), nil
}

func (s *TestStore) HasChunk(id ChunkID) (bool, error) {
	if s.HasChunkFunc != nil {
		return s.HasChunkFunc(id)
	}
	_, ok := s.Chunks[id]
	return ok, nil
}

func (s *TestStore) StoreChunk(chunk *Chunk) error {
	if s.StoreChunkFunc != nil {
		return s.StoreChunkFunc(chunk)
	}
	if s.Chunks == nil {
		s.Chunks = make(map[ChunkID][]byte)
	}
	b, _ := chunk.Data()
	s.Chunks[chunk.ID()] = b
	return nil
}

func (s *TestStore) String() string { return "TestStore" }

func (s *TestStore) Close() error { return nil }
