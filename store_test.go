package desync

var _ Store = TestStore{}

type TestStore map[ChunkID][]byte

func (s TestStore) GetChunk(id ChunkID) (*Chunk, error) {
	b, ok := s[id]
	if !ok {
		return nil, ChunkMissing{id}
	}
	return &Chunk{compressed: b}, nil
}

func (s TestStore) HasChunk(id ChunkID) (bool, error) {
	_, ok := s[id]
	return ok, nil
}

func (s TestStore) String() string { return "TestStore" }

func (s TestStore) Close() error { return nil }
