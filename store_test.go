package desync

type TestStore map[ChunkID][]byte

func (s TestStore) GetChunk(id ChunkID) ([]byte, error) {
	b, ok := s[id]
	if !ok {
		return nil, ChunkMissing{id}
	}
	return b, nil
}

func (s TestStore) HasChunk(id ChunkID) bool {
	return false
}

func (s TestStore) String() string { return "TestStore" }

func (s TestStore) Close() error { return nil }
