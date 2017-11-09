package casync

type Store interface {
	GetChunk(id ChunkID) ([]byte, error)
}
