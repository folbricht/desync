package desync

// ChunkAdditionalInfo contains detailed information about a particular chunk.
// Some of those info, e.g. CompressedSize, are only exact for the store used when
// generating it. Because other stores could potentially use different compression levels.
type ChunkAdditionalInfo struct {
	ID               ChunkID `json:"id"`
	UncompressedSize uint64  `json:"uncompressed_size"`
	CompressedSize   int64   `json:"compressed_size,omitempty"`
}
