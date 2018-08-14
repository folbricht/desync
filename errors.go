package desync

import "fmt"

// ChunkMissing is returned by a store that can't find a requested chunk
type ChunkMissing struct {
	ID ChunkID
}

// NoSuchObject is returned by a store that can't find a requested object
type NoSuchObject struct {
	location string
}

func (e ChunkMissing) Error() string {
	return fmt.Sprintf("chunk %s missing from store", e.ID)
}

func (e NoSuchObject) Error() string {
	return fmt.Sprintf("object %s missing from store", e.location)
}

// ChunkInvalid means the hash of the chunk content doesn't match its ID
type ChunkInvalid struct {
	ID  ChunkID
	Sum ChunkID
}

func (e ChunkInvalid) Error() string {
	return fmt.Sprintf("chunk id %s does not match its hash %s", e.ID, e.Sum)
}

// InvalidFormat is returned when an error occurred when parsing an archive file
type InvalidFormat struct {
	Msg string
}

func (e InvalidFormat) Error() string {
	return fmt.Sprintf("invalid archive format : %s", e.Msg)
}

// Interrupted is returned when a user interrupted a long-running operation, for
// example by pressing Ctrl+C
type Interrupted struct{}

func (e Interrupted) Error() string { return "interrupted" }
