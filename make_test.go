package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestParallelChunking(t *testing.T) {
	name := "testdata/chunker.input"

	// Chunk the file single stream first to use the results as reference for
	// the parallel chunking
	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, err := NewChunker(f, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault, 0)
	if err != nil {
		t.Fatal(err)
	}
	var expected []IndexChunk
	for {
		start, buf, err := c.Next()
		if err != nil {
			t.Fatal(err)
		}
		if len(buf) == 0 {
			break
		}
		id := ChunkID(sha512.Sum512_256(buf))
		expected = append(expected, IndexChunk{Start: start, Size: uint64(len(buf)), ID: id})
	}

	for n := 1; n < 10; n++ {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			// Split it up in parallel
			index, err := IndexFromFile(
				context.Background(),
				name,
				n,
				ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
			)
			if err != nil {
				t.Fatal(err)
			}
			// Compare the results of the single stream chunking to the parallel ones
			if !reflect.DeepEqual(index.Chunks, expected) {
				t.Fatal("chunks from parallel splitter don't match single stream chunks")
			}
		})
	}
}
