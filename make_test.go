package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestParallelChunking(t *testing.T) {
	blank, err := ioutil.TempFile("", "blank")
	if err != nil {
		t.Fatal(err)
	}
	blank.Close()
	defer os.RemoveAll(blank.Name())

	// Create a file full of zeroes to test behaviour of the chunker when no
	// boundaries can be found (the rolling hash will not produce boundaries
	// for files full of nil bytes)
	zeroes, err := ioutil.TempFile("", "zeroes")
	if err != nil {
		t.Fatal(err)
	}
	zeroes.Close()
	defer os.RemoveAll(zeroes.Name())
	if err = ioutil.WriteFile(zeroes.Name(), make([]byte, 1024*1024), 0644); err != nil {
		t.Fatal(err)
	}

	// Make an array of files we want to test the chunker with
	testFiles := []string{
		"testdata/chunker.input",
		zeroes.Name(),
		blank.Name(),
	}

	// Split each file with different values for n (number of goroutes)
	for _, name := range testFiles {
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
			t.Run(fmt.Sprintf("%s, n=%d", name, n), func(t *testing.T) {
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
}
