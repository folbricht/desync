package desync

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestExtract(t *testing.T) {
	// Make a test file that's guaranteed to have duplicate chunks.
	b, err := ioutil.ReadFile("testdata/chunker.input")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ { // Replicate it a few times to make sure we get dupes
		b = append(b, b...)
	}
	b = append(b, make([]byte, 2*ChunkSizeMaxDefault)...) // want to have at least one null-chunk in the input
	in, err := ioutil.TempFile("", "in")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(in.Name())
	if _, err := io.Copy(in, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	in.Close()

	// Record the checksum of the input file, used to compare to the output later
	inSum := md5.Sum(b)

	// Chunk the file to get an index
	index, _, err := IndexFromFile(
		context.Background(),
		in.Name(),
		10,
		ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Chop up the input file into a (temporary) local store
	store, err := ioutil.TempDir("", "store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(store)

	s, err := NewLocalStore(store, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ChopFile(context.Background(), in.Name(), index.Chunks, s, 10, nil); err != nil {
		t.Fatal(err)
	}

	// Make a blank store - used to test a case where no chunk *should* be requested
	blankstore, err := ioutil.TempDir("", "blankstore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(blankstore)
	bs, err := NewLocalStore(blankstore, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Prepare output files for each test - first a non-existing one
	out1, err := ioutil.TempFile("", "out1")
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(out1.Name())

	// This one is a complete file matching what we exepct at the end
	out2, err := ioutil.TempFile("", "out2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out2, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	out2.Close()
	defer os.Remove(out2.Name())

	// Incomplete or damaged file that has most but not all data
	out3, err := ioutil.TempFile("", "out3")
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff // flip some bits
	b[len(b)-1] ^= 0xff
	b = append(b, 0) // make it longer
	if _, err := io.Copy(out3, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	out3.Close()
	defer os.Remove(out3.Name())

	// At this point we have the data needed for the test setup
	// in - Temp file that represents the original input file
	// inSub - MD5 of the input file
	// index - Index file for the input file
	// s - Local store containing the chunks needed to rebuild the input file
	// bs - A blank local store, all GetChunk fail on it
	// out1 - Just a non-existing file that gets assembled
	// out2 - The output file already fully complete, no GetChunk should be needed
	// out3 - Partial/damaged file with most, but not all data correct
	// seedIndex + seedFile - Seed file to help assemble the input
	tests := map[string]struct {
		outfile string
		store   Store
		seed    []Seed
	}{
		"extract to new file":        {outfile: out1.Name(), store: s},
		"extract to complete file":   {outfile: out2.Name(), store: bs},
		"extract to incomplete file": {outfile: out3.Name(), store: s},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			defer os.Remove(test.outfile)
			if _, err := AssembleFile(context.Background(), test.outfile, index, test.store, nil,
				AssembleOptions{10, InvalidSeedActionBailOut}, nil,
			); err != nil {
				t.Fatal(err)
			}
			b, err := ioutil.ReadFile(test.outfile)
			if err != nil {
				t.Fatal(err)
			}
			outSum := md5.Sum(b)
			if inSum != outSum {
				t.Fatal("checksum of extracted file doesn't match expected")
			}
		})
	}
}

func TestSeed(t *testing.T) {
	// Prepare different types of data slices that'll be used to assemble target
	// and seed files with varying amount of duplication
	data1, err := ioutil.ReadFile("testdata/chunker.input")
	if err != nil {
		t.Fatal(err)
	}
	null := make([]byte, 4*ChunkSizeMaxDefault)
	rand1 := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(rand1)
	rand2 := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(rand2)

	// Setup a temporary store
	store, err := ioutil.TempDir("", "store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(store)

	s, err := NewLocalStore(store, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Define tests with files with different content, by building files out
	// of sets of byte slices to create duplication or not between the target and
	// its seeds
	tests := map[string]struct {
		target [][]byte
		seeds  [][][]byte
	}{
		"extract without seed": {
			target: [][]byte{rand1, rand2},
			seeds:  nil},
		"extract all null file": {
			target: [][]byte{null, null, null, null, null},
			seeds:  nil},
		"extract repetitive file": {
			target: [][]byte{data1, data1, data1, data1, data1},
			seeds:  nil},
		"extract with single file seed": {
			target: [][]byte{data1, null, null, rand1, null},
			seeds: [][][]byte{
				{data1, null, rand2, rand2, data1},
			},
		},
		"extract with multiple file seeds": {
			target: [][]byte{null, null, rand1, null, data1},
			seeds: [][][]byte{
				{rand2, null, rand2, rand2, data1},
				{data1, null, rand2, rand2, data1},
				{rand2},
			},
		},
		"extract with identical file seed": {
			target: [][]byte{data1, null, rand1, null, data1},
			seeds: [][][]byte{
				{data1, null, rand1, null, data1},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Build the destination file so we can chunk it
			dst, err := ioutil.TempFile("", "dst")
			if err != nil {
				t.Fatal(err)
			}
			dstBytes := join(test.target...)
			if _, err := io.Copy(dst, bytes.NewReader(dstBytes)); err != nil {
				t.Fatal(err)
			}
			dst.Close()
			defer os.Remove(dst.Name())

			// Record the checksum of the target file, used to compare to the output later
			dstSum := md5.Sum(dstBytes)

			// Chunk the file to get an index
			dstIndex, _, err := IndexFromFile(
				context.Background(),
				dst.Name(),
				10,
				ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}

			// Chop up the input file into the store
			if err := ChopFile(context.Background(), dst.Name(), dstIndex.Chunks, s, 10, nil); err != nil {
				t.Fatal(err)
			}

			// Build the seed files and indexes then populate the array of seeds
			var seeds []Seed
			for _, f := range test.seeds {
				seedFile, err := ioutil.TempFile("", "seed")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(seedFile, bytes.NewReader(join(f...))); err != nil {
					t.Fatal(err)
				}
				seedFile.Close()
				defer os.Remove(seedFile.Name())
				seedIndex, _, err := IndexFromFile(
					context.Background(),
					seedFile.Name(),
					10,
					ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
					nil,
				)
				if err != nil {
					t.Fatal(err)
				}
				seed, err := NewIndexSeed(dst.Name(), seedFile.Name(), seedIndex)
				if err != nil {
					t.Fatal(err)
				}
				seeds = append(seeds, seed)
			}

			if _, err := AssembleFile(context.Background(), dst.Name(), dstIndex, s, seeds,
				AssembleOptions{10, InvalidSeedActionBailOut}, nil,
			); err != nil {
				t.Fatal(err)
			}
			b, err := ioutil.ReadFile(dst.Name())
			if err != nil {
				t.Fatal(err)
			}
			outSum := md5.Sum(b)
			if dstSum != outSum {
				t.Fatal("checksum of extracted file doesn't match expected")
			}
		})
	}

}

func join(slices ...[]byte) []byte {
	var out []byte
	for _, b := range slices {
		out = append(out, b...)
	}
	return out
}
