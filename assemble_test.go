package desync

import (
	"bytes"
	"context"
	"crypto/md5"
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

	s, err := NewLocalStore(store)
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
	bs, err := NewLocalStore(blankstore)
	if err != nil {
		t.Fatal(err)
	}

	// Prepare output files for each test - first a non-existing one
	out1, err := ioutil.TempFile("", "out1")
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(out1.Name())

	// This one is a complete file matching what we exepct at the end
	out2, err := ioutil.TempFile("", "out2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out2, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	out2.Close()
	defer os.RemoveAll(out2.Name())

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
	defer os.RemoveAll(out3.Name())

	// At this point we have the data needed for the test setup
	// in - Temp file that represents the original input file
	// inSub - MD5 of the input file
	// index - Index file for the input file
	// s - Local store containing the chunks needed to rebuild the input file
	// bs - A blank local store, all GetChunk fail on it
	// out1 - Just a non-existing file that gets assembled
	// out2 - The output file already fully complete, no GetChunk should be needed
	// out3 - Partial/damaged file with most, but not all data correct

	tests := map[string]struct {
		outfile string
		store   Store
	}{
		"extract to new file":        {outfile: out1.Name(), store: s},
		"extract to complete file":   {outfile: out2.Name(), store: bs},
		"extract to incomplete file": {outfile: out3.Name(), store: s},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := AssembleFile(context.Background(), test.outfile, index, test.store, nil, 10, nil); err != nil {
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
