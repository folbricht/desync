package desync

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestIndexLoad(t *testing.T) {
	f, err := os.Open("testdata/index.caibx")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	index, err := IndexFromReader(f)
	if err != nil {
		t.Fatal(err)
	}

	type chunk struct {
		chunk string
		start uint64
		size  uint64
	}
	expected := []chunk{
		{"437884da2d1e61cf50b43b263ff15f25a870b0eae84bc22e4b5c307a0428764d", 0, 242168},
		{"985462e6b3293bbe61e43882686b481751ecf4b285bae4dffc2dfa8829f971ac", 242168, 75740},
		{"fadff4b303624f2be3d0e04c2f105306118a9f608ef1e4f83c1babbd23a2315f", 317908, 20012},
	}
	for i := range expected {
		id, _ := ChunkIDFromString(expected[i].chunk)
		exp := IndexChunk{ID: id, Start: expected[i].start, Size: expected[i].size}
		got := index.Chunks[i]
		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("expected %v, got %v", exp, got)
		}
	}
}

func TestIndexWrite(t *testing.T) {
	in, err := ioutil.ReadFile("testdata/index.caibx")
	if err != nil {
		t.Fatal(err)
	}

	idx, err := IndexFromReader(bytes.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}

	out := new(bytes.Buffer)
	n, err := idx.WriteTo(out)
	if err != nil {
		t.Fatal(err)
	}

	// in/out should match
	if !bytes.Equal(in, out.Bytes()) {
		t.Fatalf("decoded/encoded don't match")
	}
	if n != int64(out.Len()) {
		t.Fatalf("unexpected length")
	}
}

func TestIndexChunking(t *testing.T) {
	// Open the blob
	f, err := os.Open("testdata/chunker.input")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Create a chunker
	c, err := NewChunker(f, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
	if err != nil {
		t.Fatal(err)
	}

	// Make a temp local store
	dir, err := ioutil.TempDir("", "chunktest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up
	s, err := NewLocalStore(dir, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Split up the blob into chunks and return the index
	idx, err := ChunkStream(context.Background(), c, s, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Write the index and compare it to the expected one
	b := new(bytes.Buffer)
	if _, err = idx.WriteTo(b); err != nil {
		t.Fatal(err)
	}
	i, err := ioutil.ReadFile("testdata/chunker.index")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b.Bytes(), i) {
		t.Fatal("index doesn't match expected")
	}

	// Make sure the local store contains all the expected chunks
	expectedChunks := []string{
		"ad951d7f65c27828ce390f3c81c41d75f80e4527169ad072ad720b56220f5be4",
		"ef6df312072ccefe965f07669b2819902f4e9889ebe7c35a38f1dc11ee99f212",
		"a816e22f4105741972eb34909b6f8ffa569759a1c2cf82ab88394b3db9019f23",
		"8b8e4a274f06dc3c92d49869a699a5a8255c0bf0b48a4d3c3689aaa3e9cff090",
		"583d08fc16d8d191af362a1aaecea6af062cc8afab1b301786bb717aa1b425b4",
		"aefa8c5a3c86896110565b6a3748c2f985892e8ab0073730cac390cb478a913a",
		"8e39f02975c8d0596e46f643b90cd290b7c0386845132eee4d415c63317773a4",
		"d689ca889f2f7ba26896681214f0f0f5f5177d5820d99b1f11ddb76b693bddee",
		"259de367c7ef2f51133d04e744f05918ceb93bd4b9c2bb6621ffeae70501dd09",
		"01ae987ec457cacc8b3528e3254bc9c93b3f0c0b2a51619e15be16e678ef016d",
		"78618b2d0539ecf45c08c7334e1c61051725767a76ba9108ad5298c6fd7cde1b",
		"f44e6992cccadb08d8e18174ba3d6dd6365bdfb9906a58a9f82621ace0461c0d",
		"abbf9935aaa535538c5fbff069481c343c2770207d88b94584314ee33050ae4f",
		"a6c737b95ab514d6538c6ef4c42ef2f08b201c3426a88b95e67e517510cd1fb9",
		"51d44e2d355d5c5b846543d47ba9569f12bbc3d49970c91913a8e3efef45e47e",
		"90f7e061ed2fb1ed9594297851f8528d3ac355c98457b5dce08ee7d88f801b26",
		"2dea144e5d771420e90b6e96c1e97e9c6afeda2c37ae7c95ceaf3ee2550efa08",
		"7a94e051c82ec7abba32883b2eee9a2832e8e9bcc3b3151743fef533e2d46e70",
		"32edd2d382045ad64d5fbd1a574f8191b700b9e0a2406bd90d2eefcf77168846",
		"a8bfdadaecbee1ed16ce23d8bf771d1b3fbca2e631fc71b5adb3846c1bb2d542",
	}
	for _, sid := range expectedChunks {
		id, err := ChunkIDFromString(sid)
		if err != nil {
			t.Fatal(id)
		}
		if !s.HasChunk(id) {
			t.Fatalf("store is missing chunk %s", id)
		}
	}
}

// Global var to store benchmark output
var idx Index

func BenchmarkBlobChunking(b *testing.B) {
	for n := 0; n < b.N; n++ {
		splitBlob(b)
	}
}

func splitBlob(b *testing.B) {
	b.StopTimer()
	// Open the blob
	f, err := os.Open("testdata/chunker.input")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	// Create a chunker
	c, err := NewChunker(f, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
	if err != nil {
		b.Fatal(err)
	}

	// Make a temp local store
	dir, err := ioutil.TempDir("", "chunktest")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up
	s, err := NewLocalStore(dir, StoreOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	// Split up the blob into chunks and return the index
	idx, err = ChunkStream(context.Background(), c, s, 10)
	if err != nil {
		b.Fatal(err)
	}
}
