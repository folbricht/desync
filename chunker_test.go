package desync

import (
	"bytes"
	"crypto/sha512"
	"os"
	"testing"
)

const (
	ChunkSizeAvgDefault uint64 = 64 * 1024
	ChunkSizeMinDefault        = ChunkSizeAvgDefault / 4
	ChunkSizeMaxDefault        = ChunkSizeAvgDefault * 4
)

func TestChunkerLargeFile(t *testing.T) {
	f, err := os.Open("testdata/chunker.input")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	expected := []struct {
		Start uint64
		Size  uint64
		ID    string
	}{
		{Start: 0, Size: 81590, ID: "ad951d7f65c27828ce390f3c81c41d75f80e4527169ad072ad720b56220f5be4"},
		{Start: 81590, Size: 46796, ID: "ef6df312072ccefe965f07669b2819902f4e9889ebe7c35a38f1dc11ee99f212"},
		{Start: 128386, Size: 36543, ID: "a816e22f4105741972eb34909b6f8ffa569759a1c2cf82ab88394b3db9019f23"},
		{Start: 164929, Size: 83172, ID: "8b8e4a274f06dc3c92d49869a699a5a8255c0bf0b48a4d3c3689aaa3e9cff090"},
		{Start: 248101, Size: 76749, ID: "583d08fc16d8d191af362a1aaecea6af062cc8afab1b301786bb717aa1b425b4"},
		{Start: 324850, Size: 79550, ID: "aefa8c5a3c86896110565b6a3748c2f985892e8ab0073730cac390cb478a913a"},
		{Start: 404400, Size: 41484, ID: "8e39f02975c8d0596e46f643b90cd290b7c0386845132eee4d415c63317773a4"},
		{Start: 445884, Size: 20326, ID: "d689ca889f2f7ba26896681214f0f0f5f5177d5820d99b1f11ddb76b693bddee"},
		{Start: 466210, Size: 31652, ID: "259de367c7ef2f51133d04e744f05918ceb93bd4b9c2bb6621ffeae70501dd09"},
		{Start: 497862, Size: 19995, ID: "01ae987ec457cacc8b3528e3254bc9c93b3f0c0b2a51619e15be16e678ef016d"},
		{Start: 517857, Size: 103873, ID: "78618b2d0539ecf45c08c7334e1c61051725767a76ba9108ad5298c6fd7cde1b"},
		{Start: 621730, Size: 38087, ID: "f44e6992cccadb08d8e18174ba3d6dd6365bdfb9906a58a9f82621ace0461c0d"},
		{Start: 659817, Size: 38377, ID: "abbf9935aaa535538c5fbff069481c343c2770207d88b94584314ee33050ae4f"},
		{Start: 698194, Size: 23449, ID: "a6c737b95ab514d6538c6ef4c42ef2f08b201c3426a88b95e67e517510cd1fb9"},
		{Start: 721643, Size: 47321, ID: "51d44e2d355d5c5b846543d47ba9569f12bbc3d49970c91913a8e3efef45e47e"},
		{Start: 768964, Size: 86692, ID: "90f7e061ed2fb1ed9594297851f8528d3ac355c98457b5dce08ee7d88f801b26"},
		{Start: 855656, Size: 28268, ID: "2dea144e5d771420e90b6e96c1e97e9c6afeda2c37ae7c95ceaf3ee2550efa08"},
		{Start: 883924, Size: 65465, ID: "7a94e051c82ec7abba32883b2eee9a2832e8e9bcc3b3151743fef533e2d46e70"},
		{Start: 949389, Size: 33255, ID: "32edd2d382045ad64d5fbd1a574f8191b700b9e0a2406bd90d2eefcf77168846"},
		{Start: 982644, Size: 65932, ID: "a8bfdadaecbee1ed16ce23d8bf771d1b3fbca2e631fc71b5adb3846c1bb2d542"},
	}

	c, err := NewChunker(f, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
	if err != nil {
		t.Fatal(err)
	}

	for i, e := range expected {
		start, buf, err := c.Next()
		if err != nil {
			t.Fatal(err)
		}
		hash := ChunkID(sha512.Sum512_256(buf)).String()
		if hash != e.ID {
			t.Fatalf("chunk #%d, unexpected hash %s, expected %s", i+1, hash, e.ID)
		}
		if start != e.Start {
			t.Fatalf("chunk #%d, unexpected start %d, expected %d", i+1, start, e.Start)
		}
		if uint64(len(buf)) != e.Size {
			t.Fatalf("chunk #%d, unexpected size %d, expected %d", i+1, uint64(len(buf)), e.Size)
		}
	}
	// Should get a size of 0 at the end
	_, buf, err := c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) != 0 {
		t.Fatalf("expected size 0 at the end, got %d", len(buf))
	}
}

func TestChunkerEmptyFile(t *testing.T) {
	r := bytes.NewReader([]byte{})
	c, err := NewChunker(r, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
	if err != nil {
		t.Fatal(err)
	}
	start, buf, err := c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) != 0 {
		t.Fatalf("unexpected size %d, expected 0", len(buf))
	}
	if start != 0 {
		t.Fatalf("unexpected start position %d, expected 0", start)
	}
}

func TestChunkerSmallFile(t *testing.T) {
	b := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	r := bytes.NewReader(b)
	c, err := NewChunker(r, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
	if err != nil {
		t.Fatal(err)
	}

	start, buf, err := c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) != len(b) {
		t.Fatalf("unexpected size %d, expected %d", len(buf), len(b))
	}
	if start != 0 {
		t.Fatalf("unexpected start position %d, expected 0", start)
	}
}

// There are no chunk boundaries when all data is nil, make sure we get the
// max chunk size
func TestChunkerNoBoundary(t *testing.T) {
	b := make([]byte, 1024*1024)
	r := bytes.NewReader(b)
	c, err := NewChunker(r, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
	if err != nil {
		t.Fatal(err)
	}
	for {
		start, buf, err := c.Next()
		if err != nil {
			t.Fatal(err)
		}
		if len(buf) == 0 {
			break
		}
		if uint64(len(buf)) != ChunkSizeMaxDefault {
			t.Fatalf("unexpected size %d, expected %d", len(buf), ChunkSizeMaxDefault)
		}
		if start%ChunkSizeMaxDefault != 0 {
			t.Fatalf("unexpected start position %d, expected 0", start)
		}
	}
}

// Test with exactly min, avg, max chunk size of data
func TestChunkerBounds(t *testing.T) {
	for _, c := range []struct {
		name string
		size uint64
	}{
		{"chunker with exactly min chunk size data", ChunkSizeMinDefault},
		{"chunker with exactly avg chunk size data", ChunkSizeAvgDefault},
		{"chunker with exactly max chunk size data", ChunkSizeMaxDefault},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := make([]byte, c.size)
			r := bytes.NewReader(b)
			c, err := NewChunker(r, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
			if err != nil {
				t.Fatal(err)
			}

			start, buf, err := c.Next()
			if err != nil {
				t.Fatal(err)
			}
			if len(buf) != len(b) {
				t.Fatalf("unexpected size %d, expected %d", len(buf), len(b))
			}
			if start != 0 {
				t.Fatalf("unexpected start position %d, expected 0", start)
			}
		})
	}
}

// Global vars used for results during the benchmark to prevent optimizer
// from optimizing away some operations
var (
	chunkStart uint64
	chunkBuf   []byte
)

func BenchmarkChunker(b *testing.B) {
	for n := 0; n < b.N; n++ {
		if err := chunkFile(b, "testdata/chunker.input"); err != nil {
			b.Fatal(err)
		}
	}
}

func chunkFile(b *testing.B, name string) error {
	b.StopTimer()
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	c, err := NewChunker(f, ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
	if err != nil {
		return err
	}
	b.StartTimer()
	for {
		start, buf, err := c.Next()
		if err != nil {
			return err
		}
		if len(buf) == 0 {
			break
		}
		chunkStart = start
		chunkBuf = buf
	}
	return err
}
