package desync

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type nullChunkSeed struct {
	id         ChunkID
	blockfile  *os.File
	canReflink bool
}

func newNullChunkSeed(dstFile string, blocksize uint64, max uint64) (*nullChunkSeed, error) {
	blockfile, err := ioutil.TempFile(filepath.Dir(dstFile), ".tmp-block")
	if err != nil {
		return nil, err
	}
	var canReflink bool
	if CanClone(dstFile, blockfile.Name()) {
		canReflink = true
		b := make([]byte, blocksize)
		if _, err := blockfile.Write(b); err != nil {
			return nil, err
		}
	}
	return &nullChunkSeed{
		id:         NewNullChunk(max).ID,
		canReflink: canReflink,
		blockfile:  blockfile,
	}, nil
}

func (s *nullChunkSeed) close() error {
	if s.blockfile != nil {
		s.blockfile.Close()
		return os.Remove(s.blockfile.Name())
	}
	return nil
}

func (s *nullChunkSeed) LongestMatchWith(chunks []IndexChunk) (int, SeedSegment) {
	if len(chunks) == 0 {
		return 0, nil
	}
	var (
		n     int
		limit int
	)
	if !s.canReflink {
		limit = 100
	}
	for _, c := range chunks {
		if limit != 0 && limit == n {
			break
		}
		if c.ID != s.id {
			break
		}
		n++
	}
	if n == 0 {
		return 0, nil
	}
	return n, &nullChunkSection{
		from:       chunks[0].Start,
		to:         chunks[n-1].Start + chunks[n-1].Size,
		blockfile:  s.blockfile,
		canReflink: s.canReflink,
	}
}

func (s *nullChunkSeed) RegenerateIndex(ctx context.Context, n int, attempt int, seedNumber int) error {
	panic("A nullseed can't be regenerated")
}

func (s *nullChunkSeed) SetInvalid(value bool) {
	panic("A nullseed is never expected to be invalid")
}

func (s *nullChunkSeed) IsInvalid() bool {
	// A nullseed is never expected to be invalid
	return false
}

type nullChunkSection struct {
	from, to   uint64
	blockfile  *os.File
	canReflink bool
}

func (s *nullChunkSection) Validate(file *os.File) error {
	// We always assume a nullseed to be valid
	return nil
}

func (s *nullChunkSection) FileName() string {
	return ""
}

func (s *nullChunkSection) Size() uint64 { return s.to - s.from }

func (s *nullChunkSection) WriteInto(dst *os.File, offset, length, blocksize uint64, isBlank bool) (uint64, uint64, error) {
	if length != s.Size() {
		return 0, 0, fmt.Errorf("unable to copy %d bytes to %s : wrong size", length, dst.Name())
	}

	// When cloning isn'a available we'd normally have to copy the 0 bytes into
	// the target range. But if that's already blank (because it's a new/truncated
	// file) there's no need to copy 0 bytes.
	if !s.canReflink {
		if isBlank {
			return 0, 0, nil
		}
		return s.copy(dst, offset, s.Size())
	}
	return s.clone(dst, offset, length, blocksize)
}

func (s *nullChunkSection) copy(dst *os.File, offset, length uint64) (uint64, uint64, error) {
	if _, err := dst.Seek(int64(offset), os.SEEK_SET); err != nil {
		return 0, 0, err
	}
	// Copy using a fixed buffer. Using io.Copy() with a LimitReader will make it
	// create a buffer matching N of the LimitReader which can be too large
	copied, err := io.CopyBuffer(dst, io.LimitReader(nullReader{}, int64(length)), make([]byte, 64*1024))
	return uint64(copied), 0, err
}

func (s *nullChunkSection) clone(dst *os.File, offset, length, blocksize uint64) (uint64, uint64, error) {
	dstAlignStart := (offset/blocksize + 1) * blocksize
	dstAlignEnd := (offset + length) / blocksize * blocksize

	// fill the area before the first aligned block
	var copied, cloned uint64
	c1, _, err := s.copy(dst, offset, dstAlignStart-offset)
	if err != nil {
		return c1, 0, err
	}
	copied += c1
	// fill the area after the last aligned block
	c2, _, err := s.copy(dst, dstAlignEnd, offset+length-dstAlignEnd)
	if err != nil {
		return copied + c2, 0, err
	}
	copied += c2

	for blkOffset := dstAlignStart; blkOffset < dstAlignEnd; blkOffset += blocksize {
		if err := CloneRange(dst, s.blockfile, 0, blocksize, blkOffset); err != nil {
			return copied, cloned, err
		}
		cloned += blocksize
	}
	return copied, cloned, nil
}

type nullReader struct{}

func (r nullReader) Read(b []byte) (n int, err error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}
