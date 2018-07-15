package desync

import (
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
	var n int
	for _, c := range chunks {
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

type nullChunkSection struct {
	from, to   uint64
	blockfile  *os.File
	canReflink bool
}

func (s *nullChunkSection) Size() uint64 { return s.to - s.from }

func (s *nullChunkSection) WriteInto(dst *os.File, offset, length, blocksize uint64) error {
	if length != s.Size() {
		return fmt.Errorf("unable to copy %d bytes to %s : wrong size", length, dst.Name())
	}

	if !s.canReflink {
		return s.copy(dst, offset, s.Size())
	}
	return s.clone(dst, offset, length, blocksize)
}

func (s *nullChunkSection) copy(dst *os.File, offset, length uint64) error {
	if _, err := dst.Seek(int64(offset), os.SEEK_SET); err != nil {
		return err
	}
	_, err := io.CopyN(dst, nullReader{}, int64(length))
	return err
}

func (s *nullChunkSection) clone(dst *os.File, offset, length, blocksize uint64) error {
	dstAlignStart := (offset/blocksize + 1) * blocksize
	dstAlignEnd := (offset + length) / blocksize * blocksize

	// fill the area before the first aligned block
	if err := s.copy(dst, offset, dstAlignStart-offset); err != nil {
		return err
	}
	// fill the area after the last aligned block
	if err := s.copy(dst, dstAlignEnd, offset+length-dstAlignEnd); err != nil {
		return err
	}
	for blkOffset := dstAlignStart; blkOffset < dstAlignEnd; blkOffset += blocksize {
		if err := CloneRange(dst, s.blockfile, 0, blocksize, blkOffset); err != nil {
			return err
		}
	}
	return nil
}

type nullReader struct{}

func (r nullReader) Read(b []byte) (n int, err error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}
