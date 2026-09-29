package chunkers

import "io"

type ChunkerBase struct {
	r             io.Reader
	min, avg, max uint64

	start uint64

	buf        []byte
	backingBuf []byte // reusable backing buffer for FillBuffer
	hitEOF     bool   // true once the reader returned EOF
}

func NewChunkerBase(r io.Reader, params ChunkerParams) *ChunkerBase {
	return &ChunkerBase{
		r:   r,
		min: params.Min,
		avg: params.Avg,
		max: params.Max,
	}
}

// Make a new buffer with 10*max bytes and copy anything that may be leftover
// from before into it, then fill it up with new bytes. Don't fail on EOF.
func (c *ChunkerBase) FillBuffer() (n int, err error) {
	if c.hitEOF { // We won't get anymore here, no need for more allocations
		return
	}
	size := 10 * c.max
	// Reuse the backing buffer if it has sufficient capacity
	var buf []byte
	if uint64(cap(c.backingBuf)) >= size {
		buf = c.backingBuf[:size]
	} else {
		buf = make([]byte, int(size))
		c.backingBuf = buf
	}
	n = copy(buf, c.buf)                 // copy the remaining bytes from the old buffer
	for uint64(n) < size && err == nil { // read until the buffer is at max or we get an EOF
		var nn int
		nn, err = c.r.Read(buf[n:])
		n += nn
	}
	c.buf = buf[:n] // we are not going to get any more, resize the buffer
	if err == io.EOF {
		c.hitEOF = true
		err = nil
	}
	return
}

func (c *ChunkerBase) Split(i int, err error) (uint64, []byte, error) {
	// save the remaining bytes (after the split position) for the next round
	start := c.start
	b := c.buf[:i]
	c.buf = c.buf[i:]
	c.start += uint64(i)
	return start, b, err
}

func (c *ChunkerBase) Advance(n int) error {
	// We might still have bytes in the buffer. These count towards the move forward.
	// It's possible the advance stays within the buffer and doesn't impact the reader.
	c.start += uint64(n)
	if n <= len(c.buf) {
		c.buf = c.buf[n:]
		return nil
	}
	readerN := int64(n - len(c.buf))
	c.buf = nil
	rs, ok := c.r.(io.Seeker)
	if ok {
		_, err := rs.Seek(readerN, io.SeekCurrent)
		return err
	}
	_, err := io.CopyN(io.Discard, c.r, readerN)
	return err
}

func (c *ChunkerBase) Reset(r io.Reader) error {
	c.r = r
	c.hitEOF = false
	c.buf = nil
	c.start = 0
	return nil
}

func (c *ChunkerBase) Min() uint64 { return c.min }
func (c *ChunkerBase) Avg() uint64 { return c.avg }
func (c *ChunkerBase) Max() uint64 { return c.max }

func (c *ChunkerBase) Buf() []byte  { return c.buf }
func (c *ChunkerBase) HitEOF() bool { return c.hitEOF }
