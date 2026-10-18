package chunkers

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
)

const ()

type segmentAwareChunkerOptions struct {
	// Where and how to get segment size information from.
	segmentsSource       string
	segmentsSourceFormat string

	//
	subchunker string

	// Should "small" segments be merged for chunking, or emitted as is?
	mergeSmallSegments     bool
	smallSegmentBreakpoint uint64
}

type SegmentAwareChunker struct {
	segmentAwareChunkerOptions

	subchunker Chunker

	r            io.Reader
	segmentSizes []uint64

	// Offset right before mergedSegments or big segment being processed
	currentOffset  uint64
	nextSegmentIdx uint32

	// Holds merged bytes if `mergeSmallSegments` is enabled
	mergedSegments      []byte
	mergedSegmentsCount uint32
	mergedSegmentsSize  uint64
	mergedSegmentsTmp   []byte

	insideBigSegment bool
	// Position in the big segment, relative to its start
	bigSegmentPos uint64
}

func parseSegmentAwareChunkerOptions(optionsStr string) (options segmentAwareChunkerOptions, err error) {
	// Defaults.
	options.mergeSmallSegments = true
	options.subchunker = "fastcdc"

	optionsStr = strings.Trim(optionsStr, " \t")
	if len(optionsStr) == 0 {
		return
	}

	//
	splitOptions := strings.Split(optionsStr, ",")
	for _, optionPair := range splitOptions {
		kv := strings.SplitN(optionPair, "=", 2)
		if len(kv) != 2 {
			err = fmt.Errorf("bad chunker options: %s must be formatted as key=value", optionPair)
			return
		}

		switch kv[0] {
		case "segmentsSource":
			options.segmentsSource = kv[1]
		case "segmentsSourceFormat":
			options.segmentsSourceFormat = kv[1]

		case "subchunker":
			options.subchunker = kv[1]

		case "mergeSmallSegments":
			options.mergeSmallSegments = !(kv[1] == "false" || kv[1] == "0")
		case "smallSegmentBreakpoint":
			kbValue, err := strconv.ParseUint(kv[1], 10, 64)
			if err != nil || kbValue > math.MaxUint64/1024 {
				return options, fmt.Errorf("invalid smallSegmentBreakpoint '%s'", kv[1])
			}
			options.smallSegmentBreakpoint = kbValue * 1024

		default:
			err = fmt.Errorf("unknown key in chunker options: '%s'", kv[0])
			return
		}
	}
	return
}

func NewSegmentAwareChunker(r io.Reader, params ChunkerParams) (*SegmentAwareChunker, error) {
	segmentAwareChunkerOptions, err := parseSegmentAwareChunkerOptions(params.Options)
	if err != nil {
		return nil, err
	}

	if segmentAwareChunkerOptions.mergeSmallSegments {
		// Default to twice the avg chunk size. It is likely to produce multiple chunks by itself.
		if segmentAwareChunkerOptions.smallSegmentBreakpoint == 0 {
			segmentAwareChunkerOptions.smallSegmentBreakpoint = params.Avg * 2
		}
		segmentAwareChunkerOptions.smallSegmentBreakpoint = min(segmentAwareChunkerOptions.smallSegmentBreakpoint, params.Max)
	} else {
		segmentAwareChunkerOptions.smallSegmentBreakpoint = 0
	}

	subchunkerDesc := FindChunkerByName(segmentAwareChunkerOptions.subchunker)
	if subchunkerDesc == nil {
		return nil, fmt.Errorf("unknown subchunker: '%s'", segmentAwareChunkerOptions.subchunker)
	}
	if uint64(subchunkerDesc.WindowSize) > params.Min {
		return nil, fmt.Errorf("subchunker '%s' WindowSize %v is greater than min chunk size requested",
			segmentAwareChunkerOptions.subchunker, subchunkerDesc.WindowSize)
	}
	subchunkerParams := ChunkerParams{params.Min, params.Avg, params.Max, ""}
	subchunker, err := subchunkerDesc.Constructor(nil, subchunkerParams)
	if err != nil {
		return nil, err
	}

	result := &SegmentAwareChunker{
		segmentAwareChunkerOptions: segmentAwareChunkerOptions,
		subchunker:                 subchunker,
	}

	if segmentAwareChunkerOptions.mergeSmallSegments {
		result.mergedSegments = make([]byte, params.Max)
		result.mergedSegmentsTmp = make([]byte, params.Max)
	}

	if err := result.Reset(r); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *SegmentAwareChunker) Next() (uint64, []byte, error) {

	// Prepare next blob from merged "small" segments, up to max chunk size.
	// Feed it to subchunker and align produced chunk boundary forward to next segment.
	// Doing stuff this way should keep chunking content defined, chunk sizes fit our desires BUT
	// chunk boundaries be strictly aligned with segment boundaries.
	//
	// When we encounter a "big" segment, switch to chunking it as a separate blob and do no alignment.
	// This will most likely produce a tail chunk sized less than min, but oh well.

	if c.insideBigSegment {
		subStart, subBytes, err := c.subchunker.Next()
		if err != nil {
			return 0, nil, err
		}

		if subStart == 0 {
			return 0, nil, fmt.Errorf("subchunker produced chunk that starts at 0 (segmentIdx=%v segmentSize=%v)",
				c.nextSegmentIdx, c.segmentSizes[c.nextSegmentIdx])
		}

		// Return valid chunk from subchunker
		if len(subBytes) != 0 {
			c.bigSegmentPos = subStart + uint64(len(subBytes))
			return c.currentOffset + subStart, subBytes, nil
		}

		// The subchunker is done, which also happens if the input ended early
		if c.bigSegmentPos != c.segmentSizes[c.nextSegmentIdx] {
			return 0, nil, fmt.Errorf("input ended %d bytes into segment %d of size %d",
				c.bigSegmentPos, c.nextSegmentIdx, c.segmentSizes[c.nextSegmentIdx])
		}

		// End processing "big" segment if subchunker is done.
		c.insideBigSegment = false
		c.currentOffset += c.segmentSizes[c.nextSegmentIdx]
		c.nextSegmentIdx++

	}

	err := c.mergeMoreSmallSegments()
	if err != nil {
		return 0, nil, err
	}

	// We merged some, produce a chunk from them.
	if c.mergedSegmentsCount != 0 {
		return c.nextFromMergedSegments()
	}

	// No segments left, signal that we are done. Any data left in the input
	// means the segments don't describe all of it.
	if c.isSegmentsEOF() {
		var b [1]byte
		_, err := io.ReadFull(c.r, b[:])
		if err == nil {
			return 0, nil, fmt.Errorf("input is longer than the %d bytes covered by its segments", c.currentOffset)
		}
		if err != io.EOF {
			return 0, nil, err
		}
		return c.currentOffset, nil, nil
	}

	// Next segment is "big" enough to not be merged into mergedSegments.

	// Some sanity here?
	if int64(c.segmentSizes[c.nextSegmentIdx]) <= 0 {
		return 0, nil, fmt.Errorf("invalid segment size %v for segmentIdx %v", c.segmentSizes[c.nextSegmentIdx], c.nextSegmentIdx)
	}

	c.insideBigSegment = true
	if err := c.subchunker.Reset(io.LimitReader(c.r, int64(c.segmentSizes[c.nextSegmentIdx]))); err != nil {
		return 0, nil, err
	}

	// Subchunker should always produce non-empty chunk here that starts at 0 or error out.
	subStart, subBytes, err := c.subchunker.Next()
	if err != nil {
		return 0, nil, err
	}

	if len(subBytes) == 0 {
		return 0, nil, fmt.Errorf("input ended at the start of segment %d of size %d",
			c.nextSegmentIdx, c.segmentSizes[c.nextSegmentIdx])
	}
	if subStart != 0 {
		return 0, nil, fmt.Errorf("subchunker produced chunk that doesn't start at 0 but at %v (segmentIdx=%v segmentSize=%v)",
			subStart, c.nextSegmentIdx, c.segmentSizes[c.nextSegmentIdx])
	}

	// TODO: maybe align it to the end of file, if it's less than min bytes left and chuk will still not reach max?
	// This will avoid producing a separate tail chunk that is too small.

	c.bigSegmentPos = uint64(len(subBytes))
	return c.currentOffset + subStart, subBytes, nil
}

func (c *SegmentAwareChunker) fillSegmentSizes() error {
	var (
		chunkSourceR     io.Reader
		err              error
		segmentSourceExt string
		selfSourced      bool
	)

	// Is our source a file? We can do some checks and automagic then.
	// Segments always describe the whole file, so remember where in the
	// file chunking starts.
	fileBeingChunked, _ := c.r.(*os.File)
	var (
		fileBeingChunkedInfo os.FileInfo
		startPos             int64
	)
	if fileBeingChunked != nil {
		fileBeingChunkedInfo, err = fileBeingChunked.Stat()
		if err != nil {
			return fmt.Errorf("fillSegmentSizes: %w", err)
		}
		if !fileBeingChunkedInfo.Mode().IsRegular() {
			fileBeingChunked = nil
		}
	}
	if fileBeingChunked != nil {
		startPos, err = fileBeingChunked.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("fillSegmentSizes: %w", err)
		}
	}

	if c.segmentAwareChunkerOptions.segmentsSource != "" {
		file, err := os.Open(c.segmentAwareChunkerOptions.segmentsSource)
		if err != nil {
			return fmt.Errorf("fillSegmentSizes: %w", err)
		}
		defer file.Close()
		chunkSourceR = file
		segmentSourceExt = path.Ext(c.segmentAwareChunkerOptions.segmentsSource)
	} else {
		// No segmentSource specified - try to use the same file as a source.
		if fileBeingChunked == nil {
			return fmt.Errorf("fillSegmentSizes: no segmentsSource provided")
		}

		fileBeingChunkedExt := path.Ext(fileBeingChunkedInfo.Name())
		// csv file being chunked can't be used as a segment source info itself!
		// Need to do this as Segmenters["csv"] does exist.
		if fileBeingChunkedExt == ".csv" {
			return fmt.Errorf("fillSegmentSizes: no segmentsSource provided")
		}

		// Read the segments from the beginning of the file, then go back to
		// where chunking starts.
		if _, err := fileBeingChunked.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("fillSegmentSizes: %w", err)
		}
		chunkSourceR = fileBeingChunked
		selfSourced = true
		segmentSourceExt = fileBeingChunkedExt
	}

	format := c.segmentAwareChunkerOptions.segmentsSourceFormat
	// TODO: autodetect file formats by content, not ext?
	if format == "" && segmentSourceExt != "" {
		format = segmentSourceExt[1:]
	}

	segmentF, exists := Segmenters[format]

	if !exists {
		return fmt.Errorf("fillSegmentSizes: unknown segmentsSource format '%s'", format)
	}

	c.segmentSizes, err = segmentF(chunkSourceR)
	if err != nil {
		return fmt.Errorf("fillSegmentSizes: %w", err)
	}
	if selfSourced {
		if _, err := fileBeingChunked.Seek(startPos, io.SeekStart); err != nil {
			return fmt.Errorf("fillSegmentSizes: %w", err)
		}
	}

	// A bit of sanity.
	if fileBeingChunked != nil {
		totalSegmentsSize := uint64(0)
		for _, v := range c.segmentSizes {
			totalSegmentsSize += v
		}
		if totalSegmentsSize != uint64(fileBeingChunkedInfo.Size()) {
			return fmt.Errorf("fillSegmentSizes sanity check: totalSegmentsSize != file size (%v != %v)",
				totalSegmentsSize, fileBeingChunkedInfo.Size())
		}
		if uint64(startPos) > totalSegmentsSize {
			return fmt.Errorf("fillSegmentSizes: start position %d is past the end of the file", startPos)
		}
		c.segmentSizes = trimSegments(c.segmentSizes, uint64(startPos))
	}

	return nil
}

// trimSegments returns the segments after offset, with the one that offset
// falls into shortened. It doesn't modify sizes.
func trimSegments(sizes []uint64, offset uint64) []uint64 {
	for len(sizes) > 0 && offset >= sizes[0] {
		offset -= sizes[0]
		sizes = sizes[1:]
	}
	if offset > 0 {
		sizes = slices.Clone(sizes)
		sizes[0] -= offset
	}
	return sizes
}

func (c *SegmentAwareChunker) isSegmentsEOF() bool {
	return c.nextSegmentIdx >= uint32(len(c.segmentSizes))
}

func (c *SegmentAwareChunker) isNextSegmentSmall() bool {
	return c.segmentSizes[c.nextSegmentIdx] <= c.smallSegmentBreakpoint
}

func (c *SegmentAwareChunker) canMergeNextSegment() bool {
	return uint64(cap(c.mergedSegments)) >= c.mergedSegmentsSize+c.segmentSizes[c.nextSegmentIdx]
}

func (c *SegmentAwareChunker) mergeMoreSmallSegments() error {
	for !c.isSegmentsEOF() && c.isNextSegmentSmall() && c.canMergeNextSegment() {
		segmentSize := c.segmentSizes[c.nextSegmentIdx]
		if segmentSize <= 0 {
			return fmt.Errorf("invalid segment size %v for segmentIdx %v", c.segmentSizes[c.nextSegmentIdx], c.nextSegmentIdx)
		}

		n, err := io.ReadFull(c.r, c.mergedSegments[c.mergedSegmentsSize:int(c.mergedSegmentsSize+segmentSize)])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return fmt.Errorf("input ended %d bytes into segment %d of size %d", n, c.nextSegmentIdx, segmentSize)
		}
		if err != nil {
			return err
		}
		c.mergedSegmentsSize += uint64(segmentSize)
		c.mergedSegmentsCount++
		c.nextSegmentIdx++
	}
	return nil
}

func (c *SegmentAwareChunker) nextFromMergedSegments() (uint64, []byte, error) {
	if c.mergedSegmentsSize == 0 {
		return 0, nil, fmt.Errorf("no merged segments to chunk")
	}
	if c.mergedSegmentsCount > c.nextSegmentIdx {
		return 0, nil, fmt.Errorf("%d merged segments, but only %d segments read", c.mergedSegmentsCount, c.nextSegmentIdx)
	}

	firstMergedSegmentIdx := c.nextSegmentIdx - c.mergedSegmentsCount

	if err := c.subchunker.Reset(bytes.NewReader(c.mergedSegments[:c.mergedSegmentsSize])); err != nil {
		return 0, nil, err
	}
	// Subchunker will always produce non-empty chunk here or error out.
	subStart, subBytes, err := c.subchunker.Next()
	if err != nil {
		return 0, nil, err
	}

	subchunkLen := uint64(len(subBytes))
	if subchunkLen == 0 {
		return 0, nil, fmt.Errorf("subchunker produced no chunk for merged segments %v - %v", firstMergedSegmentIdx, c.nextSegmentIdx)
	}
	if subStart != 0 {
		return 0, nil, fmt.Errorf("subchunker produced chunk that doesn't start at 0 but at %v", subStart)
	}

	// Align to segment boundary
	segmentsConsumed := uint32(0)
	sizeConsumed := uint64(0)
	for segmentsConsumed < c.mergedSegmentsCount {
		sizeConsumed += c.segmentSizes[firstMergedSegmentIdx+segmentsConsumed]
		segmentsConsumed++
		if sizeConsumed >= subchunkLen {
			break
		}
	}

	// Remove consumed segments from mergedSegments.
	returnFromMergedBytes := false
	if segmentsConsumed == c.mergedSegmentsCount {
		c.mergedSegmentsCount = 0
		c.mergedSegmentsSize = 0
		returnFromMergedBytes = true
	} else {
		// Move data from mergedSegments to preserve it and leave unconsumed data for next `Next()` call.
		copy(c.mergedSegmentsTmp[:sizeConsumed], c.mergedSegments[:sizeConsumed])
		copy(c.mergedSegments[:c.mergedSegmentsSize-sizeConsumed], c.mergedSegments[sizeConsumed:c.mergedSegmentsSize])
		c.mergedSegmentsSize -= sizeConsumed
		c.mergedSegmentsCount -= segmentsConsumed
	}

	chunkStart := c.currentOffset

	c.currentOffset += uint64(sizeConsumed)

	if returnFromMergedBytes {
		return chunkStart, c.mergedSegments[:sizeConsumed], nil
	} else {
		return chunkStart, c.mergedSegmentsTmp[:sizeConsumed], nil
	}
}

func (c *SegmentAwareChunker) Advance(n int) error {
	// TODO:
	return fmt.Errorf("Advance is unsupported")
}

func (c *SegmentAwareChunker) Reset(r io.Reader) error {

	c.r = r

	if err := c.fillSegmentSizes(); err != nil {
		return err
	}

	c.currentOffset = 0
	c.nextSegmentIdx = 0

	// c.mergedSegments, c.mergedSegmentsTmp is fine
	c.mergedSegmentsCount = 0
	c.mergedSegmentsSize = 0

	c.insideBigSegment = false

	// Replace our reader with a buffered one, a bit of reading performance buff.
	if _, ok := c.r.(*bufio.Reader); !ok {
		c.r = bufio.NewReaderSize(r, int(c.subchunker.Max()*4))
	}

	return nil
}

func (c *SegmentAwareChunker) Min() uint64 { return c.subchunker.Min() }
func (c *SegmentAwareChunker) Avg() uint64 { return c.subchunker.Avg() }
func (c *SegmentAwareChunker) Max() uint64 { return c.subchunker.Max() }

func init() {
	err := RegisterChunker(ChunkerDesc{
		Name: "segmentaware",
		HelpText: `Chuker that relies on provided segment sizes to make informed decision during chunking process.
Splits along segment boundaries, except when segments are big enough to produce multiple chunks.
Note that it could produce multiple chunks lesser then 'min' size as a consequence.
Possible --chukner-options:
	segmentsSource - file to use as a source for segment sizes. Defaults to file being chunked.
	segmentsSourceFormat - how to interprete 'segmentsSource' contents. Deduces format from file extension if not provided.
	subchunker - chunking algorithm to use for an actual CDC.
	mergeSmallSegments - should small chunks be merged before chunking. Helps to reduce amount of small chunks produced. True by default.
	smallSegmentBreakpoint - size of segments to be merged into bigger ones (if enabled above). Provided in kb, defaults to twice the avg chunk size.
		`,

		Constructor: func(r io.Reader, params ChunkerParams) (Chunker, error) {
			return NewSegmentAwareChunker(r, params)
		},
		WindowSize:     64, // TODO
		Parallelizable: false,
	})

	if err != nil {
		panic(err)
	}
}
