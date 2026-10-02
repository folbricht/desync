package chunkers

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
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
	smallSegmentBreakpoint uint32
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
			break
		case "segmentsSourceFormat":
			options.segmentsSourceFormat = kv[1]
			break
		case "mergeSmallSegments":
			options.mergeSmallSegments = !(kv[1] == "false" || kv[1] == "0")
			break
		case "smallSegmentBreakpoint":
			var kbValue int
			kbValue, err = strconv.Atoi(kv[1])
			if err != nil {
				return
			}

			options.smallSegmentBreakpoint = uint32(kbValue * 1024)
			break
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
			segmentAwareChunkerOptions.smallSegmentBreakpoint = uint32(params.Avg * 2)
		}
		segmentAwareChunkerOptions.smallSegmentBreakpoint = min(segmentAwareChunkerOptions.smallSegmentBreakpoint, uint32(params.Max))
	} else {
		segmentAwareChunkerOptions.smallSegmentBreakpoint = 0
	}

	subchunkerDesc := FindChunkerByName(segmentAwareChunkerOptions.subchunker)
	// TODO: check subchunker window size?
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
			return c.currentOffset + subStart, subBytes, nil
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

	// No segments left, signal that we are done.
	if c.isSegmentsEOF() {
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
		return 0, nil, fmt.Errorf("subchunker produced no chunk for (segmentIdx=%v segmentSize=%v)",
			c.nextSegmentIdx, c.segmentSizes[c.nextSegmentIdx])
	}
	if subStart != 0 {
		return 0, nil, fmt.Errorf("subchunker produced chunk that doesn't start at 0 but at %v (segmentIdx=%v segmentSize=%v)",
			subStart, c.nextSegmentIdx, c.segmentSizes[c.nextSegmentIdx])
	}

	// TODO: maybe align it to the end of file, if it's less than min bytes left and chuk will still not reach max?
	// This will avoid producing a separate tail chunk that is too small.

	return c.currentOffset + subStart, subBytes, nil
}

func (c *SegmentAwareChunker) fillSegmentSizes() error {
	var (
		chunkSourceR     io.Reader
		err              error
		segmentSourceExt string
	)

	// Is our source a file? We can do some checks and automagic then.
	fileBeingChunked, _ := c.r.(*os.File)
	var fileBeingChunkedInfo os.FileInfo
	if fileBeingChunked != nil {
		fileBeingChunkedInfo, err = fileBeingChunked.Stat()
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

		// Start reading from the beginning, but reset read position after finishing.
		curPos, err := fileBeingChunked.Seek(0, 1)
		if err != nil {
			return fmt.Errorf("fillSegmentSizes: %w", err)
		}
		defer fileBeingChunked.Seek(curPos, 0)

		fileBeingChunked.Seek(curPos, 0)
		chunkSourceR = fileBeingChunked
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
	}

	return nil
}

func (c *SegmentAwareChunker) isSegmentsEOF() bool {
	return c.nextSegmentIdx >= uint32(len(c.segmentSizes))
}

func (c *SegmentAwareChunker) isNextSegmentSmall() bool {
	return c.segmentSizes[c.nextSegmentIdx] <= uint64(c.smallSegmentBreakpoint)
}

func (c *SegmentAwareChunker) canMergeNextSegment() bool {
	return uint64(cap(c.mergedSegments)) >= c.mergedSegmentsSize+c.segmentSizes[c.nextSegmentIdx]
}

func (c *SegmentAwareChunker) mergeMoreSmallSegments() error {
	for !c.isSegmentsEOF() && c.isNextSegmentSmall() && c.canMergeNextSegment() {
		readSizeLeft := c.segmentSizes[c.nextSegmentIdx]
		if readSizeLeft <= 0 {
			return fmt.Errorf("invalid segment size %v for segmentIdx %v", c.segmentSizes[c.nextSegmentIdx], c.nextSegmentIdx)
		}

		for readSizeLeft > 0 {
			n, err := c.r.Read(c.mergedSegments[c.mergedSegmentsSize:int(c.mergedSegmentsSize+readSizeLeft)])
			// EOF here is a real error condition: we rely on provided segment sizes being correct.
			if err != nil {
				return err
			}
			readSizeLeft -= uint64(n)
			c.mergedSegmentsSize += uint64(n)
		}
		c.mergedSegmentsCount++
		c.nextSegmentIdx++
	}
	return nil
}

func (c *SegmentAwareChunker) nextFromMergedSegments() (uint64, []byte, error) {
	if c.mergedSegmentsSize == 0 {
		panic(fmt.Errorf("c.mergedSegmentsSize == 0"))
	}
	if c.mergedSegmentsCount > c.nextSegmentIdx {
		panic(fmt.Errorf("c.mergedSegmentsCount > c.nextSegmentIdx"))
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

	subchunkLen := uint32(len(subBytes))
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
		if sizeConsumed > uint64(subchunkLen) {
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
