package chunkers

import (
	"fmt"
	"io"
)

const (
	DefaultChunkerName = "buzhash"

	// Note: casync's buzhash descriminator value calculation is tuned for min/avg/max sizes
	// having differ by a factor of 4. Different relation will skew average chunk size.
	DefaultChunkSizeAvg uint64 = 64 * 1024
	DefaultChunkSizeMin        = DefaultChunkSizeAvg / 4
	DefaultChunkSizeMax        = DefaultChunkSizeAvg * 4
)

// Chunker is used to break up a data stream into chunks of data.
type Chunker interface {
	// Next returns the starting position as well as the chunk data. Returns
	// an empty byte slice when complete. The returned byte slice is only valid
	// until the next call to Next; callers that pass the slice to other
	// goroutines must copy it first.
	Next() (uint64, []byte, error)

	// Advance n bytes without producing chunks. This can be used if the content of the next
	// section in the file is known (i.e. it is known that there are a number of null chunks
	// coming). This resets everything in the chunker and behaves as if the streams starts
	// at (current position+n).
	Advance(n int) error

	// Drop current state and start reading from new stream. Keeps settings.
	Reset(r io.Reader) error

	// Returns the min/avg/max chunk size
	Min() uint64
	Avg() uint64
	Max() uint64
}

// Settings to initialize chunker with.
type ChunkerParams struct {
	// Chunk sizes.
	Min, Avg, Max uint64
	// Custom params interpreted by chunker, if applicable.
	Options string
}

func DefaultChunkerParams() ChunkerParams {

	return ChunkerParams{
		Min: DefaultChunkSizeMin,
		Avg: DefaultChunkSizeAvg,
		Max: DefaultChunkSizeMax,

		Options: "",
	}
}

type ChunkerConstructor func(r io.Reader, params ChunkerParams) (Chunker, error)

type ChunkerDesc struct {
	// Unique name to identify chunker algorithm with.
	Name string

	HelpText string

	Constructor ChunkerConstructor

	// Rolling window size used by chunker.
	// Do not expect valid output until this amount of bytes is fed.
	WindowSize uint32

	// True if chunking output depends on bytes in rolling window ONLY.
	// E.g. FastCDC with enabled normalization output depends on a current chunk size and could
	// produce different output for the same byte stream if it's prepended by a span of ~avg bytes.
	// This is a strict requirement for running parallel workers in IndexFromFile.
	Parallelizable bool
}

type chunkerRegistry struct {
	chunkers []ChunkerDesc
}

var registry chunkerRegistry

func (registry *chunkerRegistry) findByName(name string) *ChunkerDesc {
	for i := range registry.chunkers {
		if registry.chunkers[i].Name == name {
			return &registry.chunkers[i]
		}
	}
	return nil
}

func (registry *chunkerRegistry) register(desc ChunkerDesc) error {

	if registry.findByName(desc.Name) != nil {
		return fmt.Errorf("chunker %s already registered", desc.Name)
	}
	registry.chunkers = append(registry.chunkers, desc)
	return nil
}

func FindChunkerByName(name string) *ChunkerDesc { return registry.findByName(name) }
func RegisterChunker(desc ChunkerDesc) error     { return registry.register(desc) }

// NewChunker initializes a chunker for a data stream according to min/avg/max chunk size.
func NewChunker(name string, r io.Reader, params ChunkerParams) (Chunker, error) {
	if params.Min > params.Max {
		return nil, fmt.Errorf("min chunk size must not be greater than max")
	}
	if params.Min > params.Avg {
		return nil, fmt.Errorf("min chunk size must not be greater than avg")
	}
	if params.Avg > params.Max {
		return nil, fmt.Errorf("avg chunk size must not be greater than max")
	}

	if name == "" {
		name = DefaultChunkerName
	}

	chunkerDesc := registry.findByName(name)
	if chunkerDesc == nil {
		return nil, fmt.Errorf("unknown chunker '%s'", name)
	}

	if params.Min < uint64(chunkerDesc.WindowSize) {
		return nil, fmt.Errorf("min chunk size too small, must be over %d", chunkerDesc.WindowSize)
	}

	return chunkerDesc.Constructor(r, params)
}

func RegisteredNames() (result []string) {
	result = make([]string, 0, len(registry.chunkers))
	for i := range registry.chunkers {
		result = append(result, registry.chunkers[i].Name)
	}
	return
}
