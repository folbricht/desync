package desync

import (
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"sync"
)

// inPlaceBuffer holds the source of an in-place move in memory when other
// steps overwrite it before the move runs, which happens in dependency cycles
// like two chunks swapping places. The first step about to overwrite the
// source fills it, and the move releases it once it wrote the data. Filling
// on demand rather than up front keeps the memory held at any time low. If
// the buffer doesn't fit into the memory budget, it's dropped instead and the
// move takes its chunk from the store.
type inPlaceBuffer struct {
	offset uint64
	size   uint64
	budget *memoryBudget

	mu    sync.Mutex
	state bufferState
	data  []byte
}

type bufferState int

const (
	bufferEmpty   bufferState = iota // the source is intact and wasn't read
	bufferFilled                     // the source is held in data
	bufferTaken                      // the move has the data
	bufferDropped                    // not held, the move uses the store
)

// fill reads the source into memory before the calling step overwrites it.
// It does nothing if that already happened, or if the move ran already.
func (b *inPlaceBuffer) fill(f *os.File) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != bufferEmpty {
		return nil
	}
	if !b.budget.take(b.size) {
		b.state = bufferDropped
		return nil
	}
	data := make([]byte, b.size)
	if _, err := f.ReadAt(data, int64(b.offset)); err != nil {
		b.budget.give(b.size)
		return fmt.Errorf("inPlaceBuffer read at %d: %w", b.offset, err)
	}
	b.data, b.state = data, bufferFilled
	return nil
}

// take returns the source data for the move and releases the buffer. It
// reads the source directly if nothing overwrote it yet. It returns nil if
// the buffer was dropped.
func (b *inPlaceBuffer) take(f *os.File) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case bufferEmpty:
		data := make([]byte, b.size)
		if _, err := f.ReadAt(data, int64(b.offset)); err != nil {
			return nil, fmt.Errorf("inPlaceCopy read at %d: %w", b.offset, err)
		}
		b.state = bufferTaken
		return data, nil
	case bufferFilled:
		data := b.data
		b.data, b.state = nil, bufferTaken
		b.budget.give(b.size)
		return data, nil
	default:
		return nil, nil
	}
}

// pending reports whether the buffer still needs to be filled.
func (b *inPlaceBuffer) pending() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == bufferEmpty
}

// dropped reports whether the buffer was dropped for lack of memory.
func (b *inPlaceBuffer) dropped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == bufferDropped
}

// memoryBudget is the memory the buffers of in-place moves may hold at the
// same time.
type memoryBudget struct {
	mu        sync.Mutex
	available int64
}

func (b *memoryBudget) take(n uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if int64(n) > b.available {
		return false
	}
	b.available -= int64(n)
	return true
}

func (b *memoryBudget) give(n uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.available += int64(n)
}

// inPlaceBufferBudget returns the memory in-place buffers may use. It is an
// indirection over memoryLimitBudget for tests.
var inPlaceBufferBudget = memoryLimitBudget

// memoryLimitBudget returns the memory left under the Go runtime's memory
// limit, set with GOMEMLIMIT. Without a limit, the budget is unbounded.
func memoryLimitBudget() int64 {
	limit := debug.SetMemoryLimit(-1)
	if limit == math.MaxInt64 {
		return math.MaxInt64
	}
	// The runtime compares the limit against all memory it mapped, less
	// what it returned to the OS.
	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
	}
	metrics.Read(samples)
	inUse := int64(samples[0].Value.Uint64() - samples[1].Value.Uint64())
	return max(limit-inUse, 0)
}
