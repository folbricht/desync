package desync

import (
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"sync"
)

// stash holds the source of an in-place move in memory when other
// steps overwrite it before the move runs, which happens in dependency cycles
// like two chunks swapping places. The first step about to overwrite the
// source fills it, and the move releases it once it wrote the data. Filling
// on demand rather than up front keeps the memory held at any time low. If
// the stash doesn't fit into the memory budget, it's dropped instead and the
// move takes its chunk from the store.
type stash struct {
	offset uint64
	size   uint64
	budget *stashBudget

	mu    sync.Mutex
	state stashState
	data  []byte
}

type stashState int

const (
	stashEmpty   stashState = iota // the source is intact and wasn't read
	stashFilled                    // the source is held in data
	stashTaken                     // the move has the data
	stashDropped                   // not held, the move uses the store
)

// fill reads the source into memory before the calling step overwrites it.
// It does nothing if that already happened, or if the move ran already.
func (b *stash) fill(f *os.File) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != stashEmpty {
		return nil
	}
	if !b.budget.take(b.size) {
		b.state = stashDropped
		return nil
	}
	data := make([]byte, b.size)
	if _, err := f.ReadAt(data, int64(b.offset)); err != nil {
		b.budget.give(b.size)
		return fmt.Errorf("stash read at %d: %w", b.offset, err)
	}
	b.data, b.state = data, stashFilled
	return nil
}

// take returns the source data for the move and releases the stash. It
// reads the source directly if nothing overwrote it yet. It returns nil if
// the stash was dropped.
func (b *stash) take(f *os.File) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stashEmpty:
		data := make([]byte, b.size)
		if _, err := f.ReadAt(data, int64(b.offset)); err != nil {
			return nil, fmt.Errorf("inPlaceCopy read at %d: %w", b.offset, err)
		}
		b.state = stashTaken
		return data, nil
	case stashFilled:
		data := b.data
		b.data, b.state = nil, stashTaken
		b.budget.give(b.size)
		return data, nil
	default:
		return nil, nil
	}
}

// pending reports whether the stash still needs to be filled.
func (b *stash) pending() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == stashEmpty
}

// dropped reports whether the stash was dropped for lack of memory.
func (b *stash) dropped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == stashDropped
}

// stashBudget is the memory the stashes of in-place moves may hold at the
// same time.
type stashBudget struct {
	mu        sync.Mutex
	available int64
}

func (b *stashBudget) take(n uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if int64(n) > b.available {
		return false
	}
	b.available -= int64(n)
	return true
}

func (b *stashBudget) give(n uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.available += int64(n)
}

// stashMemoryLimit returns the memory in-place stashes may use. It is an
// indirection over memoryLimitBudget for tests.
var stashMemoryLimit = memoryLimitBudget

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
