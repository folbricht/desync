package desync

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"
)

type planOption func(*assemblePlan)

func planWithConcurrency(n int) planOption {
	return func(p *assemblePlan) {
		p.concurrency = n
	}
}

// planWithSeeds sets the file seeds available to the plan. A seed reading
// from the target file itself must be set with planWithInPlaceSeed instead.
func planWithSeeds(seeds []Seed) planOption {
	return func(p *assemblePlan) {
		p.seeds = seeds
	}
}

// planWithInPlaceSeed sets the seed whose source is the target file itself.
// It is used to skip or rearrange data already present in the target.
func planWithInPlaceSeed(seed *FileSeed) planOption {
	return func(p *assemblePlan) {
		p.inPlaceSeed = seed
	}
}

// planWithBlocksize sets the blocksize of the target, used for reflinking.
// It's determined from the target if not set.
func planWithBlocksize(blocksize uint64) planOption {
	return func(p *assemblePlan) {
		p.blocksize = blocksize
	}
}

// planWithBufferBudget sets the memory the buffers of in-place moves may hold
// at the same time. Moves whose buffer doesn't fit take their chunk from the
// store instead. It's unbounded if not set.
func planWithBufferBudget(budget int64) planOption {
	return func(p *assemblePlan) {
		p.bufferBudget = budget
	}
}

func planWithTargetIsBlank(isBlank bool) planOption {
	return func(p *assemblePlan) {
		p.targetIsBlank = isBlank
	}
}

// assemblePlan holds a directed acyclic graph of steps.
type assemblePlan struct {
	idx           Index
	concurrency   int
	target        string
	store         Store
	seeds         []Seed
	inPlaceSeed   *FileSeed
	targetIsBlank bool
	blocksize     uint64
	bufferBudget  int64

	// Placements is an intermediate representation of the target index,
	// capturing what source is used to populate each chunk. It mirrors the
	// length of the index but a single step can span multiple chunks.
	placements []*placement

	// unique lists every placement once, in target order. A placement
	// spanning multiple chunks appears in p.placements several times but
	// executes as a single step.
	unique []*placement

	selfSeed *selfSeed

	// seedFiles records the state of every seed file when it was validated,
	// by name, to notice files that change during assembly.
	seedFiles map[string]seedFileState

	// skips holds the sources of the chunks found in place in the target,
	// computed once and shared with plans for other seeds. Consecutive
	// chunks share one source, which becomes a single step.
	skips []*skipInPlace
}

type assembleSource interface {
	fmt.Stringer
	Execute(f *os.File) (copied uint64, cloned uint64, err error)
	// recordStats adds the step's chunk accounting to stats. numChunks is
	// the number of index chunks the step covers.
	recordStats(stats *ExtractStats, numChunks int)
	// access tells what the step does to the target file, which decides
	// the order it runs in relative to the other steps.
	access() targetAccess
}

// byteRange is the range [start, end) of the target file.
type byteRange struct{ start, end uint64 }

func (r byteRange) empty() bool { return r.start >= r.end }

func (r byteRange) overlaps(o byteRange) bool {
	return !r.empty() && !o.empty() && r.start < o.end && o.start < r.end
}

// targetAccess is what a step does to the target file: the range it writes,
// and the range it reads from the target, if any.
type targetAccess struct {
	writes byteRange
	reads  byteRange

	// readsOld is set when the step reads the content from before assembly,
	// which the steps writing there must not overwrite before it's read.
	// Otherwise the step reads what other steps write, and runs after them.
	readsOld bool

	// buffer, if set, holds what the step reads. Instead of waiting for the
	// read, the steps overwriting the range fill the buffer first.
	buffer *inPlaceBuffer
}

type assembleSeedSource interface {
	assembleSource
	Seed() Seed
	// File is the file the source reads, empty if it doesn't read one.
	File() string
	Validate(file *os.File) error
}

type placement struct {
	source assembleSource
	step   *planStep // the step executing the placement, one per unique placement
}

// newPlan creates a fully populated assemblePlan.
func newPlan(name string, idx Index, s Store, opts ...planOption) *assemblePlan {
	p := &assemblePlan{
		idx:           idx,
		concurrency:   1,
		target:        name,
		store:         s,
		targetIsBlank: true,
		bufferBudget:  math.MaxInt64,
		placements:    make([]*placement, len(idx.Chunks)),
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.blocksize == 0 {
		p.blocksize = blocksizeOfFile(name)
	}
	p.selfSeed = newSelfSeed(p.target, p.idx, p.blocksize)
	p.generate()
	return p
}

// replan creates a plan for different seeds. It reuses what's known about
// the target independent of the seeds: its blocksize, the self-seed and the
// chunks already in place.
func (p *assemblePlan) replan(opts ...planOption) *assemblePlan {
	q := &assemblePlan{
		idx:           p.idx,
		concurrency:   p.concurrency,
		target:        p.target,
		store:         p.store,
		seeds:         p.seeds,
		inPlaceSeed:   p.inPlaceSeed,
		targetIsBlank: p.targetIsBlank,
		blocksize:     p.blocksize,
		bufferBudget:  p.bufferBudget,
		selfSeed:      p.selfSeed,
		skips:         p.skips,
		placements:    make([]*placement, len(p.idx.Chunks)),
	}
	for _, opt := range opts {
		opt(q)
	}
	q.generate()
	return q
}

// Validate checks that all file seed placements still match their underlying
// data. Returns a SeedInvalid error if a seed file was modified after its
// index was created.
func (p *assemblePlan) Validate() error {
	// Phase 1 — Sequential: collect unique fileSeedSource placements, open
	// their backing files, and build a list of items to validate.
	type validateItem struct {
		fs   assembleSeedSource
		file *os.File
	}

	fileMap := make(map[string]*os.File)
	defer func() {
		for _, f := range fileMap {
			_ = f.Close()
		}
	}()

	invalidSeeds := make(map[Seed]error)
	failedFiles := make(map[string]struct{})

	var items []validateItem
	for _, pl := range p.unique {
		// Sources that don't read a file, like those of the null chunk
		// seed, have no data their index could disagree with.
		fs, ok := pl.source.(assembleSeedSource)
		if !ok || fs.File() == "" {
			continue
		}

		// Skip seeds and files already known to be invalid
		if _, ok := invalidSeeds[fs.Seed()]; ok {
			continue
		}
		if _, ok := failedFiles[fs.File()]; ok {
			invalidSeeds[fs.Seed()] = fmt.Errorf("seed file %s could not be opened", fs.File())
			continue
		}

		if _, ok := fileMap[fs.File()]; !ok {
			f, err := os.Open(fs.File())
			if err != nil {
				failedFiles[fs.File()] = struct{}{}
				invalidSeeds[fs.Seed()] = err
				continue
			}
			fileMap[fs.File()] = f

			// Record the file as it is before reading its chunks, a change
			// from here on is noticed after assembly.
			info, err := f.Stat()
			if err != nil {
				invalidSeeds[fs.Seed()] = err
				continue
			}
			if p.seedFiles == nil {
				p.seedFiles = make(map[string]seedFileState)
			}
			p.seedFiles[fs.File()] = newSeedFileState(info)
		}

		items = append(items, validateItem{fs: fs, file: fileMap[fs.File()]})
	}

	// Phase 2 — Concurrent: validate each segment in parallel.
	var mu sync.Mutex
	var g errgroup.Group
	g.SetLimit(p.concurrency)
	for _, item := range items {
		g.Go(func() error {
			if err := item.fs.Validate(item.file); err != nil {
				mu.Lock()
				invalidSeeds[item.fs.Seed()] = err
				mu.Unlock()
			}
			return nil
		})
	}
	_ = g.Wait() // Validation failures are collected in invalidSeeds

	// Phase 3 — Sequential: build the error result.
	if len(invalidSeeds) > 0 {
		seeds := make([]Seed, 0, len(invalidSeeds))
		errs := make([]error, 0, len(invalidSeeds))
		for seed, err := range invalidSeeds {
			seeds = append(seeds, seed)
			errs = append(errs, err)
		}
		return SeedInvalid{Seeds: seeds, Err: errors.Join(errs...)}
	}
	return nil
}

func (p *assemblePlan) generate() {
	// When the target file already exists, mark chunks that are already
	// correct so they can be skipped during assembly. An in-place seed
	// then tells us which of the remaining chunks can be moved to their
	// position from elsewhere in the target.
	if !p.targetIsBlank {
		if p.skips == nil {
			p.skips = p.generateSkips()
		}
		p.placeSkips()
		if p.inPlaceSeed != nil {
			p.generateInPlace(p.inPlaceSeed)
		}
	}

	// Fill the remaining positions from the seeds, and from the target
	// itself once the chunks are written elsewhere in it (the self-seed).
	// Cloning is preferred over copying, and the seeds over the self-seed
	// as they don't depend on other steps.
	for _, cloning := range []bool{true, false} {
		for _, seed := range p.seeds {
			p.fillFromSeed(seed, cloning)
		}
		p.fillFromSelfSeed(cloning)
	}

	// Fill any gaps in the file by copying from the store.
	for i := range p.placements {
		if p.placements[i] != nil {
			continue
		}
		p.placements[i] = &placement{
			source: &copyFromStore{
				store: p.store,
				chunk: p.idx.Chunks[i],
			},
		}
	}

	// Every position is filled now. Create the step executing each unique
	// placement, counting the index chunks it covers.
	for _, pl := range p.placements {
		if pl.step == nil {
			pl.step = &planStep{source: pl.source}
			p.unique = append(p.unique, pl)
		}
		pl.step.numChunks++
	}
}

// placeSkips claims the positions of the chunks that are already in place.
// Their sources are shared between the plans of one assembly, the placements
// wrapping them are not.
func (p *assemblePlan) placeSkips() {
	var run *placement
	for i, src := range p.skips {
		if src == nil {
			run = nil
			continue
		}
		if run == nil || run.source != src {
			run = &placement{source: src}
		}
		p.placements[i] = run
	}
}

// fillFromSeed places the seed's matches in unfilled positions. With cloning
// set, only matches that are cloned into the target are used.
func (p *assemblePlan) fillFromSeed(seed Seed, cloning bool) {
	p.fillFromMatches(seed, cloning, func(pl *placement, from, match, n int) {
		pl.source = &fileSeedSource{
			segment:   seed.GetSegment(match, n),
			seed:      seed,
			offset:    p.idx.Chunks[from].Start,
			length:    chunkRangeLength(p.idx.Chunks[from : from+n]),
			blocksize: p.blocksize,
			isBlank:   p.targetIsBlank,
		}
	})
}

// fillFromSelfSeed places matches within the target in unfilled positions.
// Copying chunks within the target depends on the steps writing them first.
// With cloning set, only matches that are cloned are used.
func (p *assemblePlan) fillFromSelfSeed(cloning bool) {
	p.fillFromMatches(p.selfSeed, cloning, func(pl *placement, from, match, n int) {
		pl.source = p.selfSeed.GetSegment(match, n, p.idx.Chunks[from].Start)
	})
}

// fillFromMatches walks the positions that aren't filled yet, looking for the
// longest run of chunks the seed can provide for each of them. place sets the
// source of a claimed run, given its placement, the target position it starts
// at, the position of the match in the seed and the number of chunks claimed.
// With cloning set, only matches that are cloned into the target are used.
func (p *assemblePlan) fillFromMatches(seed matchingSeed, cloning bool, place func(pl *placement, from, match, n int)) {
	// Every position would be looked up only to find its match can't be
	// cloned. With a chunk repeating many times, that's most of the planning.
	cs, _ := seed.(cloningSeed)
	if cloning && (cs == nil || !cs.mayClone(p.targetIsBlank)) {
		return
	}
	for i := 0; i < len(p.idx.Chunks); i++ {
		if p.placements[i] != nil {
			continue
		}

		match, n := seed.LongestMatchFrom(p.idx.Chunks, i)
		if n < 1 {
			continue
		}
		if cloning && !cs.clones(match, p.idx.Chunks[i:i+n], p.blocksize, p.targetIsBlank) {
			continue
		}

		// The same placement covers the whole matched run, though it may
		// be cut short by positions that are already filled. We dedup runs
		// into single steps later.
		pl := &placement{}
		size := p.claimRun(pl, i, n)
		place(pl, i, match, size)

		i += size - 1 // the loop's i++ moves past the claimed run
	}
}

// claimRun assigns pl to up to n consecutive unfilled placements starting at
// position from. It stops at the first position that is already filled and
// returns the number of positions claimed.
func (p *assemblePlan) claimRun(pl *placement, from, n int) int {
	size := 0
	for ; size < n && p.placements[from+size] == nil; size++ {
		p.placements[from+size] = pl
	}
	return size
}

func (p *assemblePlan) Steps() []*planStep {
	// Order the steps by what they do to the target. The steps writing to it
	// are in target order and don't overlap, the writers of a range are found
	// by binary search.
	var (
		writers []*planStep
		writes  []IndexChunk
	)
	for _, pl := range p.unique {
		if w := pl.source.access().writes; !w.empty() {
			writers = append(writers, pl.step)
			writes = append(writes, IndexChunk{Start: w.start, Size: w.end - w.start})
		}
	}
	for _, pl := range p.unique {
		a := pl.source.access()
		if a.reads.empty() {
			continue
		}
		lo, hi := overlapping(writes, a.reads.start, a.reads.end)
		for _, w := range writers[lo:hi] {
			switch {
			case w == pl.step:
				// A step reads what it needs before it writes
			case !a.readsOld:
				link(w, pl.step) // read the data once it's written
			case a.buffer != nil:
				if !slices.Contains(w.fills, a.buffer) {
					w.fills = append(w.fills, a.buffer) // hold it before it's overwritten
				}
			default:
				link(pl.step, w) // read the data before it's overwritten
			}
		}
	}

	// List the in-place moves before the other sources so they're handed
	// out first, the steps waiting for them to read their sources follow.
	steps := make([]*planStep, 0, len(p.unique))
	for _, moves := range []bool{true, false} {
		for _, pl := range p.unique {
			if _, ok := pl.source.(*inPlaceCopy); ok == moves {
				steps = append(steps, pl.step)
			}
		}
	}
	return steps
}

// changedSeeds returns the seed files that may have been written to since they
// were validated. The target itself, the data of an in-place seed, is left
// out: assembly writes it.
func (p *assemblePlan) changedSeeds() []string {
	var changed []string
	for name, state := range p.seedFiles {
		if name != p.target && state.changed(name) {
			changed = append(changed, name)
		}
	}
	slices.Sort(changed)
	return changed
}

// generateSkips reads the target file and returns the sources for the chunks
// that are already in the correct position so they can be skipped during
// assembly. Consecutive matching chunks share a single source so they become
// one step.
func (p *assemblePlan) generateSkips() []*skipInPlace {
	found, err := chunksInTarget(p.target, p.idx, p.concurrency)
	if err != nil {
		return nil
	}

	skips := make([]*skipInPlace, len(p.idx.Chunks))
	for i, ok := range found {
		if ok {
			chunk := p.idx.Chunks[i]
			skips[i] = &skipInPlace{start: chunk.Start, end: chunk.Start + chunk.Size}
		}
	}

	// Merge consecutive in-place chunks into a single source so that they
	// become one step instead of one per chunk.
	var run *skipInPlace
	for i, sk := range skips {
		if sk == nil {
			run = nil
			continue
		}
		if run == nil {
			run = sk
			continue
		}
		// Extend the existing run and share the pointer
		run.end = p.idx.Chunks[i].Start + p.idx.Chunks[i].Size
		skips[i] = run
	}
	return skips
}

// moveOp is a chunk, or a run of consecutive chunks, that the in-place seed
// holds at a different offset in the target file.
type moveOp struct {
	targetIdx int    // index of the first chunk in the target index
	srcIdx    int    // index of the first chunk in the seed index
	chunks    int    // number of chunks the move covers
	srcStart  uint64 // byte offset the data is read from
	dstStart  uint64 // byte offset the data is written to
	size      uint64 // the chunks are the same on both ends
}

// overlaps reports whether the source and the destination of the move share
// any bytes.
func (m moveOp) overlaps() bool {
	return m.srcStart < m.dstStart+m.size && m.dstStart < m.srcStart+m.size
}

// joins reports whether next continues the move without a gap, in the source
// as well as the destination. Both being contiguous means the two travel the
// same distance. The run may not grow past that distance, beyond it the source
// and the destination of the move would overlap.
func (m moveOp) joins(next moveOp) bool {
	distance := max(m.srcStart, m.dstStart) - min(m.srcStart, m.dstStart)
	return m.srcStart+m.size == next.srcStart &&
		m.dstStart+m.size == next.dstStart &&
		m.size+next.size <= distance
}

// generateInPlace processes an in-place seed for the chunks that aren't
// already in place. Chunks that exist at different offsets in the seed get
// inPlaceCopy placements, ordered so that every move reads its source before
// it's overwritten. Dependency cycles between moves are broken by buffering
// sources in memory.
func (p *assemblePlan) generateInPlace(seed *FileSeed) {
	// Stage 1: Operation list — walk target index and classify each chunk.
	var moves []moveOp

	// Chunks that exist in the seed become move operations, sourced from
	// their first location. The chunks already in place were skipped
	// before. If the seed claims one of them is at its position anyway,
	// the seed is out of date there and the position isn't a source. Zero
	// chunks are left to the null seed, which clones them or doesn't write
	// them at all, rather than reading one zero chunk over and over.
	nullID := NewNullChunk(p.idx.Index.ChunkSizeMax).ID
	for i, c := range p.idx.Chunks {
		if p.placements[i] != nil || c.ID == nullID {
			continue // Already placed, or zeros
		}

		sources := seed.pos[c.ID]
		j := slices.IndexFunc(sources, func(pos int) bool {
			return seed.index.Chunks[pos].Start != c.Start
		})
		if j < 0 {
			continue // Not in seed; will be filled by store or file seed later
		}
		moves = append(moves, moveOp{
			targetIdx: i,
			srcIdx:    sources[j],
			chunks:    1,
			srcStart:  seed.index.Chunks[sources[j]].Start,
			dstStart:  c.Start,
			size:      c.Size,
		})
	}

	if len(moves) == 0 {
		return
	}

	// Stage 2: Dependency graph — edge from i to j when move i's source
	// overlaps move j's destination (i must read before j writes).
	succ := moveGraph(moves)

	// Stage 3: Break cycles. Moves in a dependency cycle, like two chunks
	// swapping places, can't be ordered. Some of them hold their source in
	// a buffer instead, which removes their outgoing edges.
	buffered := breakCycles(succ)

	// Stage 4: Merge the moves of one contiguous shift into a single move,
	// which then runs as one step. Moves that hold their source in a buffer
	// stay on their own, a buffer for a whole run would need far more memory.
	// Merging can create dependency cycles that the moves didn't have per
	// chunk, so the runs are checked and split again where it did.
	if merged, mergedBuffered := mergeMoves(moves, buffered); len(merged) < len(moves) {
		moves, buffered = splitCycles(merged, mergedBuffered, seed.index.Chunks)
	}

	// Stage 6: Placements. The buffers of all moves share one budget. Steps()
	// orders them by the ranges they read and write.
	budget := &memoryBudget{available: p.bufferBudget}
	for i, m := range moves {
		ipc := &inPlaceCopy{
			chunks:    seed.index.Chunks[m.srcIdx : m.srcIdx+m.chunks],
			dstOffset: m.dstStart,
			seed:      seed,
			file:      p.target,
			blocksize: p.blocksize,
			store:     p.store,
		}
		if buffered[i] {
			ipc.buffer = &inPlaceBuffer{offset: m.srcStart, size: m.size, budget: budget}
		} else {
			ipc.clone = seed.canReflink && m.chunks > 1 && !m.overlaps() &&
				reflinkable(m.srcStart, m.size, m.dstStart, p.blocksize)
		}
		pl := &placement{source: ipc}
		for j := range m.chunks {
			p.placements[m.targetIdx+j] = pl
		}
	}
}

// moveGraph returns the successors of every move: an edge from i to j means
// move i reads from where move j writes, so i has to run first. The moves are
// in target order, so their destinations are sorted and don't overlap.
func moveGraph(moves []moveOp) [][]int {
	dst := make([]IndexChunk, len(moves))
	for i, m := range moves {
		dst[i] = IndexChunk{Start: m.dstStart, Size: m.size}
	}
	succ := make([][]int, len(moves))
	for i, m := range moves {
		lo, hi := overlapping(dst, m.srcStart, m.srcStart+m.size)
		for j := lo; j < hi; j++ {
			if i != j {
				succ[i] = append(succ[i], j)
			}
		}
	}
	return succ
}

// mergeMoves combines the moves of one contiguous shift into a single move.
// Consecutive chunks that all travel the same distance describe one memmove,
// which is one step instead of one per chunk: fewer dependencies to track, and
// the data can be moved in one operation. Buffered moves are left alone.
func mergeMoves(moves []moveOp, buffered []bool) ([]moveOp, []bool) {
	merged := make([]moveOp, 0, len(moves))
	mergedBuffered := make([]bool, 0, len(moves))
	for i, m := range moves {
		last := len(merged) - 1
		if last >= 0 && !buffered[i] && !mergedBuffered[last] && merged[last].joins(m) {
			merged[last].size += m.size
			merged[last].chunks += m.chunks
			continue
		}
		merged = append(merged, m)
		mergedBuffered = append(mergedBuffered, buffered[i])
	}
	return merged, mergedBuffered
}

// splitCycles splits the merged runs that a dependency cycle runs through back
// into their chunks, so the moves can be ordered with the buffers they needed
// per chunk. Two regions of a file swapping places for example can't be moved
// as two runs, however the moves of their chunks interleave. Runs outside of
// any cycle stay merged. It returns the moves and which of them hold their
// source in a buffer.
func splitCycles(moves []moveOp, buffered []bool, seedChunks []IndexChunk) ([]moveOp, []bool) {
	graph := func() [][]int {
		succ := moveGraph(moves)
		for i := range succ {
			if buffered[i] {
				succ[i] = nil // removed to break a cycle before merging
			}
		}
		return succ
	}
	succ := graph()
	inCycle := movesInCycles(succ)
	split := make([]bool, len(moves))
	var found bool
	for i, m := range moves {
		if inCycle[i] && m.chunks > 1 {
			split[i], found = true, true
		}
	}
	if found {
		// Splitting runs only takes edges away, it can't put another run
		// into a cycle.
		moves, buffered = splitRuns(moves, buffered, split, seedChunks)
		succ = graph()
	}

	// A cycle through single chunks alone was there before merging and is
	// broken by the buffers from then. Nothing is left to buffer, but
	// checking costs little and a cycle would stall assembly.
	for i, b := range breakCycles(succ) {
		buffered[i] = buffered[i] || b
	}
	return moves, buffered
}

// movesInCycles reports the moves that take part in a dependency cycle: the
// members of strongly connected components of two moves or more. It's
// Tarjan's algorithm, iterative so that long chains of moves can't exhaust the
// stack.
func movesInCycles(succ [][]int) []bool {
	n := len(succ)
	var (
		index   = make([]int, n) // order of discovery from 1, 0 is unvisited
		low     = make([]int, n) // lowest index reachable from the node's subtree
		onStack = make([]bool, n)
		stack   []int
		inCycle = make([]bool, n)
		next    = 1
	)
	type frame struct{ node, edge int }
	visit := func(v int) {
		index[v], low[v] = next, next
		next++
		stack = append(stack, v)
		onStack[v] = true
	}
	for root := range n {
		if index[root] != 0 {
			continue
		}
		visit(root)
		call := []frame{{node: root}}
		for len(call) > 0 {
			f := &call[len(call)-1]
			v := f.node
			if f.edge < len(succ[v]) {
				w := succ[v][f.edge]
				f.edge++
				switch {
				case index[w] == 0:
					visit(w)
					call = append(call, frame{node: w})
				case onStack[w]:
					low[v] = min(low[v], index[w])
				}
				continue
			}
			// All successors are done. If none of them reached above v, v
			// and what's above it on the stack form a component.
			if low[v] == index[v] {
				i := len(stack) - 1
				for stack[i] != v {
					i--
				}
				for _, w := range stack[i:] {
					onStack[w] = false
					inCycle[w] = len(stack)-i > 1
				}
				stack = stack[:i]
			}
			call = call[:len(call)-1]
			if len(call) > 0 {
				u := call[len(call)-1].node
				low[u] = min(low[u], low[v])
			}
		}
	}
	return inCycle
}

// splitRuns expands the marked runs back into one move per chunk. It's used
// when merging a run created a dependency cycle, which is broken by buffering
// a move; only single chunks are buffered so the memory stays bounded.
func splitRuns(moves []moveOp, buffered, marked []bool, seedChunks []IndexChunk) ([]moveOp, []bool) {
	split := make([]moveOp, 0, len(moves))
	splitBuffered := make([]bool, 0, len(moves))
	for i, m := range moves {
		if !marked[i] || m.chunks == 1 {
			split = append(split, m)
			splitBuffered = append(splitBuffered, buffered[i])
			continue
		}
		dst := m.dstStart
		for k := range m.chunks {
			c := seedChunks[m.srcIdx+k]
			split = append(split, moveOp{
				targetIdx: m.targetIdx + k,
				srcIdx:    m.srcIdx + k,
				chunks:    1,
				srcStart:  c.Start,
				dstStart:  dst,
				size:      c.Size,
			})
			splitBuffered = append(splitBuffered, false)
			dst += c.Size
		}
	}
	return split, splitBuffered
}

// overlapping returns the range [lo, hi) of chunks overlapping the byte range
// [start, end). The chunks have to be sorted by offset without overlapping
// each other, like those of an index.
func overlapping(chunks []IndexChunk, start, end uint64) (lo, hi int) {
	lo = sort.Search(len(chunks), func(i int) bool {
		return chunks[i].Start+chunks[i].Size > start
	})
	hi = lo
	for hi < len(chunks) && chunks[hi].Start < end {
		hi++
	}
	return lo, hi
}

// breakCycles picks nodes of the directed graph succ whose outgoing edges
// have to be removed for the graph to become acyclic. It runs a depth-first
// search and marks the tail of every back edge, without exploring its
// remaining edges. The edges left are those of unmarked nodes, and none of
// them is a back edge of the search, so they can't form a cycle.
func breakCycles(succ [][]int) []bool {
	const (
		unvisited = iota
		onStack
		finished
	)
	type frame struct{ node, next int }
	var (
		state  = make([]uint8, len(succ))
		marked = make([]bool, len(succ))
		stack  []frame
	)
	for root := range succ {
		if state[root] != unvisited {
			continue
		}
		state[root] = onStack
		stack = append(stack, frame{node: root})
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if marked[top.node] || top.next == len(succ[top.node]) {
				state[top.node] = finished
				stack = stack[:len(stack)-1]
				continue
			}
			w := succ[top.node][top.next]
			top.next++
			switch state[w] {
			case unvisited:
				state[w] = onStack
				stack = append(stack, frame{node: w})
			case onStack:
				marked[top.node] = true
			}
		}
	}
	return marked
}
