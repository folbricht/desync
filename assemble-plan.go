package desync

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"
)

type PlanOption func(*AssemblePlan)

func PlanWithConcurrency(n int) PlanOption {
	return func(p *AssemblePlan) {
		p.concurrency = n
	}
}

// PlanWithSeeds sets the file seeds available to the plan. A seed reading
// from the target file itself must be set with PlanWithInPlaceSeed instead.
func PlanWithSeeds(seeds []Seed) PlanOption {
	return func(p *AssemblePlan) {
		p.seeds = seeds
	}
}

// PlanWithInPlaceSeed sets the seed whose source is the target file itself.
// It is used to skip or rearrange data already present in the target.
func PlanWithInPlaceSeed(seed *InPlaceSeed) PlanOption {
	return func(p *AssemblePlan) {
		p.inPlaceSeed = seed
	}
}

func PlanWithTargetIsBlank(isBlank bool) PlanOption {
	return func(p *AssemblePlan) {
		p.targetIsBlank = isBlank
	}
}

// AssemblePlan holds a directed acyclic graph of steps.
type AssemblePlan struct {
	idx           Index
	concurrency   int
	target        string
	store         Store
	seeds         []Seed
	inPlaceSeed   *InPlaceSeed
	targetIsBlank bool
	blocksize     uint64

	// Placements is an intermediate representation of the target index,
	// capturing what source is used to populate each chunk. It mirrors the
	// length of the index but a single step can span multiple chunks.
	placements []*placement

	// InPlaceReads is a list of placements with sources that read sections
	// from the target file. This needs to happen before any steps that
	// overwrite the in-place source data. This is sparsely populated and
	// used to express a dependency in the form "don't write to this chunk
	// until these chunks are read from the in-place target".
	inPlaceReads []*placement

	// inPlaceDeps records ordering constraints between in-place copy
	// placements produced by Tarjan's SCC linearization. Each entry
	// says placement[from] must complete before placement[to] starts.
	inPlaceDeps []inPlaceDep

	// inPlaceOrder lists placements from generateInPlace in their
	// desired step output order: skips first, then copies in
	// linearized cycle order. Steps() iterates this before
	// p.placements so in-place operations precede other sources.
	inPlaceOrder []*placement

	selfSeed *selfSeed
}

type inPlaceDep struct{ from, to int }

type assembleSource interface {
	fmt.Stringer
	Execute(f *os.File) (copied uint64, cloned uint64, err error)
	// recordStats adds the step's chunk accounting to stats. numChunks is
	// the number of index chunks the step covers.
	recordStats(stats *ExtractStats, numChunks int)
}

type assembleSeedSource interface {
	assembleSource
	Seed() Seed
	File() string
	Validate(file *os.File) error
	// needsValidation reports whether the source is backed by a file whose
	// content must be checked against the seed index.
	needsValidation() bool
}

type placement struct {
	source         assembleSource
	dependsOnStart int // index of another placement this one depends on
	dependsOnSize  int // number of sequential placements (from dependsOnStart) this depends on
}

// NewPlan creates a fully populated AssemblePlan.
func NewPlan(name string, idx Index, s Store, opts ...PlanOption) *AssemblePlan {
	p := &AssemblePlan{
		idx:           idx,
		concurrency:   1,
		target:        name,
		store:         s,
		targetIsBlank: true,
		placements:    make([]*placement, len(idx.Chunks)),
		inPlaceReads:  make([]*placement, len(idx.Chunks)),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.blocksize = blocksizeOfFile(name)
	p.selfSeed = newSelfSeed(p.target, p.idx)
	p.generate()
	return p
}

// Validate checks that all file seed placements still match their underlying
// data. Returns a SeedInvalid error if a seed file was modified after its
// index was created.
func (p *AssemblePlan) Validate() error {
	// Phase 1 — Sequential: collect unique fileSeedSource placements, open
	// their backing files, and build a list of items to validate.
	type validateItem struct {
		fs   assembleSeedSource
		file *os.File
	}

	seen := make(map[*placement]struct{})
	fileMap := make(map[string]*os.File)
	defer func() {
		for _, f := range fileMap {
			f.Close()
		}
	}()

	invalidSeeds := make(map[Seed]error)
	failedFiles := make(map[string]struct{})

	var items []validateItem
	for _, pl := range p.placements {
		if _, ok := seen[pl]; ok {
			continue
		}
		seen[pl] = struct{}{}

		fs, ok := pl.source.(assembleSeedSource)
		if !ok || !fs.needsValidation() {
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
	g.Wait()

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

func (p *AssemblePlan) generate() {
	// When the target file already exists, mark chunks that are already
	// correct so they can be skipped during assembly. If we have an
	// in-place seed, its index tells us what's already in place without
	// any file I/O. Otherwise fall back to reading and hashing each chunk.
	if !p.targetIsBlank {
		if p.inPlaceSeed != nil {
			p.generateInPlace(p.inPlaceSeed)
		} else {
			p.generateSkips()
		}
	}

	// Find all matches in file itself as they're written. As it's
	// populated, sections can be copied to other chunks. This involves
	// depending on earlier steps before chunks can be copied within the
	// file.
	for i := 0; i < len(p.idx.Chunks); i++ {
		if p.placements[i] != nil {
			continue // Already filled
		}

		start, n := p.selfSeed.LongestMatchFrom(p.idx.Chunks, i)
		if n < 1 {
			continue
		}

		// The same placement covers the whole matched run, though it may
		// be cut short by positions that are already filled. We dedup
		// runs into single steps later.
		pl := &placement{}
		size := p.claimRun(pl, i, n)

		seedOffset := p.idx.Chunks[start].Start
		length := chunkRangeLength(p.idx.Chunks[start : start+size])
		offset := p.idx.Chunks[i].Start

		pl.source = p.selfSeed.GetSegment(seedOffset, offset, length)
		pl.dependsOnStart = start
		pl.dependsOnSize = size

		i += size - 1 // the loop's i++ moves past the claimed run
	}

	// Check file seeds for matches in unfilled positions.
	for _, seed := range p.seeds {
		for i := 0; i < len(p.idx.Chunks); i++ {
			if p.placements[i] != nil {
				continue
			}

			match, n := seed.LongestMatchFrom(p.idx.Chunks, i)
			if n < 1 {
				continue
			}

			// The same placement covers the whole matched run, though it
			// may be cut short by positions that are already filled. We
			// dedup runs into single steps later.
			pl := &placement{}
			size := p.claimRun(pl, i, n)

			pl.source = &fileSeedSource{
				segment:   seed.GetSegment(match, size),
				seed:      seed,
				offset:    p.idx.Chunks[i].Start,
				length:    chunkRangeLength(p.idx.Chunks[i : i+size]),
				blocksize: p.blocksize,
				isBlank:   p.targetIsBlank,
			}

			i += size - 1 // the loop's i++ moves past the claimed run
		}
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

	// We now have a fully populated list of placements. Some are
	// duplicates, spanning multiple chunks. Dependencies are only defined
	// forward, like chunk-A needs chunk-B to be written first, etc.
}

// claimRun assigns pl to up to n consecutive unfilled placements starting at
// position from. It stops at the first position that is already filled and
// returns the number of positions claimed.
func (p *AssemblePlan) claimRun(pl *placement, from, n int) int {
	size := 0
	for ; size < n && p.placements[from+size] == nil; size++ {
		p.placements[from+size] = pl
	}
	return size
}

func (p *AssemblePlan) Steps() []*PlanStep {
	// Create a step for every unique placement, counting how many
	// index chunks each step covers.
	stepsPerPlacement := make(map[*placement]*PlanStep)
	for _, pl := range p.placements {
		step, ok := stepsPerPlacement[pl]
		if !ok {
			step = &PlanStep{
				source: pl.source,
			}
			stepsPerPlacement[pl] = step
		}
		step.numChunks++
	}

	// Link the steps together. Use a seen set to avoid redundant work
	// when the same placement pointer spans multiple chunks.
	linked := make(map[*placement]struct{}, len(stepsPerPlacement))
	for _, pl := range p.placements {
		if _, ok := linked[pl]; ok {
			continue
		}
		linked[pl] = struct{}{}

		for i := pl.dependsOnStart; i < pl.dependsOnStart+pl.dependsOnSize; i++ {
			link(stepsPerPlacement[p.placements[i]], stepsPerPlacement[pl])
		}
	}

	// Link in-place read dependencies: if a subsequent step (store
	// copy, file seed) writes to a byte range that an in-place
	// copy needs to read, the in-place copy must execute first.
	for i, inPlaceRead := range p.inPlaceReads {
		if inPlaceRead == nil {
			continue
		}
		target := p.placements[i]
		if target == inPlaceRead {
			continue
		}
		ipStep := stepsPerPlacement[inPlaceRead]
		step := stepsPerPlacement[target]
		if step != ipStep {
			link(ipStep, step)
		}
	}

	// Link in-place inter-operation dependencies from Tarjan
	// linearization. These ensure cycle members and cross-SCC
	// operations execute in the correct order.
	for _, dep := range p.inPlaceDeps {
		from := stepsPerPlacement[p.placements[dep.from]]
		to := stepsPerPlacement[p.placements[dep.to]]
		if from != to {
			link(from, to)
		}
	}

	// Make a slice of steps, preserving the order. Iterate
	// inPlaceOrder first so in-place seed placements (skips + copies)
	// precede other sources. Then iterate p.placements for everything
	// else. Deduplication by pointer identity ensures each step
	// appears exactly once.
	steps := make([]*PlanStep, 0, len(stepsPerPlacement))
	for _, pl := range p.inPlaceOrder {
		s, ok := stepsPerPlacement[pl]
		if !ok {
			continue
		}
		steps = append(steps, s)
		delete(stepsPerPlacement, pl)
	}
	for _, pl := range p.placements {
		s, ok := stepsPerPlacement[pl]
		if !ok {
			continue
		}
		steps = append(steps, s)
		delete(stepsPerPlacement, pl)
	}

	return steps
}

// generateSkips reads the target file and marks chunks that are already in
// the correct position so they can be skipped during assembly. Consecutive
// matching chunks are merged into a single placement for efficiency.
func (p *AssemblePlan) generateSkips() {
	f, err := os.Open(p.target)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	work := make(chan int)
	for range p.concurrency {
		wg.Go(func() {
			buf := make([]byte, p.idx.Index.ChunkSizeMax)
			for i := range work {
				chunk := p.idx.Chunks[i]
				if chunkInPlace(f, chunk, buf) {
					p.placements[i] = &placement{source: &skipInPlace{
						start: chunk.Start,
						end:   chunk.Start + chunk.Size,
					}}
				}
			}
		})
	}
	for i := range p.idx.Chunks {
		work <- i
	}
	close(work)
	wg.Wait()
	f.Close()

	// Merge consecutive in-place chunks into a single placement
	// so that Steps() produces one step per run instead of one
	// per chunk. This works because Steps() deduplicates by
	// pointer identity.
	var run *placement
	for i, pl := range p.placements {
		if pl == nil {
			run = nil
			continue
		}
		if _, ok := pl.source.(*skipInPlace); !ok {
			run = nil
			continue
		}
		if run == nil {
			run = pl
			continue
		}
		// Extend the existing run and share the pointer
		run.source.(*skipInPlace).end = p.idx.Chunks[i].Start + p.idx.Chunks[i].Size
		p.placements[i] = run
	}
}

// generateInPlace processes an in-place seed to classify each target chunk.
// Chunks already at the correct position get skipInPlace placements (detected
// by comparing the seed and target indexes, with no file I/O). Chunks that
// exist at different offsets get inPlaceCopy placements, with dependency
// cycles resolved using Tarjan's SCC algorithm.
func (p *AssemblePlan) generateInPlace(seed *InPlaceSeed) {
	// Stage 1: Source mapping — index every chunk in the seed by its
	// ChunkID, recording all byte ranges where it appears.
	type byteRange struct{ start, size uint64 }
	srcOf := make(map[ChunkID][]byteRange)
	for _, c := range seed.index.Chunks {
		srcOf[c.ID] = append(srcOf[c.ID], byteRange{c.Start, c.Size})
	}

	// Stage 2: Operation list — walk target index and classify each chunk.
	type moveOp struct {
		targetIdx int    // index into p.idx.Chunks
		srcStart  uint64 // byte offset in old file
		srcSize   uint64
		dstStart  uint64 // byte offset in target file
		dstSize   uint64
	}
	var moves []moveOp

	// Chunks whose ChunkID appears in the seed index at the same byte
	// offset and size as in the target index are already at the correct
	// position; they get inPlaceSeedSkip placements. This is a pure index
	// comparison — no file I/O. Unlike skipInPlace (created by
	// generateSkips after hashing), these carry validation info so
	// Validate() can verify the data. Chunks that exist in the seed at a
	// different offset become move operations, sourced from their first
	// location.
	for i, c := range p.idx.Chunks {
		if p.placements[i] != nil {
			continue // Already placed
		}

		sources, ok := srcOf[c.ID]
		if !ok {
			continue // Not in seed; will be filled by store or file seed later
		}

		// Sources are recorded in ascending start order, so the offset
		// check is a binary search.
		j := sort.Search(len(sources), func(k int) bool {
			return sources[k].start >= c.Start
		})
		if j < len(sources) && sources[j].start == c.Start && sources[j].size == c.Size {
			pl := &placement{source: &inPlaceSeedSkip{
				chunk: c,
				seed:  seed,
				file:  p.target,
			}}
			p.placements[i] = pl
			p.inPlaceOrder = append(p.inPlaceOrder, pl)
			continue
		}

		// Use the first available copy as the move source.
		src := sources[0]
		moves = append(moves, moveOp{
			targetIdx: i,
			srcStart:  src.start,
			srcSize:   src.size,
			dstStart:  c.Start,
			dstSize:   c.Size,
		})
	}

	if len(moves) == 0 {
		return
	}

	// Stage 3: Dependency graph — edge from i to j when move i's source
	// overlaps move j's destination (i must read before j writes).
	n := len(moves)
	succ := make([][]int, n)

	// Build a sorted index of moves by destination start for O(n log n) overlap search.
	sortedByDst := make([]int, n)
	for i := range sortedByDst {
		sortedByDst[i] = i
	}
	sort.Slice(sortedByDst, func(a, b int) bool {
		return moves[sortedByDst[a]].dstStart < moves[sortedByDst[b]].dstStart
	})

	for i := range moves {
		srcEnd := moves[i].srcStart + moves[i].srcSize
		// First move whose dstStart+dstSize > srcStart
		lo := sort.Search(len(sortedByDst), func(k int) bool {
			m := moves[sortedByDst[k]]
			return m.dstStart+m.dstSize > moves[i].srcStart
		})
		for k := lo; k < len(sortedByDst); k++ {
			j := sortedByDst[k]
			if moves[j].dstStart >= srcEnd {
				break
			}
			if i != j {
				succ[i] = append(succ[i], j)
			}
		}
	}

	// Stage 4: Tarjan's SCC + linearization
	sccs := tarjanSCC(n, succ)
	slices.Reverse(sccs) // topological order

	// Stable-sort independent SCCs by minimum target index so the
	// output order is deterministic and follows the target layout.
	minTarget := func(scc []int) int {
		m := moves[scc[0]].targetIdx
		for _, i := range scc[1:] {
			m = min(m, moves[i].targetIdx)
		}
		return m
	}
	slices.SortStableFunc(sccs, func(a, b []int) int {
		return minTarget(a) - minTarget(b)
	})

	for _, scc := range sccs {
		if len(scc) == 1 {
			// Non-cyclic: single placement.
			m := moves[scc[0]]
			pl := &placement{source: &inPlaceCopy{
				srcOffset: m.srcStart,
				srcSize:   m.srcSize,
				dstOffset: m.dstStart,
				dstSize:   m.dstSize,
			}}
			p.placements[m.targetIdx] = pl
			p.inPlaceOrder = append(p.inPlaceOrder, pl)
			continue
		}

		// Cycle: pick the member with smallest srcSize as buffer-break.
		bufIdx := scc[0]
		for _, i := range scc[1:] {
			if moves[i].srcSize < moves[bufIdx].srcSize {
				bufIdx = i
			}
		}

		// Remove bufIdx's outgoing edges and topologically sort the
		// remaining cycle members.
		localSucc := make(map[int][]int, len(scc))
		localInDeg := make(map[int]int, len(scc))
		for _, i := range scc {
			if i == bufIdx {
				continue // exclude buffer-break from topo sort
			}
			localInDeg[i] = 0
		}
		for _, i := range scc {
			if i == bufIdx {
				continue
			}
			for _, j := range succ[i] {
				// Only consider edges within this SCC (excluding bufIdx).
				if _, ok := localInDeg[j]; ok {
					localSucc[i] = append(localSucc[i], j)
					localInDeg[j]++
				}
			}
		}

		// Kahn's algorithm for topological sort within the cycle.
		var queue []int
		for _, i := range scc {
			if i == bufIdx {
				continue
			}
			if localInDeg[i] == 0 {
				queue = append(queue, i)
			}
		}
		var order []int
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			order = append(order, cur)
			for _, j := range localSucc[cur] {
				localInDeg[j]--
				if localInDeg[j] == 0 {
					queue = append(queue, j)
				}
			}
		}

		// Build the inPlaceCopy sources. The first element in order
		// gets preBuffers pointing to the buffer-break target. The
		// buffer-break target writes from writeBuf.
		bufMove := moves[bufIdx]
		bufCopy := &inPlaceCopy{
			srcOffset: bufMove.srcStart,
			srcSize:   bufMove.srcSize,
			dstOffset: bufMove.dstStart,
			dstSize:   bufMove.dstSize,
		}

		// Create placements in order, with the first one pre-buffering
		// the cycle-break target.
		var prevIdx int
		for k, i := range order {
			m := moves[i]
			ipc := &inPlaceCopy{
				srcOffset: m.srcStart,
				srcSize:   m.srcSize,
				dstOffset: m.dstStart,
				dstSize:   m.dstSize,
			}
			if k == 0 {
				ipc.preBuffers = []*inPlaceCopy{bufCopy}
			}
			pl := &placement{source: ipc}
			p.placements[m.targetIdx] = pl
			p.inPlaceOrder = append(p.inPlaceOrder, pl)

			// Record ordering dependencies between consecutive cycle members.
			if k > 0 {
				p.inPlaceDeps = append(p.inPlaceDeps, inPlaceDep{
					from: moves[prevIdx].targetIdx,
					to:   m.targetIdx,
				})
			}
			prevIdx = i
		}

		// The buffer-break target is placed last and depends on the
		// last member in order.
		bufPl := &placement{source: bufCopy}
		p.placements[bufMove.targetIdx] = bufPl
		p.inPlaceOrder = append(p.inPlaceOrder, bufPl)
		if len(order) > 0 {
			p.inPlaceDeps = append(p.inPlaceDeps, inPlaceDep{
				from: moves[order[len(order)-1]].targetIdx,
				to:   bufMove.targetIdx,
			})
		}
	}

	// Stage 5: Populate inPlaceReads — for each move, find all target
	// chunks whose byte range overlaps the move's source range and
	// record the dependency so subsequent writes wait for the read.
	// Only record dependencies for positions not yet placed (nil).
	// Non-nil positions at this point are all in-place sources whose
	// ordering is already handled by inPlaceDeps above.
	for _, m := range moves {
		srcEnd := m.srcStart + m.srcSize
		pl := p.placements[m.targetIdx]
		// Binary search for first chunk where Start+Size > srcStart.
		lo := sort.Search(len(p.idx.Chunks), func(j int) bool {
			return p.idx.Chunks[j].Start+p.idx.Chunks[j].Size > m.srcStart
		})
		for j := lo; j < len(p.idx.Chunks); j++ {
			if p.idx.Chunks[j].Start >= srcEnd {
				break
			}
			if p.placements[j] != nil {
				continue
			}
			p.inPlaceReads[j] = pl
		}
	}
}

// tarjanSCC finds all strongly connected components of a directed graph.
// adj[v] lists the successors of node v. Returns SCCs in reverse
// topological order (sinks first).
func tarjanSCC(n int, adj [][]int) [][]int {
	index := make([]int, n)
	lowlink := make([]int, n)
	onStack := make([]bool, n)
	for i := range index {
		index[i] = -1
	}

	var (
		stack []int
		sccs  [][]int
		idx   int
	)

	var visit func(v int)
	visit = func(v int) {
		index[v] = idx
		lowlink[v] = idx
		idx++
		stack = append(stack, v)
		onStack[v] = true

		for _, w := range adj[v] {
			if index[w] == -1 {
				visit(w)
				lowlink[v] = min(lowlink[v], lowlink[w])
			} else if onStack[w] {
				lowlink[v] = min(lowlink[v], index[w])
			}
		}

		if lowlink[v] == index[v] {
			var scc []int
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				scc = append(scc, w)
				if w == v {
					break
				}
			}
			sccs = append(sccs, scc)
		}
	}

	for v := range n {
		if index[v] == -1 {
			visit(v)
		}
	}
	return sccs
}
