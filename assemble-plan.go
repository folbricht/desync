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

func PlanWithSeeds(seeds []Seed) PlanOption {
	return func(p *AssemblePlan) {
		p.seeds = seeds
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
	targetIsBlank bool

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
}

type assembleSeedSource interface {
	assembleSource
	Seed() Seed
	File() string
	Validate(file *os.File) error
}

type placement struct {
	source         assembleSource
	dependsOnStart int // index of another placement this one depends on
	dependsOnSize  int // number of sequential placements (from dependsOnStart) this depends on
}

// NewPlan creates a fully populated AssemblePlan.
func NewPlan(name string, idx Index, s Store, opts ...PlanOption) (*AssemblePlan, error) {
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

	ss, err := newSelfSeed(p.target, p.idx, p.concurrency)
	if err != nil {
		return nil, err
	}
	p.selfSeed = ss

	if err := p.generate(); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Close releases resources held by the plan.
func (p *AssemblePlan) Close() {
	if p.selfSeed != nil {
		p.selfSeed.Close()
	}
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

func (p *AssemblePlan) generate() error {
	// Mark chunks that are already correct in the target file so they can
	// be skipped during assembly.
	if !p.targetIsBlank {
		f, err := os.Open(p.target)
		if err == nil {
			var g errgroup.Group
			g.SetLimit(p.concurrency)
			for i, chunk := range p.idx.Chunks {
				g.Go(func() error {
					b := make([]byte, chunk.Size)
					if _, err := f.ReadAt(b, int64(chunk.Start)); err != nil {
						return nil
					}
					if Digest.Sum(b) == chunk.ID {
						p.placements[i] = &placement{source: &skipInPlace{
							start: chunk.Start,
							end:   chunk.Start + chunk.Size,
						}}
					}
					return nil
				})
			}
			g.Wait()
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
	}

	// If we have an in-place seed, use it to find matches in the file
	// before anything gets overwritten by subsequent steps. We schedule
	// steps that re-arrange chunks that already exist in other places in
	// the target file before they get overwritten by subsequent steps like
	// copying from other seeds or the store.
	for _, seed := range p.seeds {
		inPlaceSeed, ok := seed.(*InPlaceSeed)
		if !ok {
			continue
		}

		p.generateInPlace(inPlaceSeed)
		break // There can only be one in-place seed
	}

	// Find all matches in file itself as they're written. As it's
	// populated, sections can be copied to other chunks. This involves
	// depending on earlier steps before chunks can be copied within the
	// file.
	for i := 0; i < len(p.idx.Chunks); i++ {
		if p.placements[i] != nil {
			continue // Already filled
		}

		_, _, start, n := p.selfSeed.LongestMatchFrom(p.idx.Chunks, i)
		if n < 1 {
			continue
		}

		// Repeat the same placement for all chunks in the sequence.
		// We dedup sequences later.
		pl := &placement{}

		// We can use up to n chunks from the seed, find out how much
		// we can actually use without overwriting any existing placements
		// in the list.
		var (
			to   = i
			size int
		)
		for range n {
			if p.placements[i] != nil {
				break
			}

			p.placements[i] = pl
			i++
			size++
		}
		i-- // compensate for the outer loop's i++

		// Update the step with the potentially adjusted length
		seedOffset := p.idx.Chunks[start].Start
		last := p.idx.Chunks[start+size-1]
		length := last.Start + last.Size - seedOffset
		offset := p.idx.Chunks[to].Start

		pl.source = p.selfSeed.GetSegment(seedOffset, offset, length)
		pl.dependsOnStart = start
		pl.dependsOnSize = size
	}

	// Check file seeds for matches in unfilled positions.
	for _, seed := range p.seeds {
		if _, ok := seed.(*InPlaceSeed); ok { // Skip the in-place seed, it's already handled
			continue
		}

		for i := 0; i < len(p.idx.Chunks); i++ {
			if p.placements[i] != nil {
				continue
			}

			seedOffset, _, _, n := seed.LongestMatchFrom(p.idx.Chunks, i)
			if n < 1 {
				continue
			}

			// Repeat the same placement for all chunks in the sequence.
			// We dedup sequences later.
			pl := &placement{}

			// We can use up to n chunks from the seed, find out how much
			// we can actually use without overwriting any existing placements
			// in the list.
			var (
				to   = i
				size int
			)
			for range n {
				if p.placements[i] != nil {
					break
				}
				p.placements[i] = pl
				i++
				size++
			}
			i-- // compensate for the outer loop's i++

			// Update the step with the potentially adjusted length
			offset := p.idx.Chunks[to].Start
			last := p.idx.Chunks[to+size-1]
			length := last.Start + last.Size - offset
			segment := seed.GetSegment(seedOffset, length)

			pl.source = &fileSeedSource{
				segment: segment,
				seed:    seed,
				srcFile: segment.FileName(),
				offset:  offset,
				length:  length,
				isBlank: p.targetIsBlank,
			}
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

	return nil
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
			stepsPerPlacement[pl].addDependency(stepsPerPlacement[p.placements[i]])
			stepsPerPlacement[p.placements[i]].addDependent(stepsPerPlacement[pl])
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
				step.addDependency(ipStep)
				ipStep.addDependent(step)
			}
		}
	}

	// Link in-place inter-operation dependencies from Tarjan
	// linearization. These ensure cycle members and cross-SCC
	// operations execute in the correct order.
	for _, dep := range p.inPlaceDeps {
		from := stepsPerPlacement[p.placements[dep.from]]
		to := stepsPerPlacement[p.placements[dep.to]]
		if from != to {
			to.addDependency(from)
			from.addDependent(to)
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

// generateInPlace processes an in-place seed to find chunks that exist at
// different offsets in the file and creates placements that rearrange them.
// It handles dependency cycles using Tarjan's SCC algorithm.
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

	// Collect skip placements from the initial scan that correspond to
	// seed chunks into inPlaceOrder so they precede other sources in
	// the step list.
	skipSeen := make(map[*placement]bool)
	for i, c := range p.idx.Chunks {
		pl := p.placements[i]
		if pl == nil || skipSeen[pl] {
			continue
		}
		if _, ok := pl.source.(*skipInPlace); !ok {
			continue
		}
		if _, ok := srcOf[c.ID]; !ok {
			continue
		}
		skipSeen[pl] = true
		p.inPlaceOrder = append(p.inPlaceOrder, pl)
	}

	for i, c := range p.idx.Chunks {
		if p.placements[i] != nil {
			continue // Already placed (e.g. skipInPlace from initial scan)
		}

		sources := srcOf[c.ID]
		if len(sources) == 0 {
			continue // Not in seed; will be filled by store or file seed later
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
	slices.SortStableFunc(sccs, func(a, b []int) int {
		minA := moves[a[0]].targetIdx
		for _, i := range a[1:] {
			if moves[i].targetIdx < minA {
				minA = moves[i].targetIdx
			}
		}
		minB := moves[b[0]].targetIdx
		for _, i := range b[1:] {
			if moves[i].targetIdx < minB {
				minB = moves[i].targetIdx
			}
		}
		return minA - minB
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
		localSucc := make([][]int, n)
		localInDeg := make(map[int]int)
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

// overlaps returns true if byte ranges [aStart, aStart+aSize) and
// [bStart, bStart+bSize) overlap.
func overlaps(aStart, aSize, bStart, bSize uint64) bool {
	if aSize == 0 || bSize == 0 {
		return false
	}
	return aStart < bStart+bSize && bStart < aStart+aSize
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
