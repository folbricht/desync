package desync

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// stepAccess is what a step does to the target file: the range it writes and
// the range it reads, which holds either the content from before assembly or
// what other steps write during it. It's worked out from the sources here
// rather than taken from their access() methods, which the plan orders the
// steps by, so that a wrong declaration shows up as a wrong order.
type stepAccess struct {
	writes, reads byteRange
	readsOld      bool
	buffer        *inPlaceBuffer
}

func accessOf(t *testing.T, step *planStep) stepAccess {
	t.Helper()
	switch s := step.source.(type) {
	case *skipInPlace:
		return stepAccess{} // the data is there already
	case *copyFromStore:
		return stepAccess{writes: byteRange{s.chunk.Start, s.chunk.Start + s.chunk.Size}}
	case *fileSeedSource:
		return stepAccess{writes: byteRange{s.offset, s.offset + s.length}}
	case *selfSeedSegment:
		src, size := s.segment.chunks[0].Start, s.segment.Size()
		return stepAccess{
			writes: byteRange{s.dstOffset, s.dstOffset + size},
			reads:  byteRange{src, src + size},
		}
	case *inPlaceCopy:
		src, size := s.srcOffset(), s.size()
		return stepAccess{
			writes:   byteRange{s.dstOffset, s.dstOffset + size},
			reads:    byteRange{src, src + size},
			readsOld: true,
			buffer:   s.buffer,
		}
	}
	t.Fatalf("unknown source %T, add what it reads and writes to accessOf", step.source)
	return stepAccess{}
}

// requirePlanOrder checks that the steps of a plan get the data they expect
// whichever order the dispatcher runs them in:
//   - the dependencies have no cycle, all steps can be handed out
//   - no two steps write the same bytes
//   - a step that reads what others write during assembly runs after them
//   - a step that reads the content from before assembly runs before any
//     other step overwrites it, or that step fills the reader's buffer first
func requirePlanOrder(t *testing.T, steps []*planStep, msgAndArgs ...any) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		msg := fmt.Sprintf(format, args...)
		if len(msgAndArgs) > 0 {
			msg += " for " + fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		require.Fail(t, msg)
	}
	index := make(map[*planStep]int, len(steps))
	for i, s := range steps {
		index[s] = i
	}

	// Sort the steps topologically. Every step comes out if there's no cycle.
	inDegree := make([]int, len(steps))
	var order []int
	for i, s := range steps {
		inDegree[i] = len(s.dependencies)
		if inDegree[i] == 0 {
			order = append(order, i)
		}
	}
	for k := 0; k < len(order); k++ {
		for d := range steps[order[k]].dependents {
			j := index[d]
			if inDegree[j]--; inDegree[j] == 0 {
				order = append(order, j)
			}
		}
	}
	if len(order) < len(steps) {
		var stuck []string
		for i, d := range inDegree {
			if d > 0 {
				stuck = append(stuck, steps[i].source.String())
			}
		}
		fail("dependency cycle, these steps can never run: %q", stuck)
	}

	// before[i][j] is set when step j always completes before step i starts
	before := make([][]bool, len(steps))
	for _, i := range order {
		before[i] = make([]bool, len(steps))
		for d := range steps[i].dependencies {
			j := index[d]
			before[i][j] = true
			for k, b := range before[j] {
				before[i][k] = before[i][k] || b
			}
		}
	}

	access := make([]stepAccess, len(steps))
	for i, s := range steps {
		access[i] = accessOf(t, s)
	}
	for i, a := range access {
		for j, b := range access {
			if i == j {
				continue
			}
			if i < j && a.writes.overlaps(b.writes) {
				fail("%q and %q both write the same bytes", steps[i].source, steps[j].source)
			}
			if !a.reads.overlaps(b.writes) {
				continue
			}
			switch {
			case !a.readsOld && !before[i][j]:
				fail("%q reads what %q writes, but can run first", steps[i].source, steps[j].source)
			case a.readsOld && !before[j][i] && (a.buffer == nil || !slices.Contains(steps[j].fills, a.buffer)):
				fail("%q can overwrite what %q reads before it's read or buffered", steps[j].source, steps[i].source)
			}
		}
	}
}

// Plans for random layouts, with an in-place seed, a file seed and data that
// repeats within the target, all order their steps correctly.
func TestPlanOrderRandomized(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	pool := make([]testChunk, 8)
	for i := range pool {
		pool[i] = filledChunk(64*(rng.IntN(8)+1), byte(i+1))
	}
	layout := func(maxLen int) []int {
		l := make([]int, rng.IntN(maxLen)+1)
		for i := range l {
			l[i] = rng.IntN(len(pool))
		}
		return l
	}

	dir := t.TempDir()
	for i := range 2000 {
		oldLayout, seedLayout, newLayout := layout(12), layout(6), layout(12)
		reflink := rng.IntN(2) == 0
		old := pickChunks(pool, oldLayout...)
		name := filepath.Join(dir, fmt.Sprintf("target%d", i))
		require.NoError(t, os.WriteFile(name, chunkContent(old...), 0644))
		inPlace, err := NewFileSeed(name, name, chunkIndex(old...))
		require.NoError(t, err)

		seedChunks := pickChunks(pool, seedLayout...)
		seedName := filepath.Join(dir, fmt.Sprintf("seed%d", i))
		require.NoError(t, os.WriteFile(seedName, chunkContent(seedChunks...), 0644))
		seed, err := NewFileSeed(name, seedName, chunkIndex(seedChunks...))
		require.NoError(t, err)
		inPlace.canReflink, seed.canReflink = reflink, reflink

		plan := newPlan(name, chunkIndex(pickChunks(pool, newLayout...)...), nil,
			planWithInPlaceSeed(inPlace), planWithSeeds([]Seed{seed}),
			planWithTargetIsBlank(false), planWithBlocksize(64))
		if reflink {
			plan.selfSeed.canReflink = true
			plan = plan.replan()
		}
		requirePlanOrder(t, plan.Steps(), "old %v, seed %v, new %v, reflink %v",
			oldLayout, seedLayout, newLayout, reflink)
	}
}
