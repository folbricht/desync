package desync

import "os"

type planStep struct {
	source assembleSource

	// fills are the stashes of in-place moves whose source this step
	// overwrites. They are filled before the step runs.
	fills []*stash

	// numChunks is the number of index chunks this step covers.
	numChunks int

	// Steps that depend on this one.
	dependents map[*planStep]struct{}

	// Steps that this one depends on.
	dependencies map[*planStep]struct{}
}

// before records that step "to" depends on s, setting both directions of the
// edge.
func (s *planStep) before(to *planStep) {
	if s.dependents == nil {
		s.dependents = make(map[*planStep]struct{})
	}
	s.dependents[to] = struct{}{}
	if to.dependencies == nil {
		to.dependencies = make(map[*planStep]struct{})
	}
	to.dependencies[s] = struct{}{}
}

// ready returns true when all dependencies have been resolved.
func (n *planStep) ready() bool {
	return len(n.dependencies) == 0
}

// execute fills the stashes of the in-place moves whose source the step
// overwrites, then runs it.
func (n *planStep) execute(f *os.File) (copied uint64, cloned uint64, err error) {
	for _, b := range n.fills {
		if err := b.fill(f); err != nil {
			return 0, 0, err
		}
	}
	return n.source.Execute(f)
}

// releasesStash reports whether the step is an in-place move with a stash,
// which frees the stash's memory.
func (n *planStep) releasesStash() bool {
	c, ok := n.source.(*inPlaceCopy)
	return ok && c.stash != nil
}

// fillsStash reports whether the step has to fill any stashes that aren't
// filled yet.
func (n *planStep) fillsStash() bool {
	for _, b := range n.fills {
		if b.pending() {
			return true
		}
	}
	return false
}

// stepQueue holds the steps that are ready to run. To keep the memory held by
// in-place stashes low, it hands out steps that release stashes first, and
// those that fill new stashes last. Steps are otherwise handed out in the
// order they became ready.
type stepQueue struct {
	releasing []*planStep
	other     []*planStep
	filling   []*planStep
}

func (q *stepQueue) push(s *planStep) {
	switch {
	case s.releasesStash():
		q.releasing = append(q.releasing, s)
	case s.fillsStash():
		q.filling = append(q.filling, s)
	default:
		q.other = append(q.other, s)
	}
}

// peek returns the next step without removing it, or nil if there is none.
func (q *stepQueue) peek() *planStep {
	for _, l := range []*[]*planStep{&q.releasing, &q.other, &q.filling} {
		if len(*l) > 0 {
			return (*l)[0]
		}
	}
	return nil
}

// pop removes the step returned by peek.
func (q *stepQueue) pop() {
	for _, l := range []*[]*planStep{&q.releasing, &q.other, &q.filling} {
		if len(*l) > 0 {
			*l = (*l)[1:]
			return
		}
	}
}
