package desync

import "os"

type planStep struct {
	source assembleSource

	// fills are the buffers of in-place moves whose source this step
	// overwrites. They are filled before the step runs.
	fills []*inPlaceBuffer

	// numChunks is the number of index chunks this step covers.
	numChunks int

	// Steps that depend on this one.
	dependents map[*planStep]struct{}

	// Steps that this one depends on.
	dependencies map[*planStep]struct{}
}

// link records that step "to" depends on step "from", setting both
// directions of the edge.
func link(from, to *planStep) {
	if from.dependents == nil {
		from.dependents = make(map[*planStep]struct{})
	}
	from.dependents[to] = struct{}{}
	if to.dependencies == nil {
		to.dependencies = make(map[*planStep]struct{})
	}
	to.dependencies[from] = struct{}{}
}

// ready returns true when all dependencies have been resolved.
func (n *planStep) ready() bool {
	return len(n.dependencies) == 0
}

// execute fills the buffers of the in-place moves whose source the step
// overwrites, then runs it.
func (n *planStep) execute(f *os.File) (copied uint64, cloned uint64, err error) {
	for _, b := range n.fills {
		if err := b.fill(f); err != nil {
			return 0, 0, err
		}
	}
	return n.source.Execute(f)
}

// releasesBuffer reports whether the step is an in-place move with a buffer,
// which frees the buffer's memory.
func (n *planStep) releasesBuffer() bool {
	c, ok := n.source.(*inPlaceCopy)
	return ok && c.buffer != nil
}

// fillsBuffer reports whether the step has to fill any buffers that aren't
// filled yet.
func (n *planStep) fillsBuffer() bool {
	for _, b := range n.fills {
		if b.pending() {
			return true
		}
	}
	return false
}

// stepQueue holds the steps that are ready to run. To keep the memory held by
// in-place buffers low, it hands out steps that release buffers first, and
// those that fill new buffers last. Steps are otherwise handed out in the
// order they became ready.
type stepQueue struct {
	releasing []*planStep
	other     []*planStep
	filling   []*planStep
}

func (q *stepQueue) push(s *planStep) {
	switch {
	case s.releasesBuffer():
		q.releasing = append(q.releasing, s)
	case s.fillsBuffer():
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
