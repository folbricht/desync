package desync

import (
	"iter"
	"maps"
)

type PlanStep struct {
	source assembleSource

	// numChunks is the number of index chunks this step covers.
	numChunks int

	// Steps that depend on this one.
	dependents stepSet

	// Steps that this one depends on.
	dependencies stepSet
}

// addDependent adds a step that depends on this one.
func (n *PlanStep) addDependent(other *PlanStep) {
	if n.dependents == nil {
		n.dependents = newStepSet()
	}
	n.dependents.add(other)
}

// addDependency adds a step that this one depends on.
func (n *PlanStep) addDependency(other *PlanStep) {
	if n.dependencies == nil {
		n.dependencies = newStepSet()
	}
	n.dependencies.add(other)
}

// ready returns true when all dependencies have been resolved.
func (n *PlanStep) ready() bool {
	return n.dependencies.len() == 0
}

type stepSet map[*PlanStep]struct{}

func newStepSet() stepSet {
	return make(stepSet)
}

func (s stepSet) add(n *PlanStep) {
	s[n] = struct{}{}
}

func (s stepSet) remove(n *PlanStep) {
	delete(s, n)
}

func (s stepSet) Each() iter.Seq[*PlanStep] {
	return maps.Keys(s)
}

func (s stepSet) len() int {
	return len(s)
}
