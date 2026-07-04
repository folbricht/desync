package desync

type PlanStep struct {
	source assembleSource

	// numChunks is the number of index chunks this step covers.
	numChunks int

	// Steps that depend on this one.
	dependents map[*PlanStep]struct{}

	// Steps that this one depends on.
	dependencies map[*PlanStep]struct{}
}

// link records that step "to" depends on step "from", setting both
// directions of the edge.
func link(from, to *PlanStep) {
	if from.dependents == nil {
		from.dependents = make(map[*PlanStep]struct{})
	}
	from.dependents[to] = struct{}{}
	if to.dependencies == nil {
		to.dependencies = make(map[*PlanStep]struct{})
	}
	to.dependencies[from] = struct{}{}
}

// ready returns true when all dependencies have been resolved.
func (n *PlanStep) ready() bool {
	return len(n.dependencies) == 0
}
