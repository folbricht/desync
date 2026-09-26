//go:build !race

package desync

// raceEnabled reports whether the tests run with the race detector, which
// changes allocation behavior, e.g. sync.Pool drops items at random.
const raceEnabled = false
