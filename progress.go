package desync

// ProgressBar allows clients to provide their own implementations of graphical
// progress visualizations. Optional, can be nil to disable this feature.
type ProgressBar interface {
	SetTotal(total int)
	Start()
	Finish()
	Increment() int
	Add(add int) int
	Set(current int)
}
