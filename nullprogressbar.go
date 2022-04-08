package desync

// NullProgressBar wraps https://github.com/cheggaaa/pb and is used when we don't want to show a progressbar.
type NullProgressBar struct {
}

func (p NullProgressBar) Finish() {
	/// Nothing to do
}

func (p NullProgressBar) Increment() int {
	return 0
}

func (p NullProgressBar) Add(add int) int {
	return 0
}

func (p NullProgressBar) SetTotal(total int) {
	// Nothing to do
}

func (p NullProgressBar) Start() {
	// Nothing to do
}

func (p NullProgressBar) Set(current int) {
	// Nothing to do
}

func (p NullProgressBar) Write(b []byte) (n int, err error) {
	return 0, nil
}
