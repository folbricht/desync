package desync

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/crypto/ssh/terminal"
	pb "gopkg.in/cheggaaa/pb.v1"
)

// NewProgressBar initializes a wrapper for a https://github.com/cheggaaa/pb
// progressbar that implements desync.ProgressBar
func NewProgressBar(prefix string) ProgressBar {
	if !terminal.IsTerminal(int(os.Stderr.Fd())) &&
		os.Getenv("DESYNC_PROGRESSBAR_ENABLED") == "" &&
		os.Getenv("DESYNC_ENABLE_PARSABLE_PROGRESS") == "" {
		return NullProgressBar{}
	}
	bar := pb.New(0).Prefix(prefix)
	bar.ShowCounters = false
	bar.Output = os.Stderr
	if os.Getenv("DESYNC_ENABLE_PARSABLE_PROGRESS") != "" {
		// This is likely going to a journal or redirected to a file, lower the
		// refresh rate from the default 200ms to a more manageable 500ms.
		bar.SetRefreshRate(time.Millisecond * 500)
		bar.ShowBar = false
		// Write every progress update in a separate line, instead of using
		// the default carriage returns.
		bar.Callback = func(s string) { fmt.Fprintln(os.Stderr, s) }
		bar.Output = nil
	}
	return DefaultProgressBar{bar}
}

// DefaultProgressBar wraps https://github.com/cheggaaa/pb and implements desync.ProgressBar
type DefaultProgressBar struct {
	*pb.ProgressBar
}

// SetTotal sets the upper bounds for the progress bar
func (p DefaultProgressBar) SetTotal(total int) {
	p.ProgressBar.SetTotal(total)
}

// Start displaying the progress bar
func (p DefaultProgressBar) Start() {
	p.ProgressBar.Start()
}

// Set the current value
func (p DefaultProgressBar) Set(current int) {
	p.ProgressBar.Set(current)
}

// Write the current state of the progressbar
func (p DefaultProgressBar) Write(b []byte) (n int, err error) {
	return p.ProgressBar.Write(b)
}
