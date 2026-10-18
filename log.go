package desync

import "log/slog"

// Log is the logger used by the library. It discards all output by default.
var Log = slog.New(slog.DiscardHandler)
