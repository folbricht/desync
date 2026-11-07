package desync

import "strings"

// shellQuote wraps s in single quotes for safe inclusion in a POSIX shell
// command line, escaping any embedded single quotes. This prevents shell
// injection when interpolating untrusted values (such as a store URL path) into
// the remote command string passed to ssh.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
