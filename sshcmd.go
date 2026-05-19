package desync

import (
	"fmt"
	"strings"
)

// validateSSHHost rejects an ssh destination that begins with a dash. Such a
// value (built from the store URL's user and host) would otherwise be parsed by
// ssh as a command-line option rather than a destination, allowing option
// injection such as -oProxyCommand=... and arbitrary local command execution
// (the same class of bug as Git CVE-2017-1000117 and Mercurial CVE-2017-1000116).
// Callers must reject the destination before passing it to ssh; the "--"
// argument added to the ssh invocation is a defense-in-depth measure that only
// some ssh implementations honor.
func validateSSHHost(host string) error {
	if strings.HasPrefix(host, "-") {
		return fmt.Errorf("invalid ssh destination %q: must not begin with '-'", host)
	}
	return nil
}

// shellQuote wraps s in single quotes for safe inclusion in a POSIX shell
// command line, escaping any embedded single quotes. This prevents shell
// injection when interpolating untrusted values (such as a store URL path) into
// the remote command string passed to ssh.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
