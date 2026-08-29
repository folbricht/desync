package main

import (
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// Populated with -ldflags at release time, for example by goreleaser. When
// they are empty, which is the case for 'go install' and for local builds, the
// values come from the build information the toolchain embeds instead.
var (
	version string
	commit  string
	date    string
)

// buildInfo describes the running binary.
type buildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	Date      string `json:"date,omitempty"`
	GoVersion string `json:"go-version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

// currentBuild describes the running binary, preferring values injected at
// link time and falling back to what the toolchain embedded. A module
// installed with 'go install' carries its version, and a build from a checkout
// carries the revision and its timestamp, so in practice only a build from a
// source tarball has nothing to report.
func currentBuild() buildInfo {
	b := buildInfo{
		Version:   version,
		Commit:    commit,
		Date:      date,
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}

	// Whether the version was injected at link time rather than derived below.
	linked := b.Version != ""

	if bi, ok := debug.ReadBuildInfo(); ok {
		if b.Version == "" {
			b.Version = bi.Main.Version
		}
		var modified bool
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if b.Commit == "" {
					b.Commit = s.Value
				}
			case "vcs.time":
				if b.Date == "" {
					b.Date = s.Value
				}
			case "vcs.modified":
				modified = s.Value == "true"
			}
		}
		// Built from a tree with uncommitted changes, which is worth knowing
		// when the version is quoted in a bug report. Only needed for a linked
		// version, since the toolchain already marks a derived one.
		if modified && linked {
			b.Version += "+dirty"
		}
	}

	if b.Version == "" {
		b.Version = "unknown"
	}
	return b
}

type versionOptions struct {
	format string
}

func newVersionCommand() *cobra.Command {
	var opt versionOptions

	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show the desync version",
		Long: `Prints the version of desync along with the commit it was built from, the
build time, and the Go toolchain and platform used.`,
		Example: `  desync version
  desync version --format=json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVersion(opt)
		},
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&opt.format, "format", "f", "plain", "output format, plain or json")
	return cmd
}

func runVersion(opt versionOptions) error {
	b := currentBuild()
	switch opt.format {
	case "json":
		return printJSON(stdout, b)
	case "plain", "":
		fmt.Fprintf(stdout, "desync version %s\n", b.Version)
		if b.Commit != "" {
			fmt.Fprintf(stdout, "commit:  %s\n", b.Commit)
		}
		if b.Date != "" {
			fmt.Fprintf(stdout, "built:   %s\n", b.Date)
		}
		fmt.Fprintf(stdout, "go:      %s\n", b.GoVersion)
		fmt.Fprintf(stdout, "os/arch: %s/%s\n", b.OS, b.Arch)
		return nil
	default:
		return fmt.Errorf("invalid output format '%s'", opt.format)
	}
}
