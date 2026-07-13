package main

import (
	"github.com/spf13/cobra"
)

func newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "desync",
		Short: "Content-addressed binary distribution system",
		Long: `desync is a content-addressed binary distribution system. It chunks files
into reusable, compressed pieces kept in chunk stores, and reassembles them
efficiently using indexes, seeds and caches. It is compatible with casync
archives, indexes and stores.

Store locations, used with options like -s/--store and -c/--cache, can be:
  /path/to/store              local directory store
  http(s)://host/path/        chunk/index server (see chunk-server command)
  s3+http(s)://host/bucket    S3-compatible object store
  gs://bucket/prefix          Google Cloud Storage bucket
  sftp://user@host/path       SFTP store
  ssh://user@host/path        casync protocol over SSH (read-only)

Commands that accept multiple stores try them in the order given. Several
stores can also be combined into one failover group by separating them with
'|', for example -s "http://server1/store|http://server2/store".`,
	}
	cmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default $HOME/.config/desync/config.json)")
	cmd.PersistentFlags().StringVar(&digestAlgorithm, "digest", "sha512-256", "digest algorithm, sha512-256 or sha256")
	cmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "verbose mode")
	return cmd
}
