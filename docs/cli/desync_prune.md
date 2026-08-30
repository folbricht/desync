## desync prune

Remove unreferenced chunks from a store

### Synopsis

Read chunk IDs from index files and delete all chunks from a store
that are not referenced in any of the provided index files. This is a
destructive operation; a confirmation prompt is shown before any chunks are
deleted unless --yes is used. Use '-' to read a single index from STDIN.

```
desync prune <index> [<index>...] [flags]
```

### Examples

```
  desync prune -s /path/to/local --yes file.caibx
  desync prune -s /path/to/local current.caibx previous.caibx
```

### Options

```
      --ca-cert string                       trust authorities in this file, instead of OS trust store
  -r, --cache-repair                         replace invalid chunks in the cache from source (default true)
      --client-cert string                   path to client certificate for TLS authentication
      --client-key string                    path to client key for TLS authentication
  -n, --concurrency int                      number of concurrent goroutines (default 10)
  -e, --error-retry int                      number of times to retry in case of network error (default 3)
  -b, --error-retry-base-interval duration   initial retry delay, increases linearly with each subsequent attempt (default 500ms)
  -h, --help                                 help for prune
  -s, --store string                         store to prune
  -t, --trust-insecure                       trust invalid certificates
  -y, --yes                                  do not ask for confirmation before deleting chunks
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

