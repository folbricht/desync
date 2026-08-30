## desync verify

Read chunks in a store and verify their integrity

### Synopsis

Reads all chunks in a local store and verifies their integrity. If -r is used,
invalid chunks are deleted from the store.

```
desync verify [flags]
```

### Examples

```
  desync verify -s /path/to/store
  desync verify -s /path/to/store -r
```

### Options

```
  -n, --concurrency int   number of concurrent goroutines (default 10)
  -h, --help              help for verify
  -r, --repair            remove invalid chunks from the store
  -s, --store string      local store to verify
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

