## desync chop

Read chunks from a file according to an index

### Synopsis

Reads the index and extracts all referenced chunks from the file into a store,
local or remote.

Does not modify the input file or index in any way. It's used to populate a chunk
store by chopping up a file according to an existing index. To exclude chunks that
are known to exist in the target store already, use --ignore <index> which will
skip any chunks from the given index. The same can be achieved by providing the
chunks in their ASCII representation in a text file with --ignore-chunks <file>.

Use '-' to read the index from STDIN.

```
desync chop <index> <file> [flags]
```

### Examples

```
  desync chop -s sftp://192.168.1.1/store file.caibx largefile.bin
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
  -h, --help                                 help for chop
      --ignore strings                       index(es) with chunks to be excluded
      --ignore-chunks strings                text file with chunk IDs to be excluded
  -s, --store string                         target store
  -t, --trust-insecure                       trust invalid certificates
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

