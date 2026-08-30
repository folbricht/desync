## desync cache

Read indexes and copy the referenced chunks

### Synopsis

Read chunk IDs from one or more index files (caibx or caidx) and copy the
referenced chunks from the source store(s) into the target store given with
-c, without assembling any blob on disk. This can be used to pre-populate a
cache, or to replicate the chunks referenced by indexes into another store.
Use '-' to read (a single) index from STDIN.

To exclude chunks that are known to exist in the target store already, use
--ignore <index> which will skip any chunks from the given index. The same can
be achieved by providing the chunks in their ASCII representation in a text
file with --ignore-chunks <file>.

```
desync cache <index> [<index>...] [flags]
```

### Examples

```
  desync cache -s http://192.168.1.1/ -c /path/to/local file.caibx
```

### Options

```
      --ca-cert string                       trust authorities in this file, instead of OS trust store
  -c, --cache string                         target store the chunks are copied to
  -r, --cache-repair                         replace invalid chunks in the cache from source (default true)
      --client-cert string                   path to client certificate for TLS authentication
      --client-key string                    path to client key for TLS authentication
  -n, --concurrency int                      number of concurrent goroutines (default 10)
  -e, --error-retry int                      number of times to retry in case of network error (default 3)
  -b, --error-retry-base-interval duration   initial retry delay, increases linearly with each subsequent attempt (default 500ms)
  -h, --help                                 help for cache
      --ignore strings                       index(es) with chunks to be excluded
      --ignore-chunks strings                text file with chunk IDs to be excluded
  -s, --store strings                        source store(s)
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

