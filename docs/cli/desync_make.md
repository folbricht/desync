## desync make

Chunk input file and create index

### Synopsis

Creates chunks from the input file and builds an index. If a chunk store is
provided with -s, such as a local directory or S3 store, it splits the input
file according to the index and stores the chunks. Use '-' to write the index
to STDOUT.

```
desync make <index> <file> [flags]
```

### Examples

```
  desync make -s /path/to/local file.caibx largefile.bin
  desync make -m 8:32:128 - largefile.bin > file.caibx
```

### Options

```
      --ca-cert string                       trust authorities in this file, instead of OS trust store
  -r, --cache-repair                         replace invalid chunks in the cache from source (default true)
  -m, --chunk-size string                    min:avg:max chunk size in kb (default "16:64:256")
      --client-cert string                   path to client certificate for TLS authentication
      --client-key string                    path to client key for TLS authentication
  -n, --concurrency int                      number of concurrent goroutines (default 10)
  -e, --error-retry int                      number of times to retry in case of network error (default 3)
  -b, --error-retry-base-interval duration   initial retry delay, increases linearly with each subsequent attempt (default 500ms)
  -h, --help                                 help for make
      --print-stats                          print chunking statistics to stderr when done
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

