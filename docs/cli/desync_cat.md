## desync cat

Stream a blob to stdout or a file-like object

### Synopsis

Stream a blob to stdout or a file-like object, optionally seeking and limiting
the read length.

Unlike extract, this supports output to FIFOs, named pipes, and other
non-seekable destinations.

This is inherently slower than extract as while multiple chunks can be
retrieved concurrently, writing to stdout cannot be parallelized.

Use '-' to read the index from STDIN.

```
desync cat <index> [<output>] [flags]
```

### Examples

```
  desync cat -s http://192.168.1.1/ file.caibx | grep something
  desync cat -s /path/to/store -o 1048576 -l 4096 file.caibx slice.bin
```

### Options

```
      --ca-cert string                       trust authorities in this file, instead of OS trust store
  -c, --cache string                         store to be used as cache
  -r, --cache-repair                         replace invalid chunks in the cache from source (default true)
      --client-cert string                   path to client certificate for TLS authentication
      --client-key string                    path to client key for TLS authentication
  -n, --concurrency int                      number of concurrent goroutines (default 10)
  -e, --error-retry int                      number of times to retry in case of network error (default 3)
  -b, --error-retry-base-interval duration   initial retry delay, increases linearly with each subsequent attempt (default 500ms)
  -h, --help                                 help for cat
  -l, --length int                           number of bytes to read (0 reads to the end)
  -o, --offset int                           offset in bytes to seek to before reading
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

