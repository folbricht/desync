## desync untar

Extract a directory tree from a catar archive or index

### Synopsis

Extracts a directory tree from a catar file or an index. Use '-' to read the
index from STDIN.

The input is either a catar archive, or a caidx index file (with -i and -s).

By default, the catar archive is extracted to local disk. Using --output-format=gnu-tar,
the output can be set to GNU tar, either an archive or STDOUT with '-'.


```
desync untar <catar|index> <target> [flags]
```

### Examples

```
  desync untar docs.catar /tmp/documents
  desync untar -s http://192.168.1.1/ -c /path/to/local -i docs.caidx /tmp/documents
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
  -h, --help                                 help for untar
  -i, --index                                read index file (caidx), not catar
      --no-same-owner                        extract files as current user
      --no-same-permissions                  use current user's umask instead of what is in the archive
      --output-format string                 output format, 'disk' or 'gnu-tar' (default "disk")
  -s, --store strings                        source store(s), used with -i
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

