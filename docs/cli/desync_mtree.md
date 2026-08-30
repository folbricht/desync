## desync mtree

Print the content of a catar, caidx or local directory in mtree format

### Synopsis

Reads an archive (catar), index (caidx) or local directory and prints
the content in mtree format.

The input is either a catar archive, a caidx index file (with -i and -s), or
a local directory.


```
desync mtree <catar|index|dir> [flags]
```

### Examples

```
  desync mtree docs.catar
  desync mtree -s http://192.168.1.1/ -c /path/to/local -i docs.caidx
  desync mtree /path/to/dir
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
  -h, --help                                 help for mtree
  -i, --index                                read index file (caidx), not catar
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

