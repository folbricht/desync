## desync verify-index

Verify an index matches a file

### Synopsis

Verifies an index file matches the content of a blob. Use '-' to read the index
from STDIN.

```
desync verify-index <index> <file> [flags]
```

### Examples

```
  desync verify-index sftp://192.168.1.1/myIndex.caibx largefile.bin
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
  -h, --help                                 help for verify-index
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

