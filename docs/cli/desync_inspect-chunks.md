## desync inspect-chunks

Inspect chunks from an index and an optional local store

### Synopsis

Prints a detailed JSON with information about chunks stored in an index file.
By using the '--store' option to provide a local store, the generated JSON will include, if
available, the chunks compressed size info from that particular store.

```
desync inspect-chunks <index> [<output>] [flags]
```

### Examples

```
  desync inspect-chunks file.caibx
  desync inspect-chunks --store /mnt/store file.caibx inspect_result.json
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
  -h, --help                                 help for inspect-chunks
  -s, --store string                         local source store
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

