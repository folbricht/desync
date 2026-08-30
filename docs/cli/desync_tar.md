## desync tar

Store a directory tree in a catar archive or index

### Synopsis

Encodes a directory tree into a catar archive or alternatively an index file
with the archive chunked into a store. Use '-' to write the output,
catar or index to STDOUT.

If the desired output is an index file (caidx) rather than a catar,
the -i option can be provided as well as a store. Using -i is equivalent
to first using the tar command to create a catar, then the make
command to chunk it into a store and produce an index file. With -i,
less disk space is required as no intermediary catar is created. There
can however be a difference in performance depending on file size.

By default, input is read from local disk. Using --input-format=tar,
the input can be a tar file or a stream from STDIN with '-'.


```
desync tar <catar|index> <source> [flags]
```

### Examples

```
  desync tar documents.catar $HOME/Documents
  desync tar -i -s /path/to/local pics.caidx $HOME/Pictures
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
  -h, --help                                 help for tar
  -i, --index                                create index file (caidx), not catar
      --input-format string                  input format, 'disk' or 'tar' (default "disk")
      --no-time                              set file timestamps to zero in the archive
  -x, --one-file-system                      don't cross filesystem boundaries
  -s, --store string                         target store (used with -i)
      --tar-add-root                         pretend that all tar elements have a common root directory
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

