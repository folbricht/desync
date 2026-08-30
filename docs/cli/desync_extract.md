## desync extract

Read an index and build a blob from it

### Synopsis

Reads an index and builds a blob reading chunks from one or more chunk stores.
Use '-' to read the index from STDIN.

When using -k, the blob will be extracted in-place utilizing existing data and
the target file will not be deleted on error. This can be used to restart a
failed prior extraction without having to retrieve completed chunks again.

Multiple optional seed indexes can be given with --seed. The matching blob
should have the same name as the index file without the .caibx extension. If
the blob data is in another location, or has a different name, the path can be
set explicitly by appending a colon and the data path to the index path, as in
--seed <index>:<blob>. If several seed files and indexes are available, the
--seed-dir option can be used to automatically select all .caibx files in a
directory as seeds, expecting the matching blobs next to them.

If a seed is invalid, the extract operation is aborted by default. With
--skip-invalid-seeds, invalid seeds are discarded and the extraction continues
without them. Alternatively, --regenerate-invalid-seeds regenerates invalid
seed indexes in memory from the available data; neither data nor indexes are
changed on disk. Also, if a seed changes while processing, its invalid chunks
will be taken from the self seed, or the store, instead of aborting.

```
desync extract <index> <output> [flags]
```

### Examples

```
  desync extract -s http://192.168.1.1/ -c /path/to/local file.caibx largefile.bin
  desync extract -s /mnt/store -s /tmp/other/store file.tar.caibx file.tar
  desync extract -s /mnt/store --seed /mnt/v1.caibx v2.caibx v2.vmdk
  desync extract -s /mnt/store --seed /tmp/v1.caibx:/mnt/v1 v2.caibx v2.vmdk
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
  -h, --help                                 help for extract
  -k, --in-place                             extract the file in place and keep it in case of error
      --print-stats                          print extraction statistics to stdout when done
      --regenerate-invalid-seeds             regenerate seed indexes with invalid chunks
      --seed strings                         seed indexes
      --seed-dir strings                     directory with seed index files
      --skip-invalid-seeds                   skip seeds with invalid chunks
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

