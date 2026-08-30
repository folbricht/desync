## desync mount-index

FUSE mount an index file

### Synopsis

FUSE mount of the blob in the index file. It makes the (single) file in
the index available for read access. Use 'extract' if the goal is to
assemble the whole blob locally as that is more efficient. Use '-' to read
the index from STDIN.

When a Copy-on-Read file is given (with --cor-file), the file is used as a fast cache.
All chunks that are accessed by the mount are retrieved from the store and written into
the file as read operations are performed. Once all chunks have been accessed, the COR
file is fully populated. If --cor-state-save is given, a state file is written on
termination (and on SIGHUP) containing information about which chunks of the index have
or have not been read. A state file is only valid for one cache file and one index.
When re-using it with a different index, data corruption can occur.

This command supports the --store-file option which can be used to define the stores
and caches in a JSON file. The config can then be reloaded by sending a SIGHUP without
having to unmount and remount. This can be done under load as well.


```
desync mount-index <index> <mountpoint> [flags]
```

### Examples

```
  desync mount-index -s http://192.168.1.1/ file.caibx /mnt/blob
  desync mount-index -s /path/to/store --cor-file /var/tmp/blob.cor blob.caibx /mnt/blob

```

### Options

```
      --ca-cert string                       trust authorities in this file, instead of OS trust store
  -c, --cache string                         store to be used as cache
  -r, --cache-repair                         replace invalid chunks in the cache from source (default true)
      --client-cert string                   path to client certificate for TLS authentication
      --client-key string                    path to client key for TLS authentication
  -n, --concurrency int                      number of concurrent goroutines (default 10)
      --cor-file string                      use a copy-on-read sparse file as cache
      --cor-init-n int                       number of goroutines to use for initialization (with --cor-state-init) (default 10)
      --cor-state-init string                state file to initialize the copy-on-read cache from
      --cor-state-save string                file to store the copy-on-read state in on exit or SIGHUP
  -e, --error-retry int                      number of times to retry in case of network error (default 3)
  -b, --error-retry-base-interval duration   initial retry delay, increases linearly with each subsequent attempt (default 500ms)
  -h, --help                                 help for mount-index
  -s, --store strings                        source store(s)
      --store-file string                    read store arguments from a file, supports reload on SIGHUP
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

