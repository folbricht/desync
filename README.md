desync
======

This project doesn't seek to be a full reimplementation of upstream [casync](https://github.com/systemd/casync), but is a client that operates on casync chunk stores, index files, and archives to address specific use cases.

## Goals And Non-Goals

Among the distinguishing factors:

- Where the upstream command has chosen to optimize for storage efficiency (f/e, being able to use local files as "seeds", building temporary indexes into them), this command chooses to optimize for runtime performance (maintaining a local explicit chunk store, avoiding the need to reindex) at cost to storage efficiency.
- Where the upstream command has chosen to take full advantage of Linux platform features, this client chooses to implement a minimum featureset and, while high-value platform-specific features (such as support for btrfs reflinks into a decompressed local chunk cache) might be added in the future, the ability to build without them on other platforms will be maintained.

- SHA512/256 is currently the only supported hash function.
- Only chunk store using zstd compression are supported at this point.
- Supports local stores as well as remote stores over SSH and HTTP
- Drop-in replacement for casync on SSH servers when serving chunks read-only
- Support for catar files exists, but ignores XAttr, SELinux, ACLs and FCAPs that may be present in exising catar files and those won't be present when creating a new catar with the `tar command`
- Supports chunking with the same algorithm used by casync (see `make` command). Results are identical to what casync produces, same chunks and index files, but with significantly better performance.
- While casync supports very small min chunk sizes, optimizations in desync require min chunk sizes larger than the window size of the rolling hash used (currently 48 bytes). The tool's default chunk sizes match the defaults used in casync, min 16k, avg 64k, max 256k.

### Subcommands
- `extract`      - build a blob from an index file
- `verify`       - verify the integrity of a local store
- `list-chunks`  - list all chunk IDs contained in an index file
- `cache`        - populate a cache from index files without writing to a blob
- `chop`         - split a blob according to an existing caibx and store the chunks in a local store
- `pull`         - serve chunks using the casync protocol over stdin/stdout. Set `CASYNC_REMOTE_PATH=desync` on the client to use it.
- `tar`          - pack a catar file
- `untar`        - unpack a catar file
- `prune`        - remove unreferenced chunks from a local store. Use with caution, can lead to data loss.
- `chunk-server` - start a chunk server that serves chunks via HTTP
- `make`         - split a blob into chunks and create an index file

### Options (not all apply to all commands)
- `-s <store>` Location of the chunk store, can be local directory or a URL like ssh://hostname/path/to/store. Multiple stores can be specified, they'll be queried for chunks in the same order. The `verify` command only supports one, local store.
- `-c <store>` Location of a *local* chunk store to be used as cache. Needs to be writable.
- `-n <int>` Number of concurrent download jobs and ssh sessions to the chunk store.
- `-r` Repair a local cache by removing invalid chunks. Only valid for the `verify` command.
- `-y` Answer with `yes` when asked for confirmation. Only supported by the `prune` command.
- `-l` Listening address for the HTTP chunk server. Only supported by the `chunk-server` command.
- `-m` Specify the min/avg/max chunk sizes in kb. Only applicable to the `make` command. Defaults to 16:64:256 and for best results the min should be avg/4 and the max should be 4*avg.

### Environment variables
- `CASYNC_SSH_PATH` overrides the default "ssh" with a command to run when connecting to the remove chunk store
- `CASYNC_REMOTE_PATH` defines the command to run on the chunk store when using SSH, default "casync"

### Caching
The `-c <store>` option can be used to either specify an existing local store to act as cache or to build a new one. Whenever a cache is requested, it is first looked up in the local cache before routing the request to the main (likely remote store). Any chunks downloaded from the main store are added to the local store (cache). In addition, when a chunk is read from the cache, mtime of the chunk is updated to allow for basic garbage collection based on file age. The cache directory as well as the chunks in it are expected to be writable. If the cache contains an invalid chunk (checksum does not match the chunk ID), blob assembly will fail. Invalid chunks are not skipped or removed from the cache automatically.

### Examples:

Re-assemble somefile.tar using a remote chunk store and a blob index file.
```
desync extract -s ssh://192.168.1.1/path/to/casync.store/ -c /tmp/store somefile.tar.caibx somefile.tar
```

Use multiple stores, specify the local one first to improve performance.
```
desync extract -s /some/local/store -s ssh://192.168.1.1/path/to/casync.store/ somefile.tar.caibx somefile.tar
```

Mix and match remote stores and use a local cache store to improve performance.
```
desync extract \
       -s ssh://192.168.1.1/path/to/casync.store/ \
       -s http://192.168.1.2/casync.store/ \
       -c /path/to/cache \
       somefile.tar.caibx somefile.tar
```

Verify a local cache. Errors will be reported to STDOUT, since `-r` is not given, nothing invalid will be removed.
```
desync verify -s /some/local/store
```

Cache the chunks used in a couple of index files in a local store without actually writing the blob.
```
desync cache -s ssh://192.168.1.1/path/to/casync.store/ -c /local/cache somefile.tar.caibx other.file.caibx
```

List the chunks referenced in a caibx.
```
desync list-chunks somefile.tar.caibx
```

Chop an existing file according to an existing caibx and store the chunks in a local store. This can be used
to populate a local cache from a possibly large blob that already exists on the target system.
```
desync chop -s /some/local/store somefile.tar.caibx somefile.tar
```

Pack a directory tree into a catar file.
```
desync tar archive.catar /some/dir
```

Unpack a catar file.
```
desync untar archive.catar /some/dir
```

Prune a store to only contain chunks that are referenced in the provided index files. Possible data loss.
```
desync prune -s /some/local/store index1.caibx index2.caibx
```

Start a chunk server serving up a local store via port 80.
```
desync chunk-server -s /some/local/store
```

Start a chunk server on port 8080 acting as proxy for other remote HTTP and SSH stores and populate a local cache.
```
desync chunk-server -s http://192.168.1.1/ -s ssh://192.168.1.2/store -c cache -l :8080
```

Split a blob, store the chunks and create an index file.
```
desync make -s /some/local/store index.caibx /some/blob
```

## TODOs
- Pre-allocate the output file to avoid fragmentation when using extract command
- Check output file size, compare to expected size
- Support retrieval of index files from the chunk store
- Allow on-disk chunk cache to optionally be stored uncompressed, such that blocks can be directly reflinked (rather than copied) into files, when on a platform and filesystem where reflink support is available.
