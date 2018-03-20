desync
======

This project re-implements many features of upstream [casync](https://github.com/systemd/casync) in [Go](https://golang.org/). It seeks to maintain compatibility with casync's data structures, protocols and types, such as chunk stores (castr), index files (caibx/caidx) and archives (catar) in order to function as a drop-in replacement in many use cases. It also tries to maintain support for platforms other than Linux and simplify build/installation. It consists of a [library](https://godoc.org/github.com/folbricht/desync) that implements the features, available for integration into any 3rd-party product as well as a command-line tool.

For support and discussion, see [![Gitter chat](https://badges.gitter.im/desync-casync-client/Lobby.png)](https://gitter.im/desync-casync-client/Lobby). Feature requests should be discussed there before filing, unless you're interested in doing the work to implement them yourself.

## Goals And Non-Goals

Among the distinguishing factors:
- Supported on MacOS, though there could be incompatibilities when exchanging catar-files between Linux and Mac for example since devices and filemodes differ slightly. \*BSD should work as well but hasn't been tested. Windows supports a limited subset of commands.
- Where the upstream command has chosen to optimize for storage efficiency (f/e, being able to use local files as "seeds", building temporary indexes into them), this command chooses to optimize for runtime performance (maintaining a local explicit chunk store, avoiding the need to reindex) at cost to storage efficiency.
- Where the upstream command has chosen to take full advantage of Linux platform features, this client chooses to implement a minimum featureset and, while high-value platform-specific features (such as support for btrfs reflinks into a decompressed local chunk cache) might be added in the future, the ability to build without them on other platforms will be maintained.
- SHA512/256 is currently the only supported hash function.
- Only chunk store using zstd compression are supported at this point.
- Supports local stores as well as remote stores (as client) over SSH and HTTP
- Built-in HTTP(S) chunk server that can proxy multiple local or remote stores and also supports caching.
- Drop-in replacement for casync on SSH servers when serving chunks read-only
- Support for catar files exists, but ignores XAttr, SELinux, ACLs and FCAPs that may be present in existing catar files and those won't be present when creating a new catar with the `tar` command
- Supports chunking with the same algorithm used by casync (see `make` command) but executed in parallel. Results are identical to what casync produces, same chunks and index files, but with significantly better performance. For example, up to 10x faster than casync if the chunks are already present in the store. If the chunks are new, it heavily depends on I/O, but it's still likely several times faster than casync.
- While casync supports very small min chunk sizes, optimizations in desync require min chunk sizes larger than the window size of the rolling hash used (currently 48 bytes). The tool's default chunk sizes match the defaults used in casync, min 16k, avg 64k, max 256k.
- Allows FUSE mounting of blob indexes
- S3 protocol support to access chunk stores for read operations and some some commands that write chunks

## Tool

The tool is provided for convenience. It uses the desync library and makes most features of it available in a consistent fashion. It does not match upsteam casync's syntax exactly, but tries to be similar at least.

### Installation

If GOPATH is set correctly, building the tool and installing it into `$GOPATH/bin` can be done with:
```
go get -u github.com/folbricht/desync/cmd/desync
```

### Subcommands
- `extract`      - build a blob from an index file
- `verify`       - verify the integrity of a local store
- `list-chunks`  - list all chunk IDs contained in an index file
- `cache`        - populate a cache from index files without writing to a blob
- `chop`         - split a blob according to an existing caibx and store the chunks in a local store
- `pull`         - serve chunks using the casync protocol over stdin/stdout. Set `CASYNC_REMOTE_PATH=desync` on the client to use it.
- `tar`          - pack a catar file, optionally chunk the catar and create an index file. Not available on Windows.
- `untar`        - unpack a catar file or an index referencing a catar. Not available on Windows.
- `prune`        - remove unreferenced chunks from a local store. Use with caution, can lead to data loss.
- `chunk-server` - start a chunk server that serves chunks via HTTP(S)
- `make`         - split a blob into chunks and create an index file
- `mount-index`  - FUSE mount a blob index. Will make the blob available as single file inside the mountpoint.

### Options (not all apply to all commands)
- `-s <store>` Location of the chunk store, can be local directory or a URL like ssh://hostname/path/to/store. Multiple stores can be specified, they'll be queried for chunks in the same order. The `chop`, `make`, and `tar` commands support updating chunk stores in S3, while `verify` and `prune` commands only operate on a local store.
- `-c <store>` Location of a *local* chunk store to be used as cache. Needs to be writable.
- `-n <int>` Number of concurrent download jobs and ssh sessions to the chunk store.
- `-r` Repair a local cache by removing invalid chunks. Only valid for the `verify` command.
- `-y` Answer with `yes` when asked for confirmation. Only supported by the `prune` command.
- `-l` Listening address for the HTTP chunk server. Can be used multiple times to run on more than one interface or more than one port. Only supported by the `chunk-server` command.
- `-m` Specify the min/avg/max chunk sizes in kb. Only applicable to the `make` command. Defaults to 16:64:256 and for best results the min should be avg/4 and the max should be 4*avg.
- `-i` When packing/unpacking an archive, don't create/read an archive file but instead store/read the chunks and use an index file (caidx) for the archive. Only applicable to `tar` and `untar` commands.
- `-t` Trust all certificates presented by HTTPS stores. Allows the use of self-signed certs when using a HTTPS chunk server.
- `-key` Key file in PEM format used for HTTPS `chunk-server` command. Also requires a certificate with `-cert`
- `-cert` Certificate file in PEM format used for HTTPS `chunk-server` command. Also requires `-key`.

### Environment variables
- `CASYNC_SSH_PATH` overrides the default "ssh" with a command to run when connecting to the remove chunk store
- `CASYNC_REMOTE_PATH` defines the command to run on the chunk store when using SSH, default "casync"
- `S3_ACCESS_KEY` and `S3_SECRET_KEY` are required when using S3 chunk stores

### Caching
The `-c <store>` option can be used to either specify an existing local store to act as cache or to build a new one. Whenever a cache is requested, it is first looked up in the local cache before routing the request to the main (likely remote store). Any chunks downloaded from the main store are added to the local store (cache). In addition, when a chunk is read from the cache, mtime of the chunk is updated to allow for basic garbage collection based on file age. The cache directory as well as the chunks in it are expected to be writable. If the cache contains an invalid chunk (checksum does not match the chunk ID), blob assembly will fail. Invalid chunks are not skipped or removed from the cache automatically. `verfiy -r` can be used to
evict bad chunks from a local store or cache.

### S3 chunk stores
desync supports reading from and writing to chunk stores that offer an S3 API, for example hosted in AWS or running on a local server. When using such a store, credentials are passed into the tool via environment variables `S3_ACCESS_KEY` and `S3_SECRET_KEY`. Care is required when building those URLs. Below a few examples:

#### AWS
This store is hosted in `eu-west-3` in AWS. `s3` signals that the S3 protocol is to be used, `https` should be specified for SSL connections. The first path element of the URL contains the bucket, `desync.bucket` in this example. Note, when using AWS, no port should be given in the URL!
```
s3+https://s3-eu-west-3.amazonaws.com/desync.bucket
```
It's possible to use prefixes (or "directories") to object names like so
```
s3+https://s3-eu-west-3.amazonaws.com/desync.bucket/prefix
```

#### Other service with S3 API
This is a store running on the local machine on port 9000 without SSL.
```
s3+http://127.0.0.1:9000/store
```

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

Pack a directory tree into an archive and chunk the archive, producing an index file.
```
desync tar -i -s /some/local/store archive.caidx /some/dir
```

Unpack a catar file.
```
desync untar archive.catar /some/dir
```

Unpack a directory tree using an index file referencing a chunked archive.
```
desync untar -i -s /some/local/store archive.caidx /some/dir
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

Start a TLS chunk server on port 443 acting as proxy for a remote chunk store in AWS with local cache.
```
S3_ACCESS_KEY=mykey S3_SECRET_KEY=mysecret desync chunk-server -s s3+https://s3-eu-west-3.amazonaws.com/desync.bucket/prefix -c cache -l 127.0.0.1:https -cert cert.pem -key key.pem
```

Split a blob, store the chunks and create an index file.
```
desync make -s /some/local/store index.caibx /some/blob
```

Split a blob, create an index file and store the chunks in an S3 bucket named `store`.
```
S3_ACCESS_KEY=mykey S3_SECRET_KEY=mysecret desync make -s s3+http://127.0.0.1:9000/store index.caibx /some/blob
```

FUSE mount an index file. This will make the indexed blob available as file underneath the mount point. The filename in the mount matches the name of the index with the extension removed. In this example `/some/mnt/` will contain one file `index`.
```
desync mount-index -s /some/local/store index.caibx /some/mnt
```

## Links
- casync - https://github.com/systemd/casync
- GoDoc for desync library - https://godoc.org/github.com/folbricht/desync

## TODOs
- Pre-allocate the output file to avoid fragmentation when using extract command
- Support retrieval of index files from the chunk store
- Allow on-disk chunk cache to optionally be stored uncompressed, such that blocks can be directly reflinked (rather than copied) into files, when on a platform and filesystem where reflink support is available.
