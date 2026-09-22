# desync

Distribute large files and images by transferring only the parts that changed.

[![Go Reference](https://pkg.go.dev/badge/github.com/folbricht/desync.svg)](https://pkg.go.dev/github.com/folbricht/desync)
[![CI](https://github.com/folbricht/desync/actions/workflows/validate.yaml/badge.svg)](https://github.com/folbricht/desync/actions/workflows/validate.yaml)
[![License](https://img.shields.io/github/license/folbricht/desync)](LICENSE)

desync splits a file into content-defined chunks, stores each distinct chunk once, and writes an index listing the chunks that make up the file. A client that already holds an older version reuses the chunks it has and downloads only the ones it is missing. Chunks are ordinary files addressed by their hash, so a chunk store is any static file host: a web server, an S3 bucket, an OCI registry, or a directory on disk. Nothing on the server computes a delta, which means one published copy serves every client no matter which version they are coming from.

It implements the [casync](https://github.com/systemd/casync) format and interoperates with it — same index files, archives and chunk stores — with parallel chunking, more store backends and a Go library API. It is not a drop-in replacement on the command line: the options differ, and desync has commands casync doesn't.

## Where it fits

- **Updating devices in the field.** An appliance with A/B partitions seeds from the partition it is running and writes the new image straight to the other one, downloading only the chunks that aren't already on disk. An update interrupted by a dropped connection restarts without fetching the completed chunks again.
- **Serving a fleet from one copy.** Each client works out for itself which chunks it is missing, so one set of static files serves every client, whichever version it is starting from. Hash-named chunks never change, so a CDN can cache them indefinitely, and a `chunk-server` with a local cache at each site means a chunk crosses the WAN once rather than once per machine.
- **Using an image before it has downloaded.** `mount-index` exposes an index as a file over FUSE and fetches chunks as they are read, so a VM can boot from a disk image in a remote store. With `--cor-file` it fills a local sparse copy on the way.
- **Keeping every build.** Publish each build to the same store and it grows by what changed, not by another full image. `prune` removes the chunks only old versions still reference.
- **Hosting on infrastructure you don't control.** With [chunk encryption](docs/encryption.md), the bucket, registry or CDN holding the store sees only ciphertext.

## What an update costs

Six builds of the Debian 12 `genericcloud` disk image, 3.2 GB raw with about 1 GB of data, published to one store. Each row is a client updating to the 2026-09-09 build, seeding from the build it already has:

| Client has | Downloads | Chunks reused |
| --- | ---: | ---: |
| nothing | 292.2 MB | — |
| the build from 2 days earlier | 109.8 MB | 7,749 of 11,708 |
| 3 weeks earlier | 112.2 MB | 7,675 |
| 2 months earlier | 128.5 MB | 7,025 |
| 6 months earlier | 165.4 MB | 5,750 |
| 1 year earlier | 167.8 MB | 5,607 |

All six rows are served from the same store, which holds every build in 832 MB, against 1,753 MB for six separate copies. Download sizes are compressed chunks.

How much you save depends on the data: on how much changed between versions, and on whether the build keeps unchanged data byte-identical. A compressed or encrypted payload, for example, changes throughout when a single byte of its input does. Estimate it for your own images with `desync info` before committing to a design, as described in [Update size estimation](docs/cookbook.md#update-size-estimation).

## How it compares

| | desync | casync | rsync | zsync | OCI / ORAS |
| --- | --- | --- | --- | --- | --- |
| Reuses local data as a seed | yes | yes | yes, the destination file | yes | no |
| Server-side work per client | none | none | delta computed per transfer | none | none |
| Server requirement | any static file host | any static file host | rsync daemon or SSH | static host with range requests | registry |
| Deduplication across versions in the store | yes | yes | no | no | whole layers only |
| Directory trees | catar archives | catar archives | yes | no, single file | yes, as layers |
| FUSE mount of a published image | yes | yes | no | no | no |

rsync is the right tool when both ends are machines you control and the destination is a live filesystem. desync and casync are for publishing an artifact once to a dumb file host and letting many clients, at many different starting versions, update from it. [bita](https://github.com/oll3/bita) solves a similar problem in Rust with self-contained archives rather than a shared chunk store.

## Key Features

- **[Parallel chunking](docs/concepts.md#parallel-chunking)** — byte-identical output to casync, several times faster given enough cores
- **[Store backends](docs/stores.md)** — local, HTTP(S), [S3/GCS](docs/stores-s3.md), SFTP, SSH, [OCI registries](docs/stores-oci.md)
- **[Chaining and caching](docs/stores.md#chaining-and-caching)** — combine stores behind a local cache, with [failover groups](docs/stores.md#failover-groups)
- **[Seeds and reflinks](docs/concepts.md#seeds-and-reflinks)** — reuse local data, cloning blocks instead of copying them on Btrfs/XFS
- **[Built-in servers](docs/cookbook.md#server-examples)** — HTTP(S) chunk server and index server, usable as a caching proxy
- **[FUSE mounting](docs/cli/desync_mount-index.md)** — mount blob indexes as files
- **[Tar interoperability](docs/concepts.md#tar-interoperability)** — create and extract catar archives from standard tar streams
- **[Chunk encryption](docs/encryption.md)** — optional store encryption with XChaCha20-Poly1305 or AES-256-GCM
- **[Cross-platform](#platform-support)** — Linux, macOS, Windows (subset), BSD

## Documentation

| | |
| --- | --- |
| [Concepts](docs/concepts.md) | Chunking, seeds and reflinks, how the pieces fit together |
| [Store backends](docs/stores.md) | Capabilities, chaining, caching, failover groups |
| [S3 stores](docs/stores-s3.md) | Bucket URLs, addressing styles, credentials |
| [OCI registry stores](docs/stores-oci.md) | Chunks and indexes in a container registry |
| [Chunk encryption](docs/encryption.md) | Encrypting a store at rest |
| [Configuration](docs/configuration.md) | Config file, store options, dynamic reload |
| [CLI reference](docs/cli/) | Every command and flag |
| [Cookbook](docs/cookbook.md) | Worked examples for extraction, chunking, servers, archives |

## Installation

Download an archive for your platform from the [releases page](https://github.com/folbricht/desync/releases), unpack it, and put the `desync` binary somewhere on your `PATH`. Archives are published per operating system and architecture; see [Platform Support](#platform-support) for what is covered. Each release also carries a `checksums.txt` to verify the download against.

To build from the latest source instead, into `$HOME/go/bin`:

```text
go install -v github.com/folbricht/desync/cmd/desync@latest
```

Or from a clone, which is also what you want for working on desync:

```text
git clone https://github.com/folbricht/desync.git
cd desync/cmd/desync && go install
```

## Quick Start

**Publish two versions** — chunk each into the same store. The second adds only the chunks the first doesn't already have:

```text
mkdir -p /srv/store
desync make -s /srv/store image-v1.img.caibx image-v1.img
desync make -s /srv/store image-v2.img.caibx image-v2.img
```

Serve `/srv/store` from any web server, or with `desync chunk-server -s /srv/store -l :8080`.

**Install** — a client with nothing on disk fetches every chunk, keeping a local cache:

```text
mkdir -p /var/cache/desync
desync extract -s http://server:8080/ -c /var/cache/desync image-v1.img.caibx image-v1.img
```

**Update** — a client that has v1 uses it as a seed and downloads only the chunks v2 adds:

```text
desync extract -s http://server:8080/ --seed image-v1.img.caibx image-v2.img.caibx image-v2.img
```

The seed is the file next to its index, named without the `.caibx` extension; `--seed <index>:<file>` points somewhere else, such as a block device.

## Platform Support

| Platform | Status | Notes |
| --- | --- | --- |
| Linux | Full support | All features including FUSE, reflinks (Btrfs/XFS) |
| macOS | Supported | Minor incompatibilities possible when exchanging catar files with Linux (filemodes) |
| Windows | Partial | Subset of commands. No `mount-index`. Device entries unsupported in tar; `--no-same-owner`, `--no-same-permissions` and `--no-same-xattrs` ignored in `untar`, which never applies extended attributes there. |
| FreeBSD | Supported | Tested in CI in a VM and release binaries are published, but it sees far less real-world use than Linux. |
| NetBSD | Supported | No `mount-index`. Extended attributes work only on filesystems that implement them; `tar` skips them elsewhere and `untar` needs `--no-same-xattrs` there. Tested in CI in a VM and release binaries are published, but it sees far less real-world use than Linux. |
| OpenBSD | Supported | No `mount-index`. Extended attributes are unavailable: `tar` records none, and `untar` refuses an archive that carries them unless `--no-same-xattrs` is given. Otherwise as NetBSD. |
| DragonFly | Supported | No `mount-index`. Extended attributes as on OpenBSD. `untar` also refuses device entries: `mknod` reports success there but doesn't record the device number, so the node is rejected rather than written with the wrong device. Otherwise as NetBSD. |

## Differences from casync

- **Performance over storage efficiency** — casync chunks seed files at extraction time to find data it can reuse. desync takes the seed's existing index and keeps an explicit local chunk store as a cache, which avoids reindexing at the cost of disk space.
- **Cross-platform over platform-specific features** — where upstream casync takes full advantage of Linux platform features, desync implements a minimum feature set. High-value platform-specific features (such as Btrfs reflinks) are added while maintaining the ability to build on other platforms.
- **Hash functions** — both SHA512/256 and SHA256 are supported.
- **Compression** — only zstd compression and uncompressed stores are supported.
- **Serving casync clients** — desync can stand in for the casync binary on SSH servers for read-only chunk serving. Set `CASYNC_REMOTE_PATH=desync` on the client.
- **catar limitations** — SELinux and ACLs in existing catar files are ignored and won't be present in newly created catars. FCAPs are supported only as a verbatim copy of the `security.capability` XAttr.
- **FUSE mounting** — `mount-index` needs the FUSE bindings, which cover Linux, macOS and FreeBSD. Elsewhere the command exists but reports that it's unavailable.

## Links

- casync — [https://github.com/systemd/casync](https://github.com/systemd/casync)
- Go package documentation — [https://pkg.go.dev/github.com/folbricht/desync](https://pkg.go.dev/github.com/folbricht/desync)
- casync blog post — [http://0pointer.net/blog/casync-a-tool-for-distributing-file-system-images.html](http://0pointer.net/blog/casync-a-tool-for-distributing-file-system-images.html)
