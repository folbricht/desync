# desync

Distribute large files and images by transferring only the parts that changed.

[![Go Reference](https://pkg.go.dev/badge/github.com/folbricht/desync.svg)](https://pkg.go.dev/github.com/folbricht/desync)
[![CI](https://github.com/folbricht/desync/actions/workflows/validate.yaml/badge.svg)](https://github.com/folbricht/desync/actions/workflows/validate.yaml)
[![License](https://img.shields.io/github/license/folbricht/desync)](LICENSE)

desync splits a file into content-defined chunks, stores each distinct chunk once, and writes an index listing the chunks that make up the file. A client that already holds an older version reuses the chunks it has and downloads only the ones it is missing. Chunks are ordinary files addressed by their hash, so a chunk store is any static file host: a web server, an S3 bucket, an OCI registry, or a directory on disk. Nothing on the server computes a delta, which means one published copy serves every client no matter which version they are coming from.

It implements the [casync](https://github.com/systemd/casync) format and interoperates with it — same index files, archives and chunk stores — with parallel chunking, more store backends and a Go library API. It is not a drop-in replacement on the command line: the options differ, and desync has commands casync doesn't.

## What it's for

- **A/B image updates for appliances and embedded devices.** The device has the running partition on disk. It seeds from that and pulls only the difference.
- **VM and container image distribution.** Publish each build to the same chunk store; unchanged parts of the filesystem are stored and transferred once across all of them.
- **Shipping large assets over a CDN.** Chunks are immutable, hash-named static files, which is the friendliest possible thing to cache.
- **CI artifact caching.** Deduplicate build outputs and toolchains between runs instead of re-fetching whole tarballs.

## What it saves

Two adjacent Debian point releases, exported as container root filesystems and published to the same chunk store. The client already has 12.7 on disk and uses it as a seed:

| | |
| --- | --- |
| Image size (12.8, uncompressed) | 125.2 MB |
| Full download, compressed | 50.4 MB |
| **Download with 12.7 as a seed** | **18.4 MB** |
| Chunks reused from 12.7 | 1121 of 1570 |
| Store holding both versions | 68.8 MB, against 100.8 MB for two independent copies |

```bash
mkdir store
desync make -s store v12.7.caibx v12.7.tar          # publish the old version
desync make -s store v12.8.caibx v12.8.tar          # publish the new one
desync inspect-chunks -s store v12.8.caibx > chunks.json
desync info --seed v12.7.caibx --chunks-info chunks.json -s store v12.8.caibx
```

How much you save depends entirely on how much actually changed between versions; a rebuild that shifts every file will save nothing. Measure your own data with `desync info` before committing to a design — that is what the command is for.

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

- **Parallel chunking** — byte-identical output to casync, several times faster given enough cores
- **Multiple store backends** — local, HTTP(S), S3/GCS, SFTP, SSH, OCI registries
- **Store chaining and caching** — combine stores with failover groups
- **Seeds and reflinks** — clone blocks from existing files on Btrfs/XFS
- **Built-in servers** — HTTP(S) chunk server and index server with proxy support
- **FUSE mounting** — mount blob indexes as files
- **Tar interoperability** — create/extract catar from standard tar streams
- **Chunk encryption** — optional store encryption with XChaCha20-Poly1305 or AES-256-GCM
- **Cross-platform** — Linux, macOS, Windows (subset), BSD

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

Install the latest release into `$HOME/go/bin`:

```text
go install -v github.com/folbricht/desync/cmd/desync@latest
```

Or build from source:

```text
git clone https://github.com/folbricht/desync.git
cd desync/cmd/desync && go install
```

## Quick Start

**Chunk a file** — split a blob into chunks and create an index:

```text
desync make -s /tmp/store index.caibx /path/to/largefile
```

**Extract a file** — reassemble a blob from its index and chunk store:

```text
desync extract -s /tmp/store index.caibx /path/to/largefile
```

**Extract with remote store and local cache** — fetch chunks over HTTP, cache locally:

```text
desync extract -s http://server/store -c /tmp/cache index.caibx /path/to/largefile
```

## Platform Support

| Platform | Status | Notes |
| --- | --- | --- |
| Linux | Full support | All features including FUSE, reflinks (Btrfs/XFS) |
| macOS | Supported | Minor incompatibilities possible when exchanging catar files with Linux (filemodes) |
| Windows | Partial | Subset of commands. No `mount-index`. Device entries unsupported in tar; `--no-same-owner` and `--no-same-permissions` ignored in `untar`. |
| FreeBSD | Supported | Tested in CI in a VM and release binaries are published, but it sees far less real-world use than Linux. |
| NetBSD | Supported | No `mount-index`. Extended attributes work only on filesystems that implement them, and are skipped elsewhere. Tested in CI in a VM and release binaries are published, but it sees far less real-world use than Linux. |
| OpenBSD | Supported | No `mount-index`, and extended attributes are silently dropped by `tar` and `untar`. Otherwise as NetBSD. |
| DragonFly | Supported | No `mount-index`. Extended attributes are silently dropped by `tar` and `untar`, and `untar` refuses device entries: `mknod` reports success there but doesn't record the device number, so the node is rejected rather than written with the wrong device. Otherwise as NetBSD. |

## Design Philosophy

- **Performance over storage efficiency** — where upstream casync optimizes for storage efficiency (e.g. using local files as seeds, building temporary indexes), desync optimizes for runtime performance (maintaining a local explicit chunk store, avoiding the need to reindex) at the cost of storage efficiency.
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
