# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

desync is a Go library and CLI tool that re-implements [casync](https://github.com/systemd/casync) features for content-addressed binary distribution. It chunks large files using a rolling hash, deduplicates and compresses chunks (zstd), and distributes them via multiple store backends. Chunks are identified by SHA512/256 checksums (`ChunkID [32]byte`).

## Build and Test Commands

```bash
# Build the CLI binary (output into cmd/desync/)
go build -o cmd/desync/ ./cmd/desync

# Run all tests (library + CLI)
go test ./...

# Run library tests only
go test

# Run CLI tests only
go test ./cmd/desync

# Run a single test
go test -run TestFunctionName
go test ./cmd/desync -run TestFunctionName

# Install the binary
go install ./cmd/desync

# Format code (enforced by pre-commit)
go fmt ./...

# Tidy modules (enforced by pre-commit)
go mod tidy
```

CI runs `go test` and `go build ./cmd/desync` on ubuntu, windows, and macOS.

## Architecture

### Core Interfaces (store.go)

The system is built around composable store interfaces:

- **`Store`** — read-only: `GetChunk(ChunkID)`, `HasChunk(ChunkID)`
- **`WriteStore`** — adds `StoreChunk(*Chunk)`
- **`PruneStore`** — adds `Prune(ctx, ids)`
- **`IndexStore`** — read indexes: `GetIndexReader(name)`, `GetIndex(name)`
- **`IndexWriteStore`** — adds `StoreIndex(name, idx)`

### Store Implementations

Each backend implements the store interfaces:
- **LocalStore** (`local.go`) — filesystem-based chunk storage
- **RemoteHTTP** (`remotehttp.go`) — HTTP(S) with TLS/mutual auth support
- **S3Store** (`s3.go`) — S3-compatible storage (AWS, MinIO)
- **SFTPStore** (`sftp.go`) — SFTP over SSH
- **RemoteSSH** (`remotessh.go`) — casync protocol over SSH (read-only)
- **GCStore** (`gcs.go`) — Google Cloud Storage

### Store Composition

Stores are composed for routing, caching, and failover:
- **StoreRouter** (`storerouter.go`) — tries multiple stores in order
- **FailoverGroup** (`failover.go`) — failover with active store rotation
- **Cache** (`cache.go`) — local (fast) + remote (slow) with auto-caching
- **RepairableCache** — converts ChunkInvalid to ChunkMissing for self-repair

### Data Pipeline

**Chunking:** `Chunker` (`chunker.go`) uses a rolling hash (SipHash, 48-byte window) for content-defined chunking with configurable min/avg/max sizes (default 16KB/64KB/256KB).

**Converter pipeline** (`coverter.go`): Layered data transformations applied in order for writes, reverse for reads. Currently only compression (zstd via `Compressor`), but designed for adding encryption.

**Assembly:** `AssembleFile()` (`assemble.go`) reconstructs files from an index and chunk stores, supporting self-seeding and file seeds for efficient cloning with reflink support (Btrfs/XFS).

### Index Format

Index files (`.caibx`/`.caidx`) contain a table of `IndexChunk` entries mapping `ChunkID` to byte offsets and sizes. Parsed in `index.go`.

### Seeds

Seeds optimize extraction by reusing data from existing files:
- **FileSeed** (`fileseed.go`) — existing file + its index
- **SelfSeed** (`selfseed.go`) — file being written seeds later chunks
- **NullSeed** (`nullseed.go`) — all-zero chunk optimization

### CLI Structure (cmd/desync/)

Uses `cobra` for command framework. Key commands: `extract`, `make`, `tar`, `untar`, `chop`, `cache`, `verify`, `chunk-server`, `index-server`, `mount-index`, `prune`, `info`, `cat`.

Store factory in `cmd/desync/store.go` creates store instances from URL/path strings. Multiple stores are specified via CLI flags; failover groups use `|` separator.

### Configuration

Config file at `$HOME/.config/desync/config.json` for S3 credentials (per-endpoint with glob patterns), store options, and TLS settings. Long-running processes (chunk-server, mount-index) support dynamic store config via JSON files reloaded on SIGHUP.

Key environment variables: `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `CASYNC_SSH_PATH`, `DESYNC_HTTP_AUTH`.

## Key Patterns

- **Interface-driven composition** — small focused interfaces composed via routers/caches
- **Lazy evaluation** — chunks decompress only when plain data is accessed
- **Context-based cancellation** — goroutine lifecycle via `context.Context`
- **`t.Fatal()` restriction** — do not call `t.Fatal()`/`t.FailNow()` from non-main goroutines (see PR #291)
- **Avoid recompression** — if a chunk already has compressed form, don't recompress (see PR #289)

## Module

Module path: `github.com/folbricht/desync`, Go 1.24.0.
