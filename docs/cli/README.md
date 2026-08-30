# CLI Reference

The CLI tool uses the desync library and makes most features available in a consistent fashion. It does not match upstream casync's syntax exactly, but tries to be similar.

A page per command, generated from the commands themselves, starting at [desync](desync.md). Those carry the full flag list with defaults; the tables below cover the options that need more explanation than their help text gives.

The generated pages are not edited by hand. Run `desync gendocs docs/cli` after changing a command or its flags, or CI will fail with the diff.

## Commands

### Chunking and Extraction

| Command | Description |
| --- | --- |
| `make` | Split a blob into chunks and create an index file |
| `extract` | Build a blob from an index file, optionally using seed indexes+blobs |
| `verify-index` | Verify that an index file matches a given blob |
| `mount-index` | FUSE mount a blob index as a single file |
| `cat` | Stream a blob to stdout or a file |
| `chunk` | Chunk input file and print chunk boundaries plus chunk IDs |

### Archives

| Command | Description |
| --- | --- |
| `tar` | Pack a catar file, optionally chunk and create an index |
| `untar` | Unpack a catar file or index referencing a catar |
| `mtree` | Print the content of a catar, caidx, or local directory in mtree format |

### Servers

| Command | Description |
| --- | --- |
| `chunk-server` | Start an HTTP(S) chunk server/store |
| `index-server` | Start an HTTP(S) index server/store |
| `pull` | Serve chunks using the casync protocol over stdin/stdout |

### Inspection

| Command | Description |
| --- | --- |
| `info` | Show information about an index file |
| `inspect-chunks` | Show detailed information about chunks in an index and optional local store |
| `list-chunks` | List all chunk IDs in an index file |

### Maintenance

| Command | Description |
| --- | --- |
| `verify` | Verify the integrity of a local store |
| `cache` | Populate a cache from index files without extracting |
| `chop` | Split a blob according to an existing index and store chunks |
| `prune` | Remove unreferenced chunks from a store (use with caution) |

### Utility

| Command | Description |
| --- | --- |
| `config` | Show or write the config file |
| `version` | Show the version, commit and build details. `--version` prints just the version |
| `manpage` | Generate manpages for desync |

## Common Options

Not all options apply to all commands.

**Global options:**

| Option | Description |
| --- | --- |
| `--config <file>` | Path to config file. Default: `$HOME/.config/desync/config.json`. |
| `--digest <algorithm>` | Digest algorithm: `sha512-256` (default) or `sha256`. |
| `--verbose` | Enable verbose/debug logging. |
| `--version` | Print the version and exit. See the `version` command for build details. |

**Store options:**

| Option | Description |
| --- | --- |
| `-s <store>` | Location of the chunk store, can be local directory or a URL like `ssh://hostname/path/to/store`. Multiple stores can be specified, they'll be queried in order. The `chop`, `make`, `tar` and `prune` commands support updating chunk stores in S3, while `verify` only operates on a local store. |
| `-c <store>` | Location of a chunk store to be used as cache. Needs to be writable. |
| `-n <int>` | Number of concurrent goroutines. Default: 10. |
| `-t` | Trust all certificates presented by HTTPS stores. Allows the use of self-signed certs. |
| `--ca-cert <file>` | Trust authorities in this file instead of the OS trust store. |
| `--client-cert <file>` | Client certificate for mutual TLS authentication. |
| `--client-key <file>` | Client key for mutual TLS authentication. |
| `-e` / `--error-retry <int>` | Number of times to retry on network error. |
| `-b` / `--error-retry-base-interval <duration>` | Initial retry delay; attempt N waits N times this interval. |

**Extract options:**

| Option | Description |
| --- | --- |
| `--seed <indexfile>` | Specifies a seed file and index for the `extract` command. The tool expects the matching file to have the same name as the index file, without the `.caibx` extension. |
| `--seed-dir <dir>` | Specifies a directory containing seed files and their indexes for `extract`. Each index file (`*.caibx`) needs a matching blob without the extension. |
| `-k` / `--in-place` | Keep partially assembled files in place when `extract` fails or is interrupted. Also use this option to write to block devices. |
| `--print-stats` | Print extraction statistics (`extract`) or chunking statistics (`make`) to stderr. |
| `--skip-invalid-seeds` | Skip seeds with invalid chunks instead of failing. |
| `--regenerate-invalid-seeds` | Regenerate seed indexes when invalid chunks are found. |

**Chunking and archive options:**

| Option | Description |
| --- | --- |
| `-m` | Specify the min/avg/max chunk sizes in KB. Only applicable to `make`. Defaults to 16:64:256. For best results: min = avg/4, max = 4*avg. |
| `-i` | When packing/unpacking an archive, don't create/read an archive file but instead use an index file (caidx). Only applicable to `tar` and `untar`. |
| `--input-format <format>` | Input format for `tar`: `disk` (default) or `tar`. |
| `--output-format <format>` | Output format for `untar`: `disk` (default) or `gnu-tar`. |
| `--ignore <indexfile>` | Index file(s) whose chunks should be skipped. Applies to `chop` and `cache`. |

**Server options:**

| Option | Description |
| --- | --- |
| `-l <address>` | Listening address for the HTTP chunk server. Can be used multiple times for more than one interface or port. |
| `-w` / `--writable` | Enable write support. Applies to `chunk-server` and `index-server`. |
| `-u` / `--uncompressed` | Serve uncompressed chunks. Applies to `chunk-server`. |
| `--store-file <file>` | Read store arguments from a JSON file; supports SIGHUP reload. Applies to `chunk-server` and `mount-index`. |
| `--key <file>` | Key file in PEM format for HTTPS `chunk-server` and `index-server`. Requires `--cert`. |
| `--cert <file>` | Certificate file in PEM format for HTTPS `chunk-server` and `index-server`. Requires `--key`. |
| `--mutual-tls` | Require a valid client certificate, verified against `--client-ca` (which is mandatory when this is set). Applies to `chunk-server` and `index-server`. |
| `--client-ca <file>` | Acceptable client certificate or CA for mutual TLS. Required when `--mutual-tls` is set; otherwise client certs would be verified against the system trust store. |
| `--authorization <value>` | Expected value of the Authorization header in client requests. |
| `--log <file>` | Request log file, or `-` for STDOUT. Applies to `chunk-server` and `index-server`. |

**Other options:**

| Option | Description |
| --- | --- |
| `-r` | Repair a local store by removing invalid chunks. Only valid for `verify`. |
| `-y` | Answer with `yes` when asked for confirmation. Only supported by `prune`. |
| `-f` / `--format <format>` | Output format for `info`: `json` (default) or `plain`. |


## Environment Variables

| Variable | Description |
| --- | --- |
| `CASYNC_SSH_PATH` | Overrides the default `ssh` command when connecting to remote SSH or SFTP chunk stores. |
| `CASYNC_REMOTE_PATH` | Defines the command to run on the chunk store when using SSH. Default: `casync`. |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_SESSION_TOKEN`, `S3_REGION` | S3 store credentials when using a single store. If `S3_ACCESS_KEY` and `S3_SECRET_KEY` are not defined, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` are also considered. These take precedence over config file values. |
| `DESYNC_OCI_USERNAME`, `DESYNC_OCI_PASSWORD` | OCI registry store credentials. These take precedence over config file values and the Docker credential store. |
| `DESYNC_PROGRESSBAR_ENABLED` | Enables the progress bar if set to any non-empty value. By default, the progress bar is only shown when STDERR is a terminal. |
| `DESYNC_ENABLE_PARSABLE_PROGRESS` | Prints operation name, completion percentage, and estimated remaining time to STDERR. Similar to the default progress bar but without the visual bar. |
| `DESYNC_HTTP_AUTH` | Sets the expected `Authorization` header value from clients when using `chunk-server` or `index-server`. Needs the full string including type and encoding, e.g. `"Basic dXNlcjpwYXNzd29yZAo="`. Command-line values take precedence. |
| `DESYNC_ENCRYPTION_KEY` | Hex-encoded 256-bit chunk encryption key. Used for stores that have `encryption` enabled but no `encryption-key` configured, and by `chunk-server` when encryption is enabled with `--encryption` but no `--encryption-key` is given. The variable alone never enables encryption. |
