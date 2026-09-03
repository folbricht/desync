# Configuration

For most use cases, the tool's default configuration is sufficient. A config file at `$HOME/.config/desync/config.json` allows customization of timeouts, error retry behavior, or credentials that can't be set via command-line options or environment variables. All values have sensible defaults. Only add configuration for values that differ from the defaults.

To view the current configuration, use `desync config`. If no config file is present, this shows the defaults. To create a config file, use `desync config -w` to write the current configuration, then edit the file.

## Dynamic Store Configuration

Some long-running processes, namely `chunk-server` and `mount-index`, may require reconfiguration without restart. This can be achieved by starting them with the `--store-file` option which provides the arguments normally passed via `--store` and `--cache` from a JSON file instead. A SIGHUP to the process will trigger a reload of the configuration and replace the stores internally without restart. This can be done under load. If the configuration is found to be invalid, an error is printed to STDERR and the reload is ignored.

```json
{
  "stores": [
    "/path/to/store1",
    "/path/to/store2"
  ],
  "cache": "/path/to/cache"
}
```

This can be combined with store failover by providing the same syntax as used in the command-line, for example `{"stores":["/path/to/main|/path/to/backup"]}`. See [Server Examples](cookbook.md#server-examples) for details.

## Configuration Reference

- **`s3-credentials`** — Credentials for S3 stores. The key must be the URL scheme and host used for the store, excluding the path, but including the port if used in the store URL. Keys can contain glob patterns (`*`, `?`, `[…]`). See [filepath.Match](https://pkg.go.dev/path/filepath#Match) for wildcard details. Standard [AWS credentials files](https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html) are also supported.

- **`oci-credentials`** — Credentials for OCI registry stores. The key must be the store URL, e.g. `oci+https://ghcr.io/myuser/repo`, the same format as `store-options` keys. Keys can contain glob patterns. If no entry matches and no credentials are set in the environment, the Docker credential store is used, so registries logged into with `docker login` or `oras login` are picked up automatically.

- **`store-options`** — Per-store customization of compression, timeouts, retry behavior, and keys. Not all options apply to every store type. The store location in the command line must match the key exactly for options to apply. Glob patterns are also supported; a config file where more than one key matches a single store is considered invalid.

  | Option | Description | Default |
  | --- | --- | --- |
  | `timeout` | Time limit for chunk read/write in nanoseconds. Negative = infinite. Applies to HTTP(S), S3 and OCI stores. | 1 minute |
  | `n` | Number of concurrent requests made to this store. The `--concurrency` option takes precedence when it is given. | 10 |
  | `error-retry` | Number of times to retry failed chunk requests. | 0 |
  | `error-retry-base-interval` | Nanoseconds to wait before first retry. Attempt N waits N times this interval. | 0 |
  | `client-cert` | Certificate file for mutual SSL. | — |
  | `client-key` | Key file for mutual SSL. | — |
  | `ca-cert` | Certificate file containing trusted certs or CAs. | — |
  | `trust-insecure` | Trust any certificate presented by the server. | false |
  | `skip-verify` | Disable data integrity verification on read. Only recommended when chaining stores with `chunk-server` where the final consumer still verifies. Chunk validation is also what detects renamed or swapped chunks in encrypted stores, see [Chunk Encryption](encryption.md). | false |
  | `uncompressed` | Read and write uncompressed chunks. Both formats can coexist in the same store. | false |
  | `http-auth` | Value of the `Authorization` header in HTTP requests, e.g. `"Bearer <token>"` or `"Basic dXNlcjpwYXNzd29yZAo="`. | — |
  | `http-cookie` | Value of the `Cookie` header in HTTP requests, e.g. `"name=value; name2=value2"`. | — |
  | `encryption` | Set to `true` to encrypt chunks in the store. See [Chunk Encryption](encryption.md). | false |
  | `encryption-key` | Hex-encoded 256-bit encryption key, e.g. generated with `openssl rand -hex 32`. Can also be provided via the `DESYNC_ENCRYPTION_KEY` environment variable. | — |
  | `encryption-algorithm` | Encryption algorithm, `xchacha20-poly1305` or `aes-256-gcm`. | `xchacha20-poly1305` |


## Example Config

### JSON config file

```json
{
  "s3-credentials": {
       "http://localhost": {
           "access-key": "MYACCESSKEY",
           "secret-key": "MYSECRETKEY"
       },
       "https://127.0.0.1:9000": {
           "aws-credentials-file": "/Users/user/.aws/credentials"
       },
       "https://127.0.0.1:8000": {
           "aws-credentials-file": "/Users/user/.aws/credentials",
           "aws-profile": "profile_static"
       },
       "https://s3.us-west-2.amazonaws.com": {
           "aws-credentials-file": "/Users/user/.aws/credentials",
           "aws-region": "us-west-2",
           "aws-profile": "profile_refreshable"
       }
  },
  "oci-credentials": {
    "oci+https://ghcr.io/myuser/repo": {
      "username": "myuser",
      "secret": "MYSECRET"
    }
  },
  "store-options": {
    "https://192.168.1.1/store": {
      "client-cert": "/path/to/crt",
      "client-key": "/path/to/key",
      "error-retry": 1
    },
    "https://10.0.0.1/": {
      "http-auth": "Bearer abcabcabc"
    },
    "https://example.com/*/*/": {
      "http-auth": "Bearer dXNlcjpwYXNzd29yZA=="
    },
    "https://cdn.example.com/": {
      "http-cookie": "PHPSESSID=298zf09hf012fh2; csrftoken=u32t4o3tb3gg43"
    },
    "/path/to/local/cache": {
      "uncompressed": true
    },
    "/path/to/encrypted/store": {
      "encryption": true,
      "encryption-algorithm": "xchacha20-poly1305",
      "encryption-key": "d69371915dbc4b1f0ec55e2a80f0e089accf4b32e0f8dc0e29fed7f0f30d1b26"
    }
  }
}
```

### AWS credentials file

```ini
[default]
aws_access_key_id = DEFAULT_PROFILE_KEY
aws_secret_access_key = DEFAULT_PROFILE_SECRET

[profile_static]
aws_access_key_id = OTHERACCESSKEY
aws_secret_access_key = OTHERSECRETKEY

[profile_refreshable]
aws_access_key_id = PROFILE_REFRESHABLE_KEY
aws_secret_access_key = PROFILE_REFRESHABLE_SECRET
aws_session_token = PROFILE_REFRESHABLE_TOKEN
```
