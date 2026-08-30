## desync chunk-server

Server for chunks over HTTP(S)

### Synopsis

Starts an HTTP chunk server that can be used as remote store. It supports
reading from multiple local or remote stores as well as a local cache. If
--cert and --key are provided, the server will serve over HTTPS. The -w option
enables writing to this store, but this is only allowed when just one upstream
chunk store is provided. The option --skip-verify-write disables hash validation
of chunks written to this server, avoiding the decompression step needed to
calculate checksums, to improve performance. If -u is used, only uncompressed
chunks are served (and accepted). If the upstream store serves compressed chunks,
everything will have to be decompressed server-side so it's better to also read
from uncompressed upstream stores. With --encryption, chunks are served (and
accepted) encrypted, regardless of how they are stored in the upstream store.
The key is read from the DESYNC_ENCRYPTION_KEY environment variable unless
--encryption-key is used. The environment variable on its own does not enable
encryption, one of the encryption flags is required.

While --concurrency does not limit the number of clients that can be served
concurrently, it does influence connection pools to remote upstream stores and
needs to be chosen carefully if the server is under high load.

This command supports the --store-file option which can be used to define the stores
and caches in a JSON file. The config can then be reloaded by sending a SIGHUP without
needing to restart the server. This can be done under load as well.


```
desync chunk-server [flags]
```

### Examples

```
  desync chunk-server -s sftp://192.168.1.1/store -c /path/to/cache -l :8080
  desync chunk-server -s /path/to/store -w -l :8080
  desync chunk-server -s /path/to/store --cert cert.pem --key key.pem -l :8443
```

### Options

```
      --authorization string                 expected value of the authorization header in requests
      --ca-cert string                       trust authorities in this file, instead of OS trust store
  -c, --cache string                         store to be used as cache
  -r, --cache-repair                         replace invalid chunks in the cache from source (default true)
      --cert string                          cert file in PEM format, requires --key
      --client-ca string                     acceptable client certificate or CA
      --client-cert string                   path to client certificate for TLS authentication
      --client-key string                    path to client key for TLS authentication
  -n, --concurrency int                      number of concurrent goroutines (default 10)
      --encryption                           serve chunks encrypted, expects the key in $DESYNC_ENCRYPTION_KEY unless --encryption-key is given
      --encryption-algorithm string          encryption algorithm, xchacha20-poly1305 (default) or aes-256-gcm, implies --encryption
      --encryption-key string                serve chunks encrypted with this hex-encoded 256-bit key, implies --encryption
  -e, --error-retry int                      number of times to retry in case of network error (default 3)
  -b, --error-retry-base-interval duration   initial retry delay, increases linearly with each subsequent attempt (default 500ms)
  -h, --help                                 help for chunk-server
      --key string                           key file in PEM format, requires --cert
  -l, --listen strings                       listen address(es), can be repeated (default [:http])
      --log string                           request log file or - for STDOUT
      --mutual-tls                           require valid client certificate
      --skip-verify-read                     don't verify chunk data read from upstream stores (faster) (default true)
      --skip-verify-write                    don't verify chunk data written to this server (faster) (default true)
  -s, --store strings                        upstream source store(s)
      --store-file string                    read store arguments from a file, supports reload on SIGHUP
  -t, --trust-insecure                       trust invalid certificates
  -u, --uncompressed                         serve uncompressed chunks
  -w, --writable                             support writing
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

