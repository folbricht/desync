## desync index-server

Server for indexes over HTTP(S)

### Synopsis

Starts an HTTP index server that can be used as remote store. It supports
reading from a single local store or proxying to a remote store.
If --cert and --key are provided, the server will serve over HTTPS. The -w option
enables writing to this store.

```
desync index-server [flags]
```

### Examples

```
  desync index-server -s sftp://192.168.1.1/indexes -l :8080
  desync index-server -s /path/to/indexes -w -l :8080
```

### Options

```
      --authorization string                 expected value of the authorization header in requests
      --ca-cert string                       trust authorities in this file, instead of OS trust store
  -r, --cache-repair                         replace invalid chunks in the cache from source (default true)
      --cert string                          cert file in PEM format, requires --key
      --client-ca string                     acceptable client certificate or CA
      --client-cert string                   path to client certificate for TLS authentication
      --client-key string                    path to client key for TLS authentication
  -n, --concurrency int                      number of concurrent goroutines (default 10)
  -e, --error-retry int                      number of times to retry in case of network error (default 3)
  -b, --error-retry-base-interval duration   initial retry delay, increases linearly with each subsequent attempt (default 500ms)
  -h, --help                                 help for index-server
      --key string                           key file in PEM format, requires --cert
  -l, --listen strings                       listen address(es), can be repeated (default [:http])
      --log string                           request log file or - for STDOUT
      --mutual-tls                           require valid client certificate
  -s, --store string                         upstream source index store
  -t, --trust-insecure                       trust invalid certificates
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

