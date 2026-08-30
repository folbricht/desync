## desync pull

Serve chunks via casync protocol over SSH

### Synopsis

Serves up chunks (read-only) from a local store using the casync protocol
via Stdin/Stdout. Functions as a drop-in replacement for casync on remote
stores accessed with SSH. See CASYNC_REMOTE_PATH environment variable.

```
desync pull - - - <store>
```

### Examples

```
  desync pull - - - /path/to/store
```

### Options

```
  -h, --help   help for pull
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

