## desync version

Show the desync version

### Synopsis

Prints the version of desync along with the commit it was built from, the
build time, and the Go toolchain and platform used.

```
desync version [flags]
```

### Examples

```
  desync version
  desync version --format=json
```

### Options

```
  -f, --format string   output format, plain or json (default "plain")
  -h, --help            help for version
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

