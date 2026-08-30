## desync config

Show or write config file

### Synopsis

Shows the current internal configuration settings, either the defaults,
the values from $HOME/.config/desync/config.json or the specified config file. The
output can be used to create a custom config file by writing it to the specified file
or $HOME/.config/desync/config.json by default.

```
desync config [flags]
```

### Examples

```
  desync config
  desync --config desync.json config -w
```

### Options

```
  -h, --help    help for config
  -w, --write   write current configuration to file
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

