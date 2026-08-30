## desync manpage

Generate manpages for desync

### Synopsis

Generates man pages for desync and all of its commands into the given directory.

```
desync manpage <output-directory> [flags]
```

### Examples

```
  desync manpage /tmp/man
```

### Options

```
  -h, --help             help for manpage
      --manual string    manual
      --section string   section (default "1")
      --source string    source (default "desync")
      --title string     title (default "desync")
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

