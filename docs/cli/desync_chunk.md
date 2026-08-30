## desync chunk

Chunk input file and print chunk boundaries and IDs

### Synopsis

Chunks the input file without storing anything, and prints a start/length/hash
triple for each chunk the file would be split into. Useful to inspect or tune
chunking parameters before running 'make'.

```
desync chunk <file> [flags]
```

### Examples

```
  desync chunk file.bin
```

### Options

```
  -m, --chunk-size string   min:avg:max chunk size in kb (default "16:64:256")
  -h, --help                help for chunk
  -S, --start uint          starting position in bytes
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync](desync.md)	 - Content-addressed binary distribution system

