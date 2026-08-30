## desync completion fish

Generate the autocompletion script for fish

### Synopsis

Generate the autocompletion script for the fish shell.

To load completions in your current shell session:

	desync completion fish | source

To load completions for every new session, execute once:

	desync completion fish > ~/.config/fish/completions/desync.fish

You will need to start a new shell for this setup to take effect.


```
desync completion fish [flags]
```

### Options

```
  -h, --help              help for fish
      --no-descriptions   disable completion descriptions
```

### Options inherited from parent commands

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
      --verbose         verbose mode
```

### SEE ALSO

* [desync completion](desync_completion.md)	 - Generate the autocompletion script for the specified shell

