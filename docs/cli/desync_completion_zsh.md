## desync completion zsh

Generate the autocompletion script for zsh

### Synopsis

Generate the autocompletion script for the zsh shell.

If shell completion is not already enabled in your environment you will need
to enable it.  You can execute the following once:

	echo "autoload -U compinit; compinit" >> ~/.zshrc

To load completions in your current shell session:

	source <(desync completion zsh)

To load completions for every new session, execute once:

#### Linux:

	desync completion zsh > "${fpath[1]}/_desync"

#### macOS:

	desync completion zsh > $(brew --prefix)/share/zsh/site-functions/_desync

You will need to start a new shell for this setup to take effect.


```
desync completion zsh [flags]
```

### Options

```
  -h, --help              help for zsh
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

