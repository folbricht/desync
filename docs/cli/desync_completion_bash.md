## desync completion bash

Generate the autocompletion script for bash

### Synopsis

Generate the autocompletion script for the bash shell.

This script depends on the 'bash-completion' package.
If it is not installed already, you can install it via your OS's package manager.

To load completions in your current shell session:

	source <(desync completion bash)

To load completions for every new session, execute once:

#### Linux:

	desync completion bash > /etc/bash_completion.d/desync

#### macOS:

	desync completion bash > $(brew --prefix)/etc/bash_completion.d/desync

You will need to start a new shell for this setup to take effect.


```
desync completion bash
```

### Options

```
  -h, --help              help for bash
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

