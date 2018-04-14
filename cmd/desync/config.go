package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

type Config struct {
	HTTPTimeout    time.Duration `json:"http-timeout"`
	HTTPErrorRetry int           `json:"http-error-retry"`
}

// Global config in the main packe defining the defaults. Those can be
// overridden by loading a config file.
var cfg = Config{
	HTTPTimeout: time.Minute,
}

const configUsage = `desync config

Shows the current internal config settings, either the defaults or the values
from $HOME/.config/desync/config.json. The output can be used to create a custom
config file by writing it to $HOME/.config/desync/config.json.
`

func config(ctx context.Context, args []string) error {
	var writeConfig bool
	flags := flag.NewFlagSet("config", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, configUsage)
		flags.PrintDefaults()
	}
	flags.BoolVar(&writeConfig, "w", false, "write current configuration to $HOME/.config/desync/config.json")
	flags.Parse(args)

	if flags.NArg() > 0 {
		return errors.New("Too many arguments. See -h for help.")
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	var w io.Writer = os.Stderr
	if writeConfig {
		filename, err := configFile()
		if err != nil {
			return err
		}
		if err = os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
			return err
		}
		f, err := os.Create(filename)
		if err != nil {
			return err
		}
		defer f.Close()
		fmt.Println("Writing config to", filename)
		w = f
	}
	_, err = w.Write(b)
	fmt.Println()
	return err
}

func configFile() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	filename := filepath.Join(u.HomeDir, ".config", "desync", "config.json")
	return filename, nil
}

// Look for $HOME/.config/desync and if present, load into the global config
// instance. Values defined in the file will be set accordingly, while anything
// that's not in the file will retain it's default values.
func loadConfigIfPresent() error {
	filename, err := configFile()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	}
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	err = json.NewDecoder(f).Decode(&cfg)
	return errors.Wrap(err, "reading "+filename)
}
