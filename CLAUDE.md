# CLAUDE.md

## Key Patterns

- **`t.Fatal()` restriction** — do not call `t.Fatal()`/`t.FailNow()` from non-main goroutines (see PR #291)
- **Avoid recompression** — if a chunk already has compressed form, don't recompress (see PR #289)

## Code Style

- **Tests use testify** — write new (or modified) Go tests with `github.com/stretchr/testify`: `require` for fatal checks, `assert` for non-fatal. Don't hand-roll `if … { t.Fatalf(…) }` conditionals. Existing plain-`testing` tests don't need a mass rewrite — apply this to new tests and tests you're already changing. Note: `require.*` calls `FailNow()`, so per the non-main-goroutine `t.Fatal()` restriction in Key Patterns, only use `require.*` from the test goroutine — in spawned goroutines use `assert.*` or pass errors back over a channel.
- **Build into the main package's directory** — always `go build -o cmd/desync/ ./cmd/desync`, never a bare `go build ./cmd/desync`. The bare form drops an untracked `desync` binary in the repo root (it is not git-ignored). Build artifacts belong next to their `main` package, never the project root.
