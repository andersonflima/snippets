# git-yaml-ref-bump (Node.js)

Replaces a versioned reference (e.g. `build.yml@v2` -> `build.yml@v3`) inside YAML
files across a set of release branches (`v1`..`v6` and their minors, by default).

Target files are selected by name pattern within a configurable directory
(default `.github/workflows`) and resolved **per branch**, so the same file can
live at different paths on different branches. It commits **one file per change**
and pushes **once per branch**. A dry-run mode reports the planned changes without
touching the repository.

This is the Node.js port of the Go tool in `../git-yaml-ref-bump`; CLI flags and
report format are identical.

## Requirements

- Node.js >= 18
- `git` available on `PATH`

## Usage

```bash
node index.js --files <patterns> [options]
# or, after `npm link` / install:
git-yaml-ref-bump --files <patterns> [options]
```

### Options

| Flag           | Default                  | Description                                                              |
| -------------- | ------------------------ | ------------------------------------------------------------------------ |
| `--files`      | _(required)_             | Comma-separated file-name patterns (regexp), e.g. `build.yml,deploy.yml` |
| `--dir`        | `.github/workflows`      | Directory searched for matching files in each branch                     |
| `--full-path`  | `false`                  | Match `--files` patterns against the full repo-relative path             |
| `--from`       | `build.yml@v2`           | Reference string to replace (literal match)                              |
| `--to`         | `build.yml@v3`           | Replacement reference string                                             |
| `--repo`       | `.`                      | Path to the target git repository                                        |
| `--pattern`    | `^v[1-6](\.[0-9]+)*$`    | Regexp selecting target branches                                         |
| `--remote`     | `origin`                 | Git remote used for discovery and push                                   |
| `--dry-run`    | `false`                  | Report planned changes without writing, committing or pushing            |
| `--fetch`      | `true`                   | Run `git fetch <remote> --prune` before discovery (disable: `--no-fetch`)|
| `--report`     | _(none)_                 | Also write the Markdown report to this path                              |

### Examples

Preview what would change, without touching anything:

```bash
node index.js --repo /path/to/repo --files 'build.yml' \
  --from 'build.yml@v2' --to 'build.yml@v3' --dry-run
```

Execute across all `v1`..`v6` branches, matching any `.yml`/`.yaml`:

```bash
node index.js --repo /path/to/repo --files '.*\.ya?ml' \
  --from 'reusable.yml@v2' --to 'reusable.yml@v3'
```

## Notes

- `--from`/`--to` use **literal** string replacement (not regexp).
- `--files` entries are **regexps**, full-matched against each file name (or full
  path with `--full-path`).
- Execution mode requires a clean working tree; it restores the original branch
  when finished.
