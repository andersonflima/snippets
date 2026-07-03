package main

import (
	"fmt"
	"strings"
)

// fileResult captures the outcome of processing a single file on a branch.
type fileResult struct {
	Path        string
	Exists      bool
	Occurrences int
	Changed     bool
	Committed   bool
	Commit      string
	Err         string
}

// branchResult aggregates the per-file outcomes for one branch.
type branchResult struct {
	Branch  string
	Source  string // "local" or "<remote>/<branch>"
	Files   []fileResult
	Pushed  bool
	Skipped bool // no changes -> nothing to commit/push
	Err     string
}

// changedCount returns how many files were modified on the branch.
func (b branchResult) changedCount() int {
	n := 0
	for _, f := range b.Files {
		if f.Changed {
			n++
		}
	}
	return n
}

// changedFiles returns only the files that will be (or were) impacted.
func changedFiles(files []fileResult) []fileResult {
	out := make([]fileResult, 0, len(files))
	for _, f := range files {
		if f.Changed {
			out = append(out, f)
		}
	}
	return out
}

// renderReport produces a human-readable Markdown report for all branches.
func renderReport(cfg config, results []branchResult) string {
	var sb strings.Builder

	mode := "EXECUTION"
	if cfg.DryRun {
		mode = "DRY-RUN (no changes written, no commits, no push)"
	}

	matchScope := "name"
	if cfg.MatchFullPath {
		matchScope = "full path"
	}

	fmt.Fprintf(&sb, "# YAML ref bump report\n\n")
	fmt.Fprintf(&sb, "- Mode: %s\n", mode)
	fmt.Fprintf(&sb, "- Replacement: `%s` -> `%s`\n", cfg.From, cfg.To)
	filesLabel := strings.Join(cfg.Files, ", ")
	if filesLabel == "" {
		filesLabel = "(all files under dir)"
	}
	fmt.Fprintf(&sb, "- File patterns (%s): %s\n", matchScope, filesLabel)
	fmt.Fprintf(&sb, "- Search dir: `%s`\n", cfg.Dir)
	fmt.Fprintf(&sb, "- Branch pattern: `%s`\n", cfg.Pattern.String())
	fmt.Fprintf(&sb, "- Branches matched: %d\n", len(results))
	if cfg.ChangedOnly {
		fmt.Fprintf(&sb, "- Filter: changed-only (branches/files with an impact)\n")
	}
	sb.WriteString("\n")

	for _, b := range results {
		if cfg.ChangedOnly && b.Err == "" && b.changedCount() == 0 {
			continue
		}
		fmt.Fprintf(&sb, "## %s (%s)\n", b.Branch, b.Source)
		if b.Err != "" {
			fmt.Fprintf(&sb, "- ERROR: %s\n\n", b.Err)
			continue
		}
		files := b.Files
		if cfg.ChangedOnly {
			files = changedFiles(files)
		}
		if len(files) == 0 {
			fmt.Fprintf(&sb, "- no file matched under `%s` (skipped)\n\n", cfg.Dir)
			continue
		}
		for _, f := range files {
			fmt.Fprintf(&sb, "- %s\n", renderFile(cfg, f))
		}
		fmt.Fprintf(&sb, "- changed files: %d\n", b.changedCount())
		fmt.Fprintf(&sb, "- push: %s\n\n", renderPush(cfg, b))
	}
	return sb.String()
}

func renderFile(cfg config, f fileResult) string {
	switch {
	case f.Err != "":
		return fmt.Sprintf("`%s`: ERROR: %s", f.Path, f.Err)
	case !f.Exists:
		return fmt.Sprintf("`%s`: absent on branch (skipped)", f.Path)
	case f.Occurrences == 0:
		return fmt.Sprintf("`%s`: no occurrence of `%s` (skipped)", f.Path, cfg.From)
	case cfg.DryRun:
		return fmt.Sprintf("`%s`: would replace %d occurrence(s)", f.Path, f.Occurrences)
	case f.Committed:
		return fmt.Sprintf("`%s`: replaced %d occurrence(s), committed %s", f.Path, f.Occurrences, f.Commit)
	default:
		return fmt.Sprintf("`%s`: replaced %d occurrence(s)", f.Path, f.Occurrences)
	}
}

func renderPush(cfg config, b branchResult) string {
	if cfg.DryRun {
		if b.changedCount() > 0 {
			return fmt.Sprintf("would push to %s", cfg.Remote)
		}
		return "nothing to push"
	}
	if b.Skipped {
		return "nothing to push"
	}
	if b.Pushed {
		return fmt.Sprintf("pushed to %s", cfg.Remote)
	}
	return "not pushed"
}
