// Command git-yaml-ref-bump replaces a versioned reference (e.g. build.yml@v2 ->
// build.yml@v3) inside one or more YAML files across a set of release branches
// (v1..v6 and their minors, by default), committing one file per change and
// pushing once per branch. A dry-run mode reports the planned changes without
// touching the repository.
package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// config holds the resolved CLI options for a run.
type config struct {
	RepoPath   string
	Files      []string
	From       string
	To         string
	Pattern    *regexp.Regexp
	Remote     string
	DryRun     bool
	Fetch      bool
	ReportPath string
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func parseFlags() (config, error) {
	var (
		files   = flag.String("files", "", "comma-separated YAML files to update, relative to repo root (required)")
		from    = flag.String("from", "build.yml@v2", "reference string to replace")
		to      = flag.String("to", "build.yml@v3", "replacement reference string")
		repo    = flag.String("repo", ".", "path to the target git repository")
		pattern = flag.String("pattern", `^v[1-6](\.[0-9]+)*$`, "regexp selecting target branches")
		remote  = flag.String("remote", "origin", "git remote used for discovery and push")
		dryRun  = flag.Bool("dry-run", false, "report planned changes without writing, committing or pushing")
		fetch   = flag.Bool("fetch", true, "run 'git fetch <remote>' before discovering branches")
		report  = flag.String("report", "", "optional path to also write the Markdown report to")
	)
	flag.Parse()

	if strings.TrimSpace(*files) == "" {
		return config{}, fmt.Errorf("-files is required")
	}
	re, err := regexp.Compile(*pattern)
	if err != nil {
		return config{}, fmt.Errorf("invalid -pattern: %w", err)
	}

	return config{
		RepoPath:   *repo,
		Files:      splitFiles(*files),
		From:       *from,
		To:         *to,
		Pattern:    re,
		Remote:     *remote,
		DryRun:     *dryRun,
		Fetch:      *fetch,
		ReportPath: *report,
	}, nil
}

// splitFiles turns a comma-separated list into a trimmed, non-empty slice.
func splitFiles(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func run(cfg config) error {
	if err := preflight(cfg); err != nil {
		return err
	}

	if cfg.Fetch {
		if _, err := git(cfg.RepoPath, "fetch", cfg.Remote, "--prune"); err != nil {
			return fmt.Errorf("fetch: %w", err)
		}
	}

	branches, err := discoverBranches(cfg)
	if err != nil {
		return fmt.Errorf("discover branches: %w", err)
	}
	if len(branches) == 0 {
		return fmt.Errorf("no branch matched pattern %q", cfg.Pattern.String())
	}

	original, err := currentBranch(cfg.RepoPath)
	if err != nil {
		return err
	}
	if !cfg.DryRun {
		defer restoreBranch(cfg, original)
	}

	results := make([]branchResult, 0, len(branches))
	for _, b := range branches {
		results = append(results, processBranch(cfg, b))
	}

	return emitReport(cfg, results)
}

// preflight validates that the run can proceed safely.
func preflight(cfg config) error {
	if !isGitRepo(cfg.RepoPath) {
		return fmt.Errorf("%s is not a git repository", cfg.RepoPath)
	}
	if cfg.DryRun {
		return nil
	}
	clean, err := workingTreeClean(cfg.RepoPath)
	if err != nil {
		return err
	}
	if !clean {
		return fmt.Errorf("working tree is not clean; commit or stash changes before running")
	}
	return nil
}

// restoreBranch returns the repo to the branch checked out before the run.
func restoreBranch(cfg config, branch string) {
	if _, err := git(cfg.RepoPath, "checkout", branch); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not restore branch %q: %v\n", branch, err)
	}
}

// emitReport prints the report to stdout and optionally writes it to a file.
func emitReport(cfg config, results []branchResult) error {
	report := renderReport(cfg, results)
	fmt.Print(report)

	if cfg.ReportPath != "" {
		if err := os.WriteFile(cfg.ReportPath, []byte(report), 0o644); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
		fmt.Fprintf(os.Stderr, "report written to %s\n", cfg.ReportPath)
	}
	return nil
}
