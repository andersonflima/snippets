package main

import (
	"regexp"
	"sort"
	"strings"
)

// discoverBranches returns the set of branch names (local + remote) that match
// the configured pattern. Remote names are reduced to their short branch name
// (origin/v1.2 -> v1.2) and deduplicated against the local set.
func discoverBranches(cfg config) ([]string, error) {
	seen := map[string]struct{}{}

	locals, err := git(cfg.RepoPath, "branch", "--format=%(refname:short)")
	if err != nil {
		return nil, err
	}
	collectMatches(locals, "", cfg.Pattern, seen)

	remotePrefix := cfg.Remote + "/"
	remotes, err := git(cfg.RepoPath, "branch", "-r", "--format=%(refname:short)")
	if err != nil {
		return nil, err
	}
	collectMatches(remotes, remotePrefix, cfg.Pattern, seen)

	out := make([]string, 0, len(seen))
	for b := range seen {
		out = append(out, b)
	}
	sort.Strings(out)
	return out, nil
}

// collectMatches scans newline-separated branch lines, strips the given remote
// prefix when present, skips HEAD pointers, and records pattern matches.
func collectMatches(lines, remotePrefix string, pattern *regexp.Regexp, seen map[string]struct{}) {
	for _, raw := range strings.Split(lines, "\n") {
		name := strings.TrimSpace(raw)
		if name == "" || strings.Contains(name, "HEAD") {
			continue
		}
		if remotePrefix != "" {
			if !strings.HasPrefix(name, remotePrefix) {
				continue
			}
			name = strings.TrimPrefix(name, remotePrefix)
		}
		if pattern.MatchString(name) {
			seen[name] = struct{}{}
		}
	}
}

// readRef returns the ref to use when reading a branch's content without
// checking it out: the local branch when it exists, otherwise the remote ref.
func readRef(cfg config, branch string) string {
	if localBranchExists(cfg.RepoPath, branch) {
		return branch
	}
	return cfg.Remote + "/" + branch
}
