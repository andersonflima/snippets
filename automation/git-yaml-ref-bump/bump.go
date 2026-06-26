package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// processBranch handles one branch end-to-end. In dry-run it only inspects the
// branch content (no checkout, no writes). In execution mode it checks out the
// branch, commits one change per file, and pushes once at the end.
func processBranch(cfg config, branch string) branchResult {
	res := branchResult{Branch: branch, Source: readRef(cfg, branch)}

	if cfg.DryRun {
		files, err := inspectBranch(cfg, branch)
		if err != nil {
			res.Err = err.Error()
			return res
		}
		res.Files = files
		return res
	}

	if err := checkoutBranch(cfg, branch); err != nil {
		res.Err = err.Error()
		return res
	}

	files, err := bumpBranchFiles(cfg, branch)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	res.Files = files
	res.Skipped = res.changedCount() == 0

	if res.Skipped {
		return res
	}
	if _, err := git(cfg.RepoPath, "push", cfg.Remote, branch); err != nil {
		res.Err = fmt.Sprintf("push failed: %v", err)
		return res
	}
	res.Pushed = true
	return res
}

// inspectBranch reports, per matching file, what would change on the branch
// without touching the working tree (reads content straight from the ref).
func inspectBranch(cfg config, branch string) ([]fileResult, error) {
	ref := readRef(cfg, branch)
	paths, err := listMatchingPaths(cfg, ref)
	if err != nil {
		return nil, err
	}

	out := make([]fileResult, 0, len(paths))
	for _, path := range paths {
		fr := fileResult{Path: path}
		content, exists, err := fileAtRef(cfg.RepoPath, ref, path)
		if err != nil {
			fr.Err = err.Error()
			out = append(out, fr)
			continue
		}
		fr.Exists = exists
		if exists {
			fr.Occurrences = strings.Count(content, cfg.From)
			fr.Changed = fr.Occurrences > 0
		}
		out = append(out, fr)
	}
	return out, nil
}

// bumpBranchFiles rewrites each matching file on the currently checked-out
// branch, committing one file per change so each can be reverted independently.
func bumpBranchFiles(cfg config, branch string) ([]fileResult, error) {
	paths, err := listMatchingPaths(cfg, branch)
	if err != nil {
		return nil, err
	}

	out := make([]fileResult, 0, len(paths))
	for _, path := range paths {
		out = append(out, bumpFile(cfg, branch, path))
	}
	return out, nil
}

// bumpFile applies the replacement to a single file and commits it in isolation.
func bumpFile(cfg config, branch, path string) fileResult {
	fr := fileResult{Path: path}
	abs := filepath.Join(cfg.RepoPath, path)

	raw, err := os.ReadFile(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return fr // absent on this branch: skip silently, not an error
		}
		fr.Err = err.Error()
		return fr
	}
	fr.Exists = true

	content := string(raw)
	fr.Occurrences = strings.Count(content, cfg.From)
	if fr.Occurrences == 0 {
		return fr
	}

	updated := strings.ReplaceAll(content, cfg.From, cfg.To)
	if err := os.WriteFile(abs, []byte(updated), 0o644); err != nil {
		fr.Err = err.Error()
		return fr
	}
	fr.Changed = true

	if err := commitFile(cfg, path); err != nil {
		fr.Err = err.Error()
		return fr
	}
	fr.Committed = true

	if hash, err := git(cfg.RepoPath, "rev-parse", "--short", "HEAD"); err == nil {
		fr.Commit = hash
	}
	return fr
}

// commitFile stages and commits a single path with a neutral message.
func commitFile(cfg config, path string) error {
	if _, err := git(cfg.RepoPath, "add", "--", path); err != nil {
		return err
	}
	msg := fmt.Sprintf("chore(ci): bump %s to %s in %s", cfg.From, cfg.To, path)
	_, err := git(cfg.RepoPath, "commit", "-m", msg, "--", path)
	return err
}

// checkoutBranch switches to branch, creating a tracking branch from the remote
// when only the remote ref exists locally.
func checkoutBranch(cfg config, branch string) error {
	if localBranchExists(cfg.RepoPath, branch) {
		_, err := git(cfg.RepoPath, "checkout", branch)
		return err
	}
	if remoteBranchExists(cfg.RepoPath, cfg.Remote, branch) {
		_, err := git(cfg.RepoPath, "checkout", "-b", branch,
			"--track", cfg.Remote+"/"+branch)
		return err
	}
	return fmt.Errorf("branch %q not found locally or on %s", branch, cfg.Remote)
}
