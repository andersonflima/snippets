package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

// git runs a git command inside repoPath and returns trimmed stdout.
func git(repoPath string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = repoPath

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s",
			strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// gitOK reports whether a git command succeeds, ignoring its output.
// Useful for existence checks (verify refs/paths) that signal via exit code.
func gitOK(repoPath string, args ...string) bool {
	cmd := exec.Command("git", args...)
	cmd.Dir = repoPath
	return cmd.Run() == nil
}

// isGitRepo reports whether repoPath is inside a git working tree.
func isGitRepo(repoPath string) bool {
	return gitOK(repoPath, "rev-parse", "--is-inside-work-tree")
}

// workingTreeClean reports whether there are no staged/unstaged changes.
func workingTreeClean(repoPath string) (bool, error) {
	out, err := git(repoPath, "status", "--porcelain")
	if err != nil {
		return false, err
	}
	return out == "", nil
}

// currentBranch returns the short name of the checked-out branch.
func currentBranch(repoPath string) (string, error) {
	return git(repoPath, "rev-parse", "--abbrev-ref", "HEAD")
}

// localBranchExists reports whether a local branch ref is present.
func localBranchExists(repoPath, branch string) bool {
	return gitOK(repoPath, "rev-parse", "--verify", "--quiet", "refs/heads/"+branch)
}

// remoteBranchExists reports whether <remote>/<branch> is present.
func remoteBranchExists(repoPath, remote, branch string) bool {
	return gitOK(repoPath, "rev-parse", "--verify", "--quiet",
		"refs/remotes/"+remote+"/"+branch)
}

// fileAtRef returns the content of path as seen at ref. The boolean is false
// (with a nil error) when the file simply does not exist at that ref.
func fileAtRef(repoPath, ref, path string) (string, bool, error) {
	cmd := exec.Command("git", "show", ref+":"+path)
	cmd.Dir = repoPath

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// A missing path is an expected, non-fatal outcome.
		if strings.Contains(stderr.String(), "does not exist") ||
			strings.Contains(stderr.String(), "exists on disk, but not in") {
			return "", false, nil
		}
		return "", false, fmt.Errorf("git show %s:%s: %s",
			ref, path, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), true, nil
}
