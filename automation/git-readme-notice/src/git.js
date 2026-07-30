// Thin wrappers around the git CLI. Each helper runs inside repoPath and keeps
// effects isolated so the rest of the program stays pure and testable.
import { execFileSync } from "node:child_process";

// git runs a git command inside repoPath and returns trimmed stdout. It throws
// on a non-zero exit, surfacing the trimmed stderr in the message.
export function git(repoPath, ...args) {
  try {
    const stdout = execFileSync("git", args, {
      cwd: repoPath,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return stdout.trim();
  } catch (err) {
    const stderr = (err.stderr ?? "").toString().trim();
    throw new Error(`git ${args.join(" ")}: ${stderr || err.message}`);
  }
}

// gitOK reports whether a git command succeeds, ignoring its output. Useful for
// existence checks (refs/paths) that signal via exit code.
export function gitOK(repoPath, ...args) {
  try {
    execFileSync("git", args, { cwd: repoPath, stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

// isGitRepo reports whether repoPath is inside a git working tree.
export function isGitRepo(repoPath) {
  return gitOK(repoPath, "rev-parse", "--is-inside-work-tree");
}

// workingTreeClean reports whether there are no staged/unstaged changes.
export function workingTreeClean(repoPath) {
  return git(repoPath, "status", "--porcelain") === "";
}

// currentBranch returns the short name of the checked-out branch.
export function currentBranch(repoPath) {
  return git(repoPath, "rev-parse", "--abbrev-ref", "HEAD");
}

// localBranchExists reports whether a local branch ref is present.
export function localBranchExists(repoPath, branch) {
  return gitOK(repoPath, "rev-parse", "--verify", "--quiet", `refs/heads/${branch}`);
}

// remoteBranchExists reports whether <remote>/<branch> is present.
export function remoteBranchExists(repoPath, remote, branch) {
  return gitOK(repoPath, "rev-parse", "--verify", "--quiet", `refs/remotes/${remote}/${branch}`);
}

// fileAtRef returns { content, exists } for path as seen at ref. A missing path
// is an expected, non-fatal outcome reported via exists=false (no throw).
export function fileAtRef(repoPath, ref, filePath) {
  try {
    const content = execFileSync("git", ["show", `${ref}:${filePath}`], {
      cwd: repoPath,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return { content, exists: true };
  } catch (err) {
    const stderr = (err.stderr ?? "").toString();
    if (stderr.includes("does not exist") || stderr.includes("exists on disk, but not in")) {
      return { content: "", exists: false };
    }
    throw new Error(`git show ${ref}:${filePath}: ${stderr.trim()}`);
  }
}
