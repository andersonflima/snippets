// Branch discovery. Reads refs through git so a branch can be inspected
// without checking it out.
import { git, localBranchExists } from "./git.js";

// discoverBranches returns the sorted set of branch names (local + remote) that
// match the configured pattern. Remote names are reduced to their short branch
// name (origin/develop -> develop) and deduplicated against the local set.
export function discoverBranches(cfg) {
  const seen = new Set();

  const locals = git(cfg.repoPath, "branch", "--format=%(refname:short)");
  collectMatches(locals, "", cfg.pattern, seen);

  const remotes = git(cfg.repoPath, "branch", "-r", "--format=%(refname:short)");
  collectMatches(remotes, `${cfg.remote}/`, cfg.pattern, seen);

  return [...seen].sort();
}

// collectMatches scans newline-separated branch lines, strips the given remote
// prefix when present, skips HEAD pointers, and records pattern matches.
function collectMatches(lines, remotePrefix, pattern, seen) {
  for (const rawLine of lines.split("\n")) {
    let name = rawLine.trim();
    if (name === "" || name.includes("HEAD")) {
      continue;
    }
    if (remotePrefix !== "") {
      if (!name.startsWith(remotePrefix)) {
        continue;
      }
      name = name.slice(remotePrefix.length);
    }
    if (pattern.test(name)) {
      seen.add(name);
    }
  }
}

// readRef returns the ref to use when reading a branch's content without
// checking it out: the local branch when it exists, otherwise the remote ref.
export function readRef(cfg, branch) {
  if (localBranchExists(cfg.repoPath, branch)) {
    return branch;
  }
  return `${cfg.remote}/${branch}`;
}
