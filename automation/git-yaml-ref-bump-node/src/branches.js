// Branch discovery and per-branch path selection. Reads refs through git so a
// branch can be inspected without checking it out.
import path from "node:path";
import { git, localBranchExists } from "./git.js";

// discoverBranches returns the sorted set of branch names (local + remote) that
// match the configured pattern. Remote names are reduced to their short branch
// name (origin/v1.2 -> v1.2) and deduplicated against the local set.
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

// listMatchingPaths returns the repo-relative paths under cfg.dir at the given
// ref whose name (or full path, when matchFullPath) matches any --files pattern.
// A branch lacking the directory simply yields an empty list, not an error.
export function listMatchingPaths(cfg, ref) {
  const out = git(cfg.repoPath, "ls-tree", "-r", "--name-only", ref, "--", cfg.dir);

  const paths = [];
  for (const line of out.split("\n")) {
    const p = line.trim();
    if (p === "") {
      continue;
    }
    const target = cfg.matchFullPath ? p : path.basename(p);
    if (matchesAnyPattern(cfg.filePatterns, target)) {
      paths.push(p);
    }
  }
  return paths;
}

// matchesAnyPattern reports whether value matches at least one pattern.
function matchesAnyPattern(patterns, value) {
  return patterns.some((re) => re.test(value));
}
