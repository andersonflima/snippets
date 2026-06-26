// Per-branch processing: inspect (dry-run) or rewrite + commit + push.
import fs from "node:fs";
import path from "node:path";
import { git, localBranchExists, remoteBranchExists, fileAtRef } from "./git.js";
import { readRef, listMatchingPaths } from "./branches.js";

// countOccurrences counts non-overlapping literal occurrences of needle.
function countOccurrences(haystack, needle) {
  if (needle === "") {
    return 0;
  }
  let count = 0;
  let index = haystack.indexOf(needle);
  while (index !== -1) {
    count++;
    index = haystack.indexOf(needle, index + needle.length);
  }
  return count;
}

// replaceAllLiteral replaces every literal occurrence of needle with value.
function replaceAllLiteral(haystack, needle, value) {
  return haystack.split(needle).join(value);
}

// processBranch handles one branch end-to-end. In dry-run it only inspects the
// branch content (no checkout, no writes). In execution mode it checks out the
// branch, commits one change per file, and pushes once at the end.
export function processBranch(cfg, branch) {
  const res = { branch, source: readRef(cfg, branch), files: [], pushed: false, skipped: false, err: "" };

  if (cfg.dryRun) {
    try {
      res.files = inspectBranch(cfg, branch);
    } catch (err) {
      res.err = err.message;
    }
    return res;
  }

  try {
    checkoutBranch(cfg, branch);
    res.files = bumpBranchFiles(cfg, branch);
  } catch (err) {
    res.err = err.message;
    return res;
  }

  res.skipped = changedCount(res.files) === 0;
  if (res.skipped) {
    return res;
  }

  try {
    git(cfg.repoPath, "push", cfg.remote, branch);
    res.pushed = true;
  } catch (err) {
    res.err = `push failed: ${err.message}`;
  }
  return res;
}

// changedCount returns how many files were modified on the branch.
export function changedCount(files) {
  return files.filter((f) => f.changed).length;
}

// inspectBranch reports, per matching file, what would change on the branch
// without touching the working tree (reads content straight from the ref).
function inspectBranch(cfg, branch) {
  const ref = readRef(cfg, branch);
  const paths = listMatchingPaths(cfg, ref);

  return paths.map((p) => {
    const fr = { path: p, exists: false, occurrences: 0, changed: false, committed: false, commit: "", err: "" };
    try {
      const { content, exists } = fileAtRef(cfg.repoPath, ref, p);
      fr.exists = exists;
      if (exists) {
        fr.occurrences = countOccurrences(content, cfg.from);
        fr.changed = fr.occurrences > 0;
      }
    } catch (err) {
      fr.err = err.message;
    }
    return fr;
  });
}

// bumpBranchFiles rewrites each matching file on the currently checked-out
// branch, committing one file per change so each can be reverted independently.
function bumpBranchFiles(cfg, branch) {
  return listMatchingPaths(cfg, branch).map((p) => bumpFile(cfg, p));
}

// bumpFile applies the replacement to a single file and commits it in isolation.
function bumpFile(cfg, filePath) {
  const fr = { path: filePath, exists: false, occurrences: 0, changed: false, committed: false, commit: "", err: "" };
  const abs = path.join(cfg.repoPath, filePath);

  let raw;
  try {
    raw = fs.readFileSync(abs, "utf8");
  } catch (err) {
    if (err.code === "ENOENT") {
      return fr; // absent on this branch: skip silently, not an error
    }
    fr.err = err.message;
    return fr;
  }
  fr.exists = true;

  fr.occurrences = countOccurrences(raw, cfg.from);
  if (fr.occurrences === 0) {
    return fr;
  }

  try {
    fs.writeFileSync(abs, replaceAllLiteral(raw, cfg.from, cfg.to));
    fr.changed = true;
    commitFile(cfg, filePath);
    fr.committed = true;
    fr.commit = git(cfg.repoPath, "rev-parse", "--short", "HEAD");
  } catch (err) {
    fr.err = err.message;
  }
  return fr;
}

// commitFile stages and commits a single path with a neutral message.
function commitFile(cfg, filePath) {
  git(cfg.repoPath, "add", "--", filePath);
  const msg = `chore(ci): bump ${cfg.from} to ${cfg.to} in ${filePath}`;
  git(cfg.repoPath, "commit", "-m", msg, "--", filePath);
}

// checkoutBranch switches to branch, creating a tracking branch from the remote
// when only the remote ref exists locally.
function checkoutBranch(cfg, branch) {
  if (localBranchExists(cfg.repoPath, branch)) {
    git(cfg.repoPath, "checkout", branch);
    return;
  }
  if (remoteBranchExists(cfg.repoPath, cfg.remote, branch)) {
    git(cfg.repoPath, "checkout", "-b", branch, "--track", `${cfg.remote}/${branch}`);
    return;
  }
  throw new Error(`branch "${branch}" not found locally or on ${cfg.remote}`);
}
