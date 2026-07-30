// Per-branch processing: inspect (dry-run) or checkout + prepend + commit +
// push. Each branch resolves to one of four statuses:
//   "updated" - notice added (and pushed, in execution mode)
//   "already" - branch already carries the notice; nothing to do
//   "missing" - the target file does not exist on the branch
//   "error"   - the branch could not be processed; details in err
import fs from "node:fs";
import path from "node:path";
import {
  git,
  localBranchExists,
  remoteBranchExists,
  fileAtRef,
} from "./git.js";
import { readRef } from "./branches.js";
import { hasNotice, prependNotice } from "./notice.js";
import { progress } from "./log.js";

// processBranch handles one branch end-to-end. In dry-run it only inspects the
// branch content (no checkout, no writes). In execution mode it checks out the
// branch, fast-forwards it to the remote, prepends the notice, commits the
// single file and pushes before moving on.
export function processBranch(cfg, branch) {
  const res = { branch, source: readRef(cfg, branch), status: "", commit: "", pushed: false, err: "" };

  if (cfg.dryRun) {
    try {
      progress(cfg, `    inspecting ${res.source} (no checkout)`);
      res.status = inspectBranch(cfg, res.source);
    } catch (err) {
      res.status = "error";
      res.err = err.message;
    }
    return res;
  }

  try {
    progress(cfg, `    checkout ${branch}`);
    checkoutBranch(cfg, branch);
    syncWithRemote(cfg, branch);
    applyNotice(cfg, res);
  } catch (err) {
    res.status = "error";
    res.err = err.message;
  }
  return res;
}

// inspectBranch classifies a branch by reading the file straight from the ref.
function inspectBranch(cfg, ref) {
  const { content, exists } = fileAtRef(cfg.repoPath, ref, cfg.file);
  if (!exists) {
    return "missing";
  }
  return hasNotice(content, cfg.message) ? "already" : "updated";
}

// applyNotice mutates the checked-out branch: prepend, commit the single file
// and push. Fills res.status/commit/pushed in place.
function applyNotice(cfg, res) {
  const abs = path.join(cfg.repoPath, cfg.file);

  let raw;
  try {
    raw = fs.readFileSync(abs, "utf8");
  } catch (err) {
    if (err.code === "ENOENT") {
      res.status = "missing";
      progress(cfg, `    ${cfg.file} absent on branch, skipping`);
      return;
    }
    throw err;
  }

  if (hasNotice(raw, cfg.message)) {
    res.status = "already";
    progress(cfg, `    notice already present, skipping`);
    return;
  }

  fs.writeFileSync(abs, prependNotice(raw, cfg.message));
  git(cfg.repoPath, "add", "--", cfg.file);
  git(cfg.repoPath, "commit", "-m", cfg.commitMessage, "--", cfg.file);
  res.commit = git(cfg.repoPath, "rev-parse", "--short", "HEAD");
  progress(cfg, `    commit ${res.commit} ${cfg.file}`);

  progress(cfg, `    push ${res.branch} -> ${cfg.remote}`);
  git(cfg.repoPath, "push", cfg.remote, res.branch);
  res.pushed = true;
  res.status = "updated";
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

// syncWithRemote fast-forwards a pre-existing local branch to the remote tip so
// the later push cannot be rejected as non-fast-forward. A diverged branch is a
// real conflict that needs a human, so it surfaces as an error.
function syncWithRemote(cfg, branch) {
  if (!remoteBranchExists(cfg.repoPath, cfg.remote, branch)) {
    return;
  }
  try {
    git(cfg.repoPath, "merge", "--ff-only", `${cfg.remote}/${branch}`);
  } catch (err) {
    throw new Error(`local branch diverged from ${cfg.remote}/${branch}; resolve manually (${err.message})`);
  }
}
