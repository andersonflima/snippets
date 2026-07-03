// Orchestration: preflight, fetch, discover, process each branch, emit report.
import fs from "node:fs";
import {
  git,
  isGitRepo,
  workingTreeClean,
  currentBranch,
} from "./git.js";
import { discoverBranches } from "./branches.js";
import { processBranch } from "./bump.js";
import { renderReport } from "./report.js";
import { progress } from "./log.js";

// run executes a full pass for the resolved config.
export function run(cfg) {
  preflight(cfg);
  progress(cfg, `mode: ${cfg.dryRun ? "DRY-RUN" : "EXECUTION"} | ${cfg.from} -> ${cfg.to} | dir ${cfg.dir}`);

  if (cfg.fetch) {
    progress(cfg, `fetching ${cfg.remote} ...`);
    git(cfg.repoPath, "fetch", cfg.remote, "--prune");
  }

  const branches = discoverBranches(cfg);
  if (branches.length === 0) {
    throw new Error(`no branch matched pattern "${cfg.pattern.source}"`);
  }
  progress(cfg, `${branches.length} branch(es) matched: ${branches.join(", ")}`);

  const original = currentBranch(cfg.repoPath);
  try {
    const results = branches.map((b, i) => {
      progress(cfg, `[${i + 1}/${branches.length}] ${b}`);
      return processBranch(cfg, b);
    });
    emitReport(cfg, results);
  } finally {
    if (!cfg.dryRun) {
      progress(cfg, `restoring branch ${original}`);
      restoreBranch(cfg, original);
    }
  }
}

// preflight validates that the run can proceed safely.
function preflight(cfg) {
  if (!isGitRepo(cfg.repoPath)) {
    throw new Error(`${cfg.repoPath} is not a git repository`);
  }
  if (cfg.dryRun) {
    return;
  }
  if (!workingTreeClean(cfg.repoPath)) {
    throw new Error("working tree is not clean; commit or stash changes before running");
  }
}

// restoreBranch returns the repo to the branch checked out before the run.
function restoreBranch(cfg, branch) {
  try {
    git(cfg.repoPath, "checkout", branch);
  } catch (err) {
    process.stderr.write(`warning: could not restore branch "${branch}": ${err.message}\n`);
  }
}

// emitReport prints the report to stdout and optionally writes it to a file.
function emitReport(cfg, results) {
  const report = renderReport(cfg, results);
  process.stdout.write(report);

  if (cfg.reportPath !== "") {
    fs.writeFileSync(cfg.reportPath, report);
    process.stderr.write(`report written to ${cfg.reportPath}\n`);
  }
}
