// Markdown rendering of the run results. Pure: takes config + results, returns
// a string. Mirrors the Go report format line-for-line.
import { changedCount } from "./bump.js";

// renderReport produces a human-readable Markdown report for all branches.
export function renderReport(cfg, results) {
  const lines = [];

  const mode = cfg.dryRun
    ? "DRY-RUN (no changes written, no commits, no push)"
    : "EXECUTION";
  const matchScope = cfg.matchFullPath ? "full path" : "name";

  lines.push("# YAML ref bump report", "");
  lines.push(`- Mode: ${mode}`);
  lines.push(`- Replacement: \`${cfg.from}\` -> \`${cfg.to}\``);
  lines.push(`- File patterns (${matchScope}): ${cfg.files.join(", ")}`);
  lines.push(`- Search dir: \`${cfg.dir}\``);
  lines.push(`- Branch pattern: \`${cfg.pattern.source}\``);
  lines.push(`- Branches matched: ${results.length}`, "");

  for (const b of results) {
    lines.push(`## ${b.branch} (${b.source})`);
    if (b.err !== "") {
      lines.push(`- ERROR: ${b.err}`, "");
      continue;
    }
    if (b.files.length === 0) {
      lines.push(`- no file matched under \`${cfg.dir}\` (skipped)`, "");
      continue;
    }
    for (const f of b.files) {
      lines.push(`- ${renderFile(cfg, f)}`);
    }
    lines.push(`- changed files: ${changedCount(b.files)}`);
    lines.push(`- push: ${renderPush(cfg, b)}`, "");
  }

  return lines.join("\n") + "\n";
}

function renderFile(cfg, f) {
  if (f.err !== "") {
    return `\`${f.path}\`: ERROR: ${f.err}`;
  }
  if (!f.exists) {
    return `\`${f.path}\`: absent on branch (skipped)`;
  }
  if (f.occurrences === 0) {
    return `\`${f.path}\`: no occurrence of \`${cfg.from}\` (skipped)`;
  }
  if (cfg.dryRun) {
    return `\`${f.path}\`: would replace ${f.occurrences} occurrence(s)`;
  }
  if (f.committed) {
    return `\`${f.path}\`: replaced ${f.occurrences} occurrence(s), committed ${f.commit}`;
  }
  return `\`${f.path}\`: replaced ${f.occurrences} occurrence(s)`;
}

function renderPush(cfg, b) {
  if (cfg.dryRun) {
    return changedCount(b.files) > 0 ? `would push to ${cfg.remote}` : "nothing to push";
  }
  if (b.skipped) {
    return "nothing to push";
  }
  if (b.pushed) {
    return `pushed to ${cfg.remote}`;
  }
  return "not pushed";
}
