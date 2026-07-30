#!/usr/bin/env node
// Entry point: prepends an UPPERCASE notice to the top of README.md on every
// branch of a repository (local + remote), committing and pushing one branch at
// a time. Branches that already carry the notice are left untouched and listed
// in the final Markdown report, so reruns are idempotent. A dry-run mode
// reports the plan without touching the repository.
import { resolveConfig, USAGE } from "./src/config.js";
import { run } from "./src/run.js";

function main() {
  let cfg;
  try {
    cfg = resolveConfig(process.argv.slice(2));
  } catch (err) {
    process.stderr.write(`error: ${err.message}\n\n${USAGE}`);
    process.exit(2);
  }

  if (cfg.help) {
    process.stdout.write(USAGE);
    return;
  }

  try {
    run(cfg);
  } catch (err) {
    process.stderr.write(`error: ${err.message}\n`);
    process.exit(1);
  }
}

main();
