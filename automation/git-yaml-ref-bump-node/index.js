#!/usr/bin/env node
// Entry point: replaces a versioned reference (e.g. build.yml@v2 -> build.yml@v3)
// inside YAML files across a set of release branches (v1..v6 and their minors,
// by default). Target files are selected by name pattern within a configurable
// directory (default .github/workflows) and resolved per branch, so the same
// file can live at different paths on different branches. It commits one file
// per change and pushes once per branch. A dry-run mode reports the planned
// changes without touching the repository.
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
