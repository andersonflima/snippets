// CLI parsing and validation. Produces a frozen config object consumed by the
// rest of the pipeline. Flags mirror the Go implementation one-to-one.

const DEFAULTS = {
  files: "",
  dir: ".github/workflows",
  "full-path": false,
  from: "build.yml@v2",
  to: "build.yml@v3",
  repo: ".",
  pattern: "^v[1-6](\\.[0-9]+)*$",
  remote: "origin",
  "dry-run": false,
  fetch: true,
  report: "",
};

const BOOLEAN_FLAGS = new Set(["full-path", "dry-run", "fetch"]);

export const USAGE = `Usage: git-yaml-ref-bump [--files <patterns>] [options]

Replace a versioned reference inside YAML files across release branches.

Options:
  --files <list>     comma-separated file-name patterns (regexp) to update,
                     e.g. 'build.yml,deploy.yml'; empty matches every file under --dir
  --dir <path>       directory searched for matching files (default ".github/workflows")
  --full-path        match --files patterns against the full repo-relative path
  --from <ref>       reference string to replace (default "build.yml@v2")
  --to <ref>         replacement reference string (default "build.yml@v3")
  --repo <path>      path to the target git repository (default ".")
  --pattern <re>     regexp selecting target branches (default "^v[1-6](\\.[0-9]+)*$")
  --remote <name>    git remote used for discovery and push (default "origin")
  --dry-run          report planned changes without writing, committing or pushing
  --fetch            run 'git fetch <remote>' before discovery (default true; disable with --no-fetch)
  --report <path>    optional path to also write the Markdown report to
  -h, --help         show this help
`;

// parseArgs reads process argv (without node/script) into a raw flag map,
// supporting --flag, --flag=value, --flag value, and --no-<bool> negation.
function parseArgs(argv) {
  const raw = { ...DEFAULTS };
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === "-h" || token === "--help") {
      raw.help = true;
      continue;
    }
    if (!token.startsWith("--")) {
      throw new Error(`unexpected argument: ${token}`);
    }

    let name = token.slice(2);
    let inlineValue;
    const eq = name.indexOf("=");
    if (eq !== -1) {
      inlineValue = name.slice(eq + 1);
      name = name.slice(0, eq);
    }

    if (name.startsWith("no-") && BOOLEAN_FLAGS.has(name.slice(3))) {
      raw[name.slice(3)] = false;
      continue;
    }
    if (!(name in DEFAULTS)) {
      throw new Error(`unknown flag: --${name}`);
    }
    if (BOOLEAN_FLAGS.has(name)) {
      raw[name] = inlineValue === undefined ? true : inlineValue !== "false";
      continue;
    }
    if (inlineValue !== undefined) {
      raw[name] = inlineValue;
      continue;
    }
    const next = argv[++i];
    if (next === undefined) {
      throw new Error(`flag --${name} requires a value`);
    }
    raw[name] = next;
  }
  return raw;
}

// splitFiles turns a comma-separated list into a trimmed, non-empty array.
function splitFiles(rawFiles) {
  return rawFiles
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p !== "");
}

// compileFilePatterns turns each --files entry into a full-match RegExp. A bare
// name like "build.yml" matches that file; ".*\\.ya?ml" matches a set of files.
function compileFilePatterns(entries) {
  return entries.map((entry) => {
    try {
      return new RegExp(`^(?:${entry})$`);
    } catch (err) {
      throw new Error(`invalid --files pattern "${entry}": ${err.message}`);
    }
  });
}

// resolveConfig validates raw flags and returns the frozen run config.
export function resolveConfig(argv) {
  const raw = parseArgs(argv);
  if (raw.help) {
    return { help: true };
  }

  const files = splitFiles(raw.files);
  const filePatterns = compileFilePatterns(files);

  let pattern;
  try {
    pattern = new RegExp(raw.pattern);
  } catch (err) {
    throw new Error(`invalid --pattern: ${err.message}`);
  }

  const dir = raw.dir.trim() === "" ? "." : raw.dir.trim();

  return Object.freeze({
    repoPath: raw.repo,
    files,
    filePatterns,
    dir,
    matchFullPath: raw["full-path"],
    from: raw.from,
    to: raw.to,
    pattern,
    remote: raw.remote,
    dryRun: raw["dry-run"],
    fetch: raw.fetch,
    reportPath: raw.report,
  });
}
