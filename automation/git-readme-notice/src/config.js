// CLI parsing and validation. Produces a frozen config object consumed by the
// rest of the pipeline. The notice message is always uppercased so the banner
// stays visually loud regardless of how the flag was typed.

export const DEFAULT_MESSAGE =
  "ATENÇÃO: QUALQUER ALTERAÇÃO REALIZADA NESTE TEMPLATE TERRAFORM REVOGA A " +
  "OBRIGAÇÃO DO TIME DATADEVOPS DE PRESTAR QUALQUER SUPORTE PARA EVENTUAIS " +
  "ERROS E PROBLEMAS DECORRENTES DE TAIS MUDANÇAS.";

const DEFAULTS = {
  repo: ".",
  file: "README.md",
  message: DEFAULT_MESSAGE,
  "commit-message": "docs: adiciona aviso de revogação de suporte do time DATADEVOPS no README",
  pattern: ".*",
  remote: "origin",
  "dry-run": false,
  fetch: true,
  report: "",
  quiet: false,
};

const BOOLEAN_FLAGS = new Set(["dry-run", "fetch", "quiet"]);

export const USAGE = `Usage: git-readme-notice [options]

Prepend an UPPERCASE support-waiver notice to the top of README.md on every
branch, committing and pushing per branch. Branches that already carry the
notice are skipped and listed in the report.

Options:
  --repo <path>            path to the target git repository (default ".")
  --file <path>            file to update on each branch (default "README.md")
  --message <text>         notice text; uppercased automatically (default: the
                           DATADEVOPS Terraform support-waiver notice)
  --commit-message <text>  commit message used on each branch
  --pattern <re>           regexp selecting target branches (default ".*", all)
  --remote <name>          git remote used for discovery and push (default "origin")
  --dry-run                report the plan without writing, committing or pushing
  --fetch                  run 'git fetch <remote> --prune' before discovery
                           (default true; disable with --no-fetch)
  --report <path>          optional path to also write the Markdown report to
  --quiet                  suppress the per-branch progress log on stderr
  -h, --help               show this help
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

// resolveConfig validates raw flags and returns the frozen run config.
export function resolveConfig(argv) {
  const raw = parseArgs(argv);
  if (raw.help) {
    return { help: true };
  }

  const message = raw.message.trim().toUpperCase();
  if (message === "") {
    throw new Error("--message must not be empty");
  }

  const file = raw.file.trim();
  if (file === "") {
    throw new Error("--file must not be empty");
  }

  let pattern;
  try {
    pattern = new RegExp(raw.pattern);
  } catch (err) {
    throw new Error(`invalid --pattern: ${err.message}`);
  }

  return Object.freeze({
    repoPath: raw.repo,
    file,
    message,
    commitMessage: raw["commit-message"],
    pattern,
    remote: raw.remote,
    dryRun: raw["dry-run"],
    fetch: raw.fetch,
    reportPath: raw.report,
    quiet: raw.quiet,
  });
}
