// Markdown rendering of the run results, grouped by outcome so the branches
// that already carry the notice are visible at a glance. Pure: takes config +
// results, returns a string.

const SECTIONS = [
  { status: "already", title: "Branches que já têm o aviso" },
  { status: "updated", titleExec: "Branches atualizadas", titleDry: "Branches que seriam atualizadas" },
  { status: "missing", title: "Branches sem o arquivo" },
  { status: "error", title: "Branches com erro" },
];

// renderReport produces a human-readable Markdown report for all branches.
export function renderReport(cfg, results) {
  const lines = [];

  const mode = cfg.dryRun
    ? "DRY-RUN (nenhuma escrita, commit ou push)"
    : "EXECUÇÃO";

  lines.push("# Relatório — aviso de suporte no README", "");
  lines.push(`- Modo: ${mode}`);
  lines.push(`- Arquivo: \`${cfg.file}\``);
  lines.push(`- Mensagem: ${cfg.message}`);
  lines.push(`- Padrão de branches: \`${cfg.pattern.source}\``);
  lines.push(`- Branches analisadas: ${results.length}`);
  lines.push("");

  for (const section of SECTIONS) {
    const rows = results.filter((r) => r.status === section.status);
    const title = section.title ?? (cfg.dryRun ? section.titleDry : section.titleExec);
    lines.push(`## ${title} (${rows.length})`, "");
    if (rows.length === 0) {
      lines.push("- (nenhuma)", "");
      continue;
    }
    for (const r of rows) {
      lines.push(`- ${renderBranch(cfg, r)}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

function renderBranch(cfg, r) {
  switch (r.status) {
    case "already":
      return `\`${r.branch}\``;
    case "updated":
      if (cfg.dryRun) {
        return `\`${r.branch}\`: adicionaria o aviso no topo de \`${cfg.file}\``;
      }
      return `\`${r.branch}\`: commit ${r.commit}, ${r.pushed ? `push para ${cfg.remote}` : "NÃO enviado"}`;
    case "missing":
      return `\`${r.branch}\`: \`${cfg.file}\` inexistente (pulada)`;
    default:
      return `\`${r.branch}\`: ERRO: ${r.err}`;
  }
}
