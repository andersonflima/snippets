// Visão de arquitetura estilo "archdraw"/Excalidraw (hand-drawn via roughjs).
// Topologia: API Gateway (edge) -> VPC Link -> NLB interno -> EKS (microserviços)
// -> STS:AssumeRole nas contas PRD / Time / HOMOL.
// Saída: architecture-archdraw.svg -> rsvg-convert para 4K.
import rough from "roughjs";
import fs from "fs";

const W = 3840, H = 2160;
const gen = rough.generator({ options: { roughness: 1.5, bowing: 1.2 } });
const FONT = "Chalkboard SE, Comic Sans MS, Bradley Hand, sans-serif";

const out = [];
let seed = 1;
const o = (extra = {}) => ({ seed: seed++, ...extra });

function drawDrawable(d) {
  for (const p of gen.toPaths(d)) {
    const fill = p.fill && p.fill !== "none" ? p.fill : "none";
    out.push(
      `<path d="${p.d}" stroke="${p.stroke}" stroke-width="${p.strokeWidth}" ` +
      `fill="${fill}" stroke-linecap="round" stroke-linejoin="round"/>`
    );
  }
}

function rrPath(x, y, w, h, r) {
  return `M${x + r},${y} h${w - 2 * r} a${r},${r} 0 0 1 ${r},${r} v${h - 2 * r} ` +
    `a${r},${r} 0 0 1 ${-r},${r} h${-(w - 2 * r)} a${r},${r} 0 0 1 ${-r},${-r} ` +
    `v${-(h - 2 * r)} a${r},${r} 0 0 1 ${r},${-r} z`;
}

function box(x, y, w, h, { stroke = "#1e1e1e", fill = "none", fillStyle = "solid", sw = 2.5, r = 16, dash } = {}) {
  drawDrawable(gen.path(rrPath(x, y, w, h, r), o({
    stroke, strokeWidth: sw, fill: fill === "none" ? undefined : fill,
    fillStyle, hachureGap: 8, fillWeight: 2,
    ...(dash ? { strokeLineDash: dash } : {}),
  })));
}

function cylinder(x, y, w, h, { stroke = "#1971c2", fill = "#e7f5ff", sw = 2.5 } = {}) {
  const rx = w / 2, ry = Math.min(28, h * 0.16);
  drawDrawable(gen.path(
    `M${x},${y + ry} L${x},${y + h - ry} A${rx},${ry} 0 0 0 ${x + w},${y + h - ry} L${x + w},${y + ry}`,
    o({ stroke, strokeWidth: sw, fill, fillStyle: "solid" })
  ));
  drawDrawable(gen.ellipse(x + rx, y + ry, w, ry * 2, o({ stroke, strokeWidth: sw, fill, fillStyle: "solid" })));
}

function keyIcon(cx, cy, color = "#e8590c") {
  drawDrawable(gen.circle(cx, cy, 46, o({ stroke: color, strokeWidth: 3, fill: "#fff4e6", fillStyle: "solid" })));
  drawDrawable(gen.circle(cx, cy, 16, o({ stroke: color, strokeWidth: 2.5 })));
  drawDrawable(gen.line(cx + 22, cy, cx + 74, cy, o({ stroke: color, strokeWidth: 3 })));
  drawDrawable(gen.line(cx + 60, cy, cx + 60, cy + 16, o({ stroke: color, strokeWidth: 3 })));
  drawDrawable(gen.line(cx + 74, cy, cx + 74, cy + 20, o({ stroke: color, strokeWidth: 3 })));
}

function person(cx, cy, color = "#2f9e44") {
  drawDrawable(gen.circle(cx, cy - 18, 34, o({ stroke: color, strokeWidth: 3, fill: "#ebfbee", fillStyle: "solid" })));
  drawDrawable(gen.path(`M${cx - 34},${cy + 44} a34,30 0 0 1 68,0`, o({ stroke: color, strokeWidth: 3, fill: "#ebfbee", fillStyle: "solid" })));
}

function wheel(cx, cy, color, r0 = 52, fill = "#f3f0ff") {
  drawDrawable(gen.circle(cx, cy, r0, o({ stroke: color, strokeWidth: 3, fill, fillStyle: "solid" })));
  drawDrawable(gen.circle(cx, cy, r0 * 0.4, o({ stroke: color, strokeWidth: 2.5 })));
  for (let i = 0; i < 8; i++) {
    const a = (i * Math.PI) / 4;
    drawDrawable(gen.line(cx + Math.cos(a) * r0 * 0.5, cy + Math.sin(a) * r0 * 0.5, cx + Math.cos(a) * r0 * 0.78, cy + Math.sin(a) * r0 * 0.78, o({ stroke: color, strokeWidth: 3 })));
  }
}

function arrow(x1, y1, x2, y2, { color = "#343a40", sw = 3, dash } = {}) {
  drawDrawable(gen.line(x1, y1, x2, y2, o({ stroke: color, strokeWidth: sw, ...(dash ? { strokeLineDash: dash } : {}) })));
  const a = Math.atan2(y2 - y1, x2 - x1), hl = 22, ang = 0.42;
  drawDrawable(gen.line(x2, y2, x2 - hl * Math.cos(a - ang), y2 - hl * Math.sin(a - ang), o({ stroke: color, strokeWidth: sw })));
  drawDrawable(gen.line(x2, y2, x2 - hl * Math.cos(a + ang), y2 - hl * Math.sin(a + ang), o({ stroke: color, strokeWidth: sw })));
}

function polyArrow(pts, { color = "#343a40", sw = 3, dash } = {}) {
  for (let i = 0; i < pts.length - 1; i++) {
    drawDrawable(gen.line(pts[i][0], pts[i][1], pts[i + 1][0], pts[i + 1][1], o({ stroke: color, strokeWidth: sw, ...(dash ? { strokeLineDash: dash } : {}) })));
  }
  const [px, py] = pts[pts.length - 2], [x2, y2] = pts[pts.length - 1];
  const a = Math.atan2(y2 - py, x2 - px), hl = 22, ang = 0.42;
  drawDrawable(gen.line(x2, y2, x2 - hl * Math.cos(a - ang), y2 - hl * Math.sin(a - ang), o({ stroke: color, strokeWidth: sw })));
  drawDrawable(gen.line(x2, y2, x2 - hl * Math.cos(a + ang), y2 - hl * Math.sin(a + ang), o({ stroke: color, strokeWidth: sw })));
}

function text(x, y, s, { size = 28, fill = "#1e1e1e", weight = "normal", anchor = "start" } = {}) {
  out.push(`<text x="${x}" y="${y}" font-family="${FONT}" font-size="${size}" font-weight="${weight}" fill="${fill}" text-anchor="${anchor}">${s.replace(/&/g, "&amp;").replace(/</g, "&lt;")}</text>`);
}

function numTag(x, y, n, color) {
  drawDrawable(gen.circle(x, y, 38, o({ stroke: "#fff", strokeWidth: 2, fill: color, fillStyle: "solid" })));
  text(x, y + 9, String(n), { size: 24, fill: "#fff", weight: "bold", anchor: "middle" });
}

// ---------------------------------------------------------------------------
out.push(`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">`);
out.push(`<defs><pattern id="dots" width="46" height="46" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="2" fill="#e9ecef"/></pattern></defs>`);
out.push(`<rect width="${W}" height="${H}" fill="#fffef7"/>`);
out.push(`<rect width="${W}" height="${H}" fill="url(#dots)"/>`);

text(70, 86, "Pipeline de Mascaramento — Arquitetura (PRD → HOMOL)", { size: 52, fill: "#1e1e1e", weight: "bold" });
text(72, 130, "microserviços action-driven no EKS · API Gateway + VPC Link (NLB interno) · assume-role cross-account", { size: 26, fill: "#495057" });

// === Conta da Plataforma (EKS) =============================================
const PLAT_Y = 168, PLAT_H = 486;
box(70, PLAT_Y, 3700, PLAT_H, { stroke: "#3b5bdb", fill: "#edf2ff", r: 26, sw: 3.5, dash: [16, 12] });
box(96, PLAT_Y + 22, 560, 66, { stroke: "#3b5bdb", fill: "#3b5bdb", r: 14, sw: 2 });
text(122, PLAT_Y + 66, "Conta da Plataforma (DataDevOps)", { size: 28, fill: "#fff", weight: "bold" });

// entrada: o cliente atua no frontend, que dispara cada ação
box(108, PLAT_Y + 118, 360, 120, { stroke: "#868e96", fill: "#ffffff", r: 16 });
person(192, PLAT_Y + 176, "#868e96");
text(338, PLAT_Y + 168, "Cliente", { size: 26, fill: "#495057", weight: "bold", anchor: "middle" });
text(338, PLAT_Y + 204, "(time)", { size: 22, fill: "#868e96", anchor: "middle" });
box(108, PLAT_Y + 285, 360, 150, { stroke: "#e8590c", fill: "#fff4e6", r: 16 });
text(288, PLAT_Y + 338, "Frontend (web)", { size: 26, fill: "#d9480f", weight: "bold", anchor: "middle" });
text(288, PLAT_Y + 376, "ação do cliente", { size: 23, fill: "#495057", anchor: "middle" });
text(288, PLAT_Y + 410, "dispara cada passo", { size: 23, fill: "#495057", anchor: "middle" });

// API Gateway
box(516, PLAT_Y + 150, 380, 230, { stroke: "#0c8599", fill: "#e3fafc", r: 18, sw: 3 });
text(706, PLAT_Y + 210, "API Gateway", { size: 30, fill: "#0b7285", weight: "bold", anchor: "middle" });
text(706, PLAT_Y + 250, "REST · edge", { size: 24, fill: "#0b7285", anchor: "middle" });
text(706, PLAT_Y + 300, "auth Cognito (JWT)", { size: 23, fill: "#495057", anchor: "middle" });
text(706, PLAT_Y + 338, "1 path por ação", { size: 23, fill: "#495057", anchor: "middle" });

// VPC Link
box(940, PLAT_Y + 200, 280, 130, { stroke: "#1971c2", fill: "#e7f5ff", r: 16 });
text(1080, PLAT_Y + 258, "VPC Link", { size: 27, fill: "#1864ab", weight: "bold", anchor: "middle" });
text(1080, PLAT_Y + 296, "(privado)", { size: 22, fill: "#495057", anchor: "middle" });

// NLB interno
box(1264, PLAT_Y + 200, 300, 130, { stroke: "#1971c2", fill: "#d0ebff", r: 16 });
text(1414, PLAT_Y + 258, "NLB interno", { size: 27, fill: "#1864ab", weight: "bold", anchor: "middle" });
text(1414, PLAT_Y + 296, "(internal-only)", { size: 22, fill: "#495057", anchor: "middle" });

// EKS cluster
box(1610, PLAT_Y + 96, 2140, 366, { stroke: "#5f3dc4", fill: "#f8f0fc", r: 20, sw: 3 });
wheel(1700, PLAT_Y + 150, "#5f3dc4", 36, "#eebefa");
text(1760, PLAT_Y + 142, "EKS — cluster dos microserviços action-driven", { size: 30, fill: "#862e9c", weight: "bold" });
text(1760, PLAT_Y + 178, "NLB interno → Service → pods · cada serviço: requirements.txt + Dockerfile próprios (sem packages compartilhadas)", { size: 21, fill: "#5c5f66" });
const svcs = [
  ["restore", "#2f9e44"], ["db-password", "#0c8599"], ["kms", "#e8590c"], ["replicate", "#7048e8"], ["vpc-link", "#1971c2"],
  ["modify", "#f08c00"], ["create", "#37b24d"], ["destroy", "#e03131"], ["start/stop", "#1098ad"], ["storage", "#9c36b5"],
];
const cw = 388, chh = 82, gx = 1650, gy = PLAT_Y + 200;
svcs.forEach((s, i) => {
  const col = i % 5, row = Math.floor(i / 5);
  const x = gx + col * (cw + 8), y = gy + row * (chh + 14);
  box(x, y, cw, chh, { stroke: s[1], fill: "#ffffff", r: 12, sw: 2.5 });
  drawDrawable(gen.circle(x + 36, y + chh / 2, 18, o({ stroke: s[1], strokeWidth: 3, fill: s[1], fillStyle: "solid" })));
  text(x + 64, y + chh / 2 + 9, s[0], { size: 24, fill: s[1], weight: "bold" });
});

// chain arrows
arrow(288, PLAT_Y + 240, 288, PLAT_Y + 283, { color: "#868e96", sw: 3 });
arrow(468, PLAT_Y + 360, 512, PLAT_Y + 300, { color: "#e8590c", sw: 3.5 });
arrow(896, PLAT_Y + 265, 936, PLAT_Y + 265, { color: "#0c8599", sw: 3.5 });
arrow(1220, PLAT_Y + 265, 1260, PLAT_Y + 265, { color: "#1971c2", sw: 3.5 });
arrow(1564, PLAT_Y + 265, 1606, PLAT_Y + 265, { color: "#1971c2", sw: 3.5 });
text(486, PLAT_Y + 332, "HTTPS", { size: 19, fill: "#868e96" });

// === Contas alvo ============================================================
const AY = 720, AH = 1300;
function account(x, y, w, h, color, fill, label, tag) {
  box(x, y, w, h, { stroke: color, fill, r: 26, sw: 3.5, dash: [16, 12] });
  box(x + 26, y + 24, 520, 70, { stroke: color, fill: color, r: 14, sw: 2 });
  text(x + 52, y + 70, label, { size: 32, fill: "#ffffff", weight: "bold" });
  text(x + w - 30, y + 66, tag, { size: 24, fill: color, anchor: "end" });
}

// PRD
account(70, AY, 1180, AH, "#e03131", "#fff5f5", "Conta PRD", "Produção");
text(110, AY + 150, "VPC", { size: 26, fill: "#e03131", weight: "bold" });
box(110, AY + 168, 1100, 1100, { stroke: "#e03131", fill: "none", r: 18, sw: 2, dash: [10, 9] });
cylinder(180, AY + 230, 360, 180, { stroke: "#1971c2" });
text(360, AY + 450, "RDS DB Real (prod)", { size: 26, fill: "#0b5394", anchor: "middle", weight: "bold" });
cylinder(180, AY + 510, 360, 180, { stroke: "#1971c2" });
text(360, AY + 730, "RDS DB Cópia", { size: 26, fill: "#0b5394", anchor: "middle", weight: "bold" });
numTag(520, AY + 525, 1, "#2f9e44"); numTag(520, AY + 585, 2, "#0c8599");
box(660, AY + 230, 510, 150, { stroke: "#1971c2", fill: "#e7f5ff", r: 14 });
text(915, AY + 295, "PrivateLink", { size: 28, fill: "#1971c2", anchor: "middle", weight: "bold" });
text(915, AY + 335, "endpoint p/ a conta do time", { size: 22, fill: "#495057", anchor: "middle" });
numTag(1140, AY + 240, 3, "#1971c2");
box(660, AY + 420, 510, 150, { stroke: "#7048e8", fill: "#f3f0ff", r: 14 });
text(915, AY + 485, "Snapshot Mascarado", { size: 27, fill: "#5f3dc4", anchor: "middle", weight: "bold" });
text(915, AY + 525, "do DB Cópia", { size: 22, fill: "#495057", anchor: "middle" });
numTag(1140, AY + 430, 6, "#2f9e44");
box(660, AY + 610, 510, 180, { stroke: "#e8590c", fill: "#fff4e6", r: 14 });
keyIcon(760, AY + 700, "#e8590c");
text(950, AY + 690, "Custom KMS", { size: 27, fill: "#d9480f", anchor: "middle", weight: "bold" });
text(950, AY + 730, "(substitui default)", { size: 22, fill: "#495057", anchor: "middle" });
numTag(1140, AY + 620, 7, "#e8590c");
box(180, AY + 880, 990, 110, { stroke: "#e03131", fill: "#fff5f5", r: 14, sw: 2.5, dash: [9, 7] });
numTag(255, AY + 935, 13, "#e03131");
text(330, AY + 925, "Cleanup PRD:", { size: 27, fill: "#c92a2a", weight: "bold" });
text(330, AY + 965, "destroy + stop + storage apagam os recursos temporários", { size: 23, fill: "#495057" });

// MASK
account(1320, AY, 1080, AH, "#1971c2", "#e7f5ff", "Conta do Time", "Mascaramento");
text(1360, AY + 150, "VPC", { size: 26, fill: "#1971c2", weight: "bold" });
box(1360, AY + 168, 1000, 1100, { stroke: "#1971c2", fill: "none", r: 18, sw: 2, dash: [10, 9] });
box(1410, AY + 240, 900, 220, { stroke: "#1971c2", fill: "#ffffff", r: 16 });
wheel(1500, AY + 350, "#1971c2", 50, "#d0ebff");
text(1600, AY + 330, "Ferramenta de", { size: 28, fill: "#1864ab", weight: "bold" });
text(1600, AY + 368, "Mascaramento", { size: 28, fill: "#1864ab", weight: "bold" });
text(1600, AY + 420, "(SaaS / EC2 contratada)", { size: 22, fill: "#495057" });
numTag(2270, AY + 250, 5, "#1971c2");
box(1410, AY + 510, 900, 150, { stroke: "#1971c2", fill: "#d0ebff", r: 14 });
text(1860, AY + 575, "PrivateLink consumer", { size: 27, fill: "#1864ab", anchor: "middle", weight: "bold" });
text(1860, AY + 615, "conecta direto no DB Cópia", { size: 22, fill: "#495057", anchor: "middle" });
numTag(1450, AY + 520, 4, "#868e96");
box(1410, AY + 710, 900, 180, { stroke: "#1971c2", fill: "#ffffff", r: 14 });
person(1520, AY + 790, "#1971c2");
text(1660, AY + 780, "Avalia mascaramento OK?", { size: 26, fill: "#1864ab", weight: "bold" });
text(1660, AY + 820, "dispara snapshot/kms/promote", { size: 22, fill: "#495057" });
numTag(2270, AY + 720, 8, "#1971c2");

// HOMOL
account(2460, AY, 1300, AH, "#2f9e44", "#ebfbee", "Conta HOMOL", "Homologação");
text(2500, AY + 150, "VPC", { size: 26, fill: "#2f9e44", weight: "bold" });
box(2500, AY + 168, 1220, 1100, { stroke: "#2f9e44", fill: "none", r: 18, sw: 2, dash: [10, 9] });
cylinder(2580, AY + 240, 420, 210, { stroke: "#1971c2" });
text(2790, AY + 500, "RDS DB HOMOL", { size: 27, fill: "#0b5394", anchor: "middle", weight: "bold" });
text(2790, AY + 538, "(restaurado, mascarado)", { size: 22, fill: "#495057", anchor: "middle" });
numTag(2980, AY + 250, 10, "#2f9e44"); numTag(2980, AY + 320, 11, "#0c8599");
box(3120, AY + 240, 510, 200, { stroke: "#e8590c", fill: "#fff4e6", r: 16 });
keyIcon(3230, AY + 340, "#e8590c");
text(3430, AY + 325, "Custom KMS", { size: 27, fill: "#d9480f", anchor: "middle", weight: "bold" });
text(3430, AY + 365, "(HOMOL)", { size: 23, fill: "#495057", anchor: "middle" });
numTag(3600, AY + 250, 9, "#7048e8");
box(2580, AY + 630, 1050, 240, { stroke: "#2f9e44", fill: "#ffffff", r: 16 });
person(2700, AY + 740, "#2f9e44");
text(2860, AY + 720, "Devs / QA", { size: 30, fill: "#2b8a3e", weight: "bold" });
text(2860, AY + 762, "testam com dados mascarados", { size: 24, fill: "#495057" });
text(2860, AY + 802, "(ambiente seguro, sem PII real)", { size: 22, fill: "#868e96" });
numTag(3560, AY + 640, 12, "#868e96");

// === Connectors entre EKS e contas (assume-role) ===========================
[[660, "#e03131"], [1860, "#1971c2"], [3110, "#2f9e44"]].forEach(([cx]) => {
  arrow(cx, PLAT_Y + PLAT_H, cx, AY - 2, { color: "#7048e8", sw: 3, dash: [12, 9] });
});
text(1872, AY - 12, "STS:AssumeRole (executa a ação no recurso)", { size: 22, fill: "#7048e8", weight: "bold" });

// PRD PrivateLink -> MASK consumer
polyArrow([[1170, AY + 305], [1295, AY + 305], [1295, AY + 585], [1405, AY + 585]], { color: "#1971c2", sw: 3.5 });
text(1208, AY + 270, "acesso privado", { size: 22, fill: "#1971c2", weight: "bold" });

// replicate cross-account: PRD snapshot -> HOMOL DB (gutter)
polyArrow([
  [1170, AY + 495], [1285, AY + 495], [1285, 2070], [2790, 2070], [2790, AY + 450],
], { color: "#7048e8", sw: 5 });
box(1810, 2034, 1000, 62, { stroke: "#7048e8", fill: "#ffffff", r: 12, sw: 2.5 });
text(1840, 2074, "⑨ replicate cross-account:  Snapshot Mascarado + Custom KMS Key → HOMOL", { size: 24, fill: "#5f3dc4", weight: "bold" });

out.push("</svg>");

const dir = new URL(".", import.meta.url).pathname;
fs.writeFileSync(dir + "architecture-archdraw.svg", out.join("\n"));
console.log("wrote architecture-archdraw.svg", out.join("\n").length, "bytes");
