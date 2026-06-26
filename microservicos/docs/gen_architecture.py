#!/usr/bin/env python3
"""Gera o diagrama 4K (3840x2160) da arquitetura do pipeline de mascaramento.

Saida: architecture.svg (vetorial) -> renderizar com rsvg-convert para 4K PNG.
Sem dependencias externas: apenas string building de SVG.
"""
from __future__ import annotations

from html import escape

W, H = 3840, 2160

# ----------------------------------------------------------------------------
# Paleta (alinhada ao tema neo de diagrams/platform.mmd)
# ----------------------------------------------------------------------------
INK = "#1A237E"
SUB = "#5B6472"
ACTION = {"fill": "#F2FBF5", "stroke": "#10B981", "text": "#0F5132"}
DATA = {"fill": "#EFF5FF", "stroke": "#2563EB", "text": "#0D47A1"}
KMS = {"fill": "#FFF6EC", "stroke": "#EA580C", "text": "#7C2D12"}
PRD = {"fill": "#FFF4F4", "stroke": "#E53935", "head": "#E53935", "text": "#9B1C1C"}
MASK = {"fill": "#F2F7FE", "stroke": "#1565C0", "head": "#1565C0", "text": "#0D47A1"}
HOMOL = {"fill": "#F1FAF3", "stroke": "#2E7D32", "head": "#2E7D32", "text": "#1B5E20"}
ORCH = {"fill": "#EDE7F6", "stroke": "#5E35B1", "head": "#5E35B1"}
STS = "#7E22CE"
BADGE = "#1A237E"

SVC_COLOR = {
    "restore": "#10B981",
    "db-password": "#0D9488",
    "kms": "#EA580C",
    "vpc-link": "#2563EB",
    "replicate": "#7E22CE",
    "destroy": "#DC2626",
    "notify": "#64748B",
    "mask": "#1565C0",
}

P: list[str] = []


def add(s: str) -> None:
    P.append(s)


def rect(x, y, w, h, fill, stroke, rx=14, sw=2.5, dash=None, shadow=True, opacity=1.0):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    sh = ' filter="url(#sh)"' if shadow else ""
    add(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{rx}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{d}{sh} opacity="{opacity}"/>'
    )


def text(x, y, s, size=26, fill="#1F2937", weight="normal", anchor="start", spacing=None):
    ls = f' letter-spacing="{spacing}"' if spacing else ""
    add(
        f'<text x="{x}" y="{y}" font-family="Helvetica, Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" '
        f'text-anchor="{anchor}"{ls}>{escape(s)}</text>'
    )


def badge(cx, cy, n, color=BADGE, r=24):
    add(f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{color}" stroke="#FFFFFF" stroke-width="3"/>')
    text(cx, cy + 9, str(n), size=26, fill="#FFFFFF", weight="bold", anchor="middle")


def line(x1, y1, x2, y2, color, sw=4, dash=None, marker="arrow"):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    m = f' marker-end="url(#{marker})"' if marker else ""
    add(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"{d}{m}/>')


def polyline(pts, color, sw=5, dash=None, marker="arrow"):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    m = f' marker-end="url(#{marker})"' if marker else ""
    pstr = " ".join(f"{x},{y}" for x, y in pts)
    add(f'<polyline points="{pstr}" fill="none" stroke="{color}" stroke-width="{sw}"{d}{m}/>')


def node(x, y, w, h, title, lines, palette, badges=None):
    rect(x, y, w, h, palette["fill"], palette["stroke"], rx=14, sw=3)
    tx = x + 26
    text(tx, y + 44, title, size=29, fill=palette["text"], weight="bold")
    for i, ln in enumerate(lines):
        text(tx, y + 80 + i * 30, ln, size=23, fill="#475569")
    if badges:
        for j, (bn, bc) in enumerate(badges):
            badge(x + w - 36 - j * 56, y + 36, bn, color=bc, r=23)


# ----------------------------------------------------------------------------
# Header / defs
# ----------------------------------------------------------------------------
add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">')
add(
    '<defs>'
    '<marker id="arrow" markerWidth="12" markerHeight="12" refX="9" refY="5" orient="auto" markerUnits="userSpaceOnUse">'
    '<path d="M0,0 L11,5 L0,10 z" fill="context-stroke"/></marker>'
    '<marker id="arrowP" markerWidth="16" markerHeight="16" refX="11" refY="6" orient="auto" markerUnits="userSpaceOnUse">'
    f'<path d="M0,0 L14,6 L0,12 z" fill="{STS}"/></marker>'
    '<filter id="sh" x="-6%" y="-6%" width="112%" height="118%">'
    '<feDropShadow dx="0" dy="4" stdDeviation="6" flood-color="#0B1B3A" flood-opacity="0.16"/></filter>'
    '</defs>'
)
add(f'<rect x="0" y="0" width="{W}" height="{H}" fill="#FFFFFF"/>')
add(f'<rect x="0" y="0" width="{W}" height="172" fill="#F7F8FC"/>')
add(f'<line x1="0" y1="172" x2="{W}" y2="172" stroke="#E2E5EE" stroke-width="2"/>')

text(56, 78, "Pipeline de Mascaramento de Dados — Cópia PRD → HOMOL", size=52, fill=INK, weight="bold")
text(
    58, 126,
    "Self-service de restore orquestrando microserviços action-driven (assume-role cross-account). "
    "Cada serviço age sobre qualquer recurso AWS que aceite a ação.",
    size=25, fill=SUB,
)

# Legend (header right)
lx = 2760
add(f'<rect x="{lx}" y="34" width="1024" height="118" rx="12" fill="#FFFFFF" stroke="#E2E5EE" stroke-width="2"/>')
def leg_swatch(x, y, fill, stroke):
    add(f'<rect x="{x}" y="{y}" width="34" height="24" rx="5" fill="{fill}" stroke="{stroke}" stroke-width="2.5"/>')
leg_swatch(lx + 22, 54, PRD["fill"], PRD["stroke"]); text(lx + 66, 73, "Conta AWS", size=21, fill="#475569")
leg_swatch(lx + 22, 96, DATA["fill"], DATA["stroke"]); text(lx + 66, 115, "Recurso / dado", size=21, fill="#475569")
leg_swatch(lx + 250, 54, ACTION["fill"], ACTION["stroke"]); text(lx + 294, 73, "Microserviço (ação)", size=21, fill="#475569")
leg_swatch(lx + 250, 96, KMS["fill"], KMS["stroke"]); text(lx + 294, 115, "Chave KMS", size=21, fill="#475569")
line(lx + 540, 65, lx + 620, 65, "#334155", sw=4); text(lx + 632, 73, "Fluxo do pipeline", size=21, fill="#475569")
line(lx + 540, 107, lx + 620, 107, STS, sw=4, dash="9,6", marker="arrowP"); text(lx + 632, 115, "STS:AssumeRole", size=21, fill="#475569")
text(lx + 840, 73, "①..⑬", size=22, fill=INK, weight="bold"); text(lx + 840, 115, "ordem de execução", size=20, fill="#475569")

# ----------------------------------------------------------------------------
# Left panel: action-driven microservices
# ----------------------------------------------------------------------------
PX, PY, PW, PH = 44, 196, 768, 1080
rect(PX, PY, PW, PH, ACTION["fill"], ACTION["stroke"], rx=18, sw=3.5)
text(PX + 28, PY + 50, "Microserviços Action-Driven", size=32, fill=ACTION["text"], weight="bold")
text(PX + 28, PY + 86, "Mapeamos AÇÕES (não recursos). Cada ação roda sobre qualquer", size=21, fill="#475569")
text(PX + 28, PY + 114, "recurso compatível, respeitando contrato e parâmetros.", size=21, fill="#475569")

services = [
    ("restore", "restaura snapshot → instância;", "também cria snapshot"),
    ("db-password", "conecta no banco e troca a", "senha do usuário informado"),
    ("kms", "cria Custom KMS Key e vincula/", "re-encripta (substitui a default)"),
    ("replicate", "copia recurso cross-account ou", "recria em outra region"),
    ("vpc-link", "cria acesso privado da conta", "do time ao banco"),
    ("modify", "modify genérico: instance class,", "engine version e afins"),
    ("create", "provisiona recursos", "(contrato por tipo)"),
    ("destroy", "remove recursos", "(cleanup pós-fluxo)"),
    ("start / stop", "liga / desliga recursos", "que suportam power"),
    ("storage", "tipo de storage e", "aumento de tamanho"),
]
cw, ch = 354, 158
gx, gy = PX + 24, PY + 156
for i, (name, l1, l2) in enumerate(services):
    col, row = i % 2, i // 2
    x = gx + col * (cw + 12)
    y = gy + row * (ch + 14)
    rect(x, y, cw, ch, "#FFFFFF", ACTION["stroke"], rx=12, sw=2.5)
    add(f'<rect x="{x}" y="{y}" width="10" height="{ch}" rx="5" fill="{SVC_COLOR.get(name.split()[0], ACTION["stroke"])}"/>')
    text(x + 28, y + 46, name, size=27, fill=ACTION["text"], weight="bold")
    text(x + 28, y + 84, l1, size=21, fill="#475569")
    text(x + 28, y + 112, l2, size=21, fill="#475569")
    text(x + 28, y + 142, "requirements.txt · Dockerfile", size=18, fill="#94A3B8", weight="bold")

note_y = gy + 5 * (ch + 14) + 4
add(f'<rect x="{PX + 24}" y="{note_y}" width="{PW - 48}" height="70" rx="12" fill="#FFF8E1" stroke="#F59E0B" stroke-width="2.5"/>')
text(PX + 44, note_y + 32, "Sem packages compartilhadas — cada serviço é autocontido.", size=22, fill="#7C5A00", weight="bold")
text(PX + 44, note_y + 58, "Entrada: conta AWS + nome do recurso + role p/ assume-role.", size=21, fill="#7C5A00")

# ----------------------------------------------------------------------------
# Orchestrator bar
# ----------------------------------------------------------------------------
OX, OY, OW, OH = 840, 196, 2952, 116
rect(OX, OY, OW, OH, ORCH["fill"], ORCH["stroke"], rx=16, sw=3.5)
text(OX + 32, OY + 50, "Orquestrador / Control Plane", size=32, fill=ORCH["head"], weight="bold")
text(OX + 32, OY + 88, "Step Functions + Event Bus — sequencia as ações e recebe os triggers do time de mascaramento.", size=23, fill="#5B4B8A")
text(OX + OW - 32, OY + 50, "invoca os serviços", size=23, fill="#5B4B8A", weight="bold", anchor="end")
text(OX + OW - 32, OY + 84, "via STS:AssumeRole", size=23, fill=STS, weight="bold", anchor="end")

# panel -> orchestrator (invoca)
polyline([(PX + PW, OY + OH // 2 + 40), (OX - 14, OY + OH // 2)], "#334155", sw=4)
text(PX + PW + 18, OY + OH // 2 + 78, "invoca", size=21, fill="#334155", weight="bold")

# ----------------------------------------------------------------------------
# Account containers
# ----------------------------------------------------------------------------
AY, ABOT = 360, 1300
AH = ABOT - AY


def account(x, w, pal, title, subtitle):
    rect(x, AY, w, AH, pal["fill"], pal["stroke"], rx=18, sw=4)
    add(f'<rect x="{x}" y="{AY}" width="{w}" height="72" rx="18" fill="{pal["head"]}"/>')
    add(f'<rect x="{x}" y="{AY + 40}" width="{w}" height="32" fill="{pal["head"]}"/>')
    text(x + 28, AY + 48, title, size=30, fill="#FFFFFF", weight="bold")
    text(x + w - 24, AY + 48, subtitle, size=22, fill="#FFFFFF", anchor="end")


PRD_X, PRD_W = 840, 940
MASK_X, MASK_W = 1816, 900
HOM_X, HOM_W = 2752, 1040

account(PRD_X, PRD_W, PRD, "Conta PRD (Produção)", "conta + role")
account(MASK_X, MASK_W, MASK, "Conta do Time de Mascaramento", "ferramenta contratada")
account(HOM_X, HOM_W, HOMOL, "Conta HOMOL (Homologação)", "devs / QA")

# orchestrator -> each account header (STS)
for cx in (PRD_X + PRD_W // 2, MASK_X + MASK_W // 2, HOM_X + HOM_W // 2):
    line(cx, OY + OH, cx, AY - 2, STS, sw=4, dash="9,7", marker="arrowP")
text(MASK_X + MASK_W // 2 + 14, AY - 18, "STS:AssumeRole", size=20, fill=STS, weight="bold")

# --- PRD nodes
nx, nw = PRD_X + 30, PRD_W - 60
row_y = [465, 623, 781, 939, 1097]
node(nx, row_y[0], nw, 130, "DB Real (Produção)", ["instância produtiva — origem da cópia"], DATA)
node(nx, row_y[1], nw, 130, "DB Cópia", ["restaurada a partir do DB real"], DATA,
     badges=[(2, SVC_COLOR["db-password"]), (1, SVC_COLOR["restore"])])
node(nx, row_y[2], nw, 130, "VPC-Link / PrivateLink", ["acesso privado p/ a conta do time"], DATA,
     badges=[(3, SVC_COLOR["vpc-link"])])
node(nx, row_y[3], nw, 130, "Snapshot Mascarado", ["snapshot do DB Cópia já mascarado"], DATA,
     badges=[(6, SVC_COLOR["restore"])])
node(nx, row_y[4], nw, 130, "Custom KMS Key (PRD)", ["substitui a default / herdada"], KMS,
     badges=[(7, SVC_COLOR["kms"])])

# restore: DB Real -> DB Cópia
line(nx + 60, row_y[0] + 130, nx + 60, row_y[1] - 2, SVC_COLOR["restore"], sw=4)

# --- MASK nodes
mx, mw = MASK_X + 28, MASK_W - 56
node(mx, 465, mw, 130, "Ferramenta de Mascaramento", ["mascara os dados produtivos"], MASK)
node(mx, 623, mw, 130, "Recebe acesso", ["endpoint + credenciais do DB Cópia"], MASK,
     badges=[(4, SVC_COLOR["notify"])])
node(mx, 781, mw, 130, "Conecta e mascara", ["trabalha direto via VPC-Link"], MASK,
     badges=[(5, SVC_COLOR["mask"])])
node(mx, 939, mw, 130, "Mascaramento OK?", ["dispara: snapshot ⑥ · kms ⑦ · promote ⑨"], MASK,
     badges=[(8, SVC_COLOR["mask"])])

# VPC-link (PRD) <-> Conecta (MASK)
line(PRD_X + PRD_W, row_y[2] + 65, MASK_X + 2, 781 + 65, DATA["stroke"], sw=4, marker="arrow")
text(PRD_X + PRD_W + 8, row_y[2] + 40, "acesso", size=19, fill=DATA["stroke"], weight="bold")
text(PRD_X + PRD_W + 8, row_y[2] + 64, "privado", size=19, fill=DATA["stroke"], weight="bold")

# --- HOMOL nodes
hx, hw = HOM_X + 30, HOM_W - 60
node(hx, 465, hw, 130, "Custom KMS Key (HOMOL)", ["chave da conta de homologação"], KMS,
     badges=[(9, SVC_COLOR["replicate"])])
node(hx, 623, hw, 130, "DB HOMOL (restaurado)", ["snapshot mascarado restaurado"], DATA,
     badges=[(11, SVC_COLOR["db-password"]), (10, SVC_COLOR["restore"])])
node(hx, 781, hw, 130, "Devs / QA", ["testam com dados mascarados"], HOMOL,
     badges=[(12, SVC_COLOR["notify"])])

# --- replicate cross-account: PRD snapshot -> HOMOL DB (rota pelos gaps)
rp = [
    (nx + nw, row_y[3] + 65),      # saida snapshot (direita)
    (PRD_X + PRD_W + 18, row_y[3] + 65),
    (PRD_X + PRD_W + 18, 1255),
    (MASK_X + MASK_W + 18, 1255),  # gap MASK/HOMOL
    (MASK_X + MASK_W + 18, 623 + 65),
    (HOM_X - 2, 623 + 65),         # entra DB HOMOL pela esquerda
]
polyline(rp, SVC_COLOR["replicate"], sw=6, marker="arrow")
add(f'<rect x="1980" y="1226" width="700" height="58" rx="10" fill="#FFFFFF" stroke="{SVC_COLOR["replicate"]}" stroke-width="2.5"/>')
text(2000, 1252, "⑨ replicate cross-account:", size=22, fill=SVC_COLOR["replicate"], weight="bold")
text(2000, 1276, "Snapshot Mascarado + Custom KMS Key → HOMOL", size=21, fill="#475569")

# ----------------------------------------------------------------------------
# Bottom timeline: phases + numbered steps
# ----------------------------------------------------------------------------
TY = 1336
text(60, TY + 36, "Fluxo de execução do pipeline", size=34, fill=INK, weight="bold")

phases = [
    ("1 · PRD — Provisionar cópia", PRD["stroke"], 900, [
        ("1", "restore", "restaura snapshot do DB real → DB Cópia (PRD)"),
        ("2", "db-password", "troca a senha do usuário no DB Cópia"),
        ("3", "vpc-link", "cria acesso privado p/ a conta do time"),
        ("4", "notify", "notifica o time: pode conectar (endpoint+cred)"),
    ]),
    ("2 · Time — Mascarar dados", MASK["stroke"], 916, [
        ("5", "mask", "time conecta e mascara os dados produtivos"),
        ("6", "restore", "create-snapshot do DB Cópia mascarado"),
        ("7", "kms", "cria Custom KMS Key e re-encripta o snapshot"),
        ("8", "mask", "avalia: mascaramento OK? → dispara promoção"),
    ]),
    ("3 · Promover", STS, 470, [
        ("9", "replicate", "leva Snapshot + KMS Key de PRD → HOMOL"),
    ]),
    ("4 · HOMOL — Entregar", HOMOL["stroke"], 740, [
        ("10", "restore", "restaura snapshot em DB HOMOL"),
        ("11", "db-password", "troca a senha do usuário no DB HOMOL"),
        ("12", "notify", "notifica devs: banco restaurado em HOMOL"),
    ]),
    ("5 · Cleanup PRD", PRD["stroke"], 540, [
        ("13", "destroy", "destroy (+stop/storage): apaga recursos temporários"),
    ]),
]

bx = 60
band_y = TY + 64
band_h = 720
for ptitle, pcolor, pw, steps in phases:
    rect(bx, band_y, pw, band_h, "#FFFFFF", pcolor, rx=16, sw=3)
    add(f'<rect x="{bx}" y="{band_y}" width="{pw}" height="60" rx="16" fill="{pcolor}"/>')
    add(f'<rect x="{bx}" y="{band_y + 30}" width="{pw}" height="30" fill="{pcolor}"/>')
    text(bx + 22, band_y + 40, ptitle, size=25, fill="#FFFFFF", weight="bold")
    sy = band_y + 110
    for num, svc, desc in steps:
        badge(bx + 50, sy, num, color=SVC_COLOR.get(svc, BADGE), r=27)
        text(bx + 92, sy - 6, svc, size=23, fill=SVC_COLOR.get(svc, BADGE), weight="bold")
        # wrap desc to width
        words = desc.split()
        lines_w, cur = [], ""
        maxc = int((pw - 120) / 12.2)
        for wd in words:
            if len(cur) + len(wd) + 1 <= maxc:
                cur = (cur + " " + wd).strip()
            else:
                lines_w.append(cur); cur = wd
        if cur:
            lines_w.append(cur)
        for k, lw in enumerate(lines_w[:2]):
            text(bx + 92, sy + 24 + k * 26, lw, size=21, fill="#475569")
        sy += 150
    # arrow to next band
    bx_next = bx + pw + 18
    if ptitle != phases[-1][0]:
        line(bx + pw + 1, band_y + band_h // 2, bx_next - 3, band_y + band_h // 2, "#94A3B8", sw=5)
    bx = bx_next

add('</svg>')

out = __file__.rsplit("/", 1)[0] + "/architecture.svg"
with open(out, "w") as f:
    f.write("".join(P))
print("wrote", out, len("".join(P)), "bytes")
