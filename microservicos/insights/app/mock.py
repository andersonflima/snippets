"""Geradores de dados sintéticos (modo INSIGHTS_MODE=mock).

Determinístico: cada recurso/série é semeado por um hash estável de
(produto, nome, ...), então a mesma chamada sempre devolve os mesmos números —
útil para o frontend renderizar dashboards previsíveis sem AWS real. Os textos de
recomendação/insight são em pt-BR e propositalmente plausíveis/acionáveis.
"""
from __future__ import annotations

import hashlib
import math
import random
from datetime import datetime, timedelta, timezone

from . import products as C

# --- infra determinística ----------------------------------------------------
def _rng(*parts) -> random.Random:
    key = "|".join(str(p) for p in parts)
    seed = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big")
    return random.Random(seed)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hexid(r: random.Random, n: int = 17) -> str:
    return "".join(r.choice("0123456789abcdef") for _ in range(n))


_NAMES: dict[str, list[str]] = {
    "rds": ["orders-db", "billing-db", "users-db", "analytics-db", "inventory-db",
            "payments-db", "catalog-db", "sessions-db", "audit-db", "reporting-db"],
    "ec2": ["api", "worker", "web", "scheduler", "bastion", "jenkins",
            "search", "batch", "gateway", "cache"],
    "ebs": ["orders-data", "billing-data", "app-logs", "backup", "es-data",
            "kafka-data", "prometheus", "artifacts", "scratch", "db-data"],
    "elb": ["public-api", "web", "internal", "checkout", "admin", "grpc", "legacy"],
    "eip": ["nat-a", "nat-b", "bastion", "vpn", "egress", "legacy-api"],
    "snapshot": ["orders-db", "billing-db", "users-db", "analytics-db",
                 "daily-backup", "pre-migration", "audit-db", "inventory-db"],
    "kms": ["rds", "ebs", "s3-app", "secrets", "app-logs", "backups", "sqs"],
    "vpc-endpoint": ["s3", "ecr-api", "ecr-dkr", "secretsmanager", "logs",
                     "sts", "dynamodb", "kms"],
}

_RDS_CLASSES = ["db.r6g.large", "db.t3.medium", "db.m5.large", "db.r5.xlarge",
                "db.r6g.xlarge", "db.t3.large", "db.m5.xlarge", "db.r5.large"]
_EC2_TYPES = ["t3.medium", "m5.large", "c5.xlarge", "t3.large", "m5.xlarge",
              "c5.large", "r5.large"]
_EBS_TYPES = ["gp3", "gp2", "io1"]

_RDS_SPECS = {"db.t3.medium": (2, 4), "db.t3.large": (2, 8), "db.m5.large": (2, 8),
              "db.m5.xlarge": (4, 16), "db.r5.large": (2, 16), "db.r5.xlarge": (4, 32),
              "db.r6g.large": (2, 16), "db.r6g.xlarge": (4, 32)}
_EC2_SPECS = {"t3.medium": (2, 4), "t3.large": (2, 8), "m5.large": (2, 8),
              "m5.xlarge": (4, 16), "c5.large": (2, 4), "c5.xlarge": (4, 8),
              "r5.large": (2, 16)}


# --- catálogo de recursos ----------------------------------------------------
def _identity(product: str, name: str, env: str, r: random.Random) -> tuple[str, str]:
    """Retorna (id, displayName) plausível por produto."""
    disp = f"{env}-{name}"
    if product == "rds":
        return disp, disp
    if product == "ec2":
        return f"i-{_hexid(r)}", disp
    if product == "ebs":
        return f"vol-{_hexid(r)}", disp
    if product == "elb":
        return f"{disp}-alb", f"{disp}-alb"
    if product == "eip":
        return f"eipalloc-{_hexid(r)}", disp
    if product == "snapshot":
        return f"snap-{_hexid(r)}", f"{disp}-snap"
    if product == "kms":
        return _hexid(r, 8) + "-" + _hexid(r, 4) + "-" + _hexid(r, 12), f"alias/{env}-{name}"
    return f"vpce-{_hexid(r)}", f"{disp}-endpoint"  # vpc-endpoint


def _sizing(product: str, r: random.Random) -> tuple[str, str, float]:
    """Retorna (type, size, monthlyCost)."""
    if product == "rds":
        klass = r.choice(_RDS_CLASSES)
        return klass, klass, C.PRICE_RDS.get(klass, 200.0)
    if product == "ec2":
        itype = r.choice(_EC2_TYPES)
        return itype, itype, C.PRICE_EC2.get(itype, 100.0)
    if product == "ebs":
        vtype = r.choice(_EBS_TYPES)
        gib = r.choice([50, 100, 200, 400, 500, 1000])
        return vtype, f"{gib} GiB", round(gib * C.PRICE_EBS_GB_MONTH.get(vtype, 0.15), 2)
    if product == "elb":
        lbtype = r.choice(["application", "network"])
        return lbtype, lbtype, C.PRICE_ELB_MONTH
    if product == "eip":
        return "standard", "-", C.PRICE_EIP_MONTH
    if product == "snapshot":
        gib = r.choice([50, 100, 200, 400, 800])
        stype = r.choice(["manual", "automated"])
        return stype, f"{gib} GiB", round(gib * C.PRICE_SNAPSHOT_GB_MONTH, 2)
    if product == "kms":
        return "SYMMETRIC_DEFAULT", "-", C.PRICE_KMS_MONTH
    etype = r.choice(["Interface", "Gateway"])  # vpc-endpoint
    return etype, etype, (0.0 if etype == "Gateway" else C.PRICE_VPC_ENDPOINT_MONTH)


def _status(product: str, r: random.Random) -> str:
    table = {
        "rds": ["available", "available", "available", "stopped", "backing-up"],
        "ec2": ["running", "running", "running", "stopped"],
        "ebs": ["in-use", "in-use", "available"],
        "elb": ["active", "active", "provisioning"],
        "eip": ["associated", "associated", "unassociated"],
        "snapshot": ["completed", "completed", "pending"],
        "kms": ["Enabled", "Enabled", "Disabled"],
        "vpc-endpoint": ["available", "available", "pendingAcceptance"],
    }
    return r.choice(table.get(product, ["available"]))


def _item(product: str, name: str, idx: int) -> dict:
    r = _rng(product, name, idx)
    env = C.ENVS[idx % len(C.ENVS)]
    rid, disp = _identity(product, name, env, r)
    typ, size, cost = _sizing(product, r)
    created = datetime.now(timezone.utc) - timedelta(days=r.randint(20, 900))
    util = round(r.uniform(3.0, 92.0), 1)
    tags = {
        "env": env,
        "team": r.choice(["orders", "billing", "platform", "data", "payments"]),
        "app": name.replace("-db", "").replace("-data", ""),
        "managed-by": "terraform",
    }
    return {
        "id": rid, "name": disp, "product": product, "type": typ, "env": env,
        "region": "sa-east-1", "status": _status(product, r), "size": size,
        "createdAt": _iso(created), "tags": tags,
        "monthlyCost": round(cost, 2), "utilizationPct": util,
    }


def catalog(product: str) -> list[dict]:
    """Lista estável de recursos de um produto (~6-10 itens)."""
    return [_item(product, name, i) for i, name in enumerate(_NAMES[product])]


def _all_catalog(products: list[str]) -> list[dict]:
    out: list[dict] = []
    for p in products:
        out.extend(catalog(p))
    return out


def _infer_product(resource_id: str | None) -> str:
    if not resource_id:
        return "rds"
    rid = resource_id.lower()
    prefixes = {"i-": "ec2", "vol-": "ebs", "snap-": "snapshot",
                "eipalloc-": "eip", "vpce-": "vpc-endpoint"}
    for pfx, prod in prefixes.items():
        if rid.startswith(pfx):
            return prod
    if "-alb" in rid or "-nlb" in rid:
        return "elb"
    if "alias/" in rid:
        return "kms"
    return "rds"


# --- action: resources -------------------------------------------------------
def resources(products: list[str], filters: dict | None, limit: int | None) -> dict:
    items = _all_catalog(products)
    f = filters or {}
    search = (f.get("search") or "").lower()
    if search:
        items = [it for it in items if search in it["id"].lower() or search in it["name"].lower()]
    if f.get("status"):
        items = [it for it in items if it["status"] == f["status"]]
    if f.get("env"):
        items = [it for it in items if it["env"] == f["env"]]
    if f.get("type"):
        items = [it for it in items if it["type"] == f["type"]]
    if f.get("tag"):
        tag = str(f["tag"]).lower()
        items = [it for it in items if any(tag in f"{k}:{v}".lower() for k, v in it["tags"].items())]
    total = len(items)
    if limit:
        items = items[:limit]
    return {
        "count": len(items), "total": total,
        "products": products, "items": items,
    }


# --- action: metrics ---------------------------------------------------------
_PROFILE: dict[str, dict] = {
    "CPUUtilization": {"base": 35, "amp": 22, "lo": 0, "hi": 100, "noise": 8},
    "FreeableMemory": {"base": 4.0e9, "amp": 6e8, "lo": 1e8, "hi": 8e9, "noise": 2e8},
    "DatabaseConnections": {"base": 45, "amp": 30, "lo": 0, "hi": 200, "noise": 10},
    "ReadIOPS": {"base": 800, "amp": 500, "lo": 0, "hi": 5000, "noise": 150},
    "WriteIOPS": {"base": 400, "amp": 300, "lo": 0, "hi": 5000, "noise": 120},
    "FreeStorageSpace": {"base": 2.0e10, "amp": 2e9, "lo": 1e9, "hi": 1e11, "noise": 5e8},
    "NetworkIn": {"base": 5e6, "amp": 3e6, "lo": 0, "hi": 1e8, "noise": 1e6},
    "NetworkOut": {"base": 4e6, "amp": 2.5e6, "lo": 0, "hi": 1e8, "noise": 1e6},
    "VolumeReadOps": {"base": 1200, "amp": 800, "lo": 0, "hi": 1e5, "noise": 200},
    "VolumeWriteOps": {"base": 900, "amp": 600, "lo": 0, "hi": 1e5, "noise": 200},
    "BurstBalance": {"base": 85, "amp": 12, "lo": 0, "hi": 100, "noise": 5},
    "RequestCount": {"base": 1500, "amp": 1200, "lo": 0, "hi": 1e5, "noise": 300},
    "TargetResponseTime": {"base": 0.12, "amp": 0.08, "lo": 0.01, "hi": 5, "noise": 0.03},
    "HTTPCode_Target_5XX_Count": {"base": 3, "amp": 4, "lo": 0, "hi": 500, "noise": 2},
}


def _stats(values: list[float]) -> dict:
    if not values:
        return {"avg": 0.0, "max": 0.0, "min": 0.0, "p95": 0.0}
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]
    return {
        "avg": round(sum(values) / len(values), 3),
        "max": round(max(values), 3),
        "min": round(min(values), 3),
        "p95": round(p95, 3),
    }


def _series(resource_id: str, spec: dict, lookback_min: int) -> dict:
    name = spec["metric"]
    prof = _PROFILE.get(name, {"base": 50, "amp": 20, "lo": 0, "hi": 100, "noise": 8})
    n = min(120, max(12, lookback_min // 5))
    step = max(1, lookback_min // n)
    end = datetime.now(timezone.utc)
    r = _rng(resource_id, name)
    period = r.randint(8, 24)
    points = []
    for i in range(n):
        t = end - timedelta(minutes=step * (n - 1 - i))
        wave = prof["amp"] * math.sin(2 * math.pi * i / period)
        noise = r.uniform(-prof["noise"], prof["noise"])
        val = max(prof["lo"], min(prof["hi"], prof["base"] + wave + noise))
        points.append({"t": _iso(t), "value": round(val, 3)})
    return {
        "metric": name, "unit": spec["unit"],
        "points": points, "stats": _stats([p["value"] for p in points]),
    }


def metrics(product: str, resource_id: str | None, metric: str | None, lookback_min: int) -> dict:
    prod = product if product in C.METRICS else _infer_product(resource_id)
    rid = resource_id or (catalog(prod)[0]["id"] if prod in C.METRICS else "unknown")
    specs = C.METRICS.get(prod, [])
    wanted = C.METRIC_ALIASES.get((metric or "").lower(), metric)
    if wanted:
        specs = [s for s in specs if s["metric"] == wanted] or specs
    return {
        "resourceId": rid, "product": prod, "lookbackMinutes": lookback_min,
        "series": [_series(rid, s, lookback_min) for s in specs],
    }


# --- action: logs ------------------------------------------------------------
_LOG_TEMPLATES = {
    "error": [
        "conexão recusada ao pool: too many connections (max_connections=200 atingido)",
        "deadlock detectado na tabela orders; transação abortada e re-tentada",
        "timeout após 30s executando query em billing_invoices",
        "falha ao renovar credencial do Secrets Manager: AccessDenied",
    ],
    "warn": [
        "query lenta (1240ms) em SELECT * FROM orders WHERE created_at > $1",
        "uso de índice sequencial (seq scan) em events (120M linhas)",
        "replicação com lag de 4200ms para a réplica de leitura",
        "conexões acima de 80% do limite; considere connection pooling",
    ],
    "info": [
        "checkpoint concluído em 820ms (12 buffers)",
        "autovacuum finalizado na tabela sessions",
        "backup automático iniciado (snapshot diário)",
        "nova conexão aceita de 10.0.3.44 (app=orders-api)",
    ],
}


def logs(product: str, resource_id: str | None, level: str | None,
         search: str | None, limit: int | None, lookback_min: int) -> dict:
    prod = product if product != "all" else _infer_product(resource_id)
    rid = resource_id or (catalog(prod)[0]["id"] if prod in _NAMES else "unknown")
    r = _rng(rid, "logs", lookback_min)
    levels = [level] if level in _LOG_TEMPLATES else ["error", "warn", "info"]
    weights = {"error": 1, "warn": 3, "info": 6}
    end = datetime.now(timezone.utc)
    n = limit or 60
    entries = []
    for i in range(n * 2):  # gera excedente e filtra
        lvl = r.choices(levels, weights=[weights[x] for x in levels])[0]
        msg = r.choice(_LOG_TEMPLATES[lvl])
        ts = end - timedelta(seconds=r.randint(0, lookback_min * 60))
        entries.append({
            "ts": _iso(ts), "level": lvl, "message": msg,
            "source": f"{prod}/{rid}",
        })
    if search:
        s = search.lower()
        entries = [e for e in entries if s in e["message"].lower()]
    entries.sort(key=lambda e: e["ts"], reverse=True)
    total = len(entries)
    entries = entries[: (limit or 60)]
    return {"resourceId": rid, "product": prod, "entries": entries, "total": total}


# --- action: metadata --------------------------------------------------------
_TABLES = [
    "orders", "order_items", "customers", "payments", "invoices", "products",
    "inventory", "shipments", "events", "sessions", "audit_log", "users",
    "addresses", "coupons", "refunds",
]


def _db_metadata(resource_id: str) -> dict:
    r = _rng(resource_id, "metadata")
    engine_version = r.choice(["15.4", "14.9", "16.2"])
    max_conn = r.choice([100, 200, 500])
    tables = []
    unused_indexes = []
    bloat = []
    for name in _TABLES:
        tr = _rng(resource_id, "table", name)
        rows = tr.choice([1_200, 45_000, 380_000, 2_400_000, 18_000_000, 120_000_000])
        size_mb = round(rows / tr.uniform(900, 2600), 1)
        partitioned = name in ("events", "audit_log", "orders") and rows > 1_000_000
        n_part = tr.randint(6, 36) if partitioned else 0
        indexes = []
        for j in range(tr.randint(1, 4)):
            iname = f"idx_{name}_{tr.choice(['created', 'status', 'customer', 'sku', 'email'])}_{j}"
            scans = tr.choice([0, 0, 3, 180, 5400, 92000])
            indexes.append({
                "name": iname, "unique": j == 0,
                "columns": tr.sample(["id", "created_at", "status", "customer_id", "sku", "email"], tr.randint(1, 2)),
                "sizeMb": round(size_mb * tr.uniform(0.05, 0.3), 1),
                "scans": scans, "unused": scans == 0,
            })
            if scans == 0:
                unused_indexes.append({"table": name, "index": iname, "sizeMb": indexes[-1]["sizeMb"]})
        if size_mb > 500 and tr.random() < 0.4:
            bloat.append({"table": name, "wastedMb": round(size_mb * tr.uniform(0.15, 0.4), 1)})
        tables.append({
            "schema": "public", "name": name, "rows": rows, "sizeMb": size_mb,
            "partitioned": partitioned, "partitions": n_part, "indexes": indexes,
        })

    slow_queries = [
        {"query": "SELECT * FROM orders WHERE created_at > $1 ORDER BY created_at DESC",
         "meanMs": 1240.5, "calls": 18420, "rowsAvg": 5200},
        {"query": "SELECT e.* FROM events e JOIN sessions s ON s.id = e.session_id WHERE e.type = $1",
         "meanMs": 880.2, "calls": 9310, "rowsAvg": 120000},
        {"query": "UPDATE inventory SET qty = qty - $1 WHERE sku = $2",
         "meanMs": 320.7, "calls": 65200, "rowsAvg": 1},
    ]
    recommendations = []
    if unused_indexes:
        u = unused_indexes[0]
        recommendations.append({
            "title": "Índice não utilizado",
            "detail": f"índice {u['index']} não utilizado (0 scans, {u['sizeMb']}MB) — considere remover para reduzir I/O de escrita",
            "severity": "medium",
        })
    big = next((t for t in tables if t["rows"] > 100_000_000 and not t["partitioned"]), None)
    if big:
        recommendations.append({
            "title": "Tabela sem particionamento",
            "detail": f"tabela {big['name']} sem particionamento com {big['rows'] // 1_000_000}M linhas — particionar por mês (RANGE em created_at)",
            "severity": "high",
        })
    if bloat:
        recommendations.append({
            "title": "Bloat elevado",
            "detail": f"tabela {bloat[0]['table']} com ~{bloat[0]['wastedMb']}MB desperdiçados — executar VACUUM FULL/pg_repack em janela",
            "severity": "medium",
        })
    recommendations.append({
        "title": "Conexões próximas do limite",
        "detail": f"picos de conexões próximos de max_connections={max_conn} — avaliar PgBouncer/connection pooling",
        "severity": "low",
    })

    total_gb = round(sum(t["sizeMb"] for t in tables) / 1024, 1)
    return {
        "engine": "postgres", "engineVersion": engine_version,
        "instanceClass": r.choice(_RDS_CLASSES),
        "storage": {"type": "gp3", "allocatedGb": r.choice([100, 200, 500]),
                    "usedGb": max(total_gb, 1.0), "iops": 3000, "throughput": 125},
        "connections": {"max": max_conn, "current": r.randint(10, max_conn - 5)},
        "tables": tables,
        "unusedIndexes": unused_indexes,
        "slowQueries": slow_queries,
        "bloat": bloat,
        "recommendations": recommendations,
    }


def _generic_metadata(product: str, resource_id: str) -> dict:
    r = _rng(resource_id, "meta", product)
    item = next((it for it in catalog(product) if it["id"] == resource_id), None) or catalog(product)[0]
    config = {"type": item["type"], "size": item["size"], "status": item["status"],
              "region": item["region"], "createdAt": item["createdAt"]}
    recs = []
    if product == "ebs" and item["type"] == "gp2":
        recs.append({"title": "Migrar gp2 -> gp3", "detail": "volume gp2 pode migrar para gp3 com ~20% de economia por GB", "severity": "low"})
    if product == "eip" and item["status"] == "unassociated":
        recs.append({"title": "Elastic IP ocioso", "detail": "IP sem associação gera cobrança — liberar se não usado", "severity": "medium"})
    if product == "kms" and item["status"] == "Disabled":
        recs.append({"title": "Chave KMS desabilitada", "detail": "chave desabilitada ainda pode reter custo — agendar exclusão se órfã", "severity": "low"})
    if not recs:
        recs.append({"title": "Sem ações críticas", "detail": "nenhuma recomendação relevante detectada", "severity": "info"})
    related = [x["id"] for x in catalog(product) if x["env"] == item["env"] and x["id"] != item["id"]][:3]
    return {"config": config, "tags": item["tags"], "related": related, "recommendations": recs}


def metadata(product: str, resource_id: str | None) -> dict:
    prod = product if product != "all" else _infer_product(resource_id)
    rid = resource_id or (catalog(prod)[0]["id"] if prod in _NAMES else "unknown")
    if prod in C.DB_PRODUCTS:
        return {"resourceId": rid, "product": prod, **_db_metadata(rid)}
    return {"resourceId": rid, "product": prod, **_generic_metadata(prod, rid)}


# --- action: finops ----------------------------------------------------------
def _provisioned(product: str, typ: str, size: str) -> dict:
    if product == "rds":
        cpu, mem = _RDS_SPECS.get(typ, (2, 8))
        return {"cpu": cpu, "memoryGb": mem, "storageGb": 200, "iops": 3000}
    if product == "ec2":
        cpu, mem = _EC2_SPECS.get(typ, (2, 8))
        return {"cpu": cpu, "memoryGb": mem}
    if product == "ebs":
        gib = int(size.split()[0]) if size and size[0].isdigit() else 100
        return {"storageGb": gib, "iops": 3000}
    return {}


def _verdict(product: str, item: dict) -> tuple[str, float, str]:
    util = item["utilizationPct"]
    cost = item["monthlyCost"]
    typ = item["type"]
    if util < 8:
        return "idle", round(cost * 0.9, 2), "Recurso ocioso — avaliar parada/remoção (economia ~90% do custo mensal)."
    if util < 25 and typ in C.DOWNGRADE:
        target = C.DOWNGRADE[typ]
        price = C.PRICE_RDS if product == "rds" else C.PRICE_EC2
        savings = max(0.0, cost - price.get(target, cost * 0.5))
        return "oversized", round(savings, 2), f"Subutilizado ({util}%) — reduzir {typ} para {target}."
    return "ok", 0.0, "Utilização saudável — sem ação necessária."


def finops(products: list[str], lookback_days: int) -> dict:
    utilization = []
    idle = oversized = 0
    for it in _all_catalog(products):
        prod = it["product"]
        verdict, savings, rec = _verdict(prod, it)
        if verdict == "idle":
            idle += 1
        elif verdict == "oversized":
            oversized += 1
        util = it["utilizationPct"]
        utilization.append({
            "resourceId": it["id"], "name": it["name"], "product": prod,
            "provisioned": _provisioned(prod, it["type"], it["size"]),
            "used": {"cpuPct": util, "memoryPct": round(min(100, util * 1.1), 1),
                     "storagePct": round(min(100, util * 0.8), 1),
                     "iopsPct": round(min(100, util * 0.6), 1)},
            "utilizationPct": util, "verdict": verdict,
            "monthlySavings": savings, "recommendation": rec,
        })

    by_type: dict[str, float] = {}
    for u in utilization:
        by_type[u["product"]] = by_type.get(u["product"], 0.0) + u["monthlySavings"]
    savings_by_type = [{"type": k, "savings": round(v, 2)} for k, v in sorted(by_type.items(), key=lambda x: -x[1]) if v > 0]

    total_savings = round(sum(u["monthlySavings"] for u in utilization), 2)
    now = datetime.now(timezone.utc)
    trend = []
    for m in range(11, -1, -1):
        month = (now - timedelta(days=30 * m))
        r = _rng("trend", month.strftime("%Y-%m"))
        cost = round(r.uniform(18000, 26000) - m * 120, 2)
        trend.append({
            "month": month.strftime("%Y-%m"),
            "cost": cost,
            "savings": round(total_savings * (1 - m / 18.0), 2),
        })

    return {
        "summary": {
            "estimatedMonthlySavings": total_savings, "currency": "USD",
            "idleCount": idle, "oversizedCount": oversized,
            "analyzedCount": len(utilization),
        },
        "utilization": utilization,
        "savingsByType": savings_by_type,
        "savingsTrend": trend,
        "lookbackDays": lookback_days,
    }
