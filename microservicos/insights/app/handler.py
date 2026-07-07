"""Dispatch da ação de insights + caminho AWS real (boto3/psycopg).

Modo controlado por `INSIGHTS_MODE` (default 'mock'):
  - mock  -> dados sintéticos determinísticos (app.mock), sem credenciais.
  - aws   -> assume-role na conta-alvo e coleta dados reais (describe*/CloudWatch/
             CloudWatch Logs/Secrets Manager + psycopg no pg_catalog).

Ambos os caminhos produzem exatamente o mesmo shape de `detail`, então o frontend
não distingue mock de real — só o operador liga o modo via env.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

from . import mock
from . import products as C
from .aws import ActionError, assumed_session
from .models import ActionResult, InsightsRequest


def _mode() -> str:
    return os.getenv("INSIGHTS_MODE", "mock").strip().lower()


def execute(req: InsightsRequest) -> ActionResult:
    p = req.params
    prods = C.resolve_products(p.product)
    if req.dryRun:
        return _result(req, {"dryRun": True, "mode": _mode(), "products": prods, "action": p.action})
    detail = _run_aws(req, prods) if _mode() == "aws" else _run_mock(req, prods)
    return _result(req, detail)


def _result(req: InsightsRequest, detail: dict) -> ActionResult:
    return ActionResult(
        operationId=str(uuid.uuid4()),
        product=req.params.product,
        action=req.params.action,
        detail=detail,
    )


# --- caminho mock ------------------------------------------------------------
def _run_mock(req: InsightsRequest, prods: list[str]) -> dict:
    p = req.params
    if p.action == "resources":
        return mock.resources(prods, p.filters, p.limit)
    if p.action == "metrics":
        return mock.metrics(p.product, p.resourceId, p.metric, p.lookback or 180)
    if p.action == "logs":
        return mock.logs(p.product, p.resourceId, p.level, (p.filters or {}).get("search"), p.limit, p.lookback or 60)
    if p.action == "metadata":
        return mock.metadata(p.product, p.resourceId)
    if p.action == "finops":
        return mock.finops(prods, p.lookback or 30)
    raise ActionError("validation_error", f"ação não suportada: {p.action}", 400)


# --- caminho AWS real --------------------------------------------------------
def _run_aws(req: InsightsRequest, prods: list[str]) -> dict:
    session = assumed_session(req.account, req.roleArn, req.region)
    p = req.params
    if p.action == "resources":
        return _aws_resources(session, prods, p.filters, p.limit, req.region)
    if p.action == "metrics":
        return _aws_metrics(session, p)
    if p.action == "logs":
        return _aws_logs(session, p)
    if p.action == "metadata":
        return _aws_metadata(session, req)
    if p.action == "finops":
        return _aws_finops(session, prods, p.lookback or 14)
    raise ActionError("validation_error", f"ação não suportada: {p.action}", 400)


# --- helpers de coleta -------------------------------------------------------
def _iso(dt) -> str | None:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _pages(client, op, key, **kwargs):
    out = []
    for page in client.get_paginator(op).paginate(**kwargs):
        out.extend(page.get(key, []))
    return out


def _tags(pairs, key="Key", value="Value") -> dict:
    return {t[key]: t[value] for t in (pairs or []) if key in t}


def _norm(rid, name, product, typ, status, size, created, tags, region, cost=None, util=None) -> dict:
    return {
        "id": rid, "name": name or rid, "product": product, "type": typ,
        "env": tags.get("env") or tags.get("Environment") or "unknown",
        "region": region, "status": status, "size": size, "createdAt": created,
        "tags": tags, "monthlyCost": cost, "utilizationPct": util,
    }


# --- resources (describe*) ---------------------------------------------------
def _desc_rds(session, region):
    rds = session.client("rds")
    out = []
    for db in _pages(rds, "describe_db_instances", "DBInstances"):
        klass = db.get("DBInstanceClass", "")
        out.append(_norm(
            db["DBInstanceIdentifier"], db["DBInstanceIdentifier"], "rds", klass,
            db.get("DBInstanceStatus"), klass, _iso(db.get("InstanceCreateTime")),
            _tags(db.get("TagList")), region, C.PRICE_RDS.get(klass),
        ))
    return out


def _desc_ec2(session, region):
    ec2 = session.client("ec2")
    out = []
    for res in _pages(ec2, "describe_instances", "Reservations"):
        for inst in res.get("Instances", []):
            itype = inst.get("InstanceType", "")
            tags = _tags(inst.get("Tags"))
            out.append(_norm(
                inst["InstanceId"], tags.get("Name"), "ec2", itype,
                inst.get("State", {}).get("Name"), itype, _iso(inst.get("LaunchTime")),
                tags, region, C.PRICE_EC2.get(itype),
            ))
    return out


def _desc_ebs(session, region):
    ec2 = session.client("ec2")
    out = []
    for vol in _pages(ec2, "describe_volumes", "Volumes"):
        vtype = vol.get("VolumeType", "")
        size = float(vol.get("Size", 0))
        tags = _tags(vol.get("Tags"))
        out.append(_norm(
            vol["VolumeId"], tags.get("Name"), "ebs", vtype, vol.get("State"),
            f"{size:.0f} GiB", _iso(vol.get("CreateTime")), tags, region,
            round(size * C.PRICE_EBS_GB_MONTH.get(vtype, 0.15), 2),
        ))
    return out


def _desc_elb(session, region):
    v2 = session.client("elbv2")
    out = []
    for lb in _pages(v2, "describe_load_balancers", "LoadBalancers"):
        out.append(_norm(
            lb["LoadBalancerArn"], lb.get("LoadBalancerName"), "elb", lb.get("Type"),
            lb.get("State", {}).get("Code"), lb.get("Type"), _iso(lb.get("CreatedTime")),
            {}, region, C.PRICE_ELB_MONTH,
        ))
    return out


def _desc_eip(session, region):
    ec2 = session.client("ec2")
    out = []
    for addr in ec2.describe_addresses().get("Addresses", []):
        associated = bool(addr.get("AssociationId"))
        tags = _tags(addr.get("Tags"))
        out.append(_norm(
            addr.get("AllocationId") or addr.get("PublicIp"), addr.get("PublicIp"),
            "eip", "standard", "associated" if associated else "unassociated", "-",
            None, tags, region, C.PRICE_EIP_MONTH,
        ))
    return out


def _desc_snapshot(session, region):
    ec2 = session.client("ec2")
    out = []
    for snap in _pages(ec2, "describe_snapshots", "Snapshots", OwnerIds=["self"]):
        size = float(snap.get("VolumeSize", 0))
        tags = _tags(snap.get("Tags"))
        out.append(_norm(
            snap["SnapshotId"], tags.get("Name"), "snapshot",
            "manual", snap.get("State"), f"{size:.0f} GiB", _iso(snap.get("StartTime")),
            tags, region, round(size * C.PRICE_SNAPSHOT_GB_MONTH, 2),
        ))
    return out


def _desc_kms(session, region):
    kms = session.client("kms")
    out = []
    for k in _pages(kms, "list_keys", "Keys"):
        meta = kms.describe_key(KeyId=k["KeyId"]).get("KeyMetadata", {})
        if meta.get("KeyManager") == "AWS":  # ignora chaves gerenciadas pela AWS
            continue
        out.append(_norm(
            meta.get("KeyId"), meta.get("Description") or meta.get("KeyId"), "kms",
            meta.get("KeySpec", "SYMMETRIC_DEFAULT"),
            "Enabled" if meta.get("Enabled") else "Disabled", "-",
            _iso(meta.get("CreationDate")), {}, region, C.PRICE_KMS_MONTH,
        ))
    return out


def _desc_vpc_endpoint(session, region):
    ec2 = session.client("ec2")
    out = []
    for ep in _pages(ec2, "describe_vpc_endpoints", "VpcEndpoints"):
        etype = ep.get("VpcEndpointType", "Interface")
        tags = _tags(ep.get("Tags"))
        cost = 0.0 if etype == "Gateway" else C.PRICE_VPC_ENDPOINT_MONTH
        out.append(_norm(
            ep["VpcEndpointId"], ep.get("ServiceName"), "vpc-endpoint", etype,
            ep.get("State"), etype, _iso(ep.get("CreationTimestamp")), tags, region, cost,
        ))
    return out


_DESCRIBE = {
    "rds": _desc_rds, "ec2": _desc_ec2, "ebs": _desc_ebs, "elb": _desc_elb,
    "eip": _desc_eip, "snapshot": _desc_snapshot, "kms": _desc_kms,
    "vpc-endpoint": _desc_vpc_endpoint,
}


def _apply_filters(items, filters, limit):
    f = filters or {}
    search = (f.get("search") or "").lower()
    if search:
        items = [it for it in items if search in str(it["id"]).lower() or search in str(it["name"]).lower()]
    for field in ("status", "env", "type"):
        if f.get(field):
            items = [it for it in items if it.get(field) == f[field]]
    if f.get("tag"):
        tag = str(f["tag"]).lower()
        items = [it for it in items if any(tag in f"{k}:{v}".lower() for k, v in (it.get("tags") or {}).items())]
    total = len(items)
    if limit:
        items = items[:limit]
    return items, total


def _aws_resources(session, prods, filters, limit, region):
    items = []
    for prod in prods:
        try:
            items.extend(_DESCRIBE[prod](session, region))
        except Exception as exc:  # um produto indisponível não derruba a coleta
            items.append(_norm(f"{prod}-error", str(exc), prod, "error", "error", "-", None, {}, region))
    items, total = _apply_filters(items, filters, limit)
    return {"count": len(items), "total": total, "products": prods, "items": items}


# --- metrics (cloudwatch.get_metric_data) ------------------------------------
def _aws_metrics(session, p):
    prod = p.product if p.product in C.METRICS else mock._infer_product(p.resourceId)
    if not p.resourceId:
        raise ActionError("validation_error", "resourceId é obrigatório para metrics", 400)
    specs = C.METRICS.get(prod, [])
    wanted = C.METRIC_ALIASES.get((p.metric or "").lower(), p.metric)
    if wanted:
        specs = [s for s in specs if s["metric"] == wanted] or specs
    dim = C.CW_DIMENSION.get(prod)
    lookback = p.lookback or 180
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=lookback)
    period = max(60, (lookback * 60) // 120)
    cw = session.client("cloudwatch")
    queries = [{
        "Id": f"m{i}",
        "MetricStat": {
            "Metric": {"Namespace": s["namespace"], "MetricName": s["metric"],
                       "Dimensions": [{"Name": dim, "Value": p.resourceId}] if dim else []},
            "Period": period, "Stat": s["stat"],
        },
        "ReturnData": True,
    } for i, s in enumerate(specs)]
    resp = cw.get_metric_data(MetricDataQueries=queries, StartTime=start, EndTime=end) if queries else {"MetricDataResults": []}
    by_id = {r["Id"]: r for r in resp.get("MetricDataResults", [])}
    series = []
    for i, s in enumerate(specs):
        r = by_id.get(f"m{i}", {})
        points = [{"t": _iso(ts), "value": round(float(v), 3)}
                  for ts, v in zip(r.get("Timestamps", []), r.get("Values", []))]
        points.sort(key=lambda x: x["t"])
        series.append({"metric": s["metric"], "unit": s["unit"], "points": points,
                       "stats": mock._stats([pt["value"] for pt in points])})
    return {"resourceId": p.resourceId, "product": prod, "lookbackMinutes": lookback, "series": series}


# --- logs (logs.filter_log_events) -------------------------------------------
def _infer_level(msg: str) -> str:
    low = msg.lower()
    if any(k in low for k in ("error", "fatal", "exception", "denied", "deadlock", "timeout")):
        return "error"
    if any(k in low for k in ("warn", "slow", "lag", "retry")):
        return "warn"
    return "info"


def _aws_logs(session, p):
    prod = p.product if p.product != "all" else mock._infer_product(p.resourceId)
    rid = p.resourceId or "unknown"
    group = C.LOG_GROUP_TEMPLATE.get(prod, "/aws/{product}/{resourceId}").format(product=prod, resourceId=rid)
    lookback = p.lookback or 60
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=lookback)
    kwargs = {
        "logGroupName": group,
        "startTime": int(start.timestamp() * 1000),
        "endTime": int(end.timestamp() * 1000),
        "limit": p.limit or 100,
    }
    search = (p.filters or {}).get("search")
    if search:
        kwargs["filterPattern"] = f'"{search}"'
    client = session.client("logs")
    events = client.filter_log_events(**kwargs).get("events", [])
    entries = []
    for ev in events:
        msg = ev.get("message", "")
        lvl = _infer_level(msg)
        if p.level and lvl != p.level:
            continue
        entries.append({
            "ts": _iso(datetime.fromtimestamp(ev.get("timestamp", 0) / 1000, tz=timezone.utc)),
            "level": lvl, "message": msg, "source": ev.get("logStreamName") or group,
        })
    entries.sort(key=lambda e: e["ts"], reverse=True)
    return {"resourceId": rid, "product": prod, "logGroup": group, "entries": entries, "total": len(entries)}


# --- metadata (psycopg no pg_catalog para RDS) -------------------------------
_SQL_TABLES = """
SELECT n.nspname AS schema, c.relname AS name, c.reltuples::bigint AS rows,
       round(pg_total_relation_size(c.oid) / 1048576.0, 1) AS size_mb,
       (pt.partrelid IS NOT NULL) AS partitioned,
       (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) AS partitions
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 50;
"""
_SQL_INDEXES = """
SELECT s.relname AS table, s.indexrelname AS name, s.idx_scan AS scans,
       ix.indisunique AS is_unique,
       round(pg_relation_size(s.indexrelid) / 1048576.0, 1) AS size_mb,
       pg_get_indexdef(s.indexrelid) AS indexdef
FROM pg_stat_user_indexes s
JOIN pg_index ix ON ix.indexrelid = s.indexrelid
ORDER BY s.relname, s.idx_scan ASC;
"""
_SQL_SLOW = """
SELECT query, round(mean_exec_time::numeric, 2) AS mean_ms, calls,
       (rows / GREATEST(calls, 1)) AS rows_avg
FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;
"""
_SQL_CONNECTIONS = """
SELECT (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_conn,
       count(*) AS current_conn FROM pg_stat_activity;
"""
_SQL_META = "SELECT current_setting('server_version') AS version;"


def _resolve_db_credentials(session, req):
    """Lê credenciais do Secrets Manager (id via filters.secretId ou req.resource)."""
    secret_id = (req.params.filters or {}).get("secretId") or req.resource
    if not secret_id:
        raise ActionError("validation_error", "secretId (Secrets Manager) obrigatório para metadata de banco", 400)
    import json
    sm = session.client("secretsmanager")
    raw = sm.get_secret_value(SecretId=secret_id).get("SecretString", "{}")
    return json.loads(raw)


def _rds_endpoint(session, resource_id):
    rds = session.client("rds")
    dbs = rds.describe_db_instances(DBInstanceIdentifier=resource_id).get("DBInstances", [])
    if not dbs:
        raise ActionError("not_found", f"instância RDS não encontrada: {resource_id}", 404)
    db = dbs[0]
    ep = db.get("Endpoint", {})
    return db, ep.get("Address"), ep.get("Port", 5432)


def _aws_db_metadata(session, req):
    import psycopg  # import tardio: só necessário no caminho de banco real

    rid = req.params.resourceId
    if not rid:
        raise ActionError("validation_error", "resourceId (identificador RDS) obrigatório", 400)
    db, host, port = _rds_endpoint(session, rid)
    creds = _resolve_db_credentials(session, req)
    dbname = creds.get("dbname") or db.get("DBName") or "postgres"
    conninfo = (
        f"host={creds.get('host', host)} port={creds.get('port', port)} "
        f"dbname={dbname} user={creds['username']} password={creds['password']} "
        f"connect_timeout=5 sslmode=require"
    )
    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(_SQL_META)
        version = cur.fetchone()[0]
        cur.execute(_SQL_CONNECTIONS)
        max_conn, current_conn = cur.fetchone()
        cur.execute(_SQL_TABLES)
        table_rows = cur.fetchall()
        cur.execute(_SQL_INDEXES)
        index_rows = cur.fetchall()
        slow = _query_slow(cur)

    idx_by_table: dict[str, list] = {}
    unused_indexes = []
    for tname, iname, scans, is_unique, size_mb, _def in index_rows:
        entry = {"name": iname, "unique": bool(is_unique), "columns": _columns_from_def(_def),
                 "sizeMb": float(size_mb or 0), "scans": int(scans or 0), "unused": (scans or 0) == 0}
        idx_by_table.setdefault(tname, []).append(entry)
        if entry["unused"]:
            unused_indexes.append({"table": tname, "index": iname, "sizeMb": entry["sizeMb"]})

    tables = []
    bloat = []
    for schema, name, rows, size_mb, partitioned, partitions in table_rows:
        tables.append({
            "schema": schema, "name": name, "rows": int(rows or 0), "sizeMb": float(size_mb or 0),
            "partitioned": bool(partitioned), "partitions": int(partitions or 0),
            "indexes": idx_by_table.get(name, []),
        })

    allocated = db.get("AllocatedStorage", 0)
    recommendations = _db_recommendations(tables, unused_indexes, int(max_conn or 0))
    return {
        "resourceId": rid, "product": "rds", "engine": db.get("Engine", "postgres"),
        "engineVersion": version, "instanceClass": db.get("DBInstanceClass"),
        "storage": {"type": db.get("StorageType"), "allocatedGb": allocated,
                    "usedGb": round(sum(t["sizeMb"] for t in tables) / 1024, 1),
                    "iops": db.get("Iops"), "throughput": db.get("StorageThroughput")},
        "connections": {"max": int(max_conn or 0), "current": int(current_conn or 0)},
        "tables": tables, "unusedIndexes": unused_indexes,
        "slowQueries": slow, "bloat": bloat, "recommendations": recommendations,
    }


def _query_slow(cur):
    try:
        cur.execute(_SQL_SLOW)
        return [{"query": q, "meanMs": float(m), "calls": int(c), "rowsAvg": int(ra)}
                for q, m, c, ra in cur.fetchall()]
    except Exception:  # pg_stat_statements pode não estar habilitada
        return []


def _columns_from_def(indexdef: str) -> list[str]:
    if "(" not in indexdef:
        return []
    inner = indexdef[indexdef.index("(") + 1: indexdef.rindex(")")]
    return [c.strip().split()[0] for c in inner.split(",") if c.strip()]


def _db_recommendations(tables, unused_indexes, max_conn):
    recs = []
    if unused_indexes:
        u = unused_indexes[0]
        recs.append({"title": "Índice não utilizado",
                     "detail": f"índice {u['index']} não utilizado (0 scans, {u['sizeMb']}MB) — considere remover",
                     "severity": "medium"})
    big = next((t for t in tables if t["rows"] > 100_000_000 and not t["partitioned"]), None)
    if big:
        recs.append({"title": "Tabela sem particionamento",
                     "detail": f"tabela {big['name']} sem particionamento com {big['rows'] // 1_000_000}M linhas — particionar por mês",
                     "severity": "high"})
    if max_conn:
        recs.append({"title": "Conexões", "detail": f"max_connections={max_conn} — avaliar PgBouncer se houver saturação",
                     "severity": "low"})
    return recs


def _aws_generic_metadata(session, req):
    prod = req.params.product if req.params.product != "all" else mock._infer_product(req.params.resourceId)
    items = _DESCRIBE[prod](session, req.region)
    rid = req.params.resourceId
    item = next((it for it in items if it["id"] == rid), items[0] if items else None)
    if not item:
        raise ActionError("not_found", f"recurso não encontrado: {rid}", 404)
    related = [x["id"] for x in items if x.get("env") == item.get("env") and x["id"] != item["id"]][:3]
    return {"resourceId": item["id"], "product": prod,
            "config": {"type": item["type"], "size": item["size"], "status": item["status"],
                       "region": item["region"], "createdAt": item["createdAt"]},
            "tags": item["tags"], "related": related,
            "recommendations": [{"title": "Coletado", "detail": "metadados via describe*", "severity": "info"}]}


def _aws_metadata(session, req):
    prod = req.params.product if req.params.product != "all" else mock._infer_product(req.params.resourceId)
    if prod in C.DB_PRODUCTS:
        return _aws_db_metadata(session, req)
    return _aws_generic_metadata(session, req)


# --- finops (describe* + cloudwatch utilization) -----------------------------
def _avg_cpu(cw, namespace, dim_name, rid, days):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    resp = cw.get_metric_statistics(
        Namespace=namespace, MetricName="CPUUtilization",
        Dimensions=[{"Name": dim_name, "Value": rid}],
        StartTime=start, EndTime=end, Period=3600, Statistics=["Average"],
    )
    pts = [d["Average"] for d in resp.get("Datapoints", [])]
    return round(sum(pts) / len(pts), 1) if pts else None


def _aws_finops(session, prods, days):
    cw = session.client("cloudwatch")
    utilization = []
    idle = oversized = 0
    for prod in prods:
        if prod not in ("rds", "ec2", "ebs"):
            continue
        try:
            items = _DESCRIBE[prod](session, session.region_name)
        except Exception:
            continue
        for it in items:
            util = None
            if prod == "rds":
                util = _avg_cpu(cw, "AWS/RDS", "DBInstanceIdentifier", it["id"], days)
            elif prod == "ec2":
                util = _avg_cpu(cw, "AWS/EC2", "InstanceId", it["id"], days)
            util = util if util is not None else 50.0
            it = dict(it, utilizationPct=util)
            verdict, savings, rec = mock._verdict(prod, {"utilizationPct": util,
                                                         "monthlyCost": it.get("monthlyCost") or 0.0,
                                                         "type": it["type"]})
            if verdict == "idle":
                idle += 1
            elif verdict == "oversized":
                oversized += 1
            utilization.append({
                "resourceId": it["id"], "name": it["name"], "product": prod,
                "provisioned": mock._provisioned(prod, it["type"], it["size"]),
                "used": {"cpuPct": util, "memoryPct": None, "storagePct": None, "iopsPct": None},
                "utilizationPct": util, "verdict": verdict,
                "monthlySavings": savings, "recommendation": rec,
            })
    by_type: dict[str, float] = {}
    for u in utilization:
        by_type[u["product"]] = by_type.get(u["product"], 0.0) + u["monthlySavings"]
    total = round(sum(u["monthlySavings"] for u in utilization), 2)
    return {
        "summary": {"estimatedMonthlySavings": total, "currency": "USD",
                    "idleCount": idle, "oversizedCount": oversized, "analyzedCount": len(utilization)},
        "utilization": utilization,
        "savingsByType": [{"type": k, "savings": round(v, 2)} for k, v in by_type.items() if v > 0],
        "savingsTrend": [],  # tendência histórica requer Cost Explorer; vazio no caminho describe.
        "lookbackDays": days,
    }
