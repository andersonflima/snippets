"""Ação finops: varredura de desperdício (read-only) e recomendações de economia.

Analisa RDS, EC2, EBS, Elastic IPs, ELBs e snapshots via describe* + métricas do
CloudWatch, com heurísticas próprias. Thresholds e tabela de preços (sa-east-1)
vêm de regras externalizadas (S3/DynamoDB), atualizáveis sem redeploy; defaults
embutidos garantem operação. Nada é alterado (read-only)."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from .aws import ActionError, assumed_session
from .models import ActionAccepted, FinopsRequest
from .rules import load_rules

DEFAULT_RULES = {
    "idleCpuPct": 5.0,
    "oversizedAvgCpuPct": 20.0,
    "oversizedMaxCpuPct": 40.0,
    "idleConnections": 1.0,
    "orphanSnapshotAgeDays": 90,
    "pricing": {
        "currency": "USD",
        "rds": {
            "db.t3.micro": 25.0, "db.t3.small": 50.0, "db.t3.medium": 100.0,
            "db.t3.large": 200.0, "db.m5.large": 260.0, "db.m5.xlarge": 520.0,
            "db.m5.2xlarge": 1040.0, "db.r5.large": 330.0, "db.r5.xlarge": 660.0,
        },
        "ec2": {
            "t3.micro": 9.0, "t3.small": 18.0, "t3.medium": 36.0, "t3.large": 72.0,
            "m5.large": 100.0, "m5.xlarge": 200.0, "m5.2xlarge": 400.0,
            "c5.large": 88.0, "c5.xlarge": 176.0,
        },
        "ebsGbMonth": {"gp2": 0.19, "gp3": 0.152, "io1": 0.238, "io2": 0.238, "standard": 0.11, "sc1": 0.018, "st1": 0.05},
        "snapshotGbMonth": 0.055,
        "eipMonth": 3.65,
        "elbMonth": 22.0,
    },
    "instanceDowngrade": {
        "db.m5.2xlarge": "db.m5.xlarge", "db.m5.xlarge": "db.m5.large",
        "db.m5.large": "db.t3.large", "db.r5.xlarge": "db.r5.large",
        "m5.2xlarge": "m5.xlarge", "m5.xlarge": "m5.large", "m5.large": "t3.large",
    },
}

_ALL = ("rds", "ec2", "ebs", "eip", "elb", "snapshots")


def execute(req: FinopsRequest) -> ActionAccepted:
    p = req.params
    scope = (p.scope or "all").lower()
    families = _ALL if scope == "all" else tuple(f for f in _ALL if f == scope)
    if not families:
        raise ActionError("validation_error", f"scope inválido: {p.scope}", 400)

    rules = load_rules(DEFAULT_RULES)
    days = max(1, int(p.lookbackDays or 14))

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "region": req.region, "scope": scope, "families": list(families)},
        )

    session = assumed_session(req.account, req.roleArn, req.region)
    findings: list[dict] = []
    for fam in families:
        try:
            findings.extend(_SCANNERS[fam](session, days, rules))
        except Exception as exc:  # um recurso indisponível não derruba a varredura
            findings.append(_error_finding(fam, exc))

    by_type: dict[str, int] = {}
    total = 0.0
    for f in findings:
        by_type[f["resourceType"]] = by_type.get(f["resourceType"], 0) + 1
        total += float(f.get("estimatedMonthlySavings") or 0.0)

    detail = {
        "region": req.region,
        "scope": scope,
        "summary": {
            "estimatedMonthlySavings": round(total, 2),
            "currency": rules.get("pricing", {}).get("currency", "USD"),
            "findingsCount": len(findings),
            "byResourceType": by_type,
        },
        "findings": findings,
        "notes": [
            "Estimativas aproximadas (tabela de preços sa-east-1 das regras externalizadas).",
            "Análise read-only: nenhuma alteração é aplicada nos recursos.",
        ],
    }
    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)


# --- helpers -----------------------------------------------------------------
def _pages(client, op, key, **kwargs):
    out = []
    for page in client.get_paginator(op).paginate(**kwargs):
        out.extend(page.get(key, []))
    return out


def _window(days):
    end = datetime.now(timezone.utc)
    return end - timedelta(days=days), end


def _stat(cw, namespace, metric, dims, days, stat):
    start, end = _window(days)
    resp = cw.get_metric_statistics(
        Namespace=namespace, MetricName=metric, Dimensions=dims,
        StartTime=start, EndTime=end, Period=3600, Statistics=[stat],
    )
    return [d[stat] for d in resp.get("Datapoints", [])]


def _avg(xs):
    return sum(xs) / len(xs) if xs else None


def _finding(rtype, rid, issue, severity, recommendation, savings, evidence):
    return {
        "resourceType": rtype, "resourceId": rid, "issue": issue, "severity": severity,
        "recommendation": recommendation,
        "estimatedMonthlySavings": round(float(savings or 0.0), 2), "evidence": evidence,
    }


def _error_finding(fam, exc):
    return _finding(fam, "-", "scan_error", "low", f"Falha ao varrer {fam}: {exc}", 0.0, {"error": str(exc)})


# --- scanners ----------------------------------------------------------------
def _scan_rds(session, days, rules):
    rds = session.client("rds")
    cw = session.client("cloudwatch")
    price = rules["pricing"]["rds"]
    ebs_price = rules["pricing"]["ebsGbMonth"]
    downgrade = rules.get("instanceDowngrade", {})
    out = []
    for db in _pages(rds, "describe_db_instances", "DBInstances"):
        if db.get("DBInstanceStatus") != "available":
            continue
        rid = db["DBInstanceIdentifier"]
        klass = db.get("DBInstanceClass", "")
        monthly = float(price.get(klass, 0.0))
        dims = [{"Name": "DBInstanceIdentifier", "Value": rid}]
        max_cpu = max(_stat(cw, "AWS/RDS", "CPUUtilization", dims, days, "Maximum") or [0.0])
        avg_cpu = _avg(_stat(cw, "AWS/RDS", "CPUUtilization", dims, days, "Average"))
        max_conn = max(_stat(cw, "AWS/RDS", "DatabaseConnections", dims, days, "Maximum") or [0.0])
        evidence = {"instanceClass": klass, "maxCpuPct": round(max_cpu, 2),
                    "avgCpuPct": round(avg_cpu, 2) if avg_cpu is not None else None,
                    "maxConnections": round(max_conn, 2), "lookbackDays": days}
        if max_cpu < rules["idleCpuPct"] and max_conn <= rules["idleConnections"]:
            out.append(_finding("rds", rid, "idle_instance", "high",
                f"Instância ociosa ({days}d): CPU máx {max_cpu:.1f}%, conexões máx {max_conn:.0f}. Avaliar parada ou downsize.",
                monthly, evidence))
        elif (avg_cpu is not None and avg_cpu < rules["oversizedAvgCpuPct"]
              and max_cpu < rules["oversizedMaxCpuPct"] and klass in downgrade):
            target = downgrade[klass]
            savings = max(0.0, monthly - float(price.get(target, 0.0)))
            out.append(_finding("rds", rid, "oversized", "medium",
                f"Subutilizada: CPU méd {avg_cpu:.1f}% / máx {max_cpu:.1f}%. Reduzir {klass} para {target}.",
                savings, dict(evidence, suggestedClass=target)))
        if db.get("StorageType") == "gp2":
            alloc = float(db.get("AllocatedStorage", 0))
            savings = alloc * (float(ebs_price.get("gp2", 0)) - float(ebs_price.get("gp3", 0)))
            out.append(_finding("rds", rid, "gp2_storage", "low",
                f"Storage gp2 ({alloc:.0f} GiB): migrar para gp3 reduz custo por GB.",
                savings, {"allocatedStorageGiB": alloc, "storageType": "gp2"}))
    return out


def _scan_ec2(session, days, rules):
    ec2 = session.client("ec2")
    cw = session.client("cloudwatch")
    price = rules["pricing"]["ec2"]
    downgrade = rules.get("instanceDowngrade", {})
    out = []
    for res in _pages(ec2, "describe_instances", "Reservations"):
        for inst in res.get("Instances", []):
            if inst.get("State", {}).get("Name") != "running":
                continue
            iid = inst["InstanceId"]
            itype = inst.get("InstanceType", "")
            monthly = float(price.get(itype, 0.0))
            dims = [{"Name": "InstanceId", "Value": iid}]
            max_cpu = max(_stat(cw, "AWS/EC2", "CPUUtilization", dims, days, "Maximum") or [0.0])
            avg_cpu = _avg(_stat(cw, "AWS/EC2", "CPUUtilization", dims, days, "Average"))
            evidence = {"instanceType": itype, "maxCpuPct": round(max_cpu, 2),
                        "avgCpuPct": round(avg_cpu, 2) if avg_cpu is not None else None, "lookbackDays": days}
            if max_cpu < rules["idleCpuPct"]:
                out.append(_finding("ec2", iid, "idle_instance", "high",
                    f"EC2 ociosa ({days}d): CPU máx {max_cpu:.1f}%. Avaliar parada ou encerramento.",
                    monthly, evidence))
            elif (avg_cpu is not None and avg_cpu < rules["oversizedAvgCpuPct"]
                  and max_cpu < rules["oversizedMaxCpuPct"] and itype in downgrade):
                target = downgrade[itype]
                savings = max(0.0, monthly - float(price.get(target, 0.0)))
                out.append(_finding("ec2", iid, "oversized", "medium",
                    f"EC2 subutilizada: CPU méd {avg_cpu:.1f}%. Reduzir {itype} para {target}.",
                    savings, dict(evidence, suggestedType=target)))
    return out


def _scan_ebs(session, days, rules):
    ec2 = session.client("ec2")
    ebs_price = rules["pricing"]["ebsGbMonth"]
    out = []
    for vol in _pages(ec2, "describe_volumes", "Volumes"):
        vid = vol["VolumeId"]
        size = float(vol.get("Size", 0))
        vtype = vol.get("VolumeType", "")
        if vol.get("State") == "available":
            savings = size * float(ebs_price.get(vtype, ebs_price.get("gp3", 0)))
            out.append(_finding("ebs", vid, "unattached_volume", "high",
                f"Volume {vtype} {size:.0f} GiB não anexado: avaliar snapshot e remoção.",
                savings, {"sizeGiB": size, "volumeType": vtype, "state": "available"}))
        elif vtype == "gp2":
            savings = size * (float(ebs_price.get("gp2", 0)) - float(ebs_price.get("gp3", 0)))
            out.append(_finding("ebs", vid, "gp2_volume", "low",
                f"Volume gp2 {size:.0f} GiB: migrar para gp3 reduz custo por GB.",
                savings, {"sizeGiB": size, "volumeType": "gp2"}))
    return out


def _scan_eip(session, days, rules):
    ec2 = session.client("ec2")
    monthly = float(rules["pricing"]["eipMonth"])
    out = []
    for addr in ec2.describe_addresses().get("Addresses", []):
        if not addr.get("AssociationId"):
            aid = addr.get("AllocationId") or addr.get("PublicIp", "-")
            out.append(_finding("eip", aid, "unassociated_eip", "medium",
                f"Elastic IP {addr.get('PublicIp')} sem associação: liberar para evitar cobrança.",
                monthly, {"publicIp": addr.get("PublicIp")}))
    return out


def _scan_elb(session, days, rules):
    monthly = float(rules["pricing"]["elbMonth"])
    out = []
    v2 = session.client("elbv2")
    for lb in _pages(v2, "describe_load_balancers", "LoadBalancers"):
        arn = lb["LoadBalancerArn"]
        name = lb.get("LoadBalancerName", arn)
        tgs = v2.describe_target_groups(LoadBalancerArn=arn).get("TargetGroups", [])
        healthy = 0
        for tg in tgs:
            hs = v2.describe_target_health(TargetGroupArn=tg["TargetGroupArn"]).get("TargetHealthDescriptions", [])
            healthy += sum(1 for h in hs if h.get("TargetHealth", {}).get("State") == "healthy")
        if healthy == 0:
            out.append(_finding("elb", name, "idle_load_balancer", "medium",
                "Load balancer sem targets saudáveis: avaliar remoção.",
                monthly, {"type": lb.get("Type"), "healthyTargets": 0, "targetGroups": len(tgs)}))
    classic = session.client("elb")
    for lb in _pages(classic, "describe_load_balancers", "LoadBalancerDescriptions"):
        name = lb["LoadBalancerName"]
        if not lb.get("Instances"):
            out.append(_finding("elb", name, "idle_load_balancer", "medium",
                "Classic ELB sem instâncias registradas: avaliar remoção.",
                monthly, {"type": "classic", "instances": 0}))
    return out


def _scan_snapshots(session, days, rules):
    ec2 = session.client("ec2")
    rds = session.client("rds")
    snap_price = float(rules["pricing"]["snapshotGbMonth"])
    age_days = int(rules["orphanSnapshotAgeDays"])
    now = datetime.now(timezone.utc)
    out = []
    vol_ids = {v["VolumeId"] for v in _pages(ec2, "describe_volumes", "Volumes")}
    for snap in _pages(ec2, "describe_snapshots", "Snapshots", OwnerIds=["self"]):
        sid = snap["SnapshotId"]
        vol = snap.get("VolumeId")
        size = float(snap.get("VolumeSize", 0))
        started = snap.get("StartTime")
        age = (now - started).days if started else 0
        orphan = bool(vol) and vol not in vol_ids
        if orphan or age >= age_days:
            issue = "orphan_snapshot" if orphan else "old_snapshot"
            desc = "órfão" if orphan else f"antigo ({age}d)"
            out.append(_finding("snapshot", sid, issue, "medium" if orphan else "low",
                f"Snapshot EBS {desc} {size:.0f} GiB: avaliar remoção.",
                size * snap_price, {"volumeId": vol, "ageDays": age, "sizeGiB": size}))
    for snap in _pages(rds, "describe_db_snapshots", "DBSnapshots", SnapshotType="manual"):
        sid = snap["DBSnapshotIdentifier"]
        created = snap.get("SnapshotCreateTime")
        age = (now - created).days if created else 0
        size = float(snap.get("AllocatedStorage", 0))
        if age >= age_days:
            out.append(_finding("snapshot", sid, "old_snapshot", "low",
                f"Snapshot RDS manual antigo ({age}d) {size:.0f} GiB: avaliar remoção.",
                size * snap_price, {"ageDays": age, "sizeGiB": size, "engine": snap.get("Engine")}))
    return out


_SCANNERS = {
    "rds": _scan_rds,
    "ec2": _scan_ec2,
    "ebs": _scan_ebs,
    "eip": _scan_eip,
    "elb": _scan_elb,
    "snapshots": _scan_snapshots,
}
