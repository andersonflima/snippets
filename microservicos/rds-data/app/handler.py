"""Ação rds-data: wrapper seguro do RDS Data API com avaliação de SQL via regras (S3)."""
from __future__ import annotations

import json
import os
import re
import time
import uuid

import boto3
import sqlparse
from botocore.exceptions import ClientError

from .aws import ActionError, assumed_session
from .models import ActionAccepted, RdsDataRequest

RULES_BUCKET = os.getenv("RULES_BUCKET")
RULES_KEY = os.getenv("RULES_KEY", "rds-data/rules.json")
RULES_TTL = int(os.getenv("RULES_CACHE_TTL", "60"))
WRITE_TYPES = {"UPDATE", "DELETE"}
TABLE_RE = re.compile(r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([a-zA-Z_][\w\.]*)", re.IGNORECASE)

_RULES_CACHE: dict = {}


def _load_rules(bucket: str, key: str) -> dict:
    if not bucket:
        raise ActionError("validation_error", "bucket de regras não configurado (RULES_BUCKET)", 400)
    cache_key = f"{bucket}/{key}"
    now = time.time()
    cached = _RULES_CACHE.get(cache_key)
    if cached and now - cached[0] < RULES_TTL:
        return cached[1]
    s3 = boto3.client("s3")  # identidade da plataforma (IRSA), não da conta-alvo
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        rules = json.loads(body)
    except ClientError as exc:
        raise ActionError("not_found", f"regras não encontradas em s3://{bucket}/{key}: {exc}", 404) from exc
    except (ValueError, KeyError) as exc:
        raise ActionError("validation_error", f"regras inválidas (JSON): {exc}", 400) from exc
    _RULES_CACHE[cache_key] = (now, rules)
    return rules


def _merge_env(rules: dict, environment: str) -> dict:
    merged = {k: v for k, v in rules.items() if k != "environments"}
    merged.update((rules.get("environments") or {}).get(environment) or {})
    return merged


def _statement_type(stmt) -> str:
    kind = (stmt.get_type() or "UNKNOWN").upper()
    if kind != "UNKNOWN":
        return kind
    for token in stmt.tokens:
        if token.ttype in (sqlparse.tokens.Keyword.DDL, sqlparse.tokens.Keyword.DML, sqlparse.tokens.Keyword):
            return token.value.upper()
    return "UNKNOWN"


def _has_where(stmt) -> bool:
    return any(isinstance(token, sqlparse.sql.Where) for token in stmt.tokens)


def _referenced_tables(sql: str) -> set:
    return {match.group(1).lower() for match in TABLE_RE.finditer(sql)}


def evaluate_sql(sql: str, rules: dict, environment: str):
    rule = _merge_env(rules, environment)
    statements = [s for s in sqlparse.parse(sql) if str(s).strip()]
    if not statements:
        return False, "sql vazio"

    max_statements = rule.get("maxStatements", 1)
    if len(statements) > max_statements:
        return False, f"múltiplas instruções não permitidas (max={max_statements})"

    upper = sql.upper()
    for keyword in rule.get("deniedKeywords", []):
        if keyword.upper() in upper:
            return False, f"keyword proibida: {keyword}"
    for pattern in rule.get("denyPatterns", []):
        if re.search(pattern, sql, re.IGNORECASE):
            return False, f"padrão proibido: {pattern}"

    tables = rule.get("tables") or {}
    refs = _referenced_tables(sql)
    deny_tables = {t.lower() for t in tables.get("deny", [])}
    allow_tables = {t.lower() for t in tables.get("allow", [])}
    if deny_tables & refs:
        return False, f"tabela negada: {', '.join(sorted(deny_tables & refs))}"
    if allow_tables and not refs.issubset(allow_tables):
        return False, f"tabela fora da allowlist: {', '.join(sorted(refs - allow_tables))}"

    allowed = {s.upper() for s in rule.get("allowedStatements", [])}
    denied = {s.upper() for s in rule.get("deniedStatements", [])}
    require_where = rule.get("requireWhereOnWrite", True)
    default = str(rule.get("default", "deny")).lower()

    for stmt in statements:
        st = _statement_type(stmt)
        if st in denied:
            return False, f"statement '{st}' negado"
        if require_where and st in WRITE_TYPES and not _has_where(stmt):
            return False, f"{st} sem WHERE não permitido"
        if allowed:
            if st not in allowed:
                return False, f"statement '{st}' fora da allowlist"
        elif default == "deny":
            return False, f"default deny: statement '{st}' não permitido"
    return True, "permitido"


def _to_sql_parameters(params: dict) -> list:
    out = []
    for name, value in params.items():
        if value is None:
            field = {"isNull": True}
        elif isinstance(value, bool):
            field = {"booleanValue": value}
        elif isinstance(value, int):
            field = {"longValue": value}
        elif isinstance(value, float):
            field = {"doubleValue": value}
        else:
            field = {"stringValue": str(value)}
        out.append({"name": name, "value": field})
    return out


def execute(req: RdsDataRequest) -> ActionAccepted:
    p = req.params
    rules = _load_rules(p.rulesBucket or RULES_BUCKET, p.rulesKey or RULES_KEY)
    allowed, reason = evaluate_sql(p.sql, rules, req.environment)
    if not allowed:
        raise ActionError("sql_forbidden", f"SQL bloqueado pelas regras: {reason}", 403)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "allowed": True, "reason": reason},
        )

    session = assumed_session(req.account, req.roleArn, req.region)
    client = session.client("rds-data")
    kwargs = {
        "resourceArn": p.resourceArn or req.resource,
        "secretArn": p.secretArn,
        "sql": p.sql,
        "includeResultMetadata": p.includeResultMetadata,
    }
    if p.database:
        kwargs["database"] = p.database
    if p.schema:
        kwargs["schema"] = p.schema
    if p.parameters:
        kwargs["parameters"] = _to_sql_parameters(p.parameters)

    result = client.execute_statement(**kwargs)
    detail = {
        "allowed": True,
        "reason": reason,
        "numberOfRecordsUpdated": result.get("numberOfRecordsUpdated"),
        "records": result.get("records"),
    }
    if p.includeResultMetadata:
        detail["columnMetadata"] = result.get("columnMetadata")
    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
