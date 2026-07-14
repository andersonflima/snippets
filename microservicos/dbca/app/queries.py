"""Catálogo de queries de metadados configuradas por admin.

O nome (label) de cada query vira um botão-ação no frontend. Cada query declara
a implementação por engine (Aurora Postgres/MySQL = SQL; DynamoDB = operação de
metadados). O catálogo embutido é o default; um JSON externo em S3
(QUERIES_BUCKET/QUERIES_KEY) sobrepõe (deep-merge) sem redeploy.

Também mapeia recurso -> secretArn (fallback, quando o cluster não tem a tag
`dbca:secretArn`).
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

import boto3

QUERIES_BUCKET = os.getenv("QUERIES_BUCKET")
QUERIES_KEY = os.getenv("QUERIES_KEY", "dbca/queries.json")
_TTL = int(os.getenv("QUERIES_CACHE_TTL", "60"))
_CACHE: dict[str, tuple[float, dict]] = {}

# Catálogo default (admin-editável via S3). SQL 100% read-only (metadados).
DEFAULT_CATALOG: dict[str, Any] = {
    "queries": [
        {
            "id": "db-overview",
            "label": "Visão geral do banco",
            "description": "Versão, tamanho total e conexões ativas do banco.",
            "category": "Visão geral",
            "engines": {
                "aurora-postgresql": {
                    "sql": "SELECT current_setting('server_version') AS versao, "
                    "pg_size_pretty(pg_database_size(current_database())) AS tamanho, "
                    "(SELECT count(*) FROM pg_stat_activity) AS conexoes"
                },
                "aurora-mysql": {
                    "sql": "SELECT VERSION() AS versao, "
                    "(SELECT ROUND(SUM(data_length+index_length)/1024/1024,1) FROM information_schema.tables) AS tamanho_mb, "
                    "(SELECT COUNT(*) FROM information_schema.processlist) AS conexoes"
                },
                "dynamodb": {"op": "overview"},
            },
        },
        {
            "id": "table-sizes",
            "label": "Tamanho das tabelas",
            "description": "Top tabelas por tamanho em disco.",
            "category": "Storage",
            "engines": {
                "aurora-postgresql": {
                    "sql": "SELECT schemaname AS schema, relname AS tabela, "
                    "pg_size_pretty(pg_total_relation_size(relid)) AS tamanho, n_live_tup AS linhas_estimadas "
                    "FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 20"
                },
                "aurora-mysql": {
                    "sql": "SELECT table_schema AS schema_, table_name AS tabela, "
                    "ROUND((data_length+index_length)/1024/1024,1) AS tamanho_mb, table_rows AS linhas_estimadas "
                    "FROM information_schema.tables WHERE table_type='BASE TABLE' "
                    "ORDER BY (data_length+index_length) DESC LIMIT 20"
                },
                "dynamodb": {"op": "table-metadata"},
            },
        },
        {
            "id": "active-connections",
            "label": "Conexões ativas",
            "description": "Sessões/conexões abertas por estado e usuário.",
            "category": "Atividade",
            "engines": {
                "aurora-postgresql": {
                    "sql": "SELECT usename AS usuario, state AS estado, count(*) AS conexoes "
                    "FROM pg_stat_activity GROUP BY usename, state ORDER BY conexoes DESC"
                },
                "aurora-mysql": {
                    "sql": "SELECT USER AS usuario, COMMAND AS comando, COUNT(*) AS conexoes "
                    "FROM information_schema.processlist GROUP BY USER, COMMAND ORDER BY conexoes DESC"
                },
            },
        },
        {
            "id": "long-running",
            "label": "Queries longas em execução",
            "description": "Consultas ativas há mais tempo (possíveis gargalos).",
            "category": "Atividade",
            "engines": {
                "aurora-postgresql": {
                    "sql": "SELECT pid, usename AS usuario, state AS estado, "
                    "EXTRACT(EPOCH FROM (now()-query_start))::int AS segundos, left(query,120) AS query "
                    "FROM pg_stat_activity WHERE state<>'idle' AND query_start IS NOT NULL "
                    "ORDER BY query_start ASC LIMIT 20"
                },
                "aurora-mysql": {
                    "sql": "SELECT ID AS pid, USER AS usuario, COMMAND AS comando, TIME AS segundos, LEFT(INFO,120) AS query "
                    "FROM information_schema.processlist WHERE COMMAND<>'Sleep' ORDER BY TIME DESC LIMIT 20"
                },
            },
        },
        {
            "id": "index-usage",
            "label": "Uso de índices",
            "description": "Índices por número de leituras (identifica índices ociosos).",
            "category": "Performance",
            "engines": {
                "aurora-postgresql": {
                    "sql": "SELECT schemaname AS schema, relname AS tabela, indexrelname AS indice, "
                    "idx_scan AS leituras FROM pg_stat_user_indexes ORDER BY idx_scan ASC LIMIT 20"
                },
            },
        },
        {
            "id": "capacity",
            "label": "Capacidade & throughput",
            "description": "Modo de cobrança, capacidade e índices (DynamoDB).",
            "category": "Capacidade",
            "engines": {
                "dynamodb": {"op": "capacity"},
            },
        },
    ],
    "environments": {
        "dev": {"readOnly": True},
        "homol": {"readOnly": True},
        "staging": {"readOnly": True},
        "prod": {"readOnly": True},
    },
    # Fallback recurso -> secretArn quando o cluster não tem a tag dbca:secretArn.
    "secretMap": {},
}


def _merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        elif k == "queries" and isinstance(v, list):
            out[k] = v  # lista externa substitui o catálogo default por completo
        else:
            out[k] = v
    return out


def _fetch_s3() -> dict:
    if not QUERIES_BUCKET:
        return {}
    cache_key = f"{QUERIES_BUCKET}/{QUERIES_KEY}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and now - cached[0] < _TTL:
        return cached[1]
    try:
        s3 = boto3.client("s3")  # identidade da plataforma (IRSA)
        body = s3.get_object(Bucket=QUERIES_BUCKET, Key=QUERIES_KEY)["Body"].read()
        data = json.loads(body)
    except Exception:
        data = _CACHE.get(cache_key, (0, {}))[1]
    _CACHE[cache_key] = (now, data)
    return data


def load_catalog() -> dict:
    return _merge(DEFAULT_CATALOG, _fetch_s3())


def list_queries() -> list[dict]:
    """Catálogo público (sem SQL) para o frontend montar os botões."""
    out = []
    for q in load_catalog().get("queries", []):
        out.append(
            {
                "id": q.get("id"),
                "label": q.get("label"),
                "description": q.get("description", ""),
                "category": q.get("category", ""),
                "engines": sorted((q.get("engines") or {}).keys()),
            }
        )
    return out


def get_query(query_id: str) -> dict | None:
    for q in load_catalog().get("queries", []):
        if q.get("id") == query_id:
            return q
    return None


def env_config(catalog: dict, environment: str) -> dict:
    return (catalog.get("environments") or {}).get(environment) or {}


def secret_from_map(catalog: dict, resource: str) -> str | None:
    return (catalog.get("secretMap") or {}).get(resource)
