"""Provedor de regras de negócio externalizadas (S3 ou DynamoDB).

Regras ficam fora da imagem e são atualizáveis sem redeploy. O backend é
escolhido por RULES_BACKEND (s3|dynamodb) — obrigatório, sem default. A leitura
usa a identidade da plataforma (IRSA), com cache TTL e fallback resiliente: se a
regra não existir ou o backend falhar, os defaults embutidos continuam valendo.

Env:
  RULES_BACKEND    s3 | dynamodb (obrigatório)
  RULES_CACHE_TTL  segundos de cache (default 60)
  RULES_REGION     região do backend (default AWS_REGION | sa-east-1)
  # s3
  RULES_BUCKET      bucket das regras (obrigatório p/ s3)
  RULES_KEY_PREFIX  prefixo das chaves (default "rules") -> <prefix>/<service>.json
  # dynamodb
  RULES_TABLE  tabela (obrigatório p/ dynamodb)
  RULES_PK     nome da partition key (default "service")
  RULES_ATTR   atributo com as regras JSON/Map (default "rules")
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

import boto3

SERVICE = "rds-data"

_CACHE: dict[str, tuple[float, dict]] = {}


class RulesConfigError(Exception):
    """Backend de regras ausente/mal configurado."""


def _ttl() -> int:
    try:
        return int(os.getenv("RULES_CACHE_TTL", "60"))
    except ValueError:
        return 60


def _region() -> str:
    return (
        os.getenv("RULES_REGION")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or "sa-east-1"
    )


def _fetch_s3(service: str) -> dict:
    bucket = os.getenv("RULES_BUCKET")
    if not bucket:
        raise RulesConfigError("RULES_BUCKET não configurado para RULES_BACKEND=s3")
    prefix = os.getenv("RULES_KEY_PREFIX", "rules").strip("/")
    key = f"{prefix}/{service}.json" if prefix else f"{service}.json"
    s3 = boto3.client("s3", region_name=_region())
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def _from_ddb(value: dict) -> Any:
    if "S" in value:
        return value["S"]
    if "N" in value:
        return float(value["N"]) if "." in value["N"] else int(value["N"])
    if "BOOL" in value:
        return value["BOOL"]
    if "NULL" in value:
        return None
    if "M" in value:
        return {k: _from_ddb(v) for k, v in value["M"].items()}
    if "L" in value:
        return [_from_ddb(v) for v in value["L"]]
    return None


def _fetch_dynamodb(service: str) -> dict:
    table = os.getenv("RULES_TABLE")
    if not table:
        raise RulesConfigError("RULES_TABLE não configurado para RULES_BACKEND=dynamodb")
    pk = os.getenv("RULES_PK", "service")
    attr = os.getenv("RULES_ATTR", "rules")
    ddb = boto3.client("dynamodb", region_name=_region())
    item = ddb.get_item(TableName=table, Key={pk: {"S": service}}).get("Item")
    if not item or attr not in item:
        return {}
    raw = item[attr]
    if "S" in raw:
        return json.loads(raw["S"])
    return _from_ddb(raw) or {}


def _fetch(service: str) -> dict:
    backend = (os.getenv("RULES_BACKEND") or "").strip().lower()
    if backend == "s3":
        return _fetch_s3(service)
    if backend == "dynamodb":
        return _fetch_dynamodb(service)
    raise RulesConfigError("RULES_BACKEND obrigatório: defina 's3' ou 'dynamodb'")


def _merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def load_rules(defaults: dict | None = None, service: str | None = None) -> dict:
    """Regras efetivas: defaults embutidos sobrepostos pelas regras externas.

    Nunca levanta por regra ausente — em miss/falha retorna os defaults (ou o
    último valor em cache), preservando a operação do serviço.
    """
    svc = service or SERVICE
    base = dict(defaults or {})
    now = time.time()
    cached = _CACHE.get(svc)
    if cached and now - cached[0] < _ttl():
        return _merge(base, cached[1])
    try:
        fetched = _fetch(svc) or {}
        _CACHE[svc] = (now, fetched)
        return _merge(base, fetched)
    except Exception:
        if cached:
            return _merge(base, cached[1])
        return base
