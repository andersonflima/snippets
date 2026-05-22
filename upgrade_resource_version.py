#!/usr/bin/env python3
"""
upgrade_resource_version.py

Exemplos de version-map por cenario:

1) ElastiCache (cluster/replication-group) com troca de tipo de instancia:
{
  "redis:7.0": {"engine": "redis", "version": "7.1", "instanceType": "cache.r7g.large"},
  "valkey:7.2": {"engine": "valkey", "version": "8.0", "instanceType": "cache.r7g.large"}
}
CLI:
  --resource elasticache --instance-type cache.r7g.large

2) RDS (resource_kind=db-instance) com DBInstanceClass:
{
  "mysql:8.0": {"engine": "mysql", "version": "8.4", "instanceType": "db.r7g.large"},
  "postgres:14": {"engine": "postgres", "version": "16", "instanceType": "db.r7g.large"}
}
CLI:
  --resource rds --instance-type db.r7g.large

3) DocDB (resource_kind=db-instance):
{
  "docdb:4.0": {"engine": "docdb", "version": "5.0", "instanceType": "db.r6g.large"}
}
CLI:
  --resource docdb --instance-type db.r6g.large

4) Neptune (resource_kind=db-instance):
{
  "neptune:1.2": {"engine": "neptune", "version": "1.3", "instanceType": "db.r6g.large"}
}
CLI:
  --resource neptune --instance-type db.r6g.large

5) Redshift (resource_kind=cluster) com NodeType:
{
  "redshift:1.0": {"engine": "redshift", "version": "1.0.70720", "instanceType": "ra3.xlplus"}
}
CLI:
  --resource redshift --instance-type ra3.xlplus

Notas:
- O version-map e sempre JSON objeto "origem":"destino" ou "origem":{"engine","version"}.
- O tipo de instancia pode ser informado no map via "instanceType".
- Quando houver "instanceType" no map, ele tem prioridade sobre --instance-type.
- --instance-type requer execucao com um unico --resource por vez.
- ElastiCache serverless-cache nao suporta --instance-type.
- RDS/DocDB/Neptune resource_kind=db-cluster nao suporta --instance-type no fluxo atual.
- O plano de update parte do version-map: se currentVersion == targetVersion e
  currentEngine == targetEngine, o recurso nao entra no update.
"""

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cmp_to_key, reduce
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError

GLOBAL_AWS = {
    "AWS_REGION": os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "sa-east-1",
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or "sa-east-1",
    "AWS_PROFILE": os.getenv("AWS_PROFILE", ""),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN", ""),
}

if GLOBAL_AWS["AWS_PROFILE"] and not os.getenv("AWS_PROFILE"):
    os.environ["AWS_PROFILE"] = GLOBAL_AWS["AWS_PROFILE"]

SUPPORTED_RESOURCES = {
    "elasticache",
    "rds",
    "eks",
    "opensearch",
    "lambda",
    "docdb",
    "neptune",
    "redshift",
}

RESOURCE_TYPE_ALL = "all"
RESOURCE_TYPE_ALIASES: Dict[str, str] = {
    "elasticache": "elasticache",
    "elastic-cache": "elasticache",
    "ec": "elasticache",
    "rds": "rds",
    "rds-instance": "rds",
    "rds-cluster": "rds",
    "eks": "eks",
    "opensearch": "opensearch",
    "elasticsearch": "opensearch",
    "es": "opensearch",
    "lambda": "lambda",
    "lambda-function": "lambda",
    "functions": "lambda",
    "docdb": "docdb",
    "documentdb": "docdb",
    "document-db": "docdb",
    "neptune": "neptune",
    "neptune-db": "neptune",
    "redshift": "redshift",
    "redshift-cluster": "redshift",
}

SUCCESS_HEADERS = [
    "account_id",
    "resource_type",
    "resource_kind",
    "resource_id",
    "engine",
    "target_engine",
    "parameter_group",
    "target_parameter_group",
    "option_group",
    "target_option_group",
    "region",
    "current_version",
    "target_version",
    "status",
    "message",
    "arn",
]

ERROR_HEADERS = [
    "stage",
    "account_id",
    "resource_type",
    "resource_kind",
    "resource_id",
    "engine",
    "target_engine",
    "parameter_group",
    "target_parameter_group",
    "option_group",
    "target_option_group",
    "region",
    "current_version",
    "target_version",
    "error_category",
    "retryable",
    "recommended_action",
    "error_type",
    "error_message",
]

PROGRESS_BAR_WIDTH = 30
DEFAULT_DISCOVERY_THREADS = max(1, min(os.cpu_count() or 4, 12))
DEFAULT_UPDATE_THREADS = max(1, min(os.cpu_count() or 4, 12))
DEFAULT_REGION_THREADS = 6
DEFAULT_AWS_MAX_ATTEMPTS = 4
AWS_RETRY_BASE_DELAY_MS = 250
AWS_RETRY_MAX_DELAY_MS = 5000
RDS_STATUS_POLL_INTERVAL_MS = 30_000
RDS_STATUS_TIMEOUT_MS = 60 * 60 * 1000
RUNTIME_STATE_LOCK = threading.RLock()

ACCOUNT_ID_KEYS = ("account_id", "accountId", "account", "id")
ACCOUNT_ROLE_ARN_KEYS = ("role_arn", "roleArn", "role")
ACCOUNT_ROLE_NAME_KEYS = ("role_name", "roleName")
ROLE_NAME_ENV_KEYS = ("ROLE_NAME", "UPGRADE_ROLE_NAME", "AWS_ASSUME_ROLE_NAME")
ENGINE_KEY_SEPARATOR = ":"
KNOWN_ENGINE_KEYS = {
    "redis",
    "valkey",
    "memcached",
    "mysql",
    "postgres",
    "postgresql",
    "mariadb",
    "aurora",
    "aurora-mysql",
    "aurora-postgresql",
    "kubernetes",
    "opensearch",
    "elasticsearch",
    "lambda",
    "nodejs",
    "python",
    "java",
    "dotnet",
    "go",
    "provided",
    "ruby",
    "docdb",
    "neptune",
    "redshift",
}


class RdsOptionGroupQuotaPrecheckError(RuntimeError):
    pass


def build_local_client_kwargs(region: Optional[str]) -> Dict[str, str]:
    kwargs: Dict[str, str] = {"region_name": region or GLOBAL_AWS["AWS_REGION"]}
    if GLOBAL_AWS["AWS_ACCESS_KEY_ID"] and GLOBAL_AWS["AWS_SECRET_ACCESS_KEY"]:
        kwargs["aws_access_key_id"] = GLOBAL_AWS["AWS_ACCESS_KEY_ID"]
        kwargs["aws_secret_access_key"] = GLOBAL_AWS["AWS_SECRET_ACCESS_KEY"]
        if GLOBAL_AWS["AWS_SESSION_TOKEN"]:
            kwargs["aws_session_token"] = GLOBAL_AWS["AWS_SESSION_TOKEN"]
    return kwargs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Atualiza versoes de recursos AWS por conta (assume role) e gera CSV de sucesso e erros."
        )
    )
    parser.add_argument(
        "--resource",
        required=True,
        help=(
            "Tipo de recurso para upgrade "
            "(elasticache, rds, docdb, neptune, eks, opensearch/elasticsearch, redshift, lambda ou all)."
        ),
    )
    parser.add_argument("--accounts-csv", required=True, help="CSV com as contas")
    parser.add_argument(
        "--role-name",
        required=False,
        help=(
            "Role padrao para contas sem role_arn/role_name no CSV. "
            "Fallback por env: ROLE_NAME, UPGRADE_ROLE_NAME, AWS_ASSUME_ROLE_NAME."
        ),
    )
    parser.add_argument(
        "--external-id",
        required=False,
        help="ExternalId opcional para assume role",
    )
    parser.add_argument(
        "--parameter-group-name",
        required=False,
        help="Nome explicito de parameter group para upgrades de engine em recursos que suportam parameter group.",
    )
    parser.add_argument(
        "--cluster-parameter-group-name",
        required=False,
        help="Nome explicito de cluster parameter group para upgrades de DB cluster Aurora.",
    )
    parser.add_argument(
        "--instance-parameter-group-name",
        required=False,
        help="Nome explicito de DB parameter group de instancia para upgrades de DB cluster Aurora.",
    )
    parser.add_argument(
        "--instance-type",
        required=False,
        help=(
            "Tipo de instancia alvo para recursos que suportam scale vertical "
            "(ex.: cache.r7g.large, db.r7g.large, ra3.xlplus)."
        ),
    )
    parser.add_argument("--version-map", required=False, help="JSON de de/para inline")
    parser.add_argument(
        "--version-map-file",
        required=False,
        help="Arquivo JSON de de/para",
    )
    parser.add_argument(
        "--threads",
        required=False,
        type=int,
        help="Quantidade de threads para discovery",
    )
    parser.add_argument(
        "--update-threads",
        required=False,
        type=int,
        help="Quantidade de threads para updates",
    )
    parser.add_argument(
        "--region-threads",
        required=False,
        type=int,
        help="Quantidade de regioes processadas em paralelo",
    )
    parser.add_argument("--region", required=False, help="Regiao unica (ex.: sa-east-1)")
    parser.add_argument("--out-csv", required=False, help="CSV de sucesso")
    parser.add_argument("--error-csv", required=False, help="CSV de erros")
    parser.add_argument(
        "--dry-run",
        nargs="?",
        const="true",
        default="false",
        help="Nao envia updates. Aceita true/false (default: false)",
    )

    args = parser.parse_args()
    args.dry_run = parse_boolean(args.dry_run)
    return args


def parse_boolean(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    if normalized in ("", "true", "1", "yes"):
        return True
    if normalized in ("false", "0", "no"):
        return False
    raise ValueError(f"Valor booleano invalido: {value}")


def normalize_role_name(role_name: Any) -> str:
    return str(role_name or "").strip()


def resolve_global_role_name(role_name_arg: Optional[str]) -> str:
    explicit_role_name = normalize_role_name(role_name_arg)
    if explicit_role_name:
        return explicit_role_name

    env_role_names = (
        normalize_role_name(os.getenv(env_key, "")) for env_key in ROLE_NAME_ENV_KEYS
    )
    return next((role_name for role_name in env_role_names if role_name), "")


def validate_args(args: argparse.Namespace) -> None:
    resolved_resources = resolve_resource_types(args.resource)
    if not resolved_resources:
        raise ValueError("Parametro invalido: --resource sem tipos resolvidos.")

    if not args.accounts_csv:
        raise ValueError("Parametro obrigatorio ausente: --accounts-csv")

    accounts_csv_path = os.path.abspath(args.accounts_csv)
    if not os.path.exists(accounts_csv_path):
        raise FileNotFoundError(f"Arquivo de contas nao encontrado: {accounts_csv_path}")

    if not args.version_map and not args.version_map_file:
        raise ValueError("Informe --version-map ou --version-map-file")

    if args.version_map and args.version_map_file:
        raise ValueError("Use apenas um parametro: --version-map ou --version-map-file")

    if args.version_map_file:
        version_map_path = os.path.abspath(args.version_map_file)
        if not os.path.exists(version_map_path):
            raise FileNotFoundError(f"Arquivo de de/para nao encontrado: {version_map_path}")

    validate_positive_integer_arg(args.threads, "--threads")
    validate_positive_integer_arg(args.update_threads, "--update-threads")
    validate_positive_integer_arg(args.region_threads, "--region-threads")
    validate_instance_type_option(
        instance_type=args.instance_type,
        resolved_resources=resolved_resources,
    )


def validate_positive_integer_arg(value: Optional[int], name: str) -> None:
    if value is None:
        return
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"Parametro invalido: {name} deve ser um inteiro positivo.")


def validate_instance_type_option(
    *,
    instance_type: Any,
    resolved_resources: Sequence[str],
) -> None:
    normalized_instance_type = normalize_version(instance_type)
    if not normalized_instance_type:
        return

    if any(char.isspace() for char in normalized_instance_type):
        raise ValueError(
            "Parametro invalido: --instance-type nao pode conter espacos."
        )

    if len(resolved_resources) != 1:
        raise ValueError(
            "Parametro invalido: --instance-type requer apenas um --resource por execucao. "
            "Use chamadas separadas por tipo de recurso."
        )

    resource_type = normalize_resource(resolved_resources[0])
    validate_instance_type_for_resource(
        instance_type=normalized_instance_type,
        resource_type=resource_type,
    )


def validate_instance_type_for_resource(
    *,
    instance_type: str,
    resource_type: str,
) -> None:
    normalized_instance_type = normalize_version(instance_type)
    normalized_resource_type = normalize_resource(resource_type)
    if not normalized_instance_type:
        return

    supported_resources = {"elasticache", "rds", "docdb", "neptune", "redshift"}
    if normalized_resource_type not in supported_resources:
        raise ValueError(
            "Parametro invalido: instanceType suportado apenas para "
            "elasticache, rds, docdb, neptune e redshift."
        )

    if (
        normalized_resource_type == "elasticache"
        and not normalized_instance_type.lower().startswith("cache.")
    ):
        raise ValueError(
            "Parametro invalido: instanceType para ElastiCache deve iniciar com "
            "'cache.' (ex.: cache.r7g.large)."
        )

    if (
        normalized_resource_type in {"rds", "docdb", "neptune"}
        and not normalized_instance_type.lower().startswith("db.")
    ):
        raise ValueError(
            "Parametro invalido: instanceType para RDS/DocDB/Neptune deve iniciar com "
            "'db.' (ex.: db.r7g.large)."
        )


def normalize_resource(resource: Any) -> str:
    return str(resource or "").strip().lower()


def normalize_resource_type(resource: Any) -> str:
    normalized_resource = normalize_resource(resource).replace("_", "-")
    return RESOURCE_TYPE_ALIASES.get(normalized_resource, normalized_resource)


def resource_type_from_cli(raw_resource: Any) -> str:
    normalized_resource = normalize_resource_type(raw_resource)
    if normalized_resource == RESOURCE_TYPE_ALL:
        return RESOURCE_TYPE_ALL
    return normalized_resource


def resolve_resource_types(raw_resource: Any) -> List[str]:
    requested_resource = resource_type_from_cli(raw_resource)
    if requested_resource == RESOURCE_TYPE_ALL:
        return sorted(SUPPORTED_RESOURCES)

    if requested_resource not in SUPPORTED_RESOURCES:
        raise ValueError(
            f"Tipo de recurso nao suportado: {requested_resource}. "
            f"Suportados: {', '.join(sorted(SUPPORTED_RESOURCES))}"
        )

    return [requested_resource]


def normalize_engine(engine: Any) -> str:
    return str(engine or "").strip().lower()


def normalize_version(version: Any) -> str:
    return str(version or "").strip()


def normalize_rds_status(status: Any) -> str:
    return normalize_version(status).lower()


def build_message(*parts: Any) -> str:
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def resolve_target_instance_type_for_update(
    *,
    resource: Dict[str, str],
    adapter_options: Dict[str, Any],
) -> str:
    mapped_instance_type = normalize_version(resource.get("targetInstanceType"))
    if mapped_instance_type:
        return mapped_instance_type
    return normalize_version(safe_read(adapter_options, "instance_type", ""))


def normalize_elasticache_cluster_mode(cluster_mode: Any) -> str:
    if isinstance(cluster_mode, bool):
        return "on" if cluster_mode else "off"

    normalized = str(cluster_mode or "").strip().lower()
    if normalized in ("on", "enabled", "true", "1", "yes", "compatible"):
        return "on"
    if normalized in ("off", "disabled", "false", "0", "no"):
        return "off"
    return ""


def resolve_replication_group_cluster_mode(group: Dict[str, Any]) -> str:
    explicit_mode = normalize_elasticache_cluster_mode(group.get("ClusterMode"))
    if explicit_mode:
        return explicit_mode

    return normalize_elasticache_cluster_mode(group.get("ClusterEnabled"))


def load_version_map(args: argparse.Namespace) -> Tuple[Dict[str, str], ...]:
    source_text = (
        read_text_file(os.path.abspath(args.version_map_file))
        if args.version_map_file
        else str(args.version_map)
    )

    try:
        parsed = json.loads(source_text)
    except json.JSONDecodeError as error:
        raise ValueError(f"JSON de de/para invalido: {error}") from error

    if not isinstance(parsed, dict):
        raise ValueError("JSON de de/para deve ser um objeto chave/valor.")

    entries: List[Dict[str, str]] = []
    for source_raw, target_raw in parsed.items():
        source = parse_version_map_source(source_raw)
        target = parse_version_map_target(target_raw)
        if not source.get("version") or not target.get("version"):
            continue

        entries.append(
            {
                "source_engine": source.get("engine", ""),
                "source_version": source.get("version", ""),
                "target_engine": target.get("engine", ""),
                "target_version": target.get("version", ""),
                "target_instance_type": target.get("instance_type", ""),
            }
        )

    if not entries:
        raise ValueError("JSON de de/para vazio apos normalizacao.")

    return tuple(entries)


def parse_version_map_source(source_raw: Any) -> Dict[str, str]:
    normalized_source = normalize_version(source_raw)
    if not normalized_source:
        return {"engine": "", "version": ""}

    separator_index = normalized_source.find(ENGINE_KEY_SEPARATOR)
    if separator_index <= 0:
        return {"engine": "", "version": normalized_source}

    source_engine = normalize_engine(normalized_source[:separator_index])
    source_version = normalize_version(normalized_source[separator_index + 1 :])
    if (
        not source_engine
        or not source_version
        or source_engine not in KNOWN_ENGINE_KEYS
    ):
        return {"engine": "", "version": normalized_source}

    return {"engine": source_engine, "version": source_version}


def parse_version_map_target(target_raw: Any) -> Dict[str, str]:
    if isinstance(target_raw, (str, int, float)):
        normalized_target = normalize_version(target_raw)
        separator_index = normalized_target.find(ENGINE_KEY_SEPARATOR)
        if separator_index > 0:
            parsed_engine = normalize_engine(normalized_target[:separator_index])
            parsed_version = normalize_version(normalized_target[separator_index + 1 :])
            if parsed_engine and parsed_version and parsed_engine in KNOWN_ENGINE_KEYS:
                return {
                    "engine": parsed_engine,
                    "version": parsed_version,
                    "instance_type": "",
                }
        return {"engine": "", "version": normalized_target, "instance_type": ""}

    if not isinstance(target_raw, dict):
        return {"engine": "", "version": "", "instance_type": ""}

    target_engine = normalize_engine(
        target_raw.get("engine") or target_raw.get("targetEngine") or ""
    )
    target_version = normalize_version(
        target_raw.get("version")
        or target_raw.get("targetVersion")
        or target_raw.get("engineVersion")
        or target_raw.get("majorVersion")
        or target_raw.get("runtime")
        or ""
    )
    target_instance_type = normalize_version(
        target_raw.get("instanceType")
        or target_raw.get("targetInstanceType")
        or target_raw.get("instance_type")
        or target_raw.get("cacheNodeType")
        or target_raw.get("dbInstanceClass")
        or target_raw.get("nodeType")
        or ""
    )

    return {
        "engine": target_engine,
        "version": target_version,
        "instance_type": target_instance_type,
    }


def build_update_plan(
    resources: Sequence[Dict[str, str]],
    version_map_entries: Sequence[Dict[str, str]],
) -> List[Dict[str, str]]:
    return [
        planned_resource
        for planned_resource in (
            build_resource_update_plan_entry(resource, version_map_entries)
            for resource in resources
        )
        if planned_resource is not None
    ]


def build_resource_update_plan_entry(
    resource: Dict[str, str],
    version_map_entries: Sequence[Dict[str, str]],
) -> Optional[Dict[str, str]]:
    target = resolve_target_for_resource(resource, version_map_entries)
    if target is None:
        return None

    current_version = normalize_version(resource.get("currentVersion"))
    current_engine = normalize_engine(resource.get("engine"))
    target_version = normalize_version(target.get("target_version"))
    target_engine = normalize_engine(target.get("target_engine") or current_engine)
    current_instance_type = normalize_version(resource.get("instanceType"))
    target_instance_type = normalize_version(target.get("target_instance_type"))
    if target_instance_type:
        validate_instance_type_for_resource(
            instance_type=target_instance_type,
            resource_type=resource.get("resourceType", ""),
        )

    if (
        current_version == target_version
        and current_engine == target_engine
        and (not target_instance_type or current_instance_type == target_instance_type)
    ):
        return None

    return {
        **resource,
        "targetVersion": target_version,
        "targetEngine": target_engine,
        "targetInstanceType": target_instance_type,
        "targetOptionGroupName": infer_planned_rds_target_option_group_name(
            resource=resource,
            target_engine=target_engine,
            target_version=target_version,
        ),
    }


def should_migrate_rds_option_group(
    *,
    resource: Dict[str, str],
    target_engine: str,
    target_version: str,
) -> bool:
    return (
        normalize_resource(resource.get("resourceType")) == "rds"
        and normalize_resource(resource.get("resourceKind")) == "db-instance"
        and bool(normalize_version(resource.get("optionGroupName")))
        and has_rds_major_version_change(
            current_engine=resource.get("engine", ""),
            current_version=resource.get("currentVersion", ""),
            target_engine=target_engine,
            target_version=target_version,
        )
    )


def infer_planned_rds_target_option_group_name(
    *,
    resource: Dict[str, str],
    target_engine: str,
    target_version: str,
) -> str:
    if not should_migrate_rds_option_group(
        resource=resource,
        target_engine=target_engine,
        target_version=target_version,
    ):
        return ""

    source_option_group_name = normalize_version(resource.get("optionGroupName"))

    normalized_target_engine = normalize_engine(target_engine or resource.get("engine"))
    target_major_engine_version = infer_rds_major_engine_version(
        target_engine=normalized_target_engine,
        target_version=target_version,
    )
    if not target_major_engine_version:
        return ""

    if not should_create_custom_rds_option_group(
        source_option_group_name=source_option_group_name
    ):
        return build_default_rds_option_group_name(
            target_engine=normalized_target_engine,
            target_major_engine_version=target_major_engine_version,
        )

    return build_unique_rds_option_group_name(
        source_option_group_name=source_option_group_name,
        target_engine=normalized_target_engine,
        target_major_engine_version=target_major_engine_version,
        resource=resource,
    )


def resolve_target_for_resource(
    resource: Dict[str, str],
    version_map_entries: Sequence[Dict[str, str]],
) -> Optional[Dict[str, str]]:
    current_version = normalize_version(resource.get("currentVersion"))
    current_engine = normalize_engine(resource.get("engine"))
    if not current_version:
        return None

    matching_entries = sorted(
        [
            entry
            for entry in version_map_entries
            if version_map_entry_matches_resource(entry, current_version, current_engine)
        ],
        key=cmp_to_key(compare_version_map_specificity),
    )

    if not matching_entries:
        return None

    selected = matching_entries[0]
    return {
        "target_version": normalize_version(selected.get("target_version")),
        "target_engine": normalize_engine(selected.get("target_engine") or current_engine),
        "target_instance_type": normalize_version(selected.get("target_instance_type")),
    }


def version_map_entry_matches_resource(
    entry: Dict[str, str],
    current_version: str,
    current_engine: str,
) -> bool:
    source_version = normalize_version(entry.get("source_version"))
    target_version = normalize_version(entry.get("target_version"))
    source_engine = normalize_engine(entry.get("source_engine"))

    if not source_version or not target_version:
        return False

    if source_engine and source_engine != current_engine:
        return False

    return version_matches_source(current_version, source_version)


def version_matches_source(current_version: str, source_version: str) -> bool:
    if current_version == source_version:
        return True
    return current_version.startswith(f"{source_version}.")


def compare_version_map_specificity(left: Dict[str, str], right: Dict[str, str]) -> int:
    left_engine_weight = 1 if left.get("source_engine") else 0
    right_engine_weight = 1 if right.get("source_engine") else 0
    if left_engine_weight != right_engine_weight:
        return right_engine_weight - left_engine_weight

    left_exact_weight = 1 if "." in normalize_version(left.get("source_version")) else 0
    right_exact_weight = 1 if "." in normalize_version(right.get("source_version")) else 0
    if left_exact_weight != right_exact_weight:
        return right_exact_weight - left_exact_weight

    return len(normalize_version(right.get("source_version"))) - len(
        normalize_version(left.get("source_version"))
    )


def read_text_file(path: str) -> str:
    with open(path, mode="r", encoding="utf-8") as handler:
        return handler.read()


def read_accounts_csv(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, mode="r", encoding="utf-8-sig", newline="") as handler:
        reader = csv.DictReader(handler)
        if not reader.fieldnames:
            return []

        return [
            normalize_account_record(row, line_index)
            for line_index, row in enumerate(reader, start=2)
        ]


def normalize_account_record(row: Dict[str, str], line_number: int) -> Dict[str, str]:
    account_id_raw = read_first(row, ACCOUNT_ID_KEYS)
    role_arn = read_first(row, ACCOUNT_ROLE_ARN_KEYS)
    role_name = read_first(row, ACCOUNT_ROLE_NAME_KEYS)

    if not account_id_raw:
        raise ValueError(f"Linha {line_number}: coluna account_id obrigatoria.")

    account_id = re.sub(r"\D", "", str(account_id_raw))
    if len(account_id) != 12:
        raise ValueError(f'Linha {line_number}: account_id invalido "{account_id_raw}".')

    return {
        "accountId": account_id,
        "roleArn": role_arn,
        "roleName": role_name,
    }


def deduplicate_accounts_by_id(accounts: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    def merge_account_records(
        merged_accounts: Dict[str, Dict[str, str]],
        account: Dict[str, str],
    ) -> Dict[str, Dict[str, str]]:
        account_id = account.get("accountId", "")
        existing = merged_accounts.get(account_id)
        if existing is None:
            return {**merged_accounts, account_id: account}

        return {
            **merged_accounts,
            account_id: {
                "accountId": existing.get("accountId") or account_id,
                "roleArn": existing.get("roleArn") or account.get("roleArn") or "",
                "roleName": existing.get("roleName") or account.get("roleName") or "",
            },
        }

    accounts_by_id = reduce(
        merge_account_records,
        accounts,
        {},
    )
    return list(accounts_by_id.values())


def read_first(record: Dict[str, Any], keys: Sequence[str]) -> str:
    return next(
        (
            value
            for value in (
                str(record.get(key) or "").strip()
                for key in keys
                if key in record
            )
            if value
        ),
        "",
    )


def build_discovery_account_result(
    *,
    account_id: str,
    account_resources: Sequence[Dict[str, str]],
    account_errors: Sequence[Dict[str, str]],
    skipped: int,
) -> Dict[str, Any]:
    return {
        "discoveredResources": list(account_resources),
        "discoveredErrors": list(account_errors),
        "skipped": skipped,
        "accountId": account_id,
        "resourceCount": len(account_resources),
        "errorCount": len(account_errors),
        "resources": len(account_resources),
        "errors": len(account_errors),
    }


def build_discovery_error_result(
    *,
    account: Dict[str, str],
    resource_type: str,
    error: Exception,
) -> Dict[str, Any]:
    return {
        "discoveredResources": [],
        "discoveredErrors": [
            build_error_row(
                stage="discover",
                account_id=account.get("accountId", ""),
                resource_type=resource_type,
                resource_kind="",
                resource_id="",
                engine="",
                region="",
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "AccountDiscoveryError",
                error_message=str(error),
            )
        ],
        "skipped": 0,
        "accountId": account.get("accountId", ""),
        "resourceCount": 0,
        "errorCount": 1,
        "resources": 0,
        "errors": 1,
    }


def discover_account_resources(
    *,
    accounts: Sequence[Dict[str, str]],
    adapter: Dict[str, Callable[..., Any]],
    resource_type: str,
    role_name: Optional[str],
    external_id: Optional[str],
    forced_regions: Sequence[str],
    threads: int,
    region_threads: int,
    on_progress: Callable[[Dict[str, Any]], None],
) -> Dict[str, Any]:
    def worker(account: Dict[str, str], _: int) -> Dict[str, Any]:
        try:
            credentials = assume_role_for_account(account, role_name, external_id)
            described = adapter["describe_resources"](
                account_id=account.get("accountId", ""),
                credentials=credentials,
                forced_regions=list(forced_regions),
                region_threads=region_threads,
            )

            account_resources = described.get("resources", [])
            account_errors = described.get("errors", [])
            skipped = 1 if not account_resources and not account_errors else 0

            return build_discovery_account_result(
                account_id=account.get("accountId", ""),
                account_resources=account_resources,
                account_errors=account_errors,
                skipped=skipped,
            )
        except Exception as error:  # noqa: BLE001
            return build_discovery_error_result(
                account=account,
                resource_type=resource_type,
                error=error,
            )

    worker_results = run_in_workers(
        items=list(accounts),
        worker_count=max(1, min(threads, len(accounts) or 1)),
        worker=worker,
        on_step=on_progress,
        on_error=lambda account, error: build_discovery_error_result(
            account=account,
            resource_type=resource_type,
            error=error,
        ),
    )

    aggregated_discovery = reduce_discovery_results(worker_results)

    return {
        "resources": aggregated_discovery["resources"],
        "errors": aggregated_discovery["errors"],
        "skippedAccounts": aggregated_discovery["skippedAccounts"],
    }


def execute_updates(
    *,
    updates: Sequence[Dict[str, str]],
    adapter: Dict[str, Callable[..., Any]],
    resource_type: str,
    role_name: Optional[str],
    external_id: Optional[str],
    account_by_id: Dict[str, Dict[str, str]],
    threads: int,
    dry_run: bool,
    adapter_options: Dict[str, Any],
    on_progress: Callable[[Dict[str, Any]], None],
) -> Dict[str, Any]:
    credentials_by_account: Dict[str, Dict[str, str]] = {}
    credentials_lock = threading.Lock()

    def get_account_credentials(account_id: str) -> Dict[str, str]:
        with credentials_lock:
            cached = credentials_by_account.get(account_id)
            if cached:
                return cached

        account = account_by_id.get(account_id)
        if account is None:
            raise ValueError(f"Conta nao encontrada para update: {account_id}")

        credentials = assume_role_for_account(account, role_name, external_id)

        with credentials_lock:
            credentials_by_account[account_id] = credentials

        return credentials

    def worker(resource: Dict[str, str], _: int) -> Dict[str, Any]:
        if dry_run:
            return build_update_dry_run_result(resource)

        try:
            credentials = get_account_credentials(resource.get("accountId", ""))
            update_response = adapter["submit_update"](
                resource=resource,
                target_version=resource.get("targetVersion", ""),
                target_engine=resource.get("targetEngine", ""),
                adapter_options=adapter_options,
                credentials=credentials,
            )
            return build_update_success_result(
                resource=resource,
                update_response=update_response,
            )
        except Exception as error:  # noqa: BLE001
            return build_update_error_result(
                resource=resource,
                resource_type=resource_type,
                error=error,
            )

    worker_results = run_in_workers(
        items=list(updates),
        worker_count=max(1, min(threads, len(updates) or 1)),
        worker=worker,
        on_step=on_progress,
        on_error=lambda resource, error: build_update_error_result(
            resource=resource,
            resource_type=resource_type,
            error=error,
        ),
    )

    aggregated_updates = reduce_update_results(worker_results)
    return {"success": aggregated_updates["success"], "errors": aggregated_updates["errors"]}

def build_update_error_result(
    *,
    resource: Dict[str, str],
    resource_type: str,
    error: Exception,
) -> Dict[str, Any]:
    return {
        "success": [],
        "errors": [
            build_error_row(
                stage="update",
                account_id=resource.get("accountId", ""),
                resource_type=resource_type,
                resource_kind=resource.get("resourceKind", ""),
                resource_id=resource.get("resourceId", ""),
                engine=resource.get("engine", ""),
                target_engine=resource.get("targetEngine", ""),
                parameter_group_name=format_parameter_group_value_for_resource(resource),
                target_parameter_group_name=format_parameter_group_value_for_resource(
                    resource,
                    target=True,
                ),
                option_group_name=format_option_group_value_for_resource(resource),
                target_option_group_name=format_option_group_value_for_resource(
                    resource,
                    target=True,
                ),
                region=resource.get("region", ""),
                current_version=resource.get("currentVersion", ""),
                target_version=resource.get("targetVersion", ""),
                error_type=type(error).__name__ or "UpdateError",
                error_message=str(error),
            )
        ],
        "accountId": resource.get("accountId", ""),
        "resourceId": resource.get("resourceId", ""),
        "resources": 0,
        "errorsCount": 1,
    }


def build_update_dry_run_result(resource: Dict[str, str]) -> Dict[str, Any]:
    return {
        "success": [
            build_success_row(
                resource=resource,
                status="dry-run",
                message="Update nao enviado (dry-run).",
            )
        ],
        "errors": [],
        "accountId": resource.get("accountId", ""),
        "resourceId": resource.get("resourceId", ""),
        "resources": 1,
        "errorsCount": 0,
    }


def build_update_success_result(
    *,
    resource: Dict[str, str],
    update_response: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "success": [
            build_success_row(
                resource={
                    **resource,
                    "targetParameterGroupName": (
                        update_response.get("targetParameterGroupName")
                        or resource.get("targetParameterGroupName")
                        or ""
                    ),
                    "targetOptionGroupName": (
                        update_response.get("targetOptionGroupName")
                        or resource.get("targetOptionGroupName")
                        or ""
                    ),
                    "targetClusterParameterGroupName": (
                        update_response.get("targetClusterParameterGroupName")
                        or resource.get("targetClusterParameterGroupName")
                        or ""
                    ),
                    "targetInstanceParameterGroupName": (
                        update_response.get("targetInstanceParameterGroupName")
                        or resource.get("targetInstanceParameterGroupName")
                        or ""
                    ),
                },
                status=update_response.get("status", "submitted"),
                message=update_response.get("message", "Solicitacao de update enviada."),
            )
        ],
        "errors": [],
        "accountId": resource.get("accountId", ""),
        "resourceId": resource.get("resourceId", ""),
        "resources": 1,
        "errorsCount": 0,
    }


def reduce_discovery_results(worker_results: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "resources": [
            resource
            for result in worker_results
            for resource in result.get("discoveredResources", [])
        ],
        "errors": [
            error
            for result in worker_results
            for error in result.get("discoveredErrors", [])
        ],
        "skippedAccounts": sum(int(result.get("skipped") or 0) for result in worker_results),
    }


def reduce_update_results(worker_results: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "success": [
            success_row
            for result in worker_results
            for success_row in result.get("success", [])
        ],
        "errors": [
            error_row
            for result in worker_results
            for error_row in result.get("errors", [])
        ],
    }


def run_in_workers(
    *,
    items: Sequence[Any],
    worker_count: int,
    worker: Callable[[Any, int], Dict[str, Any]],
    on_step: Optional[Callable[[Dict[str, Any]], None]],
    on_error: Optional[Callable[[Any, Exception], Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    if not items:
        return []

    safe_on_step = on_step or (lambda _: None)
    processed = 0
    success_count = 0
    error_count = 0
    results: List[Optional[Dict[str, Any]]] = [None] * len(items)

    with ThreadPoolExecutor(max_workers=max(1, worker_count)) as executor:
        future_to_index = {
            executor.submit(worker, item, index): (index, item)
            for index, item in enumerate(items)
        }

        for future in as_completed(future_to_index):
            index, item = future_to_index[future]
            try:
                result = future.result()
            except Exception as error:  # noqa: BLE001
                if on_error is None:
                    raise
                result = on_error(item, error) or {}

            if not isinstance(result, dict):
                result = {}
            results[index] = result

            resource_delta = resolve_result_count(
                primary_value=result.get("resources"),
                fallback_value=result.get("resourceCount"),
            )
            error_delta = resolve_result_count(
                primary_value=result.get("errors"),
                fallback_value=result.get("errorCount") or result.get("errorsCount"),
            )
            processed += 1
            success_count += resource_delta
            error_count += error_delta

            safe_on_step(
                {
                    "processed": processed,
                    "currentItem": format_progress_current_item(result),
                    "successCount": success_count,
                    "errorCount": error_count,
                }
            )

    return [result for result in results if result is not None]


def resolve_result_count(primary_value: Any, fallback_value: Any) -> int:
    if isinstance(primary_value, bool):
        return int(primary_value)

    if isinstance(primary_value, int):
        return primary_value

    if isinstance(primary_value, float):
        return int(primary_value)

    if isinstance(primary_value, (list, tuple, set, dict)):
        return len(primary_value)

    if isinstance(fallback_value, bool):
        return int(fallback_value)

    if isinstance(fallback_value, int):
        return fallback_value

    if isinstance(fallback_value, float):
        return int(fallback_value)

    if isinstance(fallback_value, (list, tuple, set, dict)):
        return len(fallback_value)

    return 0


def format_progress_current_item(step_result: Dict[str, Any]) -> str:
    account_id = str(step_result.get("accountId") or "").strip()
    resource_id = str(step_result.get("resourceId") or "").strip()

    if account_id and resource_id:
        return f"{account_id}/{resource_id}"
    if account_id:
        return account_id
    if resource_id:
        return resource_id
    return "-"


def create_progress_tracker(
    *,
    total: int,
    label: str,
    success_label: str,
    error_label: str,
) -> Dict[str, Callable[..., None]]:
    safe_total = total if isinstance(total, int) and total >= 0 else 0
    supports_inline = bool(getattr(sys.stdout, "isatty", lambda: False)())
    lock = threading.Lock()

    state: Dict[str, Any] = {
        "total": safe_total,
        "processed": 0,
        "currentItem": "-",
        "successCount": 0,
        "errorCount": 0,
        "maxLineLength": 0,
    }

    def render(next_state: Dict[str, Any]) -> Dict[str, Any]:
        line = build_progress_line(
            total=next_state["total"],
            processed=next_state["processed"],
            current_item=next_state["currentItem"],
            success_count=next_state["successCount"],
            error_count=next_state["errorCount"],
            label=label,
            success_label=success_label,
            error_label=error_label,
        )
        max_line_length = max(next_state["maxLineLength"], len(line))

        if supports_inline:
            sys.stdout.write("\r" + line.ljust(max_line_length))
            sys.stdout.flush()
        else:
            print(line)

        return {**next_state, "maxLineLength": max_line_length}

    state = render(state)

    def update(step: Dict[str, Any]) -> None:
        nonlocal state
        with lock:
            state = render(
                {
                    **state,
                    "processed": clamp_progress(
                        int(step.get("processed") or 0),
                        safe_total,
                    ),
                    "currentItem": str(step.get("currentItem") or "-")[:90],
                    "successCount": int(step.get("successCount") or state["successCount"]),
                    "errorCount": int(step.get("errorCount") or state["errorCount"]),
                }
            )

    def finish() -> None:
        nonlocal state
        with lock:
            state = render({**state, "processed": safe_total})
            if supports_inline:
                sys.stdout.write("\n")
                sys.stdout.flush()

    return {"update": update, "finish": finish}


def build_progress_line(
    *,
    total: int,
    processed: int,
    current_item: str,
    success_count: int,
    error_count: int,
    label: str,
    success_label: str,
    error_label: str,
) -> str:
    percentage = (
        100
        if total == 0
        else max(0, min(100, round((processed / max(1, total)) * 100)))
    )
    filled = min(PROGRESS_BAR_WIDTH, round((percentage / 100) * PROGRESS_BAR_WIDTH))
    bar = "=" * filled + "." * (PROGRESS_BAR_WIDTH - filled)

    return (
        f"[{label}] [{bar}] {str(percentage).rjust(3, ' ')}% "
        f"{processed}/{total} atual={current_item or '-'} "
        f"{success_label}={success_count} {error_label}={error_count}"
    )


def clamp_progress(processed: int, total: int) -> int:
    if total <= 0:
        return 0
    return max(0, min(total, int(processed)))


def describe_elasticache_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
) -> Dict[str, List[Dict[str, str]]]:
    resources: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []

    regions, region_discovery_error = discover_regions(credentials, list(forced_regions))
    if region_discovery_error is not None:
        errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="elasticache",
                resource_kind="",
                resource_id="",
                engine="",
                region="",
                current_version="",
                target_version="",
                error_type="RegionDiscoveryError",
                error_message=str(region_discovery_error),
            )
        )

    region_results = collect_regions_in_batches(
        regions=regions,
        region_threads=region_threads,
        collect_region=lambda region: collect_elasticache_region(
            account_id=account_id,
            credentials=credentials,
            region=region,
        ),
    )

    aggregated_region_results = append_region_results(region_results)
    return {
        "resources": [*resources, *aggregated_region_results["resources"]],
        "errors": [*errors, *aggregated_region_results["errors"]],
    }


def collect_elasticache_region(
    *,
    account_id: str,
    credentials: Dict[str, str],
    region: str,
) -> Dict[str, List[Dict[str, str]]]:
    region_resources: List[Dict[str, str]] = []
    region_errors: List[Dict[str, str]] = []
    seen: set = set()
    cluster_details_by_id: Dict[str, Dict[str, str]] = {}

    client = boto3.client("elasticache", region_name=region, **credentials)

    try:
        marker: Optional[str] = None
        while True:
            request: Dict[str, Any] = {"MaxRecords": 100}
            if marker:
                request["Marker"] = marker

            response = send_aws_call(lambda: client.describe_cache_clusters(**request))
            clusters = response.get("CacheClusters", [])

            for cluster in clusters:
                resource_id = str(cluster.get("CacheClusterId") or "").strip()
                if not resource_id:
                    continue

                parameter_group_name = normalize_version(
                    safe_read(
                        cluster,
                        "CacheParameterGroup.CacheParameterGroupName",
                        "",
                    )
                )
                cluster_details_by_id[resource_id] = {
                    "cacheClusterId": resource_id,
                    "replicationGroupId": str(cluster.get("ReplicationGroupId") or "").strip(),
                    "engine": normalize_engine(cluster.get("Engine") or "redis"),
                    "currentVersion": normalize_version(cluster.get("EngineVersion")),
                    "arn": str(cluster.get("ARN") or ""),
                    "parameterGroupName": parameter_group_name,
                    "instanceType": normalize_version(cluster.get("CacheNodeType")),
                }

                if cluster.get("ReplicationGroupId"):
                    continue

                dedup_key = f"cluster|{resource_id}"
                if dedup_key in seen:
                    continue

                seen.add(dedup_key)
                region_resources.append(
                    build_resource_row(
                        account_id=account_id,
                        resource_type="elasticache",
                        resource_kind="cluster",
                        resource_id=resource_id,
                        engine=normalize_engine(cluster.get("Engine") or "redis"),
                        region=region,
                        current_version=normalize_version(cluster.get("EngineVersion")),
                        arn=str(cluster.get("ARN") or ""),
                        parameter_group_name=parameter_group_name,
                        instance_type=normalize_version(cluster.get("CacheNodeType")),
                        cluster_mode=normalize_elasticache_cluster_mode(
                            cluster.get("ClusterEnabled")
                        )
                        or "off",
                    )
                )

            marker = response.get("Marker")
            if not marker:
                break
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="elasticache",
                resource_kind="cluster",
                resource_id="",
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "DescribeCacheClustersError",
                error_message=str(error),
            )
        )

    try:
        marker = None
        while True:
            request = {"MaxRecords": 100}
            if marker:
                request["Marker"] = marker

            response = send_aws_call(lambda: client.describe_replication_groups(**request))
            groups = response.get("ReplicationGroups", [])

            for group in groups:
                resource_id = str(group.get("ReplicationGroupId") or "").strip()
                if not resource_id:
                    continue

                dedup_key = f"replication-group|{resource_id}"
                if dedup_key in seen:
                    continue

                member_cluster_id = str((group.get("MemberClusters") or [""])[0] or "").strip()
                member_cluster = (
                    cluster_details_by_id.get(member_cluster_id)
                    if member_cluster_id
                    else None
                )

                seen.add(dedup_key)
                region_resources.append(
                    build_resource_row(
                        account_id=account_id,
                        resource_type="elasticache",
                        resource_kind="replication-group",
                        resource_id=resource_id,
                        engine=normalize_engine(
                            group.get("Engine")
                            or safe_read(member_cluster or {}, "engine", "redis")
                        ),
                        region=region,
                        current_version=normalize_version(
                            group.get("EngineVersion")
                            or safe_read(member_cluster or {}, "currentVersion", "")
                        ),
                        arn=str(
                            group.get("ARN")
                            or safe_read(member_cluster or {}, "arn", "")
                        ),
                        instance_type=normalize_version(
                            group.get("CacheNodeType")
                            or safe_read(member_cluster or {}, "instanceType", "")
                        ),
                        parameter_group_name=normalize_version(
                            safe_read(member_cluster or {}, "parameterGroupName", "")
                        ),
                        cluster_mode=resolve_replication_group_cluster_mode(group),
                    )
                )

            marker = response.get("Marker")
            if not marker:
                break
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="elasticache",
                resource_kind="replication-group",
                resource_id="",
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "DescribeReplicationGroupsError",
                error_message=str(error),
            )
        )

    try:
        next_token: Optional[str] = None
        while True:
            request = {"MaxResults": 100}
            if next_token:
                request["NextToken"] = next_token

            response = send_aws_call(lambda: client.describe_serverless_caches(**request))
            serverless_caches = response.get("ServerlessCaches", [])

            for cache in serverless_caches:
                resource_id = str(cache.get("ServerlessCacheName") or "").strip()
                if not resource_id:
                    continue

                dedup_key = f"serverless-cache|{resource_id}"
                if dedup_key in seen:
                    continue

                seen.add(dedup_key)
                region_resources.append(
                    build_resource_row(
                        account_id=account_id,
                        resource_type="elasticache",
                        resource_kind="serverless-cache",
                        resource_id=resource_id,
                        engine=normalize_engine(cache.get("Engine") or "redis"),
                        region=region,
                        current_version=normalize_version(
                            cache.get("MajorEngineVersion")
                            or cache.get("FullEngineVersion")
                            or ""
                        ),
                        arn=str(cache.get("ARN") or ""),
                        parameter_group_name="",
                    )
                )

            next_token = response.get("NextToken")
            if not next_token:
                break
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="elasticache",
                resource_kind="serverless-cache",
                resource_id="",
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "DescribeServerlessCachesError",
                error_message=str(error),
            )
        )

    return {"resources": region_resources, "errors": region_errors}


def submit_elasticache_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    client = boto3.client("elasticache", region_name=resource.get("region"), **credentials)

    normalized_target_engine = normalize_engine(target_engine)
    normalized_current_engine = normalize_engine(resource.get("engine"))
    is_engine_switch = bool(
        normalized_target_engine and normalized_target_engine != normalized_current_engine
    )

    explicit_group_name = normalize_version(safe_read(adapter_options, "parameter_group_name", ""))
    explicit_instance_type = resolve_target_instance_type_for_update(
        resource=resource,
        adapter_options=adapter_options,
    )

    resolved_target_parameter_group_name = (
        resolve_elasticache_target_parameter_group(
            client=client,
            resource=resource,
            target_engine=normalized_target_engine or normalized_current_engine,
            target_version=target_version,
            explicit_cache_parameter_group_name=explicit_group_name,
            adapter_options=adapter_options,
        )
        if resource.get("resourceKind") in ("replication-group", "cluster")
        else ""
    )

    resource_kind = resource.get("resourceKind")
    if resource_kind == "replication-group":
        modify_input: Dict[str, Any] = {
            "ReplicationGroupId": resource.get("resourceId"),
            "EngineVersion": target_version,
            "ApplyImmediately": True,
        }
        if is_engine_switch:
            modify_input["Engine"] = normalized_target_engine
        if resolved_target_parameter_group_name:
            modify_input["CacheParameterGroupName"] = resolved_target_parameter_group_name
        if explicit_instance_type:
            modify_input["CacheNodeType"] = explicit_instance_type

        response = send_aws_call(lambda: client.modify_replication_group(**modify_input))
        return {
            "status": safe_read(response, "ReplicationGroup.Status", "modifying"),
            "message": build_message(
                "Engine alterada para "
                f"{normalized_target_engine or normalized_current_engine or 'default'} "
                f"e versao para {target_version}",
                (
                    f"Tipo de instancia alterado para {explicit_instance_type}."
                    if explicit_instance_type
                    else ""
                ),
            ),
            "targetParameterGroupName": resolved_target_parameter_group_name,
        }

    if resource_kind == "cluster":
        modify_input = {
            "CacheClusterId": resource.get("resourceId"),
            "EngineVersion": target_version,
            "ApplyImmediately": True,
        }
        if is_engine_switch:
            modify_input["Engine"] = normalized_target_engine
        if resolved_target_parameter_group_name:
            modify_input["CacheParameterGroupName"] = resolved_target_parameter_group_name
        if explicit_instance_type:
            modify_input["CacheNodeType"] = explicit_instance_type

        response = send_aws_call(lambda: client.modify_cache_cluster(**modify_input))
        return {
            "status": safe_read(response, "CacheCluster.CacheClusterStatus", "modifying"),
            "message": build_message(
                "Engine alterada para "
                f"{normalized_target_engine or normalized_current_engine or 'default'} "
                f"e versao para {target_version}",
                (
                    f"Tipo de instancia alterado para {explicit_instance_type}."
                    if explicit_instance_type
                    else ""
                ),
            ),
            "targetParameterGroupName": resolved_target_parameter_group_name,
        }

    if resource_kind == "serverless-cache":
        if explicit_instance_type:
            raise ValueError(
                "ElastiCache serverless-cache nao suporta --instance-type "
                "(usa capacidade serverless, sem CacheNodeType)."
            )
        major_engine_version = extract_major_version(target_version) or normalize_version(
            target_version
        )
        modify_input = {
            "ServerlessCacheName": resource.get("resourceId"),
            "MajorEngineVersion": major_engine_version,
        }
        if is_engine_switch:
            modify_input["Engine"] = normalized_target_engine

        response = send_aws_call(lambda: client.modify_serverless_cache(**modify_input))
        return {
            "status": safe_read(response, "ServerlessCache.Status", "modifying"),
            "message": (
                "Engine alterada para "
                f"{normalized_target_engine or normalized_current_engine or 'default'} "
                f"e major_version para {major_engine_version}"
            ),
            "targetParameterGroupName": "",
        }

    raise ValueError(f"resource_kind invalido para elasticache: {resource_kind}")


def resolve_elasticache_target_parameter_group(
    *,
    client: Any,
    resource: Dict[str, str],
    target_engine: str,
    target_version: str,
    explicit_cache_parameter_group_name: str,
    adapter_options: Dict[str, Any],
) -> str:
    if explicit_cache_parameter_group_name:
        return explicit_cache_parameter_group_name

    source_parameter_group_name = normalize_version(resource.get("parameterGroupName"))
    if not source_parameter_group_name:
        return ""

    source_family = resolve_elasticache_source_parameter_group_family(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
        adapter_options=adapter_options,
    )
    source_cluster_mode = resolve_elasticache_cluster_mode_for_resource(
        resource=resource,
        source_family=source_family,
    )
    target_family = resolve_elasticache_parameter_group_family(
        client=client,
        target_engine=target_engine,
        target_version=target_version,
        adapter_options=adapter_options,
    )
    target_family = align_elasticache_target_family_with_source(
        target_family=target_family,
        source_family=source_family,
        source_cluster_mode=source_cluster_mode,
    )
    if not target_family:
        return ""

    if (
        not is_default_parameter_group(source_parameter_group_name)
        and normalize_version(source_family).lower() == normalize_version(target_family).lower()
    ):
        return source_parameter_group_name

    if is_default_parameter_group(source_parameter_group_name):
        return f"default.{target_family}"

    return ensure_custom_target_parameter_group_for_migration(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
        target_family=target_family,
        resource=resource,
        adapter_options=adapter_options,
    )


def ensure_elasticache_runtime_state(adapter_options: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    with RUNTIME_STATE_LOCK:
        runtime_state = safe_read(adapter_options, "elasticache_runtime_state", None)
        if not isinstance(runtime_state, dict):
            adapter_options["elasticache_runtime_state"] = {}
            runtime_state = adapter_options["elasticache_runtime_state"]

        runtime_state.setdefault("family_by_engine_version", {})
        runtime_state.setdefault("source_family_by_parameter_group", {})
        runtime_state.setdefault("custom_group_by_key", {})
        return runtime_state


def resolve_elasticache_source_parameter_group_family(
    *,
    client: Any,
    source_parameter_group_name: str,
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_elasticache_runtime_state(adapter_options)
    source_family_by_parameter_group: Dict[str, str] = runtime_state[
        "source_family_by_parameter_group"
    ]

    key = normalize_version(source_parameter_group_name).lower()
    with RUNTIME_STATE_LOCK:
        cached = source_family_by_parameter_group.get(key)
    if cached:
        return cached

    response = send_aws_call(
        lambda: client.describe_cache_parameter_groups(
            CacheParameterGroupName=source_parameter_group_name,
            MaxRecords=20,
        )
    )
    groups = response.get("CacheParameterGroups", [])
    selected = next(
        (
            group
            for group in groups
            if normalize_version(group.get("CacheParameterGroupName")).lower() == key
        ),
        groups[0] if groups else {},
    )

    family = normalize_version(selected.get("CacheParameterGroupFamily"))
    with RUNTIME_STATE_LOCK:
        source_family_by_parameter_group[key] = family
    return family


def align_elasticache_target_family_with_source(
    *,
    target_family: str,
    source_family: str,
    source_cluster_mode: str,
) -> str:
    normalized_target_family = normalize_version(target_family)
    normalized_source_family = normalize_version(source_family).lower()
    normalized_source_cluster_mode = normalize_elasticache_cluster_mode(
        source_cluster_mode
    )
    if not normalized_target_family:
        return ""

    source_cluster_mode_on = (
        normalized_source_cluster_mode == "on"
        if normalized_source_cluster_mode
        else normalized_source_family.endswith(".cluster.on")
    )
    target_cluster_mode_on = normalized_target_family.lower().endswith(".cluster.on")

    if source_cluster_mode_on and not target_cluster_mode_on:
        return f"{normalized_target_family}.cluster.on"

    if not source_cluster_mode_on and target_cluster_mode_on:
        return normalized_target_family[: -len(".cluster.on")]

    return normalized_target_family


def resolve_elasticache_cluster_mode_for_resource(
    *,
    resource: Dict[str, str],
    source_family: str,
) -> str:
    resource_cluster_mode = normalize_elasticache_cluster_mode(resource.get("clusterMode"))
    if resource_cluster_mode:
        return resource_cluster_mode

    normalized_source_family = normalize_version(source_family).lower()
    if not normalized_source_family:
        return ""

    return "on" if normalized_source_family.endswith(".cluster.on") else "off"


def resolve_elasticache_parameter_group_family(
    *,
    client: Any,
    target_engine: str,
    target_version: str,
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_elasticache_runtime_state(adapter_options)
    family_by_engine_version: Dict[str, str] = runtime_state["family_by_engine_version"]

    key = f"{target_engine}|{target_version}"
    with RUNTIME_STATE_LOCK:
        cached = family_by_engine_version.get(key)
    if cached:
        return cached

    response = send_aws_call(
        lambda: client.describe_cache_engine_versions(
            Engine=target_engine,
            EngineVersion=target_version,
            MaxRecords=100,
        )
    )

    versions = response.get("CacheEngineVersions", [])
    selected = next(
        (
            version
            for version in versions
            if normalize_version(version.get("EngineVersion"))
            == normalize_version(target_version)
        ),
        versions[0] if versions else {},
    )

    family = normalize_version(selected.get("CacheParameterGroupFamily"))
    if not family:
        major_version = extract_major_version(target_version)
        family = f"{target_engine}{major_version}" if target_engine and major_version else ""

    with RUNTIME_STATE_LOCK:
        family_by_engine_version[key] = family
    return family


def is_default_parameter_group(parameter_group_name: str) -> bool:
    return normalize_version(parameter_group_name).lower().startswith("default.")


def ensure_custom_target_parameter_group_for_migration(
    *,
    client: Any,
    source_parameter_group_name: str,
    target_family: str,
    resource: Dict[str, str],
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_elasticache_runtime_state(adapter_options)
    custom_group_by_key: Dict[str, str] = runtime_state["custom_group_by_key"]

    cache_key = "|".join(
        [
            resource.get("accountId", ""),
            resource.get("region", ""),
            source_parameter_group_name,
            target_family,
        ]
    )
    with RUNTIME_STATE_LOCK:
        cached = custom_group_by_key.get(cache_key)
    if cached:
        return cached

    target_parameter_group_name = build_unique_cache_parameter_group_name(
        source_parameter_group_name=source_parameter_group_name,
        target_family=target_family,
        resource=resource,
    )

    try:
        send_aws_call(
            lambda: client.create_cache_parameter_group(
                CacheParameterGroupFamily=target_family,
                CacheParameterGroupName=target_parameter_group_name,
                Description=(
                    f"Generated from {source_parameter_group_name} for {target_family}"
                ),
            )
        )
    except Exception as error:  # noqa: BLE001
        if not is_cache_parameter_group_already_exists_error(error):
            raise

    user_defined_parameters = list_user_defined_cache_parameters(
        client=client,
        cache_parameter_group_name=source_parameter_group_name,
    )

    apply_user_defined_parameters(
        client=client,
        target_parameter_group_name=target_parameter_group_name,
        parameters=user_defined_parameters,
    )

    with RUNTIME_STATE_LOCK:
        custom_group_by_key[cache_key] = target_parameter_group_name
    return target_parameter_group_name


def is_cache_parameter_group_already_exists_error(error: Exception) -> bool:
    error_code = extract_client_error_code(error)
    return error_code in (
        "CacheParameterGroupAlreadyExists",
        "CacheParameterGroupAlreadyExistsFault",
    )


def build_unique_cache_parameter_group_name(
    *,
    source_parameter_group_name: str,
    target_family: str,
    resource: Dict[str, str],
) -> str:
    cleaned_source_name = sanitize_cache_parameter_group_name(source_parameter_group_name)
    cleaned_family = sanitize_cache_parameter_group_name(target_family)
    hash_value = create_stable_hash(
        "|".join(
            [
                resource.get("accountId", ""),
                resource.get("region", ""),
                source_parameter_group_name,
                target_family,
                resource.get("resourceId", ""),
            ]
        )
    )
    base_name = re.sub(r"-+", "-", f"{cleaned_source_name}-{cleaned_family}")
    max_base_length = max(1, 255 - len(hash_value) - 1)
    trimmed_base_name = re.sub(r"-+$", "", base_name[:max_base_length])
    return f"{trimmed_base_name}-{hash_value}"


def sanitize_cache_parameter_group_name(name: str) -> str:
    normalized = normalize_version(name).lower()
    sanitized = re.sub(r"[^a-z0-9-]", "-", normalized)
    sanitized = re.sub(r"-+", "-", sanitized)
    sanitized = re.sub(r"^-+|-+$", "", sanitized)
    return sanitized or "cache-parameter-group"


def create_stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]


def list_user_defined_cache_parameters(
    *,
    client: Any,
    cache_parameter_group_name: str,
) -> List[Dict[str, Any]]:
    parameters: List[Dict[str, Any]] = []
    marker: Optional[str] = None

    while True:
        request = {
            "CacheParameterGroupName": cache_parameter_group_name,
            "Source": "user",
            "MaxRecords": 100,
        }
        if marker:
            request["Marker"] = marker

        response = send_aws_call(lambda: client.describe_cache_parameters(**request))
        parameters.extend(response.get("Parameters", []))

        marker = response.get("Marker")
        if not marker:
            break

    return parameters


def apply_user_defined_parameters(
    *,
    client: Any,
    target_parameter_group_name: str,
    parameters: Sequence[Dict[str, Any]],
) -> None:
    parameter_name_values = [
        {
            "ParameterName": parameter.get("ParameterName"),
            "ParameterValue": str(parameter.get("ParameterValue")),
        }
        for parameter in parameters
        if parameter
        and parameter.get("ParameterName")
        and parameter.get("ParameterValue") is not None
    ]

    for batch in chunk_array(parameter_name_values, 20):
        if not batch:
            continue

        try:
            send_aws_call(
                lambda: client.modify_cache_parameter_group(
                    CacheParameterGroupName=target_parameter_group_name,
                    ParameterNameValues=batch,
                )
            )
        except Exception:  # noqa: BLE001
            for parameter in batch:
                try:
                    send_aws_call(
                        lambda: client.modify_cache_parameter_group(
                            CacheParameterGroupName=target_parameter_group_name,
                            ParameterNameValues=[parameter],
                        )
                    )
                except Exception:  # noqa: BLE001
                    continue


def describe_rds_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
) -> Dict[str, List[Dict[str, str]]]:
    return describe_rds_like_resources(
        account_id=account_id,
        credentials=credentials,
        forced_regions=forced_regions,
        region_threads=region_threads,
        resource_type="rds",
        include_engine=lambda engine: True,
        is_cluster_engine=is_aurora_engine,
    )


def describe_docdb_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
) -> Dict[str, List[Dict[str, str]]]:
    return describe_rds_like_resources(
        account_id=account_id,
        credentials=credentials,
        forced_regions=forced_regions,
        region_threads=region_threads,
        resource_type="docdb",
        include_engine=is_docdb_engine,
        is_cluster_engine=is_docdb_engine,
    )


def describe_neptune_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
) -> Dict[str, List[Dict[str, str]]]:
    return describe_rds_like_resources(
        account_id=account_id,
        credentials=credentials,
        forced_regions=forced_regions,
        region_threads=region_threads,
        resource_type="neptune",
        include_engine=is_neptune_engine,
        is_cluster_engine=is_neptune_engine,
    )


def describe_redshift_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
) -> Dict[str, List[Dict[str, str]]]:
    resources: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []

    regions, region_discovery_error = discover_regions(credentials, list(forced_regions))
    if region_discovery_error is not None:
        errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="redshift",
                resource_kind="",
                resource_id="",
                engine="",
                region="",
                current_version="",
                target_version="",
                error_type="RegionDiscoveryError",
                error_message=str(region_discovery_error),
            )
        )

    region_results = collect_regions_in_batches(
        regions=regions,
        region_threads=region_threads,
        collect_region=lambda region: collect_redshift_region(
            account_id=account_id,
            credentials=credentials,
            region=region,
        ),
    )

    aggregated_region_results = append_region_results(region_results)
    return {
        "resources": [*resources, *aggregated_region_results["resources"]],
        "errors": [*errors, *aggregated_region_results["errors"]],
    }


def collect_redshift_region(
    *,
    account_id: str,
    credentials: Dict[str, str],
    region: str,
) -> Dict[str, List[Dict[str, str]]]:
    region_resources: List[Dict[str, str]] = []
    region_errors: List[Dict[str, str]] = []
    client = boto3.client("redshift", region_name=region, **credentials)

    try:
        marker: Optional[str] = None
        while True:
            request: Dict[str, Any] = {"MaxRecords": 100}
            if marker:
                request["Marker"] = marker

            response = send_aws_call(lambda: client.describe_clusters(**request))
            clusters = response.get("Clusters", [])

            for cluster in clusters:
                resource_id = str(cluster.get("ClusterIdentifier") or "").strip()
                if not resource_id:
                    continue

                parameter_group_name = resolve_redshift_cluster_parameter_group_name(
                    cluster
                )
                region_resources.append(
                    build_resource_row(
                        account_id=account_id,
                        resource_type="redshift",
                        resource_kind="cluster",
                        resource_id=resource_id,
                        engine="redshift",
                        region=region,
                        current_version=normalize_version(cluster.get("ClusterVersion")),
                        arn=str(cluster.get("ClusterArn") or ""),
                        parameter_group_name=parameter_group_name,
                        instance_type=normalize_version(cluster.get("NodeType")),
                    )
                )

            marker = response.get("Marker")
            if not marker:
                break
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="redshift",
                resource_kind="cluster",
                resource_id="",
                engine="redshift",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "DescribeClustersError",
                error_message=str(error),
            )
        )

    return {"resources": region_resources, "errors": region_errors}


def describe_rds_like_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
    resource_type: str,
    include_engine: Callable[[str], bool],
    is_cluster_engine: Callable[[str], bool],
) -> Dict[str, List[Dict[str, str]]]:
    resources: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []

    regions, region_discovery_error = discover_regions(credentials, list(forced_regions))
    if region_discovery_error is not None:
        errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type=resource_type,
                resource_kind="",
                resource_id="",
                engine="",
                region="",
                current_version="",
                target_version="",
                error_type="RegionDiscoveryError",
                error_message=str(region_discovery_error),
            )
        )

    region_results = collect_regions_in_batches(
        regions=regions,
        region_threads=region_threads,
        collect_region=lambda region: collect_rds_region(
            account_id=account_id,
            credentials=credentials,
            region=region,
            resource_type=resource_type,
            include_engine=include_engine,
            is_cluster_engine=is_cluster_engine,
        ),
    )

    aggregated_region_results = append_region_results(region_results)
    return {
        "resources": [*resources, *aggregated_region_results["resources"]],
        "errors": [*errors, *aggregated_region_results["errors"]],
    }


def collect_rds_region(
    *,
    account_id: str,
    credentials: Dict[str, str],
    region: str,
    resource_type: str,
    include_engine: Callable[[str], bool],
    is_cluster_engine: Callable[[str], bool],
) -> Dict[str, List[Dict[str, str]]]:
    region_resources: List[Dict[str, str]] = []
    region_errors: List[Dict[str, str]] = []
    client = boto3.client("rds", region_name=region, **credentials)
    aurora_instance_parameter_groups_by_cluster_id: Dict[str, set] = {}

    try:
        marker: Optional[str] = None
        while True:
            request = {"MaxRecords": 100}
            if marker:
                request["Marker"] = marker

            response = send_aws_call(lambda: client.describe_db_instances(**request))
            instances = response.get("DBInstances", [])

            for instance in instances:
                resource_id = str(instance.get("DBInstanceIdentifier") or "").strip()
                if not resource_id:
                    continue

                engine = normalize_engine(instance.get("Engine"))
                if not include_engine(engine):
                    continue

                cluster_id = normalize_version(instance.get("DBClusterIdentifier"))
                parameter_group_name = resolve_rds_instance_parameter_group_name(instance)
                option_group_name = resolve_rds_instance_option_group_name(instance)
                if cluster_id and is_cluster_engine(engine):
                    register_aurora_cluster_instance_parameter_group(
                        aurora_instance_parameter_groups_by_cluster_id,
                        cluster_id,
                        parameter_group_name,
                    )
                    continue

                region_resources.append(
                    build_resource_row(
                        account_id=account_id,
                        resource_type=resource_type,
                        resource_kind="db-instance",
                        resource_id=resource_id,
                        engine=engine,
                        region=region,
                        current_version=normalize_version(instance.get("EngineVersion")),
                        arn=str(instance.get("DBInstanceArn") or ""),
                        parameter_group_name=parameter_group_name,
                        option_group_name=option_group_name,
                        instance_parameter_group_name=parameter_group_name,
                        instance_parameter_groups=parameter_group_name,
                        instance_type=normalize_version(instance.get("DBInstanceClass")),
                    )
                )

            marker = response.get("Marker")
            if not marker:
                break
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type=resource_type,
                resource_kind="db-instance",
                resource_id="",
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "DescribeDBInstancesError",
                error_message=str(error),
            )
        )

    try:
        marker = None
        while True:
            request = {"MaxRecords": 100}
            if marker:
                request["Marker"] = marker

            response = send_aws_call(lambda: client.describe_db_clusters(**request))
            clusters = response.get("DBClusters", [])

            for cluster in clusters:
                resource_id = str(cluster.get("DBClusterIdentifier") or "").strip()
                if not resource_id:
                    continue

                engine = normalize_engine(cluster.get("Engine"))
                if not include_engine(engine):
                    continue
                if not is_cluster_engine(engine):
                    continue

                cluster_parameter_group_name = resolve_rds_cluster_parameter_group_name(cluster)
                instance_parameter_group_names = resolve_aurora_cluster_instance_parameter_groups(
                    aurora_instance_parameter_groups_by_cluster_id,
                    resource_id,
                )
                region_resources.append(
                    build_resource_row(
                        account_id=account_id,
                        resource_type=resource_type,
                        resource_kind="db-cluster",
                        resource_id=resource_id,
                        engine=engine,
                        region=region,
                        current_version=normalize_version(cluster.get("EngineVersion")),
                        arn=str(cluster.get("DBClusterArn") or ""),
                        parameter_group_name=cluster_parameter_group_name,
                        cluster_parameter_group_name=cluster_parameter_group_name,
                        instance_parameter_group_name=(
                            instance_parameter_group_names[0]
                            if len(instance_parameter_group_names) == 1
                            else ""
                        ),
                        instance_parameter_groups=",".join(instance_parameter_group_names),
                        instance_type=normalize_version(cluster.get("DBClusterInstanceClass")),
                    )
                )

            marker = response.get("Marker")
            if not marker:
                break
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type=resource_type,
                resource_kind="db-cluster",
                resource_id="",
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "DescribeDBClustersError",
                error_message=str(error),
            )
        )

    return {"resources": region_resources, "errors": region_errors}


def is_aurora_engine(engine: Any) -> bool:
    return normalize_engine(engine).startswith("aurora")


def is_docdb_engine(engine: Any) -> bool:
    return normalize_engine(engine).startswith("docdb")


def is_neptune_engine(engine: Any) -> bool:
    return normalize_engine(engine).startswith("neptune")


def is_redshift_engine(engine: Any) -> bool:
    return normalize_engine(engine).startswith("redshift")


def describe_rds_db_instance(
    client: Any,
    db_instance_identifier: str,
) -> Dict[str, Any]:
    response = send_aws_call(
        lambda: client.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
    )
    instances = response.get("DBInstances", [])
    if instances:
        return instances[0]

    raise ValueError(f"DB instance nao encontrada: {db_instance_identifier}")


def describe_rds_db_cluster(
    client: Any,
    db_cluster_identifier: str,
) -> Dict[str, Any]:
    response = send_aws_call(
        lambda: client.describe_db_clusters(DBClusterIdentifier=db_cluster_identifier)
    )
    clusters = response.get("DBClusters", [])
    if clusters:
        return clusters[0]

    raise ValueError(f"DB cluster nao encontrado: {db_cluster_identifier}")


def describe_redshift_cluster(
    client: Any,
    cluster_identifier: str,
) -> Dict[str, Any]:
    response = send_aws_call(
        lambda: client.describe_clusters(ClusterIdentifier=cluster_identifier)
    )
    clusters = response.get("Clusters", [])
    if clusters:
        return clusters[0]

    raise ValueError(f"Redshift cluster nao encontrado: {cluster_identifier}")


def is_rds_terminal_blocked_status(status: Any) -> bool:
    normalized_status = normalize_rds_status(status)
    return (
        "delet" in normalized_status
        or "failed" in normalized_status
        or "incompatible" in normalized_status
    )


def wait_for_rds_resource_status(
    *,
    describe_resource: Callable[[], Dict[str, Any]],
    get_status: Callable[[Dict[str, Any]], Any],
    resource_label: str,
    target_status: str,
    timeout_ms: int = RDS_STATUS_TIMEOUT_MS,
    poll_interval_ms: int = RDS_STATUS_POLL_INTERVAL_MS,
) -> Dict[str, Any]:
    normalized_target_status = normalize_rds_status(target_status)
    deadline = time.time() + max(0.0, timeout_ms / 1000.0)

    while True:
        resource = describe_resource()
        current_status = normalize_rds_status(get_status(resource))
        if current_status == normalized_target_status:
            return resource

        if is_rds_terminal_blocked_status(current_status):
            raise ValueError(
                f"{resource_label} entrou em estado nao suportado para upgrade: "
                f"{current_status or 'desconhecido'}."
            )

        if time.time() >= deadline:
            raise TimeoutError(
                f"Timeout aguardando {resource_label} atingir status "
                f"{normalized_target_status or 'desconhecido'}. "
                f"Status atual: {current_status or 'desconhecido'}."
            )

        time.sleep(max(0.0, poll_interval_ms / 1000.0))


def ensure_rds_resource_ready_for_upgrade(
    *,
    describe_resource: Callable[[], Dict[str, Any]],
    get_status: Callable[[Dict[str, Any]], Any],
    start_resource: Callable[[], Any],
    resource_label: str,
) -> Dict[str, Any]:
    initial_resource = describe_resource()
    initial_status = normalize_rds_status(get_status(initial_resource))

    if initial_status == "available":
        return {
            "initial_status": initial_status,
            "auto_started": False,
            "waited_for_availability": False,
        }

    if initial_status == "stopping":
        wait_for_rds_resource_status(
            describe_resource=describe_resource,
            get_status=get_status,
            resource_label=resource_label,
            target_status="stopped",
        )

    resource_before_start = (
        describe_resource() if initial_status == "stopping" else initial_resource
    )
    status_before_start = normalize_rds_status(get_status(resource_before_start))

    if status_before_start == "stopped":
        start_resource()
        wait_for_rds_resource_status(
            describe_resource=describe_resource,
            get_status=get_status,
            resource_label=resource_label,
            target_status="available",
        )
        return {
            "initial_status": initial_status,
            "auto_started": True,
            "waited_for_availability": True,
        }

    if status_before_start == "available":
        return {
            "initial_status": initial_status,
            "auto_started": False,
            "waited_for_availability": initial_status != "available",
        }

    wait_for_rds_resource_status(
        describe_resource=describe_resource,
        get_status=get_status,
        resource_label=resource_label,
        target_status="available",
    )
    return {
        "initial_status": initial_status,
        "auto_started": False,
        "waited_for_availability": True,
    }


def build_rds_preparation_message(
    *,
    resource_label: str,
    readiness: Dict[str, Any],
) -> str:
    initial_status = normalize_rds_status(readiness.get("initial_status"))
    if readiness.get("auto_started"):
        return (
            f"{resource_label} estava {initial_status or 'desconhecido'} "
            "e foi iniciado automaticamente antes do upgrade."
        )

    if readiness.get("waited_for_availability") and initial_status != "available":
        return (
            f"{resource_label} estava {initial_status or 'desconhecido'} "
            "e o script aguardou ficar available antes do upgrade."
        )

    return ""


def maybe_stop_rds_resource_after_update(
    *,
    readiness: Dict[str, Any],
    describe_resource: Callable[[], Dict[str, Any]],
    get_status: Callable[[Dict[str, Any]], Any],
    stop_resource: Callable[[], Any],
    resource_label: str,
) -> Dict[str, str]:
    if not readiness.get("auto_started"):
        return {"finalStatus": "", "message": ""}

    wait_for_rds_resource_status(
        describe_resource=describe_resource,
        get_status=get_status,
        resource_label=resource_label,
        target_status="available",
    )
    stop_resource()
    stopped_resource = wait_for_rds_resource_status(
        describe_resource=describe_resource,
        get_status=get_status,
        resource_label=resource_label,
        target_status="stopped",
    )
    return {
        "finalStatus": normalize_rds_status(get_status(stopped_resource)),
        "message": (
            f"{resource_label} foi desligado novamente apos concluir o upgrade."
        ),
    }


def register_aurora_cluster_instance_parameter_group(
    aurora_instance_parameter_groups_by_cluster_id: Dict[str, set],
    cluster_id: str,
    parameter_group_name: str,
) -> None:
    normalized_cluster_id = normalize_version(cluster_id)
    normalized_parameter_group_name = normalize_version(parameter_group_name)
    if not normalized_cluster_id or not normalized_parameter_group_name:
        return

    aurora_instance_parameter_groups_by_cluster_id.setdefault(
        normalized_cluster_id,
        set(),
    ).add(normalized_parameter_group_name)


def resolve_aurora_cluster_instance_parameter_groups(
    aurora_instance_parameter_groups_by_cluster_id: Dict[str, set],
    cluster_id: str,
) -> List[str]:
    normalized_cluster_id = normalize_version(cluster_id)
    if not normalized_cluster_id:
        return []

    return sorted(
        aurora_instance_parameter_groups_by_cluster_id.get(normalized_cluster_id, set())
    )


def submit_rds_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
    resource_type: str = "rds",
) -> Dict[str, str]:
    resource_kind = normalize_resource(resource.get("resourceKind"))
    if resource_kind == "db-instance":
        return submit_rds_instance_update(
            resource=resource,
            target_version=target_version,
            target_engine=target_engine,
            adapter_options=adapter_options,
            credentials=credentials,
        )
    if resource_kind == "db-cluster":
        return submit_rds_cluster_update(
            resource=resource,
            target_version=target_version,
            target_engine=target_engine,
            adapter_options=adapter_options,
            credentials=credentials,
        )

    raise ValueError(f"resource_kind invalido para {resource_type}: {resource_kind}")


def submit_docdb_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    return submit_rds_update(
        resource=resource,
        target_version=target_version,
        target_engine=target_engine,
        adapter_options=adapter_options,
        credentials=credentials,
        resource_type="docdb",
    )


def submit_neptune_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    return submit_rds_update(
        resource=resource,
        target_version=target_version,
        target_engine=target_engine,
        adapter_options=adapter_options,
        credentials=credentials,
        resource_type="neptune",
    )


def submit_redshift_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    del target_engine
    client = boto3.client("redshift", region_name=resource.get("region"), **credentials)

    explicit_parameter_group_name = normalize_version(
        safe_read(adapter_options, "parameter_group_name", "")
    )
    explicit_instance_type = resolve_target_instance_type_for_update(
        resource=resource,
        adapter_options=adapter_options,
    )
    cluster_identifier = normalize_version(resource.get("resourceId"))
    if not cluster_identifier:
        raise ValueError("ResourceID do redshift vazio.")

    modify_input = {
        "ClusterIdentifier": cluster_identifier,
    }
    if explicit_parameter_group_name:
        modify_input["ClusterParameterGroupName"] = explicit_parameter_group_name
    if explicit_instance_type:
        cluster_snapshot = describe_redshift_cluster(client, cluster_identifier)
        current_number_of_nodes = safe_read(cluster_snapshot, "NumberOfNodes", 0)
        try:
            resolved_number_of_nodes = int(current_number_of_nodes)
        except Exception as error:  # noqa: BLE001
            raise ValueError(
                "Nao foi possivel resolver NumberOfNodes do cluster Redshift "
                f"{cluster_identifier} para aplicar --instance-type."
            ) from error
        if resolved_number_of_nodes <= 0:
            raise ValueError(
                f"NumberOfNodes invalido para cluster Redshift {cluster_identifier}: "
                f"{current_number_of_nodes}"
            )
        modify_input["NodeType"] = explicit_instance_type
        modify_input["NumberOfNodes"] = resolved_number_of_nodes
    if target_version:
        modify_input["ClusterVersion"] = target_version
    modify_input["ApplyImmediately"] = True

    response = send_aws_call(lambda: client.modify_cluster(**modify_input))

    return {
        "status": safe_read(response, "Cluster.ClusterStatus", "modifying"),
        "message": build_message(
            f"EngineVersion alterada para {target_version or resource.get('currentVersion', '')}",
            (
                f"NodeType alterado para {explicit_instance_type}."
                if explicit_instance_type
                else ""
            ),
        ),
        "targetParameterGroupName": explicit_parameter_group_name,
    }


def submit_rds_instance_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    client = boto3.client("rds", region_name=resource.get("region"), **credentials)
    db_instance_identifier = resource.get("resourceId", "")
    explicit_parameter_group_name = normalize_version(
        safe_read(adapter_options, "parameter_group_name", "")
    )
    explicit_instance_type = resolve_target_instance_type_for_update(
        resource=resource,
        adapter_options=adapter_options,
    )
    normalized_target_engine = normalize_engine(target_engine or resource.get("engine"))
    normalized_current_engine = normalize_engine(resource.get("engine"))
    if (
        normalized_target_engine
        and normalized_current_engine
        and normalized_target_engine != normalized_current_engine
    ):
        raise ValueError(
            "RDS DB instance nao suporta troca de engine in-place "
            f"({normalized_current_engine} -> {normalized_target_engine})."
        )
    readiness = ensure_rds_resource_ready_for_upgrade(
        describe_resource=lambda: describe_rds_db_instance(client, db_instance_identifier),
        get_status=lambda db_instance: db_instance.get("DBInstanceStatus"),
        start_resource=lambda: send_aws_call(
            lambda: client.start_db_instance(DBInstanceIdentifier=db_instance_identifier)
        ),
        resource_label=f"DB instance {db_instance_identifier}",
    )
    resolved_target_parameter_group_name = resolve_rds_target_parameter_group(
        client=client,
        resource=resource,
        target_engine=normalized_target_engine,
        target_version=target_version,
        explicit_parameter_group_name=explicit_parameter_group_name,
        adapter_options=adapter_options,
    )

    major_version_changed = resolve_rds_major_version_change(
        client=client,
        current_engine=resource.get("engine", ""),
        current_version=resource.get("currentVersion", ""),
        target_engine=normalized_target_engine,
        target_version=target_version,
        adapter_options=adapter_options,
    )
    resolved_target_option_group_name = resolve_rds_target_option_group(
        client=client,
        resource=resource,
        target_engine=normalized_target_engine,
        target_version=target_version,
        requires_option_group_migration=major_version_changed,
        adapter_options=adapter_options,
    )
    modify_input = build_rds_instance_modify_input(
        db_instance_identifier=resource.get("resourceId", ""),
        target_version=target_version,
        major_version_changed=major_version_changed,
        target_parameter_group_name=resolved_target_parameter_group_name,
        target_option_group_name=resolved_target_option_group_name,
        target_instance_class=explicit_instance_type,
    )

    response = send_aws_call(lambda: client.modify_db_instance(**modify_input))
    post_update_action = maybe_stop_rds_resource_after_update(
        readiness=readiness,
        describe_resource=lambda: describe_rds_db_instance(client, db_instance_identifier),
        get_status=lambda db_instance: db_instance.get("DBInstanceStatus"),
        stop_resource=lambda: send_aws_call(
            lambda: client.stop_db_instance(DBInstanceIdentifier=db_instance_identifier)
        ),
        resource_label=f"DB instance {db_instance_identifier}",
    )
    option_group_message = (
        f"OptionGroup migrada para {resolved_target_option_group_name}"
        if resolved_target_option_group_name
        else ""
    )

    return {
        "status": (
            post_update_action.get("finalStatus")
            or safe_read(response, "DBInstance.DBInstanceStatus", "modifying")
        ),
        "message": build_message(
            build_rds_preparation_message(
                resource_label=f"DB instance {db_instance_identifier}",
                readiness=readiness,
            ),
            f"EngineVersion alterada para {target_version}.",
            (
                f"DBInstanceClass alterada para {explicit_instance_type}."
                if explicit_instance_type
                else ""
            ),
            option_group_message,
            post_update_action.get("message", ""),
        ),
        "targetParameterGroupName": resolved_target_parameter_group_name,
        "targetOptionGroupName": resolved_target_option_group_name,
    }


def submit_rds_cluster_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    client = boto3.client("rds", region_name=resource.get("region"), **credentials)
    db_cluster_identifier = resource.get("resourceId", "")
    explicit_cluster_parameter_group_name = normalize_version(
        safe_read(adapter_options, "cluster_parameter_group_name", "")
        or safe_read(adapter_options, "parameter_group_name", "")
    )
    explicit_instance_parameter_group_name = normalize_version(
        safe_read(adapter_options, "instance_parameter_group_name", "")
    )
    explicit_instance_type = resolve_target_instance_type_for_update(
        resource=resource,
        adapter_options=adapter_options,
    )
    if explicit_instance_type:
        raise ValueError(
            "--instance-type para rds/docdb/neptune e suportado apenas em recursos "
            "resource_kind=db-instance no fluxo atual."
        )
    normalized_target_engine = normalize_engine(target_engine or resource.get("engine"))
    normalized_current_engine = normalize_engine(resource.get("engine"))
    if (
        normalized_target_engine
        and normalized_current_engine
        and normalized_target_engine != normalized_current_engine
    ):
        raise ValueError(
            "RDS DB cluster nao suporta troca de engine in-place "
            f"({normalized_current_engine} -> {normalized_target_engine})."
        )
    initial_cluster_snapshot = describe_rds_db_cluster(client, db_cluster_identifier)
    cluster_engine_mode = normalize_version(
        initial_cluster_snapshot.get("EngineMode")
    ).lower()
    readiness = ensure_rds_resource_ready_for_upgrade(
        describe_resource=lambda: describe_rds_db_cluster(client, db_cluster_identifier),
        get_status=lambda db_cluster: db_cluster.get("Status"),
        start_resource=lambda: send_aws_call(
            lambda: client.start_db_cluster(DBClusterIdentifier=db_cluster_identifier)
        ),
        resource_label=f"DB cluster {db_cluster_identifier}",
    )
    major_version_changed = resolve_rds_major_version_change(
        client=client,
        current_engine=resource.get("engine", ""),
        current_version=resource.get("currentVersion", ""),
        target_engine=normalized_target_engine,
        target_version=target_version,
        adapter_options=adapter_options,
    )
    resolved_target_cluster_parameter_group_name = (
        resolve_rds_target_cluster_parameter_group(
            client=client,
            resource=resource,
            target_engine=normalized_target_engine,
            target_version=target_version,
            explicit_parameter_group_name=explicit_cluster_parameter_group_name,
            requires_parameter_group_migration=major_version_changed,
            adapter_options=adapter_options,
        )
    )
    resolved_target_instance_parameter_group_name = (
        resolve_rds_target_cluster_instance_parameter_group(
            client=client,
            resource=resource,
            target_engine=normalized_target_engine,
            target_version=target_version,
            explicit_parameter_group_name=explicit_instance_parameter_group_name,
            requires_parameter_group_migration=major_version_changed,
            adapter_options=adapter_options,
        )
    )

    modify_input: Dict[str, Any] = {
        "DBClusterIdentifier": resource.get("resourceId"),
        "EngineVersion": target_version,
        "ApplyImmediately": True,
    }
    if major_version_changed:
        modify_input["AllowMajorVersionUpgrade"] = True
    if resolved_target_cluster_parameter_group_name:
        modify_input["DBClusterParameterGroupName"] = (
            resolved_target_cluster_parameter_group_name
        )
    if (
        resolved_target_instance_parameter_group_name
        and cluster_engine_mode in ("", "provisioned")
    ):
        modify_input["DBInstanceParameterGroupName"] = (
            resolved_target_instance_parameter_group_name
        )

    response = send_aws_call(lambda: client.modify_db_cluster(**modify_input))
    post_update_action = maybe_stop_rds_resource_after_update(
        readiness=readiness,
        describe_resource=lambda: describe_rds_db_cluster(client, db_cluster_identifier),
        get_status=lambda db_cluster: db_cluster.get("Status"),
        stop_resource=lambda: send_aws_call(
            lambda: client.stop_db_cluster(DBClusterIdentifier=db_cluster_identifier)
        ),
        resource_label=f"DB cluster {db_cluster_identifier}",
    )
    return {
        "status": (
            post_update_action.get("finalStatus")
            or safe_read(response, "DBCluster.Status", "modifying")
        ),
        "message": build_message(
            build_rds_preparation_message(
                resource_label=f"DB cluster {db_cluster_identifier}",
                readiness=readiness,
            ),
            f"EngineVersion alterada para {target_version}",
            post_update_action.get("message", ""),
        ),
        "targetParameterGroupName": resolved_target_cluster_parameter_group_name,
        "targetClusterParameterGroupName": resolved_target_cluster_parameter_group_name,
        "targetInstanceParameterGroupName": resolved_target_instance_parameter_group_name,
    }


def resolve_rds_instance_parameter_group_name(instance: Dict[str, Any]) -> str:
    parameter_groups = instance.get("DBParameterGroups")
    if not isinstance(parameter_groups, list):
        return ""

    for group in parameter_groups:
        if not isinstance(group, dict):
            continue
        parameter_group_name = normalize_version(group.get("DBParameterGroupName"))
        if parameter_group_name:
            return parameter_group_name

    return ""


def resolve_rds_instance_option_group_name(instance: Dict[str, Any]) -> str:
    option_groups = instance.get("OptionGroupMemberships")
    if not isinstance(option_groups, list):
        return ""

    return next(
        (
            option_group_name
            for option_group_name in (
                normalize_version(group.get("OptionGroupName"))
                for group in option_groups
                if isinstance(group, dict)
            )
            if option_group_name
        ),
        "",
    )


def resolve_rds_cluster_parameter_group_name(cluster: Dict[str, Any]) -> str:
    return normalize_version(cluster.get("DBClusterParameterGroup"))


def resolve_redshift_cluster_parameter_group_name(cluster: Dict[str, Any]) -> str:
    cluster_parameter_groups = cluster.get("ClusterParameterGroups")
    if not isinstance(cluster_parameter_groups, list):
        return ""

    return next(
        (
            normalize_version(group.get("ParameterGroupName"))
            for group in cluster_parameter_groups
            if isinstance(group, dict)
            and normalize_version(group.get("ParameterGroupName"))
        ),
        "",
    )


def resolve_rds_target_parameter_group(
    *,
    client: Any,
    resource: Dict[str, str],
    target_engine: str,
    target_version: str,
    explicit_parameter_group_name: str,
    adapter_options: Dict[str, Any],
) -> str:
    if explicit_parameter_group_name:
        return explicit_parameter_group_name

    source_parameter_group_name = normalize_version(resource.get("parameterGroupName"))
    if not source_parameter_group_name:
        return ""

    normalized_target_engine = normalize_engine(target_engine or resource.get("engine"))
    normalized_target_version = normalize_version(target_version)
    if not normalized_target_engine or not normalized_target_version:
        return source_parameter_group_name

    target_family = resolve_rds_parameter_group_family_for_engine_version(
        client=client,
        target_engine=normalized_target_engine,
        target_version=normalized_target_version,
        adapter_options=adapter_options,
    )
    if not target_family:
        return ""

    if is_default_parameter_group(source_parameter_group_name):
        return f"default.{target_family}"

    source_family = resolve_rds_source_db_parameter_group_family(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
        adapter_options=adapter_options,
    )
    if normalize_version(source_family).lower() == normalize_version(target_family).lower():
        return source_parameter_group_name

    return ensure_custom_rds_target_parameter_group_for_migration(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
        target_family=target_family,
        resource=resource,
        adapter_options=adapter_options,
    )


def resolve_rds_target_option_group(
    *,
    client: Any,
    resource: Dict[str, str],
    target_engine: str,
    target_version: str,
    requires_option_group_migration: bool,
    adapter_options: Dict[str, Any],
) -> str:
    if not requires_option_group_migration:
        return ""

    source_option_group_name = normalize_version(resource.get("optionGroupName"))
    if not source_option_group_name:
        return ""

    normalized_target_engine = normalize_engine(target_engine or resource.get("engine"))
    normalized_target_version = normalize_version(target_version)
    if not normalized_target_engine or not normalized_target_version:
        return source_option_group_name

    target_major_engine_version = resolve_rds_major_engine_version_for_engine_version(
        client=client,
        target_engine=normalized_target_engine,
        target_version=normalized_target_version,
        adapter_options=adapter_options,
    )
    if not target_major_engine_version:
        return ""

    return (
        ensure_custom_rds_target_option_group_for_migration(
            client=client,
            source_option_group_name=source_option_group_name,
            target_engine=normalized_target_engine,
            target_major_engine_version=target_major_engine_version,
            resource=resource,
            adapter_options=adapter_options,
        )
        if should_create_custom_rds_option_group(
            source_option_group_name=source_option_group_name
        )
        else build_default_rds_option_group_name(
            target_engine=normalized_target_engine,
            target_major_engine_version=target_major_engine_version,
        )
    )


def resolve_rds_major_version_change(
    *,
    client: Any,
    current_engine: str,
    current_version: str,
    target_engine: str,
    target_version: str,
    adapter_options: Dict[str, Any],
) -> bool:
    normalized_current_engine = normalize_engine(current_engine)
    normalized_current_version = normalize_version(current_version)
    normalized_target_engine = normalize_engine(target_engine or current_engine)
    normalized_target_version = normalize_version(target_version)

    if not normalized_current_version or not normalized_target_version:
        return True

    current_major = ""
    if normalized_current_engine:
        try:
            current_major = normalize_version(
                resolve_rds_major_engine_version_for_engine_version(
                    client=client,
                    target_engine=normalized_current_engine,
                    target_version=normalized_current_version,
                    adapter_options=adapter_options,
                )
            )
        except Exception:  # noqa: BLE001
            current_major = ""

    target_major = ""
    if normalized_target_engine:
        try:
            target_major = normalize_version(
                resolve_rds_major_engine_version_for_engine_version(
                    client=client,
                    target_engine=normalized_target_engine,
                    target_version=normalized_target_version,
                    adapter_options=adapter_options,
                )
            )
        except Exception:  # noqa: BLE001
            target_major = ""

    if current_major and target_major:
        return current_major != target_major

    return has_rds_major_version_change(
        current_engine=normalized_current_engine,
        current_version=normalized_current_version,
        target_engine=normalized_target_engine,
        target_version=normalized_target_version,
    )


def resolve_rds_target_cluster_parameter_group(
    *,
    client: Any,
    resource: Dict[str, str],
    target_engine: str,
    target_version: str,
    explicit_parameter_group_name: str,
    requires_parameter_group_migration: bool,
    adapter_options: Dict[str, Any],
) -> str:
    if explicit_parameter_group_name:
        return explicit_parameter_group_name

    if not requires_parameter_group_migration:
        return ""

    source_parameter_group_name = normalize_version(
        resource.get("clusterParameterGroupName") or resource.get("parameterGroupName")
    )
    if not source_parameter_group_name:
        return ""

    normalized_target_engine = normalize_engine(target_engine or resource.get("engine"))
    normalized_target_version = normalize_version(target_version)
    if not normalized_target_engine or not normalized_target_version:
        return source_parameter_group_name

    target_family = resolve_rds_parameter_group_family_for_engine_version(
        client=client,
        target_engine=normalized_target_engine,
        target_version=normalized_target_version,
        adapter_options=adapter_options,
    )
    if not target_family:
        return ""

    if is_default_parameter_group(source_parameter_group_name):
        return f"default.{target_family}"

    source_family = resolve_rds_source_db_cluster_parameter_group_family(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
        adapter_options=adapter_options,
    )
    if normalize_version(source_family).lower() == normalize_version(target_family).lower():
        return source_parameter_group_name

    return ensure_custom_rds_target_cluster_parameter_group_for_migration(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
        target_family=target_family,
        resource=resource,
        adapter_options=adapter_options,
    )


def resolve_rds_target_cluster_instance_parameter_group(
    *,
    client: Any,
    resource: Dict[str, str],
    target_engine: str,
    target_version: str,
    explicit_parameter_group_name: str,
    requires_parameter_group_migration: bool,
    adapter_options: Dict[str, Any],
) -> str:
    if explicit_parameter_group_name:
        return explicit_parameter_group_name

    if not requires_parameter_group_migration:
        return ""

    source_parameter_group_names = sorted(
        set(
            split_parameter_group_names(
                resource.get("instanceParameterGroups")
                or resource.get("instanceParameterGroupName")
            )
        )
    )
    if not source_parameter_group_names:
        return ""

    normalized_target_engine = normalize_engine(target_engine or resource.get("engine"))
    normalized_target_version = normalize_version(target_version)
    if not normalized_target_engine or not normalized_target_version:
        return source_parameter_group_names[0]

    target_family = resolve_rds_parameter_group_family_for_engine_version(
        client=client,
        target_engine=normalized_target_engine,
        target_version=normalized_target_version,
        adapter_options=adapter_options,
    )
    if not target_family:
        return ""

    source_parameter_group_name = next(
        (
            source_parameter_group_candidate
            for source_parameter_group_candidate in source_parameter_group_names
            if normalize_version(
                resolve_rds_source_db_parameter_group_family(
                    client=client,
                    source_parameter_group_name=source_parameter_group_candidate,
                    adapter_options=adapter_options,
                )
            ).lower()
            == normalize_version(target_family).lower()
        ),
        source_parameter_group_names[0],
    )

    if is_default_parameter_group(source_parameter_group_name):
        return f"default.{target_family}"

    source_parameter_group_family = resolve_rds_source_db_parameter_group_family(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
        adapter_options=adapter_options,
    )
    if normalize_version(source_parameter_group_family).lower() == normalize_version(
        target_family
    ).lower():
        return source_parameter_group_name

    return ensure_custom_rds_target_parameter_group_for_migration(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
        target_family=target_family,
        resource=resource,
        adapter_options=adapter_options,
    )


def ensure_rds_runtime_state(adapter_options: Dict[str, Any]) -> Dict[str, Any]:
    with RUNTIME_STATE_LOCK:
        runtime_state = safe_read(adapter_options, "rds_runtime_state", None)
        if not isinstance(runtime_state, dict):
            adapter_options["rds_runtime_state"] = {}
            runtime_state = adapter_options["rds_runtime_state"]

        runtime_state.setdefault("family_by_engine_version", {})
        runtime_state.setdefault("major_by_engine_version", {})
        runtime_state.setdefault("db_custom_group_by_key", {})
        runtime_state.setdefault("cluster_custom_group_by_key", {})
        runtime_state.setdefault("db_family_by_parameter_group", {})
        runtime_state.setdefault("cluster_family_by_parameter_group", {})
        runtime_state.setdefault("option_custom_group_by_key", {})
        runtime_state.setdefault("option_quota_reservations_by_account_region", {})
        return runtime_state


def resolve_rds_source_db_parameter_group_family(
    *,
    client: Any,
    source_parameter_group_name: str,
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_rds_runtime_state(adapter_options)
    family_by_parameter_group: Dict[str, str] = runtime_state["db_family_by_parameter_group"]
    key = normalize_version(source_parameter_group_name).lower()
    with RUNTIME_STATE_LOCK:
        cached = family_by_parameter_group.get(key)
    if cached:
        return cached

    response = send_aws_call(
        lambda: client.describe_db_parameter_groups(
            DBParameterGroupName=source_parameter_group_name,
            MaxRecords=100,
        )
    )
    parameter_groups = response.get("DBParameterGroups", [])
    selected = next(
        (
            parameter_group
            for parameter_group in parameter_groups
            if normalize_version(parameter_group.get("DBParameterGroupName")).lower() == key
        ),
        parameter_groups[0] if parameter_groups else {},
    )
    family = normalize_version(selected.get("DBParameterGroupFamily"))
    with RUNTIME_STATE_LOCK:
        family_by_parameter_group[key] = family
    return family


def resolve_rds_source_db_cluster_parameter_group_family(
    *,
    client: Any,
    source_parameter_group_name: str,
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_rds_runtime_state(adapter_options)
    family_by_parameter_group: Dict[str, str] = runtime_state[
        "cluster_family_by_parameter_group"
    ]
    key = normalize_version(source_parameter_group_name).lower()
    with RUNTIME_STATE_LOCK:
        cached = family_by_parameter_group.get(key)
    if cached:
        return cached

    response = send_aws_call(
        lambda: client.describe_db_cluster_parameter_groups(
            DBClusterParameterGroupName=source_parameter_group_name,
            MaxRecords=100,
        )
    )
    parameter_groups = response.get("DBClusterParameterGroups", [])
    selected = next(
        (
            parameter_group
            for parameter_group in parameter_groups
            if normalize_version(
                parameter_group.get("DBClusterParameterGroupName")
            ).lower()
            == key
        ),
        parameter_groups[0] if parameter_groups else {},
    )
    family = normalize_version(selected.get("DBParameterGroupFamily"))
    with RUNTIME_STATE_LOCK:
        family_by_parameter_group[key] = family
    return family


def resolve_rds_parameter_group_family_for_engine_version(
    *,
    client: Any,
    target_engine: str,
    target_version: str,
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_rds_runtime_state(adapter_options)
    family_by_engine_version: Dict[str, str] = runtime_state["family_by_engine_version"]

    key = f"{target_engine}|{target_version}"
    with RUNTIME_STATE_LOCK:
        cached = family_by_engine_version.get(key)
    if cached:
        return cached

    response = send_aws_call(
        lambda: client.describe_db_engine_versions(
            Engine=target_engine,
            EngineVersion=target_version,
            MaxRecords=100,
        )
    )

    versions = response.get("DBEngineVersions", [])
    selected = next(
        (
            version
            for version in versions
            if normalize_version(version.get("EngineVersion"))
            == normalize_version(target_version)
        ),
        versions[0] if versions else {},
    )

    family = normalize_version(selected.get("DBParameterGroupFamily"))
    with RUNTIME_STATE_LOCK:
        family_by_engine_version[key] = family
    return family


def resolve_rds_major_engine_version_for_engine_version(
    *,
    client: Any,
    target_engine: str,
    target_version: str,
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_rds_runtime_state(adapter_options)
    major_by_engine_version: Dict[str, str] = runtime_state["major_by_engine_version"]

    key = f"{target_engine}|{target_version}"
    with RUNTIME_STATE_LOCK:
        cached = major_by_engine_version.get(key)
    if cached:
        return cached

    response = send_aws_call(
        lambda: client.describe_db_engine_versions(
            Engine=target_engine,
            EngineVersion=target_version,
            MaxRecords=100,
        )
    )

    versions = response.get("DBEngineVersions", [])
    selected = next(
        (
            version
            for version in versions
            if normalize_version(version.get("EngineVersion"))
            == normalize_version(target_version)
        ),
        versions[0] if versions else {},
    )

    major_engine_version = normalize_version(
        selected.get("MajorEngineVersion")
        or infer_rds_major_engine_version(
            target_engine=target_engine,
            target_version=target_version,
        )
    )
    with RUNTIME_STATE_LOCK:
        major_by_engine_version[key] = major_engine_version
    return major_engine_version


def ensure_custom_rds_target_parameter_group_for_migration(
    *,
    client: Any,
    source_parameter_group_name: str,
    target_family: str,
    resource: Dict[str, str],
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_rds_runtime_state(adapter_options)
    custom_group_by_key: Dict[str, str] = runtime_state["db_custom_group_by_key"]

    cache_key = "|".join(
        [
            resource.get("accountId", ""),
            resource.get("region", ""),
            source_parameter_group_name,
            target_family,
        ]
    )
    with RUNTIME_STATE_LOCK:
        cached = custom_group_by_key.get(cache_key)
    if cached:
        return cached

    target_parameter_group_name = build_unique_rds_parameter_group_name(
        source_parameter_group_name=source_parameter_group_name,
        target_family=target_family,
        resource=resource,
    )

    try:
        send_aws_call(
            lambda: client.create_db_parameter_group(
                DBParameterGroupFamily=target_family,
                DBParameterGroupName=target_parameter_group_name,
                Description=(
                    f"Generated from {source_parameter_group_name} for {target_family}"
                ),
            )
        )
    except Exception as error:  # noqa: BLE001
        if not is_db_parameter_group_already_exists_error(error):
            raise

    user_defined_parameters = list_user_defined_db_parameters(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
    )

    apply_user_defined_db_parameters(
        client=client,
        target_parameter_group_name=target_parameter_group_name,
        parameters=user_defined_parameters,
    )

    with RUNTIME_STATE_LOCK:
        custom_group_by_key[cache_key] = target_parameter_group_name
    return target_parameter_group_name


def ensure_custom_rds_target_cluster_parameter_group_for_migration(
    *,
    client: Any,
    source_parameter_group_name: str,
    target_family: str,
    resource: Dict[str, str],
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_rds_runtime_state(adapter_options)
    custom_group_by_key: Dict[str, str] = runtime_state["cluster_custom_group_by_key"]

    cache_key = "|".join(
        [
            resource.get("accountId", ""),
            resource.get("region", ""),
            source_parameter_group_name,
            target_family,
        ]
    )
    with RUNTIME_STATE_LOCK:
        cached = custom_group_by_key.get(cache_key)
    if cached:
        return cached

    target_parameter_group_name = build_unique_rds_cluster_parameter_group_name(
        source_parameter_group_name=source_parameter_group_name,
        target_family=target_family,
        resource=resource,
    )

    try:
        send_aws_call(
            lambda: client.create_db_cluster_parameter_group(
                DBParameterGroupFamily=target_family,
                DBClusterParameterGroupName=target_parameter_group_name,
                Description=(
                    f"Generated from {source_parameter_group_name} for {target_family}"
                ),
            )
        )
    except Exception as error:  # noqa: BLE001
        if not is_db_cluster_parameter_group_already_exists_error(error):
            raise

    user_defined_parameters = list_user_defined_db_cluster_parameters(
        client=client,
        source_parameter_group_name=source_parameter_group_name,
    )

    apply_user_defined_db_cluster_parameters(
        client=client,
        target_parameter_group_name=target_parameter_group_name,
        parameters=user_defined_parameters,
    )

    with RUNTIME_STATE_LOCK:
        custom_group_by_key[cache_key] = target_parameter_group_name
    return target_parameter_group_name


def ensure_custom_rds_target_option_group_for_migration(
    *,
    client: Any,
    source_option_group_name: str,
    target_engine: str,
    target_major_engine_version: str,
    resource: Dict[str, str],
    adapter_options: Dict[str, Any],
) -> str:
    runtime_state = ensure_rds_runtime_state(adapter_options)
    if not should_create_custom_rds_option_group(
        source_option_group_name=source_option_group_name
    ):
        raise ValueError(
            "Option group default nao deve ser criado, clonado ou modificado."
        )

    custom_group_by_key: Dict[str, str] = runtime_state["option_custom_group_by_key"]

    cache_key = "|".join(
        [
            resource.get("accountId", ""),
            resource.get("region", ""),
            source_option_group_name,
            target_engine,
            target_major_engine_version,
        ]
    )
    with RUNTIME_STATE_LOCK:
        cached = custom_group_by_key.get(cache_key)
    if cached:
        return cached

    target_option_group_name = build_unique_rds_option_group_name(
        source_option_group_name=source_option_group_name,
        target_engine=target_engine,
        target_major_engine_version=target_major_engine_version,
        resource=resource,
    )
    quota_state = ensure_rds_option_group_quota_allows_managed_create(
        client=client,
        resource=resource,
        source_option_group_name=source_option_group_name,
        target_engine=target_engine,
        target_major_engine_version=target_major_engine_version,
        target_option_group_name=target_option_group_name,
        adapter_options=adapter_options,
    )

    try:
        if not quota_state["targetOptionGroupAlreadyExists"]:
            try:
                send_aws_call(
                    lambda: client.create_option_group(
                        OptionGroupName=target_option_group_name,
                        EngineName=target_engine,
                        MajorEngineVersion=target_major_engine_version,
                        OptionGroupDescription=(
                            f"Generated from {source_option_group_name} "
                            f"for {target_engine} {target_major_engine_version}"
                        ),
                    )
                )
            except Exception as error:  # noqa: BLE001
                if not is_option_group_already_exists_error(error):
                    raise

        source_options = list_rds_option_group_options(
            client=client,
            source_option_group_name=source_option_group_name,
        )
        apply_rds_option_group_options(
            client=client,
            target_option_group_name=target_option_group_name,
            options=source_options,
        )

        with RUNTIME_STATE_LOCK:
            custom_group_by_key[cache_key] = target_option_group_name
        return target_option_group_name
    finally:
        release_rds_option_group_quota_reservation(
            adapter_options=adapter_options,
            resource=resource,
            target_option_group_name=target_option_group_name,
            reserved=quota_state.get("reserved") is True,
        )


def ensure_rds_option_group_quota_allows_managed_create(
    *,
    client: Any,
    resource: Dict[str, str],
    source_option_group_name: str,
    target_engine: str,
    target_major_engine_version: str,
    target_option_group_name: str,
    adapter_options: Dict[str, Any],
) -> Dict[str, bool]:
    try:
        option_groups = list_all_rds_option_groups(client=client)
    except Exception:  # noqa: BLE001
        return {"targetOptionGroupAlreadyExists": False, "reserved": False}

    normalized_target_option_group_name = normalize_version(target_option_group_name)
    target_option_group_already_exists = any(
        normalize_version(safe_read(option_group, "OptionGroupName", ""))
        == normalized_target_option_group_name
        for option_group in option_groups
    )
    if target_option_group_already_exists:
        return {"targetOptionGroupAlreadyExists": True}

    try:
        option_group_quota = describe_rds_option_group_quota(client=client)
    except Exception:  # noqa: BLE001
        return {"targetOptionGroupAlreadyExists": False, "reserved": False}

    max_option_groups = int(option_group_quota.get("max") or 0)
    if max_option_groups <= 0:
        return {"targetOptionGroupAlreadyExists": False, "reserved": False}

    existing_custom_option_group_names = {
        normalize_version(safe_read(option_group, "OptionGroupName", ""))
        for option_group in option_groups
        if normalize_version(safe_read(option_group, "OptionGroupName", ""))
        and not is_default_option_group(safe_read(option_group, "OptionGroupName", ""))
    }
    reserved_count = try_reserve_rds_option_group_quota_slot(
        adapter_options=adapter_options,
        resource=resource,
        target_option_group_name=normalized_target_option_group_name,
        existing_option_group_names=existing_custom_option_group_names,
        max_option_groups=max_option_groups,
    )
    if reserved_count is not None:
        return {"targetOptionGroupAlreadyExists": False, "reserved": True}

    raise RdsOptionGroupQuotaPrecheckError(
        build_rds_option_group_quota_precheck_message(
            resource=resource,
            source_option_group_name=source_option_group_name,
            target_engine=target_engine,
            target_major_engine_version=target_major_engine_version,
            used_option_groups=max_option_groups,
            max_option_groups=max_option_groups,
        )
    )


def describe_rds_option_group_quota(*, client: Any) -> Dict[str, int]:
    response = send_aws_call(lambda: client.describe_account_attributes())
    option_group_quota = next(
        (
            quota
            for quota in safe_read(response, "AccountQuotas", [])
            if "option" in normalize_version(safe_read(quota, "AccountQuotaName", "")).lower()
            and "group" in normalize_version(safe_read(quota, "AccountQuotaName", "")).lower()
        ),
        {},
    )
    return {
        "used": int(safe_read(option_group_quota, "Used", 0) or 0),
        "max": int(safe_read(option_group_quota, "Max", 0) or 0),
    }


def list_all_rds_option_groups(*, client: Any) -> List[Dict[str, Any]]:
    option_groups: List[Dict[str, Any]] = []
    marker = None

    while True:
        request: Dict[str, Any] = {"MaxRecords": 100}
        if marker:
            request["Marker"] = marker

        response = send_aws_call(lambda: client.describe_option_groups(**request))
        option_groups.extend(safe_read(response, "OptionGroupsList", []))
        marker = safe_read(response, "Marker", "")
        if not marker:
            return option_groups


def build_rds_option_group_quota_precheck_message(
    *,
    resource: Dict[str, str],
    source_option_group_name: str,
    target_engine: str,
    target_major_engine_version: str,
    used_option_groups: int,
    max_option_groups: int,
) -> str:
    return (
        "Nao foi possivel prosseguir com o upgrade da DB instance "
        f"{resource.get('resourceId', '')}: a quota de option groups custom em "
        f"{resource.get('accountId', '')}/{resource.get('region', '')} esta em "
        f"{used_option_groups}/{max_option_groups}. O recurso precisa criar um option group "
        f"custom a partir de {source_option_group_name} para {target_engine} "
        f"{target_major_engine_version}."
    )


def try_reserve_rds_option_group_quota_slot(
    *,
    adapter_options: Dict[str, Any],
    resource: Dict[str, str],
    target_option_group_name: str,
    existing_option_group_names: set[str],
    max_option_groups: int,
) -> Optional[int]:
    normalized_target_option_group_name = normalize_version(target_option_group_name)
    if not normalized_target_option_group_name:
        return None

    runtime_state = ensure_rds_runtime_state(adapter_options)
    reservation_key = f"{resource.get('accountId', '')}|{resource.get('region', '')}"
    with RUNTIME_STATE_LOCK:
        reservations = runtime_state["option_quota_reservations_by_account_region"].get(
            reservation_key
        )
        if not isinstance(reservations, set):
            reservations = set()
            runtime_state["option_quota_reservations_by_account_region"][
                reservation_key
            ] = reservations

        pending_reserved_option_group_names = {
            option_group_name
            for option_group_name in reservations
            if option_group_name not in existing_option_group_names
        }
        current_usage = len(existing_option_group_names) + len(pending_reserved_option_group_names)
        if current_usage >= max_option_groups:
            return None

        reservations.add(normalized_target_option_group_name)
        return current_usage + 1


def release_rds_option_group_quota_reservation(
    *,
    adapter_options: Dict[str, Any],
    resource: Dict[str, str],
    target_option_group_name: str,
    reserved: bool,
) -> None:
    if not reserved:
        return

    normalized_target_option_group_name = normalize_version(target_option_group_name)
    if not normalized_target_option_group_name:
        return

    runtime_state = ensure_rds_runtime_state(adapter_options)
    reservation_key = f"{resource.get('accountId', '')}|{resource.get('region', '')}"
    with RUNTIME_STATE_LOCK:
        reservations = runtime_state["option_quota_reservations_by_account_region"].get(
            reservation_key
        )
        if not isinstance(reservations, set):
            return

        reservations.discard(normalized_target_option_group_name)
        if not reservations:
            runtime_state["option_quota_reservations_by_account_region"].pop(
                reservation_key,
                None,
            )


def list_rds_option_group_options(
    *,
    client: Any,
    source_option_group_name: str,
) -> List[Dict[str, Any]]:
    response = send_aws_call(
        lambda: client.describe_option_groups(
            OptionGroupName=source_option_group_name,
            MaxRecords=100,
        )
    )
    option_groups = response.get("OptionGroupsList")
    if not isinstance(option_groups, list) or not option_groups:
        return []

    selected_group = next(
        (
            group
            for group in option_groups
            if normalize_version(safe_read(group, "OptionGroupName", ""))
            == normalize_version(source_option_group_name)
        ),
        option_groups[0],
    )

    options = safe_read(selected_group, "Options", [])
    return options if isinstance(options, list) else []


def apply_rds_option_group_options(
    *,
    client: Any,
    target_option_group_name: str,
    options: Sequence[Dict[str, Any]],
) -> None:
    target_group_options = list_rds_option_group_options(
        client=client,
        source_option_group_name=target_option_group_name,
    )
    option_payloads_to_apply = resolve_rds_option_group_payloads_to_apply(
        source_group_options=options,
        target_group_options=target_group_options,
    )
    for option_to_include in option_payloads_to_apply:
        try:
            send_aws_call(
                lambda option_payload=option_to_include: client.modify_option_group(
                    OptionGroupName=target_option_group_name,
                    ApplyImmediately=True,
                    OptionsToInclude=[option_payload],
                )
            )
        except Exception:  # noqa: BLE001
            try:
                send_aws_call(
                    lambda: client.modify_option_group(
                        OptionGroupName=target_option_group_name,
                        ApplyImmediately=True,
                        OptionsToInclude=[
                            {"OptionName": normalize_version(option_to_include["OptionName"])}
                        ],
                    )
                )
            except Exception:  # noqa: BLE001
                continue


def resolve_rds_option_group_payloads_to_apply(
    *,
    source_group_options: Sequence[Dict[str, Any]],
    target_group_options: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    target_payload_by_name = build_rds_option_payload_map(target_group_options)
    source_payload_by_name = build_rds_option_payload_map(source_group_options)

    payloads_to_apply: List[Dict[str, Any]] = []
    for option_name, source_payload in source_payload_by_name.items():
        target_payload = target_payload_by_name.get(option_name)

        if target_payload is None:
            payloads_to_apply.append(source_payload)
            continue

        if (
            normalize_rds_option_payload_for_comparison(source_payload)
            != normalize_rds_option_payload_for_comparison(target_payload)
        ):
            payloads_to_apply.append(source_payload)

    return payloads_to_apply


def build_rds_option_payload_map(
    options: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    payload_entries = (
        (
            normalize_version(option_payload.get("OptionName")),
            option_payload,
        )
        for option_payload in (
            build_rds_option_to_include(option) for option in options
        )
    )
    return {
        option_name: option_payload
        for option_name, option_payload in payload_entries
        if option_name and option_payload
    }


def build_rds_option_to_include(option: Dict[str, Any]) -> Dict[str, Any]:
    option_name = normalize_version(safe_read(option, "OptionName", ""))
    if not option_name:
        return {}

    option_version = normalize_version(safe_read(option, "OptionVersion", ""))
    port_raw = safe_read(option, "Port", None)

    db_memberships = safe_read(option, "DBSecurityGroupMemberships", [])
    db_security_group_memberships = (
        [
            group_name
            for group_name in (
                normalize_version(safe_read(membership, "DBSecurityGroupName", ""))
                for membership in db_memberships
            )
            if group_name
        ]
        if isinstance(db_memberships, list)
        else []
    )

    vpc_memberships = safe_read(option, "VpcSecurityGroupMemberships", [])
    vpc_security_group_memberships = (
        [
            group_id
            for group_id in (
                normalize_version(safe_read(membership, "VpcSecurityGroupId", ""))
                for membership in vpc_memberships
            )
            if group_id
        ]
        if isinstance(vpc_memberships, list)
        else []
    )

    option_settings_raw = safe_read(option, "OptionSettings", [])
    option_settings = (
        [
            {"Name": name, "Value": value}
            for name, value in (
                (
                    normalize_version(safe_read(setting, "Name", "")),
                    normalize_version(
                        safe_read(setting, "Value", "")
                        or safe_read(setting, "DefaultValue", "")
                    ),
                )
                for setting in option_settings_raw
            )
            if name and value
        ]
        if isinstance(option_settings_raw, list)
        else []
    )

    return {
        "OptionName": option_name,
        **({"OptionVersion": option_version} if option_version else {}),
        **({"Port": port_raw} if isinstance(port_raw, int) and port_raw > 0 else {}),
        **(
            {"DBSecurityGroupMemberships": db_security_group_memberships}
            if db_security_group_memberships
            else {}
        ),
        **(
            {"VpcSecurityGroupMemberships": vpc_security_group_memberships}
            if vpc_security_group_memberships
            else {}
        ),
        **({"OptionSettings": option_settings} if option_settings else {}),
    }


def normalize_rds_option_payload_for_comparison(
    option_payload: Dict[str, Any],
) -> Dict[str, Any]:
    db_security_group_memberships_raw = safe_read(
        option_payload,
        "DBSecurityGroupMemberships",
        [],
    )
    db_security_group_memberships = (
        sorted(
            {
                normalize_version(group_name)
                for group_name in db_security_group_memberships_raw
                if normalize_version(group_name)
            }
        )
        if isinstance(db_security_group_memberships_raw, list)
        else []
    )

    vpc_security_group_memberships_raw = safe_read(
        option_payload,
        "VpcSecurityGroupMemberships",
        [],
    )
    vpc_security_group_memberships = (
        sorted(
            {
                normalize_version(group_id)
                for group_id in vpc_security_group_memberships_raw
                if normalize_version(group_id)
            }
        )
        if isinstance(vpc_security_group_memberships_raw, list)
        else []
    )

    option_settings_raw = safe_read(option_payload, "OptionSettings", [])
    option_settings = (
        sorted(
            [
                {"Name": name, "Value": value}
                for name, value in (
                    (
                        normalize_version(safe_read(setting, "Name", "")),
                        normalize_version(safe_read(setting, "Value", "")),
                    )
                    for setting in option_settings_raw
                )
                if name and value
            ],
            key=lambda setting: (setting["Name"], setting["Value"]),
        )
        if isinstance(option_settings_raw, list)
        else []
    )

    return {
        "OptionName": normalize_version(safe_read(option_payload, "OptionName", "")),
        "OptionVersion": normalize_version(safe_read(option_payload, "OptionVersion", "")),
        "Port": safe_read(option_payload, "Port", None),
        "DBSecurityGroupMemberships": db_security_group_memberships,
        "VpcSecurityGroupMemberships": vpc_security_group_memberships,
        "OptionSettings": option_settings,
    }


def is_db_parameter_group_already_exists_error(error: Exception) -> bool:
    error_code = extract_client_error_code(error)
    return error_code in ("DBParameterGroupAlreadyExists", "DBParameterGroupAlreadyExistsFault")


def is_db_cluster_parameter_group_already_exists_error(error: Exception) -> bool:
    error_code = extract_client_error_code(error)
    return error_code in (
        "DBClusterParameterGroupAlreadyExists",
        "DBClusterParameterGroupAlreadyExistsFault",
    )


def is_option_group_already_exists_error(error: Exception) -> bool:
    error_code = extract_client_error_code(error)
    return error_code in ("OptionGroupAlreadyExists", "OptionGroupAlreadyExistsFault")


def is_default_option_group(option_group_name: str) -> bool:
    return normalize_version(option_group_name).lower().startswith("default:")


def should_create_custom_rds_option_group(
    *,
    source_option_group_name: str,
) -> bool:
    normalized_source_option_group_name = normalize_version(source_option_group_name)
    return bool(normalized_source_option_group_name) and not is_default_option_group(
        normalized_source_option_group_name
    )


def build_default_rds_option_group_name(
    *,
    target_engine: str,
    target_major_engine_version: str,
) -> str:
    normalized_target_engine = normalize_engine(target_engine).replace("_", "-")
    normalized_target_major_engine_version = normalize_version(
        target_major_engine_version
    ).replace(".", "-")
    if not normalized_target_engine or not normalized_target_major_engine_version:
        return ""

    return f"default:{normalized_target_engine}-{normalized_target_major_engine_version}"


def build_unique_rds_option_group_name(
    *,
    source_option_group_name: str,
    target_engine: str,
    target_major_engine_version: str,
    resource: Dict[str, str],
) -> str:
    cleaned_source_name = sanitize_rds_option_group_name(source_option_group_name)
    cleaned_engine = sanitize_rds_option_group_name(target_engine)
    cleaned_major_engine_version = sanitize_rds_option_group_name(target_major_engine_version)
    hash_value = create_stable_hash(
        "|".join(
            [
                "option",
                resource.get("accountId", ""),
                resource.get("region", ""),
                source_option_group_name,
                target_engine,
                target_major_engine_version,
            ]
        )
    )

    base_name = re.sub(
        r"-+",
        "-",
        f"{cleaned_source_name}-{cleaned_engine}-{cleaned_major_engine_version}",
    )
    max_base_length = max(1, 255 - len(hash_value) - 1)
    trimmed_base_name = re.sub(r"-+$", "", base_name[:max_base_length])
    if not trimmed_base_name:
        trimmed_base_name = "og"
    if not trimmed_base_name[0].isalpha():
        prefixed_name = f"og-{trimmed_base_name}"
        trimmed_base_name = re.sub(r"-+$", "", prefixed_name[:max_base_length]) or "og"

    return f"{trimmed_base_name}-{hash_value}"


def sanitize_rds_option_group_name(name: str) -> str:
    sanitized = re.sub(r"[^a-z0-9-]", "-", normalize_version(name).lower())
    sanitized = re.sub(r"-+", "-", sanitized).strip("-")
    return sanitized or "og"


def infer_rds_major_engine_version(
    *,
    target_engine: str,
    target_version: str,
) -> str:
    normalized_target_version = normalize_version(target_version)
    normalized_target_engine = normalize_engine(target_engine)
    version_match = re.match(r"^(\d+)(?:\.(\d+))?", normalized_target_version)
    if not version_match:
        return ""

    major = normalize_version(version_match.group(1))
    minor = normalize_version(version_match.group(2))
    if uses_two_segment_rds_major_version(
        target_engine=normalized_target_engine,
        major=major,
        minor=minor,
    ):
        return f"{major}.{minor}"

    return major


def uses_two_segment_rds_major_version(
    *,
    target_engine: str,
    major: str,
    minor: str,
) -> bool:
    if not minor:
        return False

    normalized_target_engine = normalize_engine(target_engine)
    two_segment_major_engines = {
        "mysql",
        "mariadb",
        "aurora-mysql",
        "oracle-ee",
        "oracle-ee-cdb",
        "oracle-se2",
        "oracle-se2-cdb",
    }
    postgres_family = {"postgres", "aurora-postgresql"}

    return (
        normalized_target_engine in two_segment_major_engines
        or normalized_target_engine.startswith("sqlserver-")
        or (normalized_target_engine in postgres_family and major == "9")
    )


def build_unique_rds_parameter_group_name(
    *,
    source_parameter_group_name: str,
    target_family: str,
    resource: Dict[str, str],
) -> str:
    cleaned_source_name = sanitize_rds_parameter_group_name(source_parameter_group_name)
    cleaned_family = sanitize_rds_parameter_group_name(target_family)
    hash_value = create_stable_hash(
        "|".join(
            [
                resource.get("accountId", ""),
                resource.get("region", ""),
                source_parameter_group_name,
                target_family,
            ]
        )
    )

    base_name = re.sub(r"-+", "-", f"{cleaned_source_name}-{cleaned_family}")
    max_base_length = max(1, 255 - len(hash_value) - 1)
    trimmed_base_name = re.sub(r"-+$", "", base_name[:max_base_length])
    if not trimmed_base_name:
        trimmed_base_name = "pg"
    if not trimmed_base_name[0].isalpha():
        prefixed_name = f"pg-{trimmed_base_name}"
        trimmed_base_name = re.sub(r"-+$", "", prefixed_name[:max_base_length]) or "pg"

    return f"{trimmed_base_name}-{hash_value}"


def build_unique_rds_cluster_parameter_group_name(
    *,
    source_parameter_group_name: str,
    target_family: str,
    resource: Dict[str, str],
) -> str:
    cleaned_source_name = sanitize_rds_parameter_group_name(source_parameter_group_name)
    cleaned_family = sanitize_rds_parameter_group_name(target_family)
    hash_value = create_stable_hash(
        "|".join(
            [
                "cluster",
                resource.get("accountId", ""),
                resource.get("region", ""),
                source_parameter_group_name,
                target_family,
            ]
        )
    )

    base_name = re.sub(r"-+", "-", f"{cleaned_source_name}-{cleaned_family}")
    max_base_length = max(1, 255 - len(hash_value) - 1)
    trimmed_base_name = re.sub(r"-+$", "", base_name[:max_base_length])
    if not trimmed_base_name:
        trimmed_base_name = "cpg"
    if not trimmed_base_name[0].isalpha():
        prefixed_name = f"cpg-{trimmed_base_name}"
        trimmed_base_name = re.sub(r"-+$", "", prefixed_name[:max_base_length]) or "cpg"

    return f"{trimmed_base_name}-{hash_value}"


def sanitize_rds_parameter_group_name(name: str) -> str:
    normalized_name = normalize_version(name).lower()
    sanitized = re.sub(r"[^a-z0-9-]", "-", normalized_name)
    sanitized = re.sub(r"-+", "-", sanitized)
    sanitized = re.sub(r"^-+|-+$", "", sanitized)
    if not sanitized:
        return "db-parameter-group"
    if sanitized[0].isalpha():
        return sanitized
    return f"pg-{sanitized}"


def list_user_defined_db_parameters(
    *,
    client: Any,
    source_parameter_group_name: str,
) -> List[Dict[str, Any]]:
    parameters: List[Dict[str, Any]] = []
    marker: Optional[str] = None

    while True:
        request: Dict[str, Any] = {
            "DBParameterGroupName": source_parameter_group_name,
            "Source": "user",
            "MaxRecords": 100,
        }
        if marker:
            request["Marker"] = marker

        response = send_aws_call(lambda: client.describe_db_parameters(**request))
        parameters.extend(response.get("Parameters", []))

        marker = response.get("Marker")
        if not marker:
            break

    return parameters


def list_user_defined_db_cluster_parameters(
    *,
    client: Any,
    source_parameter_group_name: str,
) -> List[Dict[str, Any]]:
    parameters: List[Dict[str, Any]] = []
    marker: Optional[str] = None

    while True:
        request: Dict[str, Any] = {
            "DBClusterParameterGroupName": source_parameter_group_name,
            "Source": "user",
            "MaxRecords": 100,
        }
        if marker:
            request["Marker"] = marker

        response = send_aws_call(lambda: client.describe_db_cluster_parameters(**request))
        parameters.extend(response.get("Parameters", []))

        marker = response.get("Marker")
        if not marker:
            break

    return parameters


def resolve_db_parameter_apply_method(raw_apply_method: Any) -> str:
    normalized_apply_method = normalize_version(raw_apply_method).lower()
    if normalized_apply_method in ("immediate", "pending-reboot"):
        return normalized_apply_method
    return "pending-reboot"


def apply_user_defined_db_parameters(
    *,
    client: Any,
    target_parameter_group_name: str,
    parameters: Sequence[Dict[str, Any]],
) -> None:
    parameter_values = [
        {
            "ParameterName": parameter.get("ParameterName"),
            "ParameterValue": str(parameter.get("ParameterValue")),
            "ApplyMethod": resolve_db_parameter_apply_method(parameter.get("ApplyMethod")),
        }
        for parameter in parameters
        if parameter
        and parameter.get("ParameterName")
        and parameter.get("ParameterValue") is not None
    ]

    for batch in chunk_array(parameter_values, 20):
        if not batch:
            continue

        try:
            send_aws_call(
                lambda: client.modify_db_parameter_group(
                    DBParameterGroupName=target_parameter_group_name,
                    Parameters=batch,
                )
            )
        except Exception:  # noqa: BLE001
            for parameter in batch:
                try:
                    send_aws_call(
                        lambda: client.modify_db_parameter_group(
                            DBParameterGroupName=target_parameter_group_name,
                            Parameters=[parameter],
                        )
                    )
                except Exception:  # noqa: BLE001
                    continue


def apply_user_defined_db_cluster_parameters(
    *,
    client: Any,
    target_parameter_group_name: str,
    parameters: Sequence[Dict[str, Any]],
) -> None:
    parameter_values = [
        {
            "ParameterName": parameter.get("ParameterName"),
            "ParameterValue": str(parameter.get("ParameterValue")),
            "ApplyMethod": resolve_db_parameter_apply_method(parameter.get("ApplyMethod")),
        }
        for parameter in parameters
        if parameter
        and parameter.get("ParameterName")
        and parameter.get("ParameterValue") is not None
    ]

    for batch in chunk_array(parameter_values, 20):
        if not batch:
            continue

        try:
            send_aws_call(
                lambda: client.modify_db_cluster_parameter_group(
                    DBClusterParameterGroupName=target_parameter_group_name,
                    Parameters=batch,
                )
            )
        except Exception:  # noqa: BLE001
            for parameter in batch:
                try:
                    send_aws_call(
                        lambda: client.modify_db_cluster_parameter_group(
                            DBClusterParameterGroupName=target_parameter_group_name,
                            Parameters=[parameter],
                        )
                    )
                except Exception:  # noqa: BLE001
                    continue


def describe_eks_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
) -> Dict[str, List[Dict[str, str]]]:
    resources: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []

    regions, region_discovery_error = discover_regions(credentials, list(forced_regions))
    if region_discovery_error is not None:
        errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="eks",
                resource_kind="",
                resource_id="",
                engine="",
                region="",
                current_version="",
                target_version="",
                error_type="RegionDiscoveryError",
                error_message=str(region_discovery_error),
            )
        )

    region_results = collect_regions_in_batches(
        regions=regions,
        region_threads=region_threads,
        collect_region=lambda region: collect_eks_region(
            account_id=account_id,
            credentials=credentials,
            region=region,
        ),
    )

    aggregated_region_results = append_region_results(region_results)
    return {
        "resources": [*resources, *aggregated_region_results["resources"]],
        "errors": [*errors, *aggregated_region_results["errors"]],
    }


def collect_eks_region(
    *,
    account_id: str,
    credentials: Dict[str, str],
    region: str,
) -> Dict[str, List[Dict[str, str]]]:
    region_resources: List[Dict[str, str]] = []
    region_errors: List[Dict[str, str]] = []
    client = boto3.client("eks", region_name=region, **credentials)

    try:
        next_token: Optional[str] = None
        while True:
            request: Dict[str, Any] = {"maxResults": 100}
            if next_token:
                request["nextToken"] = next_token

            list_response = send_aws_call(lambda: client.list_clusters(**request))
            cluster_names = list_response.get("clusters", [])

            describe_results = collect_in_parallel(
                items=cluster_names,
                max_workers=max(1, min(len(cluster_names), 10)) if cluster_names else 1,
                task=lambda cluster_name: describe_eks_cluster(
                    client=client,
                    account_id=account_id,
                    region=region,
                    cluster_name=cluster_name,
                ),
                on_error=lambda cluster_name, error: {
                    "resource": None,
                    "error": build_error_row(
                        stage="discover",
                        account_id=account_id,
                        resource_type="eks",
                        resource_kind="cluster",
                        resource_id=str(cluster_name or ""),
                        engine="",
                        region=region,
                        current_version="",
                        target_version="",
                        error_type=type(error).__name__ or "DescribeClusterError",
                        error_message=str(error),
                    ),
                },
            )

            for result in describe_results:
                resource = result.get("resource")
                error_row = result.get("error")
                if resource and resource.get("resourceId"):
                    region_resources.append(resource)
                if error_row:
                    region_errors.append(error_row)

            next_token = list_response.get("nextToken")
            if not next_token:
                break
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="eks",
                resource_kind="cluster",
                resource_id="",
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "ListClustersError",
                error_message=str(error),
            )
        )

    return {"resources": region_resources, "errors": region_errors}


def describe_eks_cluster(
    *,
    client: Any,
    account_id: str,
    region: str,
    cluster_name: str,
) -> Dict[str, Any]:
    try:
        describe_response = send_aws_call(lambda: client.describe_cluster(name=cluster_name))
        cluster = describe_response.get("cluster", {})
        return {
            "resource": build_resource_row(
                account_id=account_id,
                resource_type="eks",
                resource_kind="cluster",
                resource_id=str(cluster.get("name") or cluster_name or "").strip(),
                engine="kubernetes",
                region=region,
                current_version=normalize_version(cluster.get("version")),
                arn=str(cluster.get("arn") or ""),
            ),
            "error": None,
        }
    except Exception as error:  # noqa: BLE001
        return {
            "resource": None,
            "error": build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="eks",
                resource_kind="cluster",
                resource_id=str(cluster_name or ""),
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "DescribeClusterError",
                error_message=str(error),
            ),
        }


def submit_eks_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    del target_engine, adapter_options

    client = boto3.client("eks", region_name=resource.get("region"), **credentials)
    response = send_aws_call(
        lambda: client.update_cluster_version(
            name=resource.get("resourceId"),
            version=target_version,
        )
    )

    return {
        "status": safe_read(response, "update.status", "InProgress"),
        "message": f"Versao do cluster alterada para {target_version}",
    }


def describe_opensearch_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
) -> Dict[str, List[Dict[str, str]]]:
    resources: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []

    regions, region_discovery_error = discover_regions(credentials, list(forced_regions))
    if region_discovery_error is not None:
        errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="opensearch",
                resource_kind="",
                resource_id="",
                engine="",
                region="",
                current_version="",
                target_version="",
                error_type="RegionDiscoveryError",
                error_message=str(region_discovery_error),
            )
        )

    region_results = collect_regions_in_batches(
        regions=regions,
        region_threads=region_threads,
        collect_region=lambda region: collect_opensearch_region(
            account_id=account_id,
            credentials=credentials,
            region=region,
        ),
    )

    aggregated_region_results = append_region_results(region_results)
    return {
        "resources": [*resources, *aggregated_region_results["resources"]],
        "errors": [*errors, *aggregated_region_results["errors"]],
    }


def collect_opensearch_region(
    *,
    account_id: str,
    credentials: Dict[str, str],
    region: str,
) -> Dict[str, List[Dict[str, str]]]:
    region_resources: List[Dict[str, str]] = []
    region_errors: List[Dict[str, str]] = []
    client = boto3.client("opensearch", region_name=region, **credentials)

    try:
        list_response = send_aws_call(lambda: client.list_domain_names())
        domains = list_response.get("DomainNames", [])

        describe_results = collect_in_parallel(
            items=domains,
            max_workers=max(1, min(len(domains), 10)) if domains else 1,
            task=lambda domain: describe_opensearch_domain(
                client=client,
                account_id=account_id,
                region=region,
                domain=domain,
            ),
            on_error=lambda domain, error: {
                "resource": None,
                "error": build_error_row(
                    stage="discover",
                    account_id=account_id,
                    resource_type="opensearch",
                    resource_kind="domain",
                    resource_id=str((domain or {}).get("DomainName", "")) if isinstance(domain, dict) else "",
                    engine="",
                    region=region,
                    current_version="",
                    target_version="",
                    error_type=type(error).__name__ or "DescribeDomainError",
                    error_message=str(error),
                ),
            },
        )

        for result in describe_results:
            resource = result.get("resource")
            error_row = result.get("error")
            if resource and resource.get("resourceId"):
                region_resources.append(resource)
            if error_row:
                region_errors.append(error_row)
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="opensearch",
                resource_kind="domain",
                resource_id="",
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "ListDomainNamesError",
                error_message=str(error),
            )
        )

    return {"resources": region_resources, "errors": region_errors}


def describe_opensearch_domain(
    *,
    client: Any,
    account_id: str,
    region: str,
    domain: Dict[str, Any],
) -> Dict[str, Any]:
    domain_name = str(domain.get("DomainName") or "").strip()
    if not domain_name:
        return {"resource": None, "error": None}

    try:
        describe_response = send_aws_call(
            lambda: client.describe_domain(DomainName=domain_name)
        )
        domain_status = describe_response.get("DomainStatus", {})
        current_version = normalize_version(domain_status.get("EngineVersion"))
        return {
            "resource": build_resource_row(
                account_id=account_id,
                resource_type="opensearch",
                resource_kind="domain",
                resource_id=domain_name,
                engine=infer_opensearch_engine(current_version),
                region=region,
                current_version=current_version,
                arn=str(domain_status.get("ARN") or ""),
            ),
            "error": None,
        }
    except Exception as error:  # noqa: BLE001
        return {
            "resource": None,
            "error": build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="opensearch",
                resource_kind="domain",
                resource_id=domain_name,
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "DescribeDomainError",
                error_message=str(error),
            ),
        }


def submit_opensearch_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    del target_engine, adapter_options

    client = boto3.client("opensearch", region_name=resource.get("region"), **credentials)
    response = send_aws_call(
        lambda: client.update_domain_config(
            DomainName=resource.get("resourceId"),
            EngineVersion=target_version,
        )
    )

    return {
        "status": safe_read(response, "DomainConfig.EngineVersion.Status.State", "processing"),
        "message": f"EngineVersion alterada para {target_version}",
    }


def describe_lambda_resources(
    *,
    account_id: str,
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
    region_threads: int,
) -> Dict[str, List[Dict[str, str]]]:
    resources: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []

    regions, region_discovery_error = discover_regions(credentials, list(forced_regions))
    if region_discovery_error is not None:
        errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="lambda",
                resource_kind="",
                resource_id="",
                engine="",
                region="",
                current_version="",
                target_version="",
                error_type="RegionDiscoveryError",
                error_message=str(region_discovery_error),
            )
        )

    region_results = collect_regions_in_batches(
        regions=regions,
        region_threads=region_threads,
        collect_region=lambda region: collect_lambda_region(
            account_id=account_id,
            credentials=credentials,
            region=region,
        ),
    )

    aggregated_region_results = append_region_results(region_results)
    return {
        "resources": [*resources, *aggregated_region_results["resources"]],
        "errors": [*errors, *aggregated_region_results["errors"]],
    }


def collect_lambda_region(
    *,
    account_id: str,
    credentials: Dict[str, str],
    region: str,
) -> Dict[str, List[Dict[str, str]]]:
    region_resources: List[Dict[str, str]] = []
    region_errors: List[Dict[str, str]] = []
    client = boto3.client("lambda", region_name=region, **credentials)

    try:
        marker: Optional[str] = None
        while True:
            request: Dict[str, Any] = {"MaxItems": 10000}
            if marker:
                request["Marker"] = marker

            list_response = send_aws_call(lambda: client.list_functions(**request))
            functions = list_response.get("Functions", [])

            for item in functions:
                function_name = str(item.get("FunctionName") or "").strip()
                runtime = normalize_version(item.get("Runtime"))
                if not function_name or not runtime:
                    continue

                region_resources.append(
                    build_resource_row(
                        account_id=account_id,
                        resource_type="lambda",
                        resource_kind="function",
                        resource_id=function_name,
                        engine="lambda",
                        region=region,
                        current_version=runtime,
                        arn=str(item.get("FunctionArn") or ""),
                    )
                )

            marker = list_response.get("NextMarker")
            if not marker:
                break
    except Exception as error:  # noqa: BLE001
        region_errors.append(
            build_error_row(
                stage="discover",
                account_id=account_id,
                resource_type="lambda",
                resource_kind="function",
                resource_id="",
                engine="",
                region=region,
                current_version="",
                target_version="",
                error_type=type(error).__name__ or "ListFunctionsError",
                error_message=str(error),
            )
        )

    return {"resources": region_resources, "errors": region_errors}


def submit_lambda_update(
    *,
    resource: Dict[str, str],
    target_version: str,
    target_engine: str,
    adapter_options: Dict[str, Any],
    credentials: Dict[str, str],
) -> Dict[str, str]:
    del target_engine, adapter_options

    client = boto3.client("lambda", region_name=resource.get("region"), **credentials)
    response = send_aws_call(
        lambda: client.update_function_configuration(
            FunctionName=resource.get("resourceId"),
            Runtime=target_version,
        )
    )

    return {
        "status": safe_read(response, "LastUpdateStatus", "InProgress"),
        "message": f"Runtime alterada para {target_version}",
    }


def collect_regions_in_batches(
    *,
    regions: Sequence[str],
    region_threads: int,
    collect_region: Callable[[str], Dict[str, List[Dict[str, str]]]],
) -> List[Dict[str, List[Dict[str, str]]]]:
    region_batches = chunk_array(list(regions), max(1, region_threads))
    batch_results = [
        collect_in_parallel(
            items=region_batch,
            max_workers=max(1, min(len(region_batch), region_threads)),
            task=collect_region,
        )
        for region_batch in region_batches
    ]
    return [result for batch in batch_results for result in batch]


def collect_in_parallel(
    *,
    items: Sequence[Any],
    max_workers: int,
    task: Callable[[Any], Any],
    on_error: Optional[Callable[[Any, Exception], Any]] = None,
) -> List[Any]:
    if not items:
        return []

    results: List[Optional[Any]] = [None] * len(items)
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        future_map = {
            executor.submit(task, item): (index, item)
            for index, item in enumerate(items)
        }
        for future in as_completed(future_map):
            index, item = future_map[future]
            try:
                results[index] = future.result()
            except Exception as error:  # noqa: BLE001
                if on_error is None:
                    raise
                results[index] = on_error(item, error)
    return [result for result in results if result is not None]


def append_region_results(
    region_results: Sequence[Dict[str, List[Dict[str, str]]]],
) -> Dict[str, List[Dict[str, str]]]:
    return {
        "resources": [
            resource
            for result in region_results
            if result
            for resource in result.get("resources", [])
        ],
        "errors": [
            error
            for result in region_results
            if result
            for error in result.get("errors", [])
        ],
    }


def deduplicate_resources(resources: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: set = set()
    deduplicated_resources: List[Dict[str, str]] = []
    for resource in resources:
        key = "|".join(
            [
                resource.get("accountId", ""),
                resource.get("resourceType", ""),
                resource.get("resourceKind", ""),
                resource.get("region", ""),
                resource.get("resourceId", ""),
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        deduplicated_resources.append(resource)

    return deduplicated_resources


def build_resource_row(
    *,
    account_id: str,
    resource_type: str,
    resource_kind: str,
    resource_id: str,
    engine: str,
    region: str,
    current_version: str,
    arn: str,
    parameter_group_name: str = "",
    option_group_name: str = "",
    cluster_parameter_group_name: str = "",
    instance_parameter_group_name: str = "",
    instance_parameter_groups: str = "",
    cluster_mode: str = "",
    instance_type: str = "",
) -> Dict[str, str]:
    normalized_parameter_group_name = normalize_version(parameter_group_name)
    normalized_option_group_name = normalize_version(option_group_name)
    normalized_cluster_parameter_group_name = normalize_version(cluster_parameter_group_name)
    normalized_instance_parameter_group_name = normalize_version(instance_parameter_group_name)
    normalized_instance_parameter_groups = normalize_version(instance_parameter_groups)
    normalized_instance_type = normalize_version(instance_type)

    return {
        "accountId": str(account_id or ""),
        "resourceType": str(resource_type or ""),
        "resourceKind": str(resource_kind or ""),
        "resourceId": str(resource_id or ""),
        "engine": str(engine or ""),
        "region": str(region or ""),
        "currentVersion": normalize_version(current_version),
        "arn": str(arn or ""),
        "parameterGroupName": normalized_parameter_group_name,
        "optionGroupName": normalized_option_group_name,
        "clusterParameterGroupName": normalized_cluster_parameter_group_name,
        "instanceParameterGroupName": normalized_instance_parameter_group_name,
        "instanceParameterGroups": normalized_instance_parameter_groups,
        "instanceType": normalized_instance_type,
        "clusterMode": normalize_elasticache_cluster_mode(cluster_mode),
        "targetParameterGroupName": "",
        "targetOptionGroupName": "",
        "targetClusterParameterGroupName": "",
        "targetInstanceParameterGroupName": "",
        "targetInstanceType": "",
    }


def build_success_row(
    *,
    resource: Dict[str, str],
    status: str,
    message: str,
) -> Dict[str, str]:
    return {
        "account_id": resource.get("accountId", ""),
        "resource_type": resource.get("resourceType", ""),
        "resource_kind": resource.get("resourceKind", ""),
        "resource_id": resource.get("resourceId", ""),
        "engine": resource.get("engine", ""),
        "target_engine": resource.get("targetEngine") or resource.get("engine") or "",
        "parameter_group": format_parameter_group_value_for_resource(resource),
        "target_parameter_group": format_parameter_group_value_for_resource(
            resource,
            target=True,
        ),
        "option_group": format_option_group_value_for_resource(resource),
        "target_option_group": format_option_group_value_for_resource(
            resource,
            target=True,
        ),
        "region": resource.get("region", ""),
        "current_version": resource.get("currentVersion", ""),
        "target_version": resource.get("targetVersion", ""),
        "status": status or "submitted",
        "message": truncate(str(message or ""), 1500),
        "arn": resource.get("arn", ""),
    }


def split_parameter_group_names(parameter_group_names: Any) -> List[str]:
    normalized_parameter_group_names = normalize_version(parameter_group_names)
    if not normalized_parameter_group_names:
        return []

    return [
        name
        for name in (
            normalize_version(candidate)
            for candidate in normalized_parameter_group_names.split(",")
        )
        if name
    ]


def format_parameter_group_value_for_resource(
    resource: Dict[str, str],
    *,
    target: bool = False,
) -> str:
    resource_kind = normalize_resource(resource.get("resourceKind"))
    if resource_kind != "db-cluster":
        if target:
            return (
                resource.get("targetParameterGroupName")
                or resource.get("parameterGroupName")
                or ""
            )
        return resource.get("parameterGroupName", "")

    cluster_parameter_group_name = normalize_version(
        (
            resource.get("targetClusterParameterGroupName")
            or resource.get("targetParameterGroupName")
            or resource.get("clusterParameterGroupName")
            or resource.get("parameterGroupName")
        )
        if target
        else (
            resource.get("clusterParameterGroupName")
            or resource.get("parameterGroupName")
        )
    )

    if target:
        instance_parameter_group_names = split_parameter_group_names(
            resource.get("targetInstanceParameterGroupName")
            or resource.get("instanceParameterGroups")
            or resource.get("instanceParameterGroupName")
        )
    else:
        instance_parameter_group_names = split_parameter_group_names(
            resource.get("instanceParameterGroups")
            or resource.get("instanceParameterGroupName")
        )

    if cluster_parameter_group_name and not instance_parameter_group_names:
        return cluster_parameter_group_name

    if not cluster_parameter_group_name and len(instance_parameter_group_names) == 1:
        return instance_parameter_group_names[0]

    if not cluster_parameter_group_name and instance_parameter_group_names:
        return ",".join(instance_parameter_group_names)

    if cluster_parameter_group_name and instance_parameter_group_names:
        return (
            f"cluster={cluster_parameter_group_name};"
            f"instance={','.join(instance_parameter_group_names)}"
        )

    return ""


def format_option_group_value_for_resource(
    resource: Dict[str, str],
    *,
    target: bool = False,
) -> str:
    resource_kind = normalize_resource(resource.get("resourceKind"))
    if resource_kind != "db-instance":
        return ""

    if target:
        return resource.get("targetOptionGroupName") or ""

    return resource.get("optionGroupName") or ""


def assume_role_for_account(
    account: Dict[str, str],
    fallback_role_name: Optional[str],
    external_id: Optional[str],
) -> Dict[str, str]:
    role_arn = build_role_arn(account, fallback_role_name)

    session_name = re.sub(
        r"[^a-zA-Z0-9+=,.@-]",
        "",
        f"inventory-version-upgrade-{account.get('accountId', '')}-{int(time.time() * 1000)}",
    )[:64]

    request: Dict[str, str] = {
        "RoleArn": role_arn,
        "RoleSessionName": session_name,
    }
    if external_id:
        request["ExternalId"] = external_id

    sts = boto3.client("sts", **build_local_client_kwargs(GLOBAL_AWS["AWS_REGION"]))
    response = send_aws_call(lambda: sts.assume_role(**request))
    credentials = response.get("Credentials")
    if not credentials:
        raise RuntimeError(f"AssumeRole sem credenciais para conta {account.get('accountId', '')}")

    return {
        "aws_access_key_id": credentials["AccessKeyId"],
        "aws_secret_access_key": credentials["SecretAccessKey"],
        "aws_session_token": credentials["SessionToken"],
    }


def build_role_arn(account: Dict[str, str], fallback_role_name: Optional[str]) -> str:
    if account.get("roleArn"):
        return account["roleArn"]

    role_name = account.get("roleName") or str(fallback_role_name or "").strip()
    if not role_name:
        raise ValueError(
            "Conta "
            f"{account.get('accountId', '')} sem role_arn/role_name no CSV e sem --role-name "
            f"nem variavel de ambiente ({'/'.join(ROLE_NAME_ENV_KEYS)})."
        )

    return f"arn:aws:iam::{account.get('accountId', '')}:role/{role_name}"


def discover_regions(
    credentials: Dict[str, str],
    forced_regions: Sequence[str],
) -> Tuple[List[str], Optional[Exception]]:
    if forced_regions:
        return list(forced_regions), None

    try:
        ec2 = boto3.client("ec2", region_name=GLOBAL_AWS["AWS_REGION"], **credentials)
        response = send_aws_call(lambda: ec2.describe_regions(AllRegions=True))
        regions = sorted(
            region.get("RegionName")
            for region in response.get("Regions", [])
            if region.get("OptInStatus") in ("opt-in-not-required", "opted-in")
            and region.get("RegionName")
        )

        if not regions:
            raise RuntimeError("Nenhuma regiao habilitada para a conta.")

        return [region for region in regions if region], None
    except Exception as error:  # noqa: BLE001
        return [], error


def infer_opensearch_engine(engine_version: str) -> str:
    normalized_version = normalize_version(engine_version)
    normalized_version_lower = normalized_version.lower()
    if not normalized_version_lower:
        return "opensearch"
    if normalized_version_lower.startswith("elasticsearch"):
        return "elasticsearch"
    return "opensearch"


def build_rds_instance_modify_input(
    *,
    db_instance_identifier: str,
    target_version: str,
    major_version_changed: bool,
    target_parameter_group_name: str,
    target_option_group_name: str,
    target_instance_class: str,
) -> Dict[str, Any]:
    return {
        "DBInstanceIdentifier": db_instance_identifier,
        "EngineVersion": target_version,
        "ApplyImmediately": True,
        **({"AllowMajorVersionUpgrade": True} if major_version_changed else {}),
        **(
            {"DBParameterGroupName": target_parameter_group_name}
            if target_parameter_group_name
            else {}
        ),
        **({"DBInstanceClass": target_instance_class} if target_instance_class else {}),
        **({"OptionGroupName": target_option_group_name} if target_option_group_name else {}),
    }


def has_rds_major_version_change(
    *,
    current_engine: str,
    current_version: str,
    target_engine: str,
    target_version: str,
) -> bool:
    from_major = infer_rds_major_engine_version(
        target_engine=current_engine,
        target_version=current_version,
    )
    to_major = infer_rds_major_engine_version(
        target_engine=target_engine or current_engine,
        target_version=target_version,
    )

    if not from_major or not to_major:
        return True

    return from_major != to_major


def extract_major_version(version: str) -> str:
    normalized_version = normalize_version(version)
    match = re.match(r"^(\d+)", normalized_version)
    return match.group(1) if match else ""


def chunk_array(values: Sequence[Any], size: int) -> List[List[Any]]:
    safe_values = list(values)
    safe_size = size if isinstance(size, int) and size > 0 else len(safe_values) or 1
    return [
        safe_values[index : index + safe_size]
        for index in range(0, len(safe_values), safe_size)
    ]


def classify_error_for_report(*, error_type: str, error_message: str) -> Dict[str, Any]:
    resolved_error_type = str(error_type or "Error")
    resolved_error_message = str(error_message or "")
    normalized_error_type = resolved_error_type.lower()
    normalized_error_message = resolved_error_message.lower()
    combined = f"{normalized_error_type} {normalized_error_message}"
    classification_rules: Sequence[Tuple[Callable[[], bool], Dict[str, Any]]] = (
        (
            lambda: is_retryable_aws_error_message(
                error_name=resolved_error_type,
                error_message=resolved_error_message,
            ),
            {
                "errorCategory": "transient",
                "retryable": True,
                "recommendedAction": (
                    "Erro transitorio. Reexecute o script "
                    "(ja existe retry automatico interno)."
                ),
            },
        ),
        (
            lambda: (
                "accessdenied" in combined
                or "not authorized" in combined
                or "unauthorized" in combined
                or "forbidden" in combined
            ),
            {
                "errorCategory": "permission",
                "retryable": False,
                "recommendedAction": (
                    "Validar role/politicas IAM e trust relationship do AssumeRole."
                ),
            },
        ),
        (
            lambda: (
                "notfound" in combined
                or "not found" in combined
                or "does not exist" in combined
            ),
            {
                "errorCategory": "not_found",
                "retryable": False,
                "recommendedAction": (
                    "Revalidar se o recurso ainda existe e se a regiao esta correta."
                ),
            },
        ),
        (
            lambda: (
                "invalidparameter" in combined
                or "validation" in combined
                or "invalid request" in combined
            ),
            {
                "errorCategory": "validation",
                "retryable": False,
                "recommendedAction": (
                    "Ajustar parametros de entrada "
                    "(versao alvo, engine, parameter group ou recurso)."
                ),
            },
        ),
        (
            lambda: (
                "inprogress" in combined
                or "conflict" in combined
                or "invalidstate" in combined
                or "currently being modified" in combined
            ),
            {
                "errorCategory": "state_conflict",
                "retryable": True,
                "recommendedAction": "Aguarde o recurso estabilizar e execute novamente.",
            },
        ),
        (
            lambda: (
                "rdsoptiongroupquotaprecheckerror" in combined
                or ("option group" in combined and "quota" in combined)
            ),
            {
                "errorCategory": "quota",
                "retryable": False,
                "recommendedAction": (
                    "Solicitar aumento de quota ou liberar manualmente option groups "
                    "custom nao utilizados."
                ),
            },
        ),
        (
            lambda: (
                "service quota" in combined
                or "limitexceeded" in combined
                or "quotaexceeded" in combined
            ),
            {
                "errorCategory": "quota",
                "retryable": False,
                "recommendedAction": "Solicitar aumento de quota ou reduzir paralelismo.",
            },
        ),
        (
            lambda: (
                "unsupported" in combined
                or "not supported" in combined
                or "unknownoperation" in combined
            ),
            {
                "errorCategory": "unsupported",
                "retryable": False,
                "recommendedAction": (
                    "Validar compatibilidade da operacao para "
                    "tipo de recurso, versao e regiao."
                ),
            },
        ),
    )

    matched_outcome = next(
        (outcome for matches, outcome in classification_rules if matches()),
        {
            "errorCategory": "unknown",
            "retryable": False,
            "recommendedAction": "Inspecionar mensagem de erro e logs para diagnostico.",
        },
    )

    return {
        "errorType": resolved_error_type,
        "errorMessage": resolved_error_message,
        "errorCategory": matched_outcome["errorCategory"],
        "retryable": matched_outcome["retryable"],
        "recommendedAction": matched_outcome["recommendedAction"],
    }


def build_error_row(
    *,
    stage: str,
    account_id: str,
    resource_type: str,
    resource_kind: str,
    resource_id: str,
    engine: str,
    region: str,
    current_version: str,
    target_version: str,
    error_type: str,
    error_message: str,
    target_engine: str = "",
    parameter_group_name: str = "",
    target_parameter_group_name: str = "",
    option_group_name: str = "",
    target_option_group_name: str = "",
) -> Dict[str, str]:
    classified_error = classify_error_for_report(
        error_type=error_type,
        error_message=error_message,
    )

    return {
        "stage": stage or "unknown",
        "account_id": account_id or "",
        "resource_type": resource_type or "",
        "resource_kind": resource_kind or "",
        "resource_id": resource_id or "",
        "engine": engine or "",
        "target_engine": target_engine or "",
        "parameter_group": parameter_group_name or "",
        "target_parameter_group": target_parameter_group_name or "",
        "option_group": option_group_name or "",
        "target_option_group": target_option_group_name or "",
        "region": region or "",
        "current_version": current_version or "",
        "target_version": target_version or "",
        "error_category": classified_error["errorCategory"],
        "retryable": "true" if classified_error["retryable"] else "false",
        "recommended_action": classified_error["recommendedAction"],
        "error_type": classified_error["errorType"] or "Error",
        "error_message": truncate(str(classified_error["errorMessage"] or ""), 1500),
    }


def send_aws_call(
    operation: Callable[[], Any],
    max_attempts: Optional[int] = None,
) -> Any:
    attempts = (
        max_attempts
        if isinstance(max_attempts, int) and max_attempts > 0
        else DEFAULT_AWS_MAX_ATTEMPTS
    )

    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as error:  # noqa: BLE001
            can_retry = is_retryable_aws_error(error)
            if not can_retry or attempt >= attempts:
                raise

            delay_ms = calculate_retry_delay_ms(attempt)
            time.sleep(max(0.0, delay_ms / 1000.0))

    raise RuntimeError("Falha interna de retry AWS.")


def is_retryable_aws_error(error: Exception) -> bool:
    if isinstance(error, (BotoCoreError, ClientError)):
        return is_retryable_aws_error_message(
            error_name=type(error).__name__,
            error_message=str(error),
            status_code=extract_client_error_status_code(error),
            error_code=extract_client_error_code(error),
        )

    return is_retryable_aws_error_message(
        error_name=type(error).__name__,
        error_message=str(error),
    )


def is_retryable_aws_error_message(
    *,
    error_name: str,
    error_message: str,
    status_code: int = 0,
    error_code: str = "",
) -> bool:
    normalized_name = str(error_name or "").lower()
    normalized_message = str(error_message or "").lower()
    normalized_code = str(error_code or "").lower()
    combined = f"{normalized_name} {normalized_code} {normalized_message}"

    if status_code in (429, 500, 502, 503, 504):
        return True

    if (
        "throttl" in combined
        or "too many request" in combined
        or "rate exceeded" in combined
        or "request limit exceeded" in combined
        or "timeout" in combined
        or "timed out" in combined
        or "temporarily unavailable" in combined
        or "service unavailable" in combined
        or "internalerror" in combined
        or "internal failure" in combined
    ):
        return True

    if "inprogress" in combined or "currently being modified" in combined:
        return True

    return False


def extract_client_error_status_code(error: Exception) -> int:
    if not isinstance(error, ClientError):
        return 0

    status_code = safe_read(error.response, "ResponseMetadata.HTTPStatusCode", 0)
    try:
        return int(status_code)
    except Exception:  # noqa: BLE001
        return 0


def extract_client_error_code(error: Exception) -> str:
    if not isinstance(error, ClientError):
        return ""
    return str(safe_read(error.response, "Error.Code", ""))


def calculate_retry_delay_ms(attempt: int) -> int:
    exponential_delay = AWS_RETRY_BASE_DELAY_MS * (2 ** max(0, attempt - 1))
    jitter = random.randint(0, AWS_RETRY_BASE_DELAY_MS)
    return min(AWS_RETRY_MAX_DELAY_MS, exponential_delay + jitter)


def write_csv(file_path: str, headers: Sequence[str], rows: Sequence[Dict[str, str]]) -> None:
    directory = os.path.dirname(file_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(file_path, mode="w", encoding="utf-8", newline="") as handler:
        writer = csv.DictWriter(handler, fieldnames=list(headers), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def compare_rows(left: Dict[str, str], right: Dict[str, str]) -> int:
    left_key = "|".join(
        [
            left.get("stage", ""),
            left.get("account_id", ""),
            left.get("region", ""),
            left.get("resource_id", ""),
            left.get("error_type", ""),
            left.get("target_version", ""),
        ]
    )

    right_key = "|".join(
        [
            right.get("stage", ""),
            right.get("account_id", ""),
            right.get("region", ""),
            right.get("resource_id", ""),
            right.get("error_type", ""),
            right.get("target_version", ""),
        ]
    )

    if left_key < right_key:
        return -1
    if left_key > right_key:
        return 1
    return 0


def truncate(text: str, max_length: int) -> str:
    if len(text) <= max_length:
        return text
    return text[: max(0, max_length - 3)] + "..."


def safe_read(source: Any, expression: str, fallback_value: Any) -> Any:
    keys = [key for key in str(expression or "").split(".") if key]
    current = source

    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return fallback_value
        current = current[key]

    return fallback_value if current is None else current


def build_resource_adapters() -> Dict[str, Dict[str, Callable[..., Any]]]:
    return {
        "elasticache": {
            "describe_resources": describe_elasticache_resources,
            "submit_update": submit_elasticache_update,
        },
        "rds": {
            "describe_resources": describe_rds_resources,
            "submit_update": submit_rds_update,
        },
        "docdb": {
            "describe_resources": describe_docdb_resources,
            "submit_update": submit_docdb_update,
        },
        "neptune": {
            "describe_resources": describe_neptune_resources,
            "submit_update": submit_neptune_update,
        },
        "redshift": {
            "describe_resources": describe_redshift_resources,
            "submit_update": submit_redshift_update,
        },
        "eks": {
            "describe_resources": describe_eks_resources,
            "submit_update": submit_eks_update,
        },
        "opensearch": {
            "describe_resources": describe_opensearch_resources,
            "submit_update": submit_opensearch_update,
        },
        "lambda": {
            "describe_resources": describe_lambda_resources,
            "submit_update": submit_lambda_update,
        },
    }


def run_upgrade_for_resource_type(
    *,
    resource_type: str,
    adapter: Dict[str, Callable[..., Any]],
    version_map: Tuple[Dict[str, str], ...],
    accounts: Sequence[Dict[str, str]],
    resolved_role_name: Optional[str],
    external_id: Optional[str],
    discovery_threads: int,
    update_threads: int,
    region_threads: int,
    forced_regions: Sequence[str],
    dry_run: bool,
    adapter_options: Dict[str, str],
) -> Dict[str, List[Dict[str, str]]]:
    print(
        "[start] "
        f"recurso={resource_type} "
        f"contas={len(accounts)} "
        f"discovery_threads={discovery_threads} "
        f"update_threads={update_threads} "
        f"region_threads={region_threads} "
        f"region={forced_regions[0] if forced_regions else 'all-enabled'} "
        f"dry_run={'true' if dry_run else 'false'}"
    )
    validate_instance_type_option(
        instance_type=safe_read(adapter_options, "instance_type", ""),
        resolved_resources=[resource_type],
    )

    account_by_id = {account.get("accountId", ""): account for account in accounts}
    discovery_progress = create_progress_tracker(
        total=len(accounts),
        label=f"discover:{resource_type}",
        success_label="recursos",
        error_label="erros",
    )

    discovery_result = discover_account_resources(
        accounts=accounts,
        adapter=adapter,
        resource_type=resource_type,
        role_name=resolved_role_name,
        external_id=external_id,
        forced_regions=list(forced_regions),
        threads=discovery_threads,
        region_threads=region_threads,
        on_progress=discovery_progress["update"],
    )
    discovery_progress["finish"]()

    unique_discovered_resources = deduplicate_resources(discovery_result["resources"])
    updates = build_update_plan(unique_discovered_resources, version_map)

    if discovery_result.get("skippedAccounts", 0) > 0:
        print(f"[skip] recurso={resource_type} contas_sem_recurso={discovery_result['skippedAccounts']}")

    print(
        "[plan] "
        f"recurso={resource_type} "
        f"recursos_descobertos={len(unique_discovered_resources)} "
        f"recursos_para_update={len(updates)} "
        f"erros_descoberta={len(discovery_result['errors'])}"
    )

    update_progress = create_progress_tracker(
        total=len(updates),
        label=f"update:{resource_type}",
        success_label="planejados" if dry_run else "enviados",
        error_label="erros",
    )

    update_result = execute_updates(
        updates=updates,
        adapter=adapter,
        resource_type=resource_type,
        role_name=resolved_role_name,
        external_id=external_id,
        account_by_id=account_by_id,
        threads=update_threads,
        dry_run=dry_run,
        adapter_options=adapter_options,
        on_progress=update_progress["update"],
    )
    update_progress["finish"]()

    return {
        "success": sorted(update_result["success"], key=cmp_to_key(compare_rows)),
        "errors": sorted(
            [*discovery_result["errors"], *update_result["errors"]],
            key=cmp_to_key(compare_rows),
        ),
    }


def main() -> None:
    args = parse_args()
    validate_args(args)
    resolved_role_name = resolve_global_role_name(args.role_name)

    resource_types = resolve_resource_types(args.resource)
    adapters = build_resource_adapters()

    version_map = load_version_map(args)
    accounts = deduplicate_accounts_by_id(read_accounts_csv(os.path.abspath(args.accounts_csv)))

    discovery_threads = args.threads if args.threads is not None else DEFAULT_DISCOVERY_THREADS
    update_threads = (
        args.update_threads if args.update_threads is not None else DEFAULT_UPDATE_THREADS
    )
    region_threads = (
        args.region_threads if args.region_threads is not None else DEFAULT_REGION_THREADS
    )
    forced_regions = [args.region] if args.region else []

    if not accounts:
        raise ValueError("Nenhuma conta encontrada no CSV informado.")

    adapter_options = {
        "parameter_group_name": normalize_version(args.parameter_group_name),
        "cluster_parameter_group_name": normalize_version(
            args.cluster_parameter_group_name
        ),
        "instance_parameter_group_name": normalize_version(
            args.instance_parameter_group_name
        ),
        "instance_type": normalize_version(args.instance_type),
    }
    csv_resource_label = (
        resource_types[0] if len(resource_types) == 1 else RESOURCE_TYPE_ALL
    )
    timestamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    success_csv_path = (
        os.path.abspath(args.out_csv)
        if args.out_csv
        else os.path.abspath(f"version-updates-{csv_resource_label}-{timestamp}.csv")
    )
    error_csv_path = (
        os.path.abspath(args.error_csv)
        if args.error_csv
        else os.path.abspath(
            f"version-update-errors-{csv_resource_label}-{timestamp}.csv"
        )
    )

    results_by_resource = [
        run_upgrade_for_resource_type(
            resource_type=resource_type,
            adapter=adapters[resource_type],
            version_map=version_map,
            accounts=accounts,
            resolved_role_name=resolved_role_name,
            external_id=args.external_id,
            discovery_threads=discovery_threads,
            update_threads=update_threads,
            region_threads=region_threads,
            forced_regions=forced_regions,
            dry_run=args.dry_run,
            adapter_options=adapter_options,
        )
        for resource_type in resource_types
    ]

    all_success_rows = [
        row for result in results_by_resource for row in result["success"]
    ]
    all_error_rows = [
        row for result in results_by_resource for row in result["errors"]
    ]

    error_rows = sorted(all_error_rows, key=cmp_to_key(compare_rows))
    success_rows = sorted(all_success_rows, key=cmp_to_key(compare_rows))

    write_csv(success_csv_path, SUCCESS_HEADERS, success_rows)
    write_csv(error_csv_path, ERROR_HEADERS, error_rows)

    print(
        "[done] "
        f"recursos={','.join(resource_types)} "
        f"enviados={len(success_rows)} "
        f"erros={len(error_rows)} "
        f"success_csv={success_csv_path} "
        f"error_csv={error_csv_path}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001
        print(f"[fatal] {error}", file=sys.stderr)
        sys.exit(1)
