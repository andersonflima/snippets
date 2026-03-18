"""AWS Lambda script for DynamoDB table snapshots to S3.

Capabilities:
- Full and incremental snapshots.
- Explicit target list (ARNs or table names) and ignore list.
- Multithread execution by table.
- Incremental mode with checkpoint state in DynamoDB.
- Incremental fallback by _updated_at scan when native incremental export is unavailable,
  with file partitioning and optional gzip compression.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import logging
import os
import re
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    NoRegionError,
    PartialCredentialsError,
    ProxyConnectionError,
    ReadTimeoutError,
)
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

logger = logging.getLogger()
if not logger.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


deserializer = TypeDeserializer()
serializer = TypeSerializer()
ROLE_TEMPLATE_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
FULL_EXPORT_RUN_ID_PATTERN = re.compile(r"(?:^|/)run_id=(\d{8}T\d{6}Z)(?:/|$)")
FULL_EXPORT_COMPLETION_SUFFIXES = ("manifest-summary.json", "manifest-files.json")
EXPORT_LAYOUT_PREFIX = "DDB"
EXPORT_LAYOUT_KEY_PATTERN = re.compile(
    r"^DDB/(?P<export_date>(?:\d{8}|\d{4}-\d{2}-\d{2}))/"
    r"(?P<account_id>[^/]+)/(?P<table_name>[^/]+)/"
    r"(?P<export_type>FULL|INCR(?:[1-9]\d*)?)(?:/|$)"
)
ASSUME_ROLE_TEMPLATE_ALLOWED_FIELDS = {"account_id"}
AWS_ACCESS_DENIED_ERROR_CODES = {
    "AccessDenied",
    "AccessDeniedException",
    "AllAccessDisabled",
    "UnauthorizedOperation",
    "UnrecognizedClientException",
    "InvalidClientTokenId",
    "SignatureDoesNotMatch",
}
AWS_NOT_FOUND_ERROR_CODES = {
    "NoSuchKey",
    "NotFound",
    "ResourceNotFoundException",
    "404",
}
AWS_THROTTLING_ERROR_CODES = {
    "Throttling",
    "ThrottlingException",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
    "RequestLimitExceeded",
    "SlowDown",
}
AWS_VALIDATION_ERROR_CODES = {
    "ValidationException",
    "InvalidParameterValue",
    "InvalidParameterCombination",
    "MissingRequiredParameter",
}
AWS_CONFLICT_ERROR_CODES = {
    "ConflictException",
    "ConditionalCheckFailedException",
    "ResourceInUseException",
}
AWS_TRANSIENT_ERROR_CODES = {
    "InternalServerError",
    "ServiceUnavailable",
    "RequestTimeout",
}
S3_ACCESS_DENIED_ERROR_CODES = {"AccessDenied", "AccessDeniedException", "AllAccessDisabled"}
S3_OBJECT_NOT_FOUND_ERROR_CODES = {"NoSuchKey", "NotFound", "404"}
PITR_ENABLE_POLL_SECONDS = 5
PITR_ENABLE_TIMEOUT_SECONDS = 300
INCREMENTAL_EXPORT_MIN_WINDOW = timedelta(minutes=15)
INCREMENTAL_EXPORT_MAX_WINDOW = timedelta(hours=24)
EXPORT_PENDING_STATUSES = {"STARTED", "IN_PROGRESS"}
EXPORT_TERMINAL_FAILURE_STATUSES = {"FAILED", "CANCELLED"}
CLOUDWATCH_OUTPUT_MAX_BYTES = 240000
AWS_ACCOUNT_ID_PATTERN = re.compile(r"^\d{12}$")
OUTPUT_DYNAMODB_TABLE_POLL_SECONDS = 2
OUTPUT_DYNAMODB_TABLE_TIMEOUT_SECONDS = 60
OUTPUT_DYNAMODB_PARTITION_KEY = "Export ARN"
OUTPUT_DYNAMODB_EXPORT_TIMEZONE = timezone(timedelta(hours=-3))
OUTPUT_DYNAMODB_STATUS_LABELS = {
    "STARTED": "In progress",
    "IN_PROGRESS": "In progress",
    "COMPLETED": "Completed",
    "FAILED": "Failed",
    "CANCELLED": "Cancelled",
    "PENDING": "Pending",
}
OUTPUT_DYNAMODB_EXPORT_TYPE_LABELS = {
    "FULL": "Full export",
    "FULL_EXPORT": "Full export",
    "INCREMENTAL": "Incremental export",
    "INCREMENTAL_EXPORT": "Incremental export",
}
CHECKPOINT_DYNAMODB_PARTITION_KEY = "TableName"
CHECKPOINT_DYNAMODB_SORT_KEY = "RecordType"
CHECKPOINT_DYNAMODB_CURRENT_RECORD = "CURRENT"
CHECKPOINT_DYNAMODB_SNAPSHOT_RECORD_PREFIX = "SNAPSHOT#"
CHECKPOINT_DYNAMODB_PAYLOAD_ATTR = "Payload"
CHECKPOINT_DYNAMODB_REVISION_ATTR = "Revision"
CHECKPOINT_DYNAMODB_STATE_KEY_ATTR = "StateKey"
CHECKPOINT_DYNAMODB_TABLE_ARN_ATTR = "TableArn"
CHECKPOINT_DYNAMODB_LAST_TO_ATTR = "LastTo"
CHECKPOINT_DYNAMODB_LAST_MODE_ATTR = "LastMode"
CHECKPOINT_DYNAMODB_SOURCE_ATTR = "Source"
CHECKPOINT_DYNAMODB_PENDING_EXPORTS_ATTR = "PendingExports"
CHECKPOINT_DYNAMODB_UPDATED_AT_ATTR = "UpdatedAt"
CHECKPOINT_DYNAMODB_EVENT_ID_ATTR = "EventId"
CHECKPOINT_DYNAMODB_OBSERVED_AT_ATTR = "ObservedAt"
CHECKPOINT_DYNAMODB_CLEAR_STATE_ATTR = "ClearState"
CHECKPOINT_DYNAMODB_MAX_RETRIES = 5
CHECKPOINT_DYNAMODB_TABLE_POLL_SECONDS = 2
CHECKPOINT_DYNAMODB_TABLE_TIMEOUT_SECONDS = 60


SnapshotConfig = Dict[str, Any]
_DEFAULT_AWS_SESSION_LOCK = threading.Lock()
_DEFAULT_AWS_SESSION: Any = None
_SESSION_CLIENT_CACHE_LOCK = threading.Lock()
_SESSION_CLIENT_CACHE: weakref.WeakKeyDictionary[Any, Dict[tuple[str, str], Any]] = weakref.WeakKeyDictionary()


def _resolve_optional_text(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _resolve_runtime_region(session_region: Optional[str] = None) -> Optional[str]:
    return _resolve_optional_text(
        session_region,
        os.getenv("AWS_REGION"),
        os.getenv("AWS_DEFAULT_REGION"),
    )


def _resolve_env_first_bool(event_value: Any, env_name: str, default: str) -> bool:
    env_value = os.getenv(env_name)
    if env_value is not None and str(env_value).strip():
        return _env_bool(None, env_value)
    return _env_bool(event_value, default)


def _extract_event_payload(event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        return {}

    payload = dict(event)
    raw_body = payload.get("body")
    if isinstance(raw_body, dict):
        payload.update(raw_body)
    elif isinstance(raw_body, str):
        text = raw_body.strip()
        if text:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                payload.update(parsed)
    return payload


def _get_default_aws_session() -> Any:
    global _DEFAULT_AWS_SESSION
    with _DEFAULT_AWS_SESSION_LOCK:
        if _DEFAULT_AWS_SESSION is None:
            _DEFAULT_AWS_SESSION = boto3.session.Session()
        return _DEFAULT_AWS_SESSION


def _get_session_client(
    session: Any,
    service_name: str,
    *,
    region_name: Optional[str] = None,
) -> Any:
    cache_region = region_name or ""
    cache_key = (service_name, cache_region)

    try:
        with _SESSION_CLIENT_CACHE_LOCK:
            session_cache = _SESSION_CLIENT_CACHE.get(session)
            if session_cache is None:
                session_cache = {}
                _SESSION_CLIENT_CACHE[session] = session_cache
            cached_client = session_cache.get(cache_key)
        if cached_client is not None:
            return cached_client
    except TypeError:
        session_cache = None

    if region_name:
        client = session.client(service_name, region_name=region_name)
    else:
        client = session.client(service_name)

    if session_cache is None:
        return client

    with _SESSION_CLIENT_CACHE_LOCK:
        live_session_cache = _SESSION_CLIENT_CACHE.get(session)
        if live_session_cache is None:
            live_session_cache = {}
            _SESSION_CLIENT_CACHE[session] = live_session_cache
        existing_client = live_session_cache.get(cache_key)
        if existing_client is not None:
            return existing_client
        live_session_cache[cache_key] = client
    return client


def build_snapshot_config(event: Optional[Dict[str, Any]]) -> SnapshotConfig:
    if event is None:
        event = {}
    if not isinstance(event, dict):
        raise ValueError("event deve ser um objeto JSON (dict)")

    payload = _extract_event_payload(event)

    targets = _normalize_list(
        os.getenv("TARGET_TABLE_ARNS")
        or os.getenv("TARGET_TABLES")
        or payload.get("targets")
        or payload.get("target")
        or ""
    )
    targets_csv = (
        _resolve_optional_text(
            os.getenv("TARGETS_CSV", ""),
            payload.get("targets_csv"),
            payload.get("target_csv"),
        )
        or None
    )
    ignore = _normalize_list(
        os.getenv("IGNORE_TARGETS")
        or os.getenv("IGNORE_TABLES")
        or payload.get("ignore")
        or payload.get("ignore_targets")
        or ""
    )
    ignore_csv = (
        _resolve_optional_text(
            os.getenv("IGNORE_CSV", ""),
            payload.get("ignore_csv"),
            payload.get("ignore_targets_csv"),
        )
        or None
    )

    if not targets and not targets_csv:
        raise ValueError(
            "Informe ao menos um target em 'targets', TARGET_TABLE_ARNS ou targets_csv/TARGETS_CSV"
        )

    mode = (
        _resolve_optional_text(
            os.getenv("SNAPSHOT_MODE"),
            event.get("mode"),
            "full",
        )
        or "full"
    ).strip().lower()
    if mode not in {"full", "incremental"}:
        raise ValueError("SNAPSHOT_MODE deve ser 'full' ou 'incremental'")

    try:
        max_workers = int(
            _resolve_optional_text(
                os.getenv("MAX_WORKERS"),
                event.get("max_workers"),
                "4",
            )
            or "4"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("MAX_WORKERS deve ser um inteiro válido") from exc
    if max_workers < 1:
        max_workers = 1

    bucket = _resolve_optional_text(
        os.getenv("SNAPSHOT_BUCKET", ""),
        payload.get("snapshot_bucket"),
        payload.get("snapshotBucket"),
    )
    if not bucket:
        raise ValueError("SNAPSHOT_BUCKET não definido")
    bucket_owner = _resolve_optional_text(
        os.getenv("S3_BUCKET_OWNER", ""),
        payload.get("bucket_owner"),
        payload.get("snapshot_bucket_owner"),
    )
    if bucket_owner and not AWS_ACCOUNT_ID_PATTERN.fullmatch(bucket_owner):
        raise ValueError("S3_BUCKET_OWNER deve ser um account id AWS de 12 dígitos")

    s3_prefix = (
        _resolve_optional_text(
            os.getenv("S3_PREFIX"),
            payload.get("s3_prefix"),
            payload.get("s3Prefix"),
            "dynamodb-snapshots",
        )
        or "dynamodb-snapshots"
    ).strip("/")
    wait_for_completion = _resolve_env_first_bool(
        payload.get("wait_for_completion"),
        "WAIT_FOR_COMPLETION",
        "false",
    )
    event_catch_up = (
        payload.get("catch_up")
        if "catch_up" in payload
        else payload.get("catch-up")
    )
    catch_up = _resolve_env_first_bool(
        event_catch_up,
        "CATCH_UP",
        "false",
    )
    dry_run = _resolve_env_first_bool(
        payload.get("dry_run"),
        "DRY_RUN",
        "false",
    )

    checkpoint_dynamodb_table_arn = _resolve_optional_text(
        os.getenv("CHECKPOINT_DYNAMODB_TABLE_ARN", ""),
        payload.get("checkpoint_dynamodb_table_arn"),
    )
    if not checkpoint_dynamodb_table_arn:
        raise ValueError("CHECKPOINT_DYNAMODB_TABLE_ARN não definido")
    _extract_dynamodb_table_context(
        checkpoint_dynamodb_table_arn,
        field_name="checkpoint_dynamodb_table_arn",
    )

    fallback_enabled = _resolve_env_first_bool(
        payload.get("scan_fallback_enabled"),
        "SCAN_FALLBACK_ENABLED",
        "true",
    )
    fallback_updated_attr = (
        _resolve_optional_text(
            os.getenv("SCAN_UPDATED_ATTR"),
            payload.get("scan_updated_attr"),
            payload.get("scanUpdatedAttr"),
            "_updated_at",
        )
        or "_updated_at"
    ).strip()
    fallback_updated_attr_type = (
        _resolve_optional_text(
            os.getenv("SCAN_UPDATED_ATTR_TYPE"),
            payload.get("scan_updated_attr_type"),
            payload.get("scanUpdatedAttrType"),
            "string",
        )
        or "string"
    ).strip().lower()

    if fallback_updated_attr_type not in {"string", "number"}:
        raise ValueError("SCAN_UPDATED_ATTR_TYPE deve ser 'string' ou 'number'")

    try:
        fallback_partition_size = int(
            _resolve_optional_text(
                os.getenv("SCAN_PARTITION_SIZE"),
                payload.get("scan_partition_size"),
                payload.get("scanPartitionSize"),
                "10000",
            )
            or "10000"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("SCAN_PARTITION_SIZE deve ser um inteiro válido") from exc
    if fallback_partition_size < 1:
        fallback_partition_size = 1000

    fallback_compress = _resolve_env_first_bool(
        payload.get("scan_compress"),
        "SCAN_COMPRESS",
        "true",
    )
    permission_precheck_enabled = _resolve_env_first_bool(
        payload.get("permission_precheck"),
        "PERMISSION_PRECHECK",
        "true",
    )
    output_cloudwatch_enabled = _resolve_env_first_bool(
        payload.get("output_cloudwatch_enabled"),
        "OUTPUT_CLOUDWATCH_ENABLED",
        "false",
    )
    output_dynamodb_enabled = _resolve_env_first_bool(
        payload.get("output_dynamodb_enabled"),
        "OUTPUT_DYNAMODB_ENABLED",
        "false",
    )
    output_dynamodb_table = _resolve_optional_text(
        os.getenv("OUTPUT_DYNAMODB_TABLE", ""),
        payload.get("output_dynamodb_table"),
    )
    if output_dynamodb_enabled and not output_dynamodb_table:
        raise ValueError(
            "OUTPUT_DYNAMODB_TABLE deve ser informado quando OUTPUT_DYNAMODB_ENABLED=true"
        )
    output_dynamodb_region = _resolve_optional_text(
        os.getenv("OUTPUT_DYNAMODB_REGION", ""),
        payload.get("output_dynamodb_region"),
        payload.get("outputDynamodbRegion"),
        _resolve_runtime_region(),
    )

    run_time = datetime.now(timezone.utc)
    run_id = run_time.strftime("%Y%m%dT%H%M%SZ")

    assume_role_from_event = _resolve_optional_text(
        payload.get("assume_role"),
        payload.get("assume_role_arn"),
    )
    assume_role_from_env = _resolve_optional_text(os.getenv("ASSUME_ROLE", ""))
    assume_role_arn = _resolve_optional_text(assume_role_from_env, assume_role_from_event)
    assume_role_external_id = _resolve_optional_text(
        os.getenv("ASSUME_ROLE_EXTERNAL_ID", ""),
        payload.get("assume_role_external_id"),
        payload.get("assumeRoleExternalId"),
    )
    assume_role_session_name = _sanitize_role_session_name(
        _resolve_optional_text(
            os.getenv("ASSUME_ROLE_SESSION_NAME", ""),
            payload.get("assume_role_session_name"),
            payload.get("assumeRoleSessionName"),
            f"dynamodb-snapshot-{run_id}",
        ) or f"dynamodb-snapshot-{run_id}",
        run_id,
    )
    try:
        assume_role_duration_seconds = int(
            _resolve_optional_text(
                os.getenv("ASSUME_ROLE_DURATION_SECONDS"),
                payload.get("assume_role_duration_seconds"),
                payload.get("assumeRoleDurationSeconds"),
                "3600",
            )
            or "3600"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("ASSUME_ROLE_DURATION_SECONDS deve ser um inteiro válido") from exc
    if not (900 <= assume_role_duration_seconds <= 43200):
        raise ValueError("ASSUME_ROLE_DURATION_SECONDS deve estar entre 900 e 43200")

    _log_event(
        "config.assume_role.resolved",
        assume_role_enabled=bool(assume_role_arn),
        assume_role_template_enabled=_has_role_template_fields(assume_role_arn),
        assume_role_source=(
            "environment"
            if assume_role_from_env
            else "event"
            if assume_role_from_event
            else "unset"
        ),
        has_external_id=bool(assume_role_external_id),
        session_name=assume_role_session_name,
        duration_seconds=assume_role_duration_seconds,
    )

    return {
        "bucket": bucket,
        "bucket_owner": bucket_owner,
        "targets": targets,
        "targets_csv": targets_csv,
        "ignore": ignore,
        "ignore_csv": ignore_csv,
        "s3_prefix": s3_prefix,
        "mode": mode,
        "wait_for_completion": wait_for_completion,
        "catch_up": catch_up,
        "dry_run": dry_run,
        "max_workers": max_workers,
        "checkpoint_dynamodb_table_arn": checkpoint_dynamodb_table_arn,
        "run_id": run_id,
        "run_time": run_time,
        "assume_role": assume_role_arn,
        "assume_role_arn": assume_role_arn,
        "assume_role_external_id": assume_role_external_id,
        "assume_role_session_name": assume_role_session_name,
        "assume_role_duration_seconds": assume_role_duration_seconds,
        "fallback_enabled": fallback_enabled,
        "fallback_updated_attr": fallback_updated_attr,
        "fallback_updated_attr_type": fallback_updated_attr_type,
        "fallback_partition_size": fallback_partition_size,
        "fallback_compress": fallback_compress,
        "permission_precheck_enabled": permission_precheck_enabled,
        "output_cloudwatch_enabled": output_cloudwatch_enabled,
        "output_dynamodb_enabled": output_dynamodb_enabled,
        "output_dynamodb_table": output_dynamodb_table,
        "output_dynamodb_region": output_dynamodb_region,
    }


def _normalize_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except Exception as exc:
                raise ValueError("targets/ignore não está em formato JSON válido") from exc
            if not isinstance(parsed, list):
                raise ValueError("targets/ignore deve ser uma lista no formato JSON")
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [
            part.strip()
            for part in re.split(r"[,;\n\r]+", text)
            if part.strip()
        ]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    raise TypeError("targets/ignore must be list/tuple/set/string")


def _dedupe_values(values: List[str], *, case_insensitive: bool = False) -> List[str]:
    seen = set()
    result = []
    for value in values:
        cleaned = str(value).strip()
        if not cleaned:
            continue
        key = cleaned.lower() if case_insensitive else cleaned
        if key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
    return result


def _detect_csv_delimiter(csv_text: str) -> str:
    lines = [line for line in csv_text.splitlines() if line.strip()]
    if not lines:
        return ","
    first_line = lines[0]
    candidates = [",", ";", "\t", "|"]
    best = max(candidates, key=lambda delim: first_line.count(delim))
    return best if first_line.count(best) > 0 else ","


def _parse_csv_rows(csv_text: str) -> List[List[str]]:
    delimiter = _detect_csv_delimiter(csv_text)
    rows = []
    reader = csv.reader(io.StringIO(csv_text), delimiter=delimiter)
    for row in reader:
        normalized = [str(cell).strip() for cell in row]
        if any(normalized):
            rows.append(normalized)
    return rows


def _is_header_row(row: List[str], aliases: set[str]) -> bool:
    normalized = [cell.strip().lower() for cell in row if cell and cell.strip()]
    if not normalized:
        return False
    if any(cell.startswith("arn:") for cell in normalized):
        return False
    return any(cell in aliases for cell in normalized)


def _find_column_index(row: List[str], aliases: set[str]) -> Optional[int]:
    for index, cell in enumerate(row):
        if cell.strip().lower() in aliases:
            return index
    return None


def _first_non_empty_cell(row: List[str]) -> str:
    for value in row:
        candidate = value.strip()
        if candidate and not candidate.startswith("#"):
            return candidate
    return ""


def _parse_targets_csv(csv_text: str) -> List[str]:
    rows = _parse_csv_rows(csv_text)
    if not rows:
        return []

    aliases = {
        "arn",
        "target",
        "target_arn",
        "resource",
        "resource_arn",
        "table",
        "table_arn",
        "table_name",
    }
    header = rows[0]
    has_header = _is_header_row(header, aliases)
    target_index = _find_column_index(header, aliases) if has_header else None
    data_rows = rows[1:] if has_header else rows

    refs = []
    for row in data_rows:
        value = ""
        if target_index is not None and target_index < len(row):
            value = row[target_index].strip()
        if not value:
            value = _first_non_empty_cell(row)
        if value:
            refs.append(value)
    return _dedupe_values(refs)


def _parse_ignore_csv(csv_text: str) -> List[str]:
    rows = _parse_csv_rows(csv_text)
    if not rows:
        return []

    arn_aliases = {"arn", "target_arn", "resource_arn", "table_arn"}
    table_aliases = {"table", "table_name", "target_table"}
    aliases = arn_aliases.union(table_aliases)

    header = rows[0]
    has_header = _is_header_row(header, aliases)
    arn_index = _find_column_index(header, arn_aliases) if has_header else None
    table_index = _find_column_index(header, table_aliases) if has_header else None
    data_rows = rows[1:] if has_header else rows

    ignores = []
    for row in data_rows:
        values = []
        if arn_index is not None and arn_index < len(row):
            values.append(row[arn_index].strip())
        if table_index is not None and table_index < len(row):
            values.append(row[table_index].strip())
        if not values:
            values.extend([cell.strip() for cell in row[:2]])

        for value in values:
            if value and not value.startswith("#"):
                ignores.append(value)

    return _dedupe_values(ignores, case_insensitive=True)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    text = uri.strip()
    if not text.startswith("s3://"):
        raise ValueError(f"URI inválida para S3: {uri}")
    remainder = text[5:]
    bucket, sep, key = remainder.partition("/")
    if not bucket or not sep or not key:
        raise ValueError(f"URI S3 deve estar no formato s3://bucket/chave. Recebido: {uri}")
    return bucket, key


def _extract_local_file_path(source: str) -> Optional[str]:
    text = source.strip()
    if text.startswith("file://"):
        return text[7:]
    if text.startswith("/") or text.startswith("./") or text.startswith("../"):
        return text
    return None


def _client_error_code(exc: ClientError) -> str:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    error = response.get("Error")
    if not isinstance(error, dict):
        return ""
    return str(error.get("Code", "")).strip()


def _client_error_message(exc: ClientError) -> str:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    error = response.get("Error")
    if not isinstance(error, dict):
        return str(exc)
    message = str(error.get("Message", "")).strip()
    return message or str(exc)


def _client_error_metadata(exc: ClientError) -> Dict[str, str]:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    metadata = response.get("ResponseMetadata")
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "code": _client_error_code(exc),
        "message": _client_error_message(exc),
        "request_id": str(metadata.get("RequestId", "")).strip(),
        "http_status": str(metadata.get("HTTPStatusCode", "")).strip(),
    }


def _classify_aws_error(code: str) -> str:
    if code in AWS_ACCESS_DENIED_ERROR_CODES:
        return "access_denied"
    if code in AWS_NOT_FOUND_ERROR_CODES:
        return "not_found"
    if code in AWS_THROTTLING_ERROR_CODES:
        return "throttling"
    if code in AWS_VALIDATION_ERROR_CODES:
        return "validation"
    if code in AWS_CONFLICT_ERROR_CODES:
        return "conflict"
    if code in AWS_TRANSIENT_ERROR_CODES:
        return "transient"
    return "unknown"


def _build_aws_error_detail(exc: ClientError) -> str:
    details = _client_error_metadata(exc)
    error_type = _classify_aws_error(details.get("code", ""))
    hints = {
        "access_denied": "valide IAM/role e políticas de recurso",
        "not_found": "valide nome/ARN da tabela, bucket e key",
        "throttling": "reduza paralelismo e tente novamente",
        "validation": "revise parâmetros enviados para a API",
        "conflict": "valide estado atual do recurso e idempotência",
        "transient": "erro transitório, tente novamente",
    }
    meta_parts = [
        f"code={details.get('code') or 'Unknown'}",
        f"type={error_type}",
    ]
    if details.get("http_status"):
        meta_parts.append(f"http_status={details.get('http_status')}")
    if details.get("request_id"):
        meta_parts.append(f"request_id={details.get('request_id')}")
    message = details.get("message") or "erro sem mensagem da AWS"
    suggestion = hints.get(error_type)
    if suggestion:
        return f"[{' '.join(meta_parts)}] {message}. Ação sugerida: {suggestion}."
    return f"[{' '.join(meta_parts)}] {message}"


def _build_aws_runtime_error(action: str, exc: ClientError, *, resource: Optional[str] = None) -> RuntimeError:
    resource_suffix = f" ({resource})" if resource else ""
    return RuntimeError(f"{action} falhou{resource_suffix}: {_build_aws_error_detail(exc)}")


def _find_first_exception(exc: BaseException, *expected: type[BaseException]) -> Optional[BaseException]:
    current = exc
    seen_ids = set()
    while isinstance(current, BaseException):
        if id(current) in seen_ids:
            break
        if isinstance(current, expected):
            return current
        seen_ids.add(id(current))
        current = current.__cause__ or current.__context__
    return None


def _normalize_exception(exc: BaseException) -> Dict[str, Any]:
    client_error = _find_first_exception(exc, ClientError)
    if client_error:
        details = _client_error_metadata(client_error)
        code = details.get("code", "Unknown")
        aws_type = _classify_aws_error(code)
        if aws_type == "access_denied":
            error_type = "aws_access_denied"
        elif aws_type == "not_found":
            error_type = "aws_not_found"
        elif aws_type == "throttling":
            error_type = "aws_throttling"
        elif aws_type == "validation":
            error_type = "aws_validation"
        elif aws_type == "conflict":
            error_type = "aws_conflict"
        elif aws_type == "transient":
            error_type = "aws_transient"
        else:
            error_type = "aws_unknown"
        return {
            "error_type": error_type,
            "error_category": aws_type,
            "error_code": code,
            "error_message": _build_aws_error_detail(client_error),
            "http_status": details.get("http_status"),
            "request_id": details.get("request_id"),
            "retryable": aws_type in {"throttling", "transient"},
        }

    if _find_first_exception(exc, NoCredentialsError, PartialCredentialsError):
        return {
            "error_type": "aws_credentials",
            "error_category": "credentials",
            "error_code": "NoCredentialsError",
            "error_message": "Credenciais AWS não disponíveis ou inválidas.",
            "retryable": False,
        }
    if _find_first_exception(exc, NoRegionError):
        return {
            "error_type": "aws_region",
            "error_category": "region",
            "error_code": "NoRegionError",
            "error_message": "Região AWS não configurada.",
            "retryable": False,
        }
    if _find_first_exception(exc, ConnectTimeoutError, ReadTimeoutError, EndpointConnectionError, ProxyConnectionError, BotoCoreError):
        return {
            "error_type": "aws_network",
            "error_category": "network",
            "error_code": "NetworkError",
            "error_message": "Falha de rede/endpoint ao chamar AWS.",
            "retryable": True,
        }
    if _find_first_exception(exc, json.JSONDecodeError):
        return {
            "error_type": "input_json",
            "error_category": "json",
            "error_code": "InvalidJSON",
            "error_message": "JSON inválido no payload de entrada.",
            "retryable": False,
        }
    if _find_first_exception(exc, FileNotFoundError):
        return {
            "error_type": "io_not_found",
            "error_category": "filesystem",
            "error_code": "FileNotFound",
            "error_message": "Arquivo não encontrado.",
            "retryable": False,
        }
    if _find_first_exception(exc, PermissionError):
        return {
            "error_type": "permission",
            "error_category": "filesystem",
            "error_code": "PermissionError",
            "error_message": "Sem permissão para acessar recurso.",
            "retryable": False,
        }
    if _find_first_exception(exc, OSError):
        return {
            "error_type": "io",
            "error_category": "filesystem",
            "error_code": "OSError",
            "error_message": "Falha de I/O não esperada.",
            "retryable": False,
        }
    if _find_first_exception(exc, ValueError):
        return {
            "error_type": "config",
            "error_category": "validation",
            "error_code": "ValueError",
            "error_message": str(exc),
            "retryable": False,
        }
    if _find_first_exception(exc, TimeoutError):
        return {
            "error_type": "timeout",
            "error_category": "execution",
            "error_code": "TimeoutError",
            "error_message": "Timeout durante execução.",
            "retryable": True,
        }

    return {
        "error_type": "runtime",
        "error_category": "runtime",
        "error_code": "RuntimeError",
        "error_message": str(exc),
        "retryable": False,
    }


def _message_contains(text: str, *patterns: str) -> bool:
    normalized = str(text).strip().lower()
    return any(pattern in normalized for pattern in patterns)


def _build_default_error_guidance(info: Dict[str, Any]) -> Dict[str, str]:
    error_type = str(info.get("error_type", "runtime"))
    catalog = {
        "config": {
            "user_message": "A configuração informada para o snapshot está inválida.",
            "resolution": "Revise as variáveis de ambiente e o payload do evento antes de executar novamente.",
        },
        "aws_access_denied": {
            "user_message": "A execução não tem permissão suficiente na AWS para concluir a operação.",
            "resolution": "Valide a role ou usuário utilizado e conceda as ações necessárias em DynamoDB, S3 e STS.",
        },
        "aws_not_found": {
            "user_message": "Um recurso AWS necessário não foi encontrado.",
            "resolution": "Confirme ARN ou nome da tabela, bucket, key e região antes de executar novamente.",
        },
        "aws_throttling": {
            "user_message": "A AWS limitou temporariamente a taxa de requisições desta execução.",
            "resolution": "Reduza `MAX_WORKERS`, aguarde alguns instantes e tente novamente.",
        },
        "aws_validation": {
            "user_message": "A AWS rejeitou os parâmetros enviados para a operação.",
            "resolution": "Revise o modo, os ARNs, o intervalo incremental e demais parâmetros usados nesta execução.",
        },
        "aws_conflict": {
            "user_message": "A operação entrou em conflito com o estado atual do recurso.",
            "resolution": "Verifique se já existe export em andamento ou se a mesma solicitação foi enviada recentemente.",
        },
        "aws_transient": {
            "user_message": "A AWS respondeu com uma falha transitória.",
            "resolution": "Tente novamente em alguns instantes. Se persistir, reduza paralelismo e revise a saúde do serviço na região.",
        },
        "aws_unknown": {
            "user_message": "A AWS retornou um erro não classificado automaticamente.",
            "resolution": "Consulte o código da AWS, o request ID e a mensagem detalhada para ajustar a configuração ou a permissão necessária.",
        },
        "aws_credentials": {
            "user_message": "As credenciais AWS não estão disponíveis ou não são válidas para esta execução.",
            "resolution": "Configure a cadeia de credenciais padrão da AWS ou a role da Lambda corretamente e valide se a credencial consegue acessar DynamoDB, S3 e STS.",
        },
        "aws_region": {
            "user_message": "A região AWS não foi configurada para esta execução.",
            "resolution": "Defina `AWS_REGION` ou configure a região padrão da sessão antes de executar novamente.",
        },
        "aws_network": {
            "user_message": "A execução não conseguiu se comunicar com a AWS.",
            "resolution": "Verifique conectividade de rede, endpoint, proxy e políticas de saída antes de tentar novamente.",
        },
        "input_json": {
            "user_message": "O JSON informado no evento está inválido.",
            "resolution": "Corrija a sintaxe do JSON e garanta que o payload final seja um objeto.",
        },
        "io_not_found": {
            "user_message": "Um arquivo ou objeto necessário para a execução não foi encontrado.",
            "resolution": "Revise o caminho local ou a URI S3 informada antes de executar novamente.",
        },
        "permission": {
            "user_message": "A execução encontrou um recurso sem permissão de acesso.",
            "resolution": "Ajuste as permissões de leitura ou escrita do recurso envolvido e execute novamente.",
        },
        "io": {
            "user_message": "Ocorreu uma falha de leitura ou escrita durante a execução.",
            "resolution": "Revise o recurso acessado, permissões locais e disponibilidade do destino antes de tentar novamente.",
        },
        "timeout": {
            "user_message": "A operação excedeu o tempo de espera configurado.",
            "resolution": "Revise o volume processado e o timeout da execução. Se necessário, execute sem bloqueio ou divida a carga.",
        },
        "runtime": {
            "user_message": "A execução falhou por um erro não tratado automaticamente.",
            "resolution": "Revise a mensagem detalhada do erro para identificar a etapa que falhou e ajuste a configuração correspondente.",
        },
    }
    return dict(catalog.get(error_type, catalog["runtime"]))


def _build_error_guidance(error: BaseException, info: Dict[str, Any]) -> Dict[str, str]:
    raw_message = str(error).strip()
    guidance = _build_default_error_guidance(info)

    if _message_contains(raw_message, "snapshot_bucket não definido"):
        return {
            "user_message": "O bucket de destino dos snapshots não foi configurado.",
            "resolution": "Defina `SNAPSHOT_BUCKET` no ambiente da Lambda.",
        }
    if _message_contains(raw_message, "informe ao menos um target"):
        return {
            "user_message": "Nenhuma tabela alvo foi informada para a execução.",
            "resolution": "Preencha `targets`, `TARGET_TABLE_ARNS`, `TARGET_TABLES` ou `TARGETS_CSV` com pelo menos uma tabela válida.",
        }
    if _message_contains(raw_message, "snapshot_mode deve ser"):
        return {
            "user_message": "O modo do snapshot informado é inválido.",
            "resolution": "Use `full` ou `incremental` em `SNAPSHOT_MODE` ou no campo `mode` do evento.",
        }
    if _message_contains(raw_message, "max_workers deve ser"):
        return {
            "user_message": "O paralelismo configurado para a execução é inválido.",
            "resolution": "Informe um inteiro positivo em `MAX_WORKERS` ou `max_workers`.",
        }
    if _message_contains(raw_message, "scan_partition_size deve ser"):
        return {
            "user_message": "O tamanho de partição do fallback incremental está inválido.",
            "resolution": "Informe um inteiro positivo em `SCAN_PARTITION_SIZE` ou `scan_partition_size`.",
        }
    if _message_contains(raw_message, "bucket_owner deve ser"):
        return {
            "user_message": "O owner do bucket de export está inválido.",
            "resolution": "Informe um account id AWS de 12 dígitos em `S3_BUCKET_OWNER`, `bucket_owner` ou `snapshot_bucket_owner`.",
        }
    if _message_contains(raw_message, "checkpoint_dynamodb_table_arn"):
        return {
            "user_message": "A tabela DynamoDB de checkpoint não foi configurada corretamente.",
            "resolution": "Informe um ARN válido de tabela DynamoDB em `CHECKPOINT_DYNAMODB_TABLE_ARN` ou `checkpoint_dynamodb_table_arn`.",
        }
    if _message_contains(raw_message, "output_dynamodb_table deve ser informado"):
        return {
            "user_message": "O destino DynamoDB do output foi habilitado sem tabela configurada.",
            "resolution": "Defina `OUTPUT_DYNAMODB_TABLE` ou `output_dynamodb_table` quando `OUTPUT_DYNAMODB_ENABLED` ou `output_dynamodb_enabled` estiver ativo.",
        }
    if _message_contains(raw_message, "assume_role_duration_seconds"):
        return {
            "user_message": "A duração configurada para a sessão STS está inválida.",
            "resolution": "Use um valor entre 900 e 43200 em `ASSUME_ROLE_DURATION_SECONDS` ou `assume_role_duration_seconds`.",
        }
    if _message_contains(raw_message, "payload de evento inválido", "deve ser um objeto json"):
        return {
            "user_message": "O evento informado não está em um JSON válido para o contrato da Lambda.",
            "resolution": "Corrija o JSON do evento e garanta que a raiz do payload seja um objeto.",
        }
    if _message_contains(raw_message, "arquivo não encontrado", "objeto csv não encontrado"):
        return {
            "user_message": "A origem de arquivo informada para a execução não foi encontrada.",
            "resolution": "Confirme o caminho local ou a URI S3 do CSV e execute novamente.",
        }
    if _message_contains(raw_message, "sem permissão para getobject no csv"):
        return {
            "user_message": "A execução não tem permissão para ler o CSV informado em S3.",
            "resolution": "Conceda `s3:GetObject` para o objeto CSV ou informe outra origem acessível.",
        }
    if _message_contains(raw_message, "falha ao decodificar csv"):
        return {
            "user_message": "O CSV informado não está codificado em UTF-8 válido.",
            "resolution": "Converta o arquivo para UTF-8 e execute novamente.",
        }
    if _message_contains(raw_message, "conta dona da tabela", "exporttabletopointintime exige sessão da conta dona da tabela"):
        return {
            "user_message": "A sessão AWS atual não pertence à mesma conta da tabela alvo.",
            "resolution": "Execute na conta dona da tabela ou configure `ASSUME_ROLE` para assumir uma role nessa conta antes do export.",
        }
    if _message_contains(
        raw_message,
        "describecontinuousbackups",
        "updatecontinuousbackups",
        "point-in-time recovery",
        "point in time recovery",
    ):
        return {
            "user_message": "Não foi possível validar ou habilitar o Point-in-Time Recovery da tabela.",
            "resolution": "Conceda `dynamodb:DescribeContinuousBackups` e `dynamodb:UpdateContinuousBackups`, valide se PITR é suportado na tabela e execute novamente.",
        }
    if _message_contains(raw_message, "não foi possível determinar região para assumir role"):
        return {
            "user_message": "Não foi possível descobrir a região necessária para assumir a role configurada.",
            "resolution": "Informe os targets por ARN completo ou configure `AWS_REGION` antes de executar novamente.",
        }
    if _message_contains(raw_message, "baseline incremental inválido"):
        return {
            "user_message": "O checkpoint encontrado para o incremental está inválido.",
            "resolution": "Corrija ou remova o checkpoint da tabela, ou execute um snapshot `full` para recriar a baseline.",
        }
    if _message_contains(raw_message, "timeout aguardando export"):
        return {
            "user_message": "O export demorou mais do que o limite de espera configurado nesta execução.",
            "resolution": "Consulte o `export_arn` no DynamoDB. Se não precisar bloquear a execução, use `WAIT_FOR_COMPLETION=false`; se precisar aguardar, aumente o timeout da execução.",
        }
    if _message_contains(raw_message, "export failed", "export cancelled"):
        return {
            "user_message": "O DynamoDB não concluiu o export solicitado.",
            "resolution": "Leia o `FailureMessage`, valide PITR, permissões e se a sessão pertence à conta dona da tabela antes de executar novamente.",
        }
    if _message_contains(raw_message, "cliente dynamodb ausente", "cliente s3 ausente"):
        return {
            "user_message": "A execução não conseguiu montar os clientes AWS necessários para processar a tabela.",
            "resolution": "Revise a criação da sessão AWS, a resolução de conta ou o fluxo de assume role antes de executar novamente.",
        }

    return guidance


def _build_error_response_fields(error: BaseException, *, info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    error_info = info or _normalize_exception(error)
    guidance = _build_error_guidance(error, error_info)
    payload = {
        "error": str(error),
        "error_detail": error_info.get("error_message") or str(error),
        "user_message": guidance.get("user_message"),
        "resolution": guidance.get("resolution"),
        "retryable": error_info.get("retryable", False),
    }
    if error_info.get("http_status"):
        payload["http_status"] = error_info["http_status"]
    if error_info.get("request_id"):
        payload["request_id"] = error_info["request_id"]
    return payload


def _build_table_error_result(
    table_name: str,
    table_arn: str,
    mode: str,
    error: BaseException,
    *,
    dry_run: bool,
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    info = _normalize_exception(error)
    error_fields = _build_error_response_fields(error, info=info)
    payload: Dict[str, Any] = {
        "table_name": table_name,
        "table_arn": table_arn,
        "mode": mode,
        "status": "FAILED",
        "error_type": info.get("error_type", "runtime"),
        "error_category": info.get("error_category"),
        "error_code": info.get("error_code"),
        "dry_run": dry_run,
        **error_fields,
    }
    if overrides:
        payload.update(overrides)
    return payload


def _is_s3_access_denied_error(code: str) -> bool:
    return code in S3_ACCESS_DENIED_ERROR_CODES


def _is_s3_missing_object_error(code: str) -> bool:
    return code in S3_OBJECT_NOT_FOUND_ERROR_CODES


def _env_bool(event_value: Any, env_value: str) -> bool:
    if event_value is not None:
        if isinstance(event_value, bool):
            return event_value
        env_value = str(event_value)
    return str(env_value).strip().lower() in {"1", "true", "yes", "on"}


def _has_role_template_fields(role_arn: Optional[str]) -> bool:
    return bool(role_arn and ROLE_TEMPLATE_PATTERN.search(role_arn))


def _parse_arn(arn: str, *, field_name: str = "arn") -> Dict[str, str]:
    value = _safe_str_field(arn, field_name=field_name)
    parts = value.split(":", 5)
    if len(parts) != 6 or parts[0] != "arn":
        raise ValueError(f"{field_name} inválido: {arn}")
    return {
        "partition": parts[1],
        "service": parts[2],
        "region": parts[3],
        "account_id": parts[4],
        "resource": parts[5],
    }


def _extract_dynamodb_table_context(table_arn: str, *, field_name: str) -> Dict[str, str]:
    parsed = _parse_arn(table_arn, field_name=field_name)
    if parsed.get("service") != "dynamodb":
        raise ValueError(f"ARN não é de tabela DynamoDB: {table_arn}")
    resource = parsed.get("resource", "")
    if not resource.startswith("table/"):
        raise ValueError(f"ARN de tabela DynamoDB inválido: {table_arn}")
    suffix = resource.split("table/", 1)[1]
    table_name = suffix.split("/", 1)[0]
    if not table_name:
        raise ValueError(f"Nome de tabela ausente no ARN: {table_arn}")
    return {
        "partition": parsed.get("partition", ""),
        "region": parsed.get("region", ""),
        "account_id": parsed.get("account_id", ""),
        "table_name": table_name,
        "table_arn": table_arn,
    }


def _extract_table_arn_context(table_arn: str) -> Dict[str, str]:
    return _extract_dynamodb_table_context(table_arn, field_name="table_arn")


def _resolve_export_job_id(export_arn: str) -> str:
    normalized = _safe_str_field(export_arn, field_name="export_arn", required=False)
    if not normalized:
        return ""
    try:
        parsed = _parse_arn(normalized, field_name="export_arn")
    except ValueError:
        return ""
    if parsed.get("service") != "dynamodb":
        return ""
    resource = _safe_str_field(parsed.get("resource"), field_name="export_arn.resource", required=False)
    marker = "/export/"
    if marker not in resource:
        return ""
    return resource.rsplit(marker, 1)[1].strip("/")


def _build_export_fields(export_arn: str, *, field_name: str) -> Dict[str, str]:
    normalized = _safe_str_field(export_arn, field_name=field_name)
    export_job_id = _resolve_export_job_id(normalized)
    if not export_job_id:
        return {field_name: normalized}
    return {field_name: normalized, "export_job_id": export_job_id}


def _render_role_arn_template(
    role_arn: str,
    table_arn: str,
    *,
    field_name: str,
    allowed_fields: Optional[set[str]] = None,
) -> str:
    template = _safe_str_field(role_arn, field_name=field_name)
    template_fields = ROLE_TEMPLATE_PATTERN.findall(template)
    if not template_fields:
        return template

    valid_fields = allowed_fields if allowed_fields is not None else ROLE_TEMPLATE_ALLOWED_FIELDS
    unknown_fields = [name for name in template_fields if name not in valid_fields]
    if unknown_fields:
        allowed = ", ".join(sorted(valid_fields))
        raise ValueError(
            f"{field_name} contém placeholders inválidos ({', '.join(sorted(set(unknown_fields)))})"
            f". Permitidos: {allowed}"
        )

    context = _extract_table_arn_context(table_arn)
    rendered = template.format(**context)
    return _safe_str_field(rendered, field_name=f"{field_name} renderizado")


def _extract_table_name(ref: str) -> str:
    ref = ref.strip()
    if ref.startswith("arn:"):
        try:
            context = _extract_table_arn_context(ref)
            return context.get("table_name", ref)
        except ValueError:
            return ref
    return ref


def _dt_to_iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_run_id(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def snapshot_manager_resolve_incremental_window(
    export_from: datetime,
    requested_export_to: datetime,
) -> Dict[str, Any]:
    normalized_export_from = export_from.astimezone(timezone.utc)
    normalized_requested_export_to = requested_export_to.astimezone(timezone.utc)
    effective_export_to = min(
        normalized_requested_export_to,
        normalized_export_from + INCREMENTAL_EXPORT_MAX_WINDOW,
    )
    effective_window = effective_export_to - normalized_export_from
    requested_window = normalized_requested_export_to - normalized_export_from
    return {
        "export_from": normalized_export_from,
        "requested_export_to": normalized_requested_export_to,
        "export_to": effective_export_to,
        "requested_window_seconds": int(requested_window.total_seconds()),
        "effective_window_seconds": int(effective_window.total_seconds()),
        "window_is_invalid": normalized_requested_export_to <= normalized_export_from,
        "window_too_small": effective_window < INCREMENTAL_EXPORT_MIN_WINDOW,
        "window_truncated": effective_export_to < normalized_requested_export_to,
    }


def _extract_full_export_run_id_from_key(key: str) -> Optional[str]:
    normalized = _safe_str_field(key, field_name="s3_key", required=False)
    if not normalized:
        return None
    match = FULL_EXPORT_RUN_ID_PATTERN.search(normalized)
    if not match:
        return None
    return match.group(1)


def _is_full_export_completion_key(key: str) -> bool:
    normalized = _safe_str_field(key, field_name="s3_key", required=False)
    if not normalized:
        return False
    return normalized.endswith(FULL_EXPORT_COMPLETION_SUFFIXES)


def _parse_new_layout_export_key(key: str) -> Optional[Dict[str, str]]:
    normalized = _safe_str_field(key, field_name="s3_key", required=False)
    if not normalized:
        return None
    match = EXPORT_LAYOUT_KEY_PATTERN.match(normalized)
    if not match:
        return None
    return {
        "export_date": _safe_str_field(match.group("export_date"), field_name="export_date"),
        "account_id": _safe_str_field(match.group("account_id"), field_name="account_id"),
        "table_name": _safe_str_field(match.group("table_name"), field_name="table_name"),
        "export_type": _safe_str_field(match.group("export_type"), field_name="export_type"),
    }


def _is_new_layout_full_completion_key_for_table(key: str, *, table_name: str, account_id: str) -> bool:
    parsed = _parse_new_layout_export_key(key)
    if not parsed:
        return False
    if parsed.get("export_type") != "FULL":
        return False
    if parsed.get("table_name") != _safe_str_field(table_name, field_name="table_name"):
        return False
    target_account_id = _safe_str_field(account_id, field_name="account_id", required=False)
    if target_account_id and parsed.get("account_id") != target_account_id:
        return False
    return True


def _extract_export_prefix_from_key(key: str) -> str:
    normalized = _safe_str_field(key, field_name="s3_key", required=False)
    if not normalized:
        return ""
    marker = "/AWSDynamoDB/"
    if marker in normalized:
        return normalized.split(marker, 1)[0]
    if "/" not in normalized:
        return normalized
    return normalized.rsplit("/", 1)[0]


def _resolve_full_export_reference_from_item(item: Dict[str, Any], key: str) -> Optional[Dict[str, str]]:
    run_id = _extract_full_export_run_id_from_key(key)
    if run_id:
        return {
            "order_key": run_id,
            "checkpoint_from": _dt_to_iso(_parse_run_id(run_id)),
            "full_run_id": run_id,
        }

    raw_last_modified = item.get("LastModified")
    last_modified: Optional[datetime] = None
    if isinstance(raw_last_modified, datetime):
        last_modified = raw_last_modified
    elif isinstance(raw_last_modified, str):
        try:
            last_modified = _parse_iso(raw_last_modified)
        except ValueError:
            last_modified = None

    if not isinstance(last_modified, datetime):
        return None
    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=timezone.utc)
    else:
        last_modified = last_modified.astimezone(timezone.utc)

    synthetic_run_id = last_modified.strftime("%Y%m%dT%H%M%SZ")
    return {
        "order_key": synthetic_run_id,
        "checkpoint_from": _dt_to_iso(last_modified),
        "full_run_id": synthetic_run_id,
    }


def _parse_incremental_export_index(export_type: str) -> int:
    normalized = _safe_str_field(export_type, field_name="export_type", required=False).upper()
    if not normalized.startswith("INCR"):
        return 0
    suffix = normalized[4:]
    if not suffix:
        return 1
    if not suffix.isdigit():
        return 0
    value = int(suffix)
    return value if value > 0 else 0


def _sanitize_role_session_name(raw: str, run_id: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+=,.@-_")
    source = raw.strip() or f"dynamodb-snapshot-{run_id}"
    cleaned = "".join(char if char in allowed else "-" for char in source).strip("-")
    if not cleaned:
        cleaned = f"dynamodb-snapshot-{run_id}"
    return cleaned[:64]


def _log_event(action: str, *, level: int = logging.INFO, **fields: Any) -> None:
    if not logger.isEnabledFor(level):
        return
    payload = {"action": action, **fields}
    logger.log(level, "%s", json.dumps(_safe_json(payload), default=str, ensure_ascii=False))


def _should_emit_output_to_cloudwatch(config: Optional[SnapshotConfig] = None) -> bool:
    if isinstance(config, dict) and "output_cloudwatch_enabled" in config:
        return bool(config.get("output_cloudwatch_enabled"))
    return _env_bool(None, os.getenv("OUTPUT_CLOUDWATCH_ENABLED", "false"))


def _resolve_output_snapshot_bucket(
    payload: Any,
    config: Optional[SnapshotConfig] = None,
) -> Optional[str]:
    if isinstance(payload, dict):
        payload_bucket = _resolve_optional_text(payload.get("snapshot_bucket"))
        if payload_bucket:
            return payload_bucket
    if isinstance(config, dict):
        return _resolve_optional_text(config.get("bucket"))
    return None


def snapshot_manager_build_bucket_name(base_bucket: str, region: Optional[str]) -> str:
    resolved_bucket = _safe_str_field(base_bucket, field_name="bucket")
    resolved_region = _resolve_optional_text(region)
    if not resolved_region:
        return resolved_bucket
    region_suffix = f"-{resolved_region}"
    if resolved_bucket.endswith(region_suffix):
        return resolved_bucket
    return f"{resolved_bucket}{region_suffix}"


def snapshot_manager_resolve_snapshot_bucket(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
) -> str:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    base_bucket = _safe_str_field(
        config.get("bucket"),
        field_name="bucket",
        required=False,
    )
    if not base_bucket:
        return ""
    storage_context = snapshot_manager_resolve_table_storage_context(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    return snapshot_manager_build_bucket_name(
        base_bucket,
        _resolve_optional_text(storage_context.get("region")),
    )


def snapshot_manager_attach_snapshot_bucket_to_result(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    result: Dict[str, Any],
    *,
    execution_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result
    return {
        **result,
        "snapshot_bucket": _resolve_optional_text(
            result.get("snapshot_bucket"),
            snapshot_manager_resolve_snapshot_bucket(
                manager,
                table_name,
                table_arn,
                execution_context=execution_context,
            ),
        ),
    }


def _build_export_bucket_params(
    config: Dict[str, Any],
    *,
    bucket_name: Optional[str] = None,
) -> Dict[str, str]:
    bucket = _safe_str_field(
        bucket_name if bucket_name is not None else config.get("bucket"),
        field_name="bucket",
    )
    bucket_owner = _safe_str_field(
        config.get("bucket_owner"),
        field_name="bucket_owner",
        required=False,
    )
    return snapshot_manager_compact_fields(
        {
            "S3Bucket": bucket,
            "S3BucketOwner": bucket_owner,
        }
    )


def _resolve_public_assume_role(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    return _resolve_optional_text(payload.get("assume_role"), payload.get("assume_role_arn"))


def _normalize_output_pending_export_item(raw_item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_item, dict):
        return None

    export_arn = _resolve_optional_text(raw_item.get("export_arn"))
    export_job_id = _resolve_optional_text(
        raw_item.get("export_job_id"),
        _resolve_export_job_id(export_arn or ""),
    )
    normalized_item = snapshot_manager_compact_fields(
        {
            "export_arn": export_arn,
            "export_job_id": export_job_id,
            "checkpoint_to": _resolve_optional_text(raw_item.get("checkpoint_to")),
            "checkpoint_from": _resolve_optional_text(raw_item.get("checkpoint_from")),
            "mode": _resolve_optional_text(raw_item.get("mode")),
            "source": _resolve_optional_text(raw_item.get("source")),
            "started_at": _resolve_optional_text(raw_item.get("started_at")),
        }
    )
    return normalized_item or None


def _normalize_output_pending_exports(raw_pending: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_pending, list):
        return []
    return [
        normalized_item
        for normalized_item in (
            _normalize_output_pending_export_item(raw_item)
            for raw_item in raw_pending
        )
        if normalized_item is not None
    ]


def _normalize_output_checkpoint_state(raw_state: Any) -> Any:
    if not isinstance(raw_state, dict):
        return raw_state

    normalized_state = {
        key: value
        for key, value in raw_state.items()
        if key != "pending_exports"
    }
    return snapshot_manager_compact_fields(
        {
            **normalized_state,
            "pending_exports": _normalize_output_pending_exports(raw_state.get("pending_exports")),
        }
    )


def _resolve_config_assume_role(config: Any) -> Optional[str]:
    if not isinstance(config, dict):
        return None
    return _resolve_optional_text(config.get("assume_role"), config.get("assume_role_arn"))


def _normalize_output_result_item(
    result: Dict[str, Any],
    *,
    snapshot_bucket: Optional[str],
) -> Dict[str, Any]:
    normalized_result = {}
    for key, value in result.items():
        if key in {"assume_role_arn", "export_arn"}:
            continue
        if key == "pending_exports":
            normalized_result[key] = _normalize_output_pending_exports(value)
            continue
        if key == "checkpoint_state":
            normalized_result[key] = _normalize_output_checkpoint_state(value)
            continue
        normalized_result[key] = value

    return snapshot_manager_compact_fields(
        {
            **normalized_result,
            "snapshot_bucket": _resolve_optional_text(
                result.get("snapshot_bucket"),
                snapshot_bucket,
            ),
            "assume_role": _resolve_public_assume_role(result),
            "export_arn": _resolve_optional_text(result.get("export_arn")),
            "export_job_id": _resolve_optional_text(
                result.get("export_job_id"),
                _resolve_export_job_id(
                    _resolve_optional_text(result.get("export_arn")) or ""
                ),
            ),
        }
    )


def _normalize_output_results(
    results: List[Dict[str, Any]],
    *,
    snapshot_bucket: Optional[str],
) -> List[Dict[str, Any]]:
    return [
        _normalize_output_result_item(result, snapshot_bucket=snapshot_bucket)
        if isinstance(result, dict)
        else result
        for result in results
    ]


def _serialize_output_payload(payload: Any) -> str:
    return json.dumps(_safe_json(payload), default=str, ensure_ascii=False)


def _split_utf8_text(text: str, max_bytes: int) -> List[str]:
    if len(text.encode("utf-8")) <= max_bytes:
        return [text]

    chunks: List[str] = []
    current_chunk: List[str] = []
    current_size = 0
    for char in text:
        char_size = len(char.encode("utf-8"))
        if current_chunk and current_size + char_size > max_bytes:
            chunks.append("".join(current_chunk))
            current_chunk = [char]
            current_size = char_size
            continue
        current_chunk.append(char)
        current_size += char_size

    if current_chunk:
        chunks.append("".join(current_chunk))
    return chunks


def _build_output_tracking_fields(result: Dict[str, Any]) -> Dict[str, Any]:
    export_job_id = _resolve_optional_text(result.get("export_job_id"))
    pending_exports = _normalize_output_pending_exports(result.get("pending_exports"))
    tracking_export_job_ids = _dedupe_values(
        [
            *([export_job_id] if export_job_id else []),
            *[
                pending_export_job_id
                for pending_export_job_id in (
                    _resolve_optional_text(item.get("export_job_id"))
                    for item in pending_exports
                )
                if pending_export_job_id
            ],
        ]
    )
    primary_export_job_id = export_job_id or (
        tracking_export_job_ids[0] if tracking_export_job_ids else None
    )
    return snapshot_manager_compact_fields(
        {
            "export_job_id": primary_export_job_id,
            "tracking_export_job_ids": tracking_export_job_ids,
            "pending_export_count": len(pending_exports),
        }
    )


def _resolve_output_export_arn(result: Dict[str, Any]) -> Optional[str]:
    export_arn = _resolve_optional_text(result.get("export_arn"))
    if export_arn:
        return export_arn
    pending_export = _resolve_output_primary_pending_export(result)
    if not pending_export:
        return None
    return _resolve_optional_text(pending_export.get("export_arn"))


def _resolve_output_export_started_at(result: Dict[str, Any]) -> Optional[str]:
    started_at = _resolve_optional_text(result.get("started_at"))
    if started_at:
        return started_at
    pending_export = _resolve_output_primary_pending_export(result)
    if not pending_export:
        return None
    return _resolve_optional_text(pending_export.get("started_at"))


def _resolve_output_export_type(result: Dict[str, Any]) -> Optional[str]:
    export_type = _resolve_optional_text(result.get("export_type"))
    if export_type:
        return export_type
    mode = _resolve_optional_text(result.get("mode"))
    if mode:
        return mode
    pending_export = _resolve_output_primary_pending_export(result)
    if not pending_export:
        return None
    return _resolve_optional_text(pending_export.get("mode"))


def _format_output_dynamodb_status(status: Optional[str]) -> Optional[str]:
    normalized_status = _resolve_optional_text(status)
    if not normalized_status:
        return None
    return OUTPUT_DYNAMODB_STATUS_LABELS.get(
        normalized_status.upper(),
        normalized_status.replace("_", " ").title(),
    )


def _format_output_dynamodb_export_type(export_type: Optional[str]) -> Optional[str]:
    normalized_export_type = _resolve_optional_text(export_type)
    if not normalized_export_type:
        return None
    return OUTPUT_DYNAMODB_EXPORT_TYPE_LABELS.get(
        normalized_export_type.upper(),
        normalized_export_type.replace("_", " ").title(),
    )


def _format_output_dynamodb_export_started_at(started_at: Optional[str]) -> Optional[str]:
    normalized_started_at = _resolve_optional_text(started_at)
    if not normalized_started_at:
        return None
    try:
        resolved_started_at = _parse_iso(normalized_started_at)
    except ValueError:
        return normalized_started_at
    return (
        resolved_started_at
        .astimezone(OUTPUT_DYNAMODB_EXPORT_TIMEZONE)
        .replace(microsecond=0)
        .isoformat()
    )


def _build_output_dynamodb_export_s3_destination(result: Dict[str, Any]) -> Optional[str]:
    bucket = _resolve_optional_text(result.get("snapshot_bucket"))
    if not bucket:
        return None

    prefix = _resolve_optional_text(result.get("s3_prefix"))
    path_segments = [segment.strip("/") for segment in [prefix] if segment]
    if not path_segments:
        return f"s3://{bucket}"
    return f"s3://{bucket}/{'/'.join(path_segments)}"


def _resolve_output_primary_pending_export(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pending_exports = _normalize_output_pending_exports(result.get("pending_exports"))
    return pending_exports[0] if pending_exports else None


def _resolve_output_table_arn(result: Dict[str, Any]) -> Optional[str]:
    table_arn = _resolve_optional_text(result.get("table_arn"))
    if table_arn:
        return table_arn

    checkpoint_state = result.get("checkpoint_state")
    if isinstance(checkpoint_state, dict):
        return _resolve_optional_text(checkpoint_state.get("table_arn"))
    return None


def _resolve_output_table_name(result: Dict[str, Any]) -> Optional[str]:
    table_name = _resolve_optional_text(result.get("table_name"))
    if table_name:
        return table_name

    checkpoint_state = result.get("checkpoint_state")
    if isinstance(checkpoint_state, dict):
        table_name = _resolve_optional_text(checkpoint_state.get("table_name"))
        if table_name:
            return table_name

    table_arn = _resolve_output_table_arn(result)
    if not table_arn:
        return None
    return _resolve_optional_text(_extract_table_name(table_arn))


def _resolve_output_table_status(result: Dict[str, Any]) -> Optional[str]:
    status = _resolve_optional_text(result.get("status"))
    if status:
        return status
    return "PENDING" if _resolve_output_primary_pending_export(result) else None


def _resolve_output_table_mode(result: Dict[str, Any]) -> Optional[str]:
    mode = _resolve_optional_text(result.get("mode"))
    if mode:
        return mode
    pending_export = _resolve_output_primary_pending_export(result)
    if not pending_export:
        return None
    return _resolve_optional_text(pending_export.get("mode"))


def _resolve_output_table_source(result: Dict[str, Any]) -> Optional[str]:
    source = _resolve_optional_text(result.get("source"))
    if source:
        return source
    pending_export = _resolve_output_primary_pending_export(result)
    if not pending_export:
        return None
    return _resolve_optional_text(pending_export.get("source"))


def _resolve_output_checkpoint_value(result: Dict[str, Any], field_name: str) -> Optional[str]:
    field_value = _resolve_optional_text(result.get(field_name))
    if field_value:
        return field_value
    pending_export = _resolve_output_primary_pending_export(result)
    if not pending_export:
        return None
    return _resolve_optional_text(pending_export.get(field_name))


def _build_output_table_events(
    source: str,
    payload: Any,
    *,
    aws_request_id: Optional[str],
    snapshot_bucket: Optional[str],
) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []

    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        return []

    base_fields = snapshot_manager_compact_fields(
        {
            "source": source,
            "aws_request_id": aws_request_id,
            "snapshot_bucket": snapshot_bucket,
            "run_id": _resolve_optional_text(payload.get("run_id")),
            "run_status": _resolve_optional_text(payload.get("status")),
            "run_mode": _resolve_optional_text(payload.get("mode")),
            "dry_run": payload.get("dry_run"),
        }
    )

    events: List[Dict[str, Any]] = []
    for index, result in enumerate(raw_results, start=1):
        if not isinstance(result, dict):
            continue

        normalized_result = _normalize_output_result_item(
            result,
            snapshot_bucket=snapshot_bucket,
        )
        table_name = _resolve_output_table_name(normalized_result)
        table_arn = _resolve_output_table_arn(normalized_result)
        table_status = _resolve_output_table_status(normalized_result)
        if not (table_name or table_arn) or not table_status:
            continue

        events.append(
            snapshot_manager_compact_fields(
                {
                    **base_fields,
                    "result_index": index,
                    "table_name": table_name,
                    "table_arn": table_arn,
                    "table_status": table_status,
                    "table_mode": _resolve_output_table_mode(normalized_result),
                    "table_source": _resolve_output_table_source(normalized_result),
                    "checkpoint_from": _resolve_output_checkpoint_value(normalized_result, "checkpoint_from"),
                    "checkpoint_to": _resolve_output_checkpoint_value(normalized_result, "checkpoint_to"),
                    "checkpoint_source": _resolve_optional_text(normalized_result.get("checkpoint_source")),
                    "s3_prefix": _resolve_optional_text(normalized_result.get("s3_prefix")),
                    "full_run_id": _resolve_optional_text(normalized_result.get("full_run_id")),
                    "full_export_s3_prefix": _resolve_optional_text(normalized_result.get("full_export_s3_prefix")),
                    "assume_role": _resolve_optional_text(normalized_result.get("assume_role")),
                    "table_account_id": _resolve_optional_text(normalized_result.get("table_account_id")),
                    "table_region": _resolve_optional_text(normalized_result.get("table_region")),
                    "error_type": _resolve_optional_text(normalized_result.get("error_type")),
                    "error_code": _resolve_optional_text(normalized_result.get("error_code")),
                    "retryable": normalized_result.get("retryable"),
                    **_build_output_tracking_fields(normalized_result),
                }
            )
        )
    return events


def _build_output_cloudwatch_event_specs(
    source: str,
    payload: Any,
    *,
    aws_request_id: Optional[str],
    snapshot_bucket: Optional[str],
) -> List[Dict[str, Any]]:
    serialized_payload = _serialize_output_payload(payload)
    payload_bytes = len(serialized_payload.encode("utf-8"))
    chunks = _split_utf8_text(serialized_payload, CLOUDWATCH_OUTPUT_MAX_BYTES)

    event_specs: List[Dict[str, Any]] = []
    if len(chunks) == 1:
        event_specs.append(
            {
                "action": "output.cloudwatch",
                "level": logging.INFO,
                "fields": {
                    "source": source,
                    "aws_request_id": aws_request_id,
                    "snapshot_bucket": snapshot_bucket,
                    "output": _safe_json(payload),
                },
            }
        )
    else:
        event_specs.append(
            {
                "action": "output.cloudwatch.chunked",
                "level": logging.WARNING,
                "fields": {
                    "source": source,
                    "aws_request_id": aws_request_id,
                    "snapshot_bucket": snapshot_bucket,
                    "chunk_count": len(chunks),
                    "payload_bytes": payload_bytes,
                },
            }
        )
        for index, chunk in enumerate(chunks, start=1):
            event_specs.append(
                {
                    "action": "output.cloudwatch.chunk",
                    "level": logging.INFO,
                    "fields": {
                        "source": source,
                        "aws_request_id": aws_request_id,
                        "snapshot_bucket": snapshot_bucket,
                        "chunk_index": index,
                        "chunk_count": len(chunks),
                        "output_json_chunk": chunk,
                    },
                }
            )

    event_specs.extend(
        {
            "action": "output.cloudwatch.table",
            "level": logging.INFO,
            "fields": table_event,
        }
        for table_event in _build_output_table_events(
            source,
            payload,
            aws_request_id=aws_request_id,
            snapshot_bucket=snapshot_bucket,
        )
    )
    return event_specs


def _emit_output_cloudwatch_event_specs_to_logger(event_specs: List[Dict[str, Any]]) -> None:
    for event_spec in event_specs:
        _log_event(
            event_spec["action"],
            level=int(event_spec.get("level", logging.INFO)),
            **_safe_dict_field(event_spec.get("fields"), "event_spec.fields"),
        )


def _emit_output_to_cloudwatch(
    source: str,
    payload: Any,
    *,
    config: Optional[SnapshotConfig] = None,
    context: Any = None,
) -> None:
    if not _should_emit_output_to_cloudwatch(config):
        return

    aws_request_id = getattr(context, "aws_request_id", None)
    snapshot_bucket = _resolve_output_snapshot_bucket(payload, config)

    try:
        event_specs = _build_output_cloudwatch_event_specs(
            source,
            payload,
            aws_request_id=aws_request_id,
            snapshot_bucket=snapshot_bucket,
        )
        _emit_output_cloudwatch_event_specs_to_logger(event_specs)
    except Exception as exc:
        logger.warning("Falha ao emitir output para CloudWatch: %s", exc)


def _should_emit_output_to_dynamodb(
    config: Optional[SnapshotConfig] = None,
    *,
    event: Optional[Dict[str, Any]] = None,
) -> bool:
    if isinstance(config, dict) and "output_dynamodb_enabled" in config:
        return bool(config.get("output_dynamodb_enabled"))
    event_enabled = event.get("output_dynamodb_enabled") if isinstance(event, dict) else None
    return _resolve_env_first_bool(
        event_enabled,
        "OUTPUT_DYNAMODB_ENABLED",
        "false",
    )


def _resolve_output_dynamodb_destination(
    config: Optional[SnapshotConfig] = None,
    *,
    event: Optional[Dict[str, Any]] = None,
) -> Dict[str, Optional[str] | bool]:
    event_table = event.get("output_dynamodb_table") if isinstance(event, dict) else None
    event_region = event.get("output_dynamodb_region") if isinstance(event, dict) else None
    enabled = _should_emit_output_to_dynamodb(config, event=event)
    table_name = _resolve_optional_text(
        config.get("output_dynamodb_table") if isinstance(config, dict) else None,
        os.getenv("OUTPUT_DYNAMODB_TABLE", ""),
        event_table,
    )
    region = _resolve_optional_text(
        config.get("output_dynamodb_region") if isinstance(config, dict) else None,
        os.getenv("OUTPUT_DYNAMODB_REGION", ""),
        event_region,
        _resolve_runtime_region(),
    )
    if enabled and not table_name:
        raise ValueError(
            "OUTPUT_DYNAMODB_TABLE deve ser informado quando OUTPUT_DYNAMODB_ENABLED=true"
        )
    return {
        "enabled": enabled,
        "table_name": table_name,
        "region": region,
    }


def _build_output_dynamodb_table_definition(table_name: str) -> Dict[str, Any]:
    return {
        "TableName": table_name,
        "AttributeDefinitions": [
            {"AttributeName": OUTPUT_DYNAMODB_PARTITION_KEY, "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": OUTPUT_DYNAMODB_PARTITION_KEY, "KeyType": "HASH"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }


def _resolve_output_dynamodb_table_status(response: Dict[str, Any]) -> str:
    table_description = _safe_dict_field(response.get("Table"), "DescribeTable.Table")
    return (
        _resolve_optional_text(table_description.get("TableStatus"), "ACTIVE")
        or "ACTIVE"
    ).upper()


def _resolve_output_dynamodb_table_attribute_types(
    table_response_or_description: Dict[str, Any],
) -> Dict[str, str]:
    table_description = _safe_dict_field(
        (
            table_response_or_description.get("Table")
            if "Table" in table_response_or_description
            else table_response_or_description
        ),
        "output_dynamodb_table_description",
    )
    attribute_definitions = table_description.get("AttributeDefinitions")
    if not isinstance(attribute_definitions, list):
        return {}
    attribute_types: Dict[str, str] = {}
    for item in attribute_definitions:
        if not isinstance(item, dict):
            return {}
        attribute_name = _resolve_optional_text(item.get("AttributeName"))
        attribute_type = _resolve_optional_text(item.get("AttributeType"))
        if not attribute_name or not attribute_type:
            return {}
        attribute_types[attribute_name] = attribute_type.upper()
    return attribute_types


def _resolve_output_dynamodb_table_key_schema(
    table_response_or_description: Dict[str, Any],
) -> Dict[str, str]:
    table_description = _safe_dict_field(
        (
            table_response_or_description.get("Table")
            if "Table" in table_response_or_description
            else table_response_or_description
        ),
        "output_dynamodb_table_description",
    )
    key_schema = table_description.get("KeySchema")
    if not isinstance(key_schema, list):
        return {}
    key_types: Dict[str, str] = {}
    for item in key_schema:
        if not isinstance(item, dict):
            return {}
        attribute_name = _resolve_optional_text(item.get("AttributeName"))
        key_type = _resolve_optional_text(item.get("KeyType"))
        if not attribute_name or not key_type:
            return {}
        key_types[attribute_name] = key_type.upper()
    return key_types


def _output_dynamodb_table_has_expected_schema(response: Dict[str, Any]) -> bool:
    table_description = _safe_dict_field(
        (
            response.get("Table")
            if "Table" in response
            else response
        ),
        "DescribeTable.Table",
    )
    attribute_types = _resolve_output_dynamodb_table_attribute_types(table_description)
    key_types = _resolve_output_dynamodb_table_key_schema(table_description)
    return (
        key_types == {OUTPUT_DYNAMODB_PARTITION_KEY: "HASH"}
        and attribute_types.get(OUTPUT_DYNAMODB_PARTITION_KEY) == "S"
    )


def _build_output_dynamodb_schema_error(table_name: str) -> RuntimeError:
    return RuntimeError(
        f"Esquema inválido para tabela DynamoDB de output {table_name}. "
        f"Use partition key {OUTPUT_DYNAMODB_PARTITION_KEY} do tipo String e sem chave secundária."
    )


def _wait_for_output_dynamodb_table_active(
    ddb_client: Any,
    *,
    table_name: str,
) -> None:
    deadline = time.time() + OUTPUT_DYNAMODB_TABLE_TIMEOUT_SECONDS
    while True:
        try:
            table_status = _resolve_output_dynamodb_table_status(
                ddb_client.describe_table(TableName=table_name)
            )
        except ClientError as exc:
            if _classify_aws_error(_client_error_code(exc)) == "not_found":
                if time.time() >= deadline:
                    raise TimeoutError(
                        f"Timeout aguardando tabela DynamoDB de output {table_name} ficar disponível"
                    ) from exc
                _log_event(
                    "output.dynamodb.table.wait",
                    table_name=table_name,
                    status="NOT_FOUND",
                )
                time.sleep(OUTPUT_DYNAMODB_TABLE_POLL_SECONDS)
                continue
            raise _build_aws_runtime_error("DynamoDB DescribeTable", exc, resource=table_name) from exc
        except Exception as exc:
            raise RuntimeError(
                f"Falha inesperada ao consultar tabela de output DynamoDB {table_name}: {exc}"
            ) from exc

        if table_status == "ACTIVE":
            return
        if time.time() >= deadline:
            raise TimeoutError(
                f"Timeout aguardando tabela DynamoDB de output {table_name} ficar ACTIVE"
            )
        _log_event(
            "output.dynamodb.table.wait",
            table_name=table_name,
            status=table_status,
        )
        time.sleep(OUTPUT_DYNAMODB_TABLE_POLL_SECONDS)


def _create_output_dynamodb_table(
    ddb_client: Any,
    *,
    table_name: str,
) -> None:
    try:
        ddb_client.create_table(**_build_output_dynamodb_table_definition(table_name))
        _log_event(
            "output.dynamodb.table.create.started",
            table_name=table_name,
            billing_mode="PAY_PER_REQUEST",
        )
    except ClientError as exc:
        if _classify_aws_error(_client_error_code(exc)) == "conflict":
            _log_event(
                "output.dynamodb.table.create.race",
                table_name=table_name,
            )
            return
        raise _build_aws_runtime_error("DynamoDB CreateTable", exc, resource=table_name) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Falha inesperada ao criar tabela de output DynamoDB {table_name}: {exc}"
        ) from exc


def _ensure_output_dynamodb_table_exists(
    ddb_client: Any,
    *,
    table_name: str,
) -> None:
    response: Optional[Dict[str, Any]] = None
    try:
        response = ddb_client.describe_table(TableName=table_name)
        table_status = _resolve_output_dynamodb_table_status(response)
    except ClientError as exc:
        if _classify_aws_error(_client_error_code(exc)) != "not_found":
            raise _build_aws_runtime_error("DynamoDB DescribeTable", exc, resource=table_name) from exc
        _log_event(
            "output.dynamodb.table.missing",
            table_name=table_name,
        )
        _create_output_dynamodb_table(
            ddb_client,
            table_name=table_name,
        )
        _wait_for_output_dynamodb_table_active(
            ddb_client,
            table_name=table_name,
        )
        response = ddb_client.describe_table(TableName=table_name)
        table_status = _resolve_output_dynamodb_table_status(response)
    except Exception as exc:
        raise RuntimeError(
            f"Falha inesperada ao consultar tabela de output DynamoDB {table_name}: {exc}"
        ) from exc

    if table_status == "ACTIVE":
        if response is None:
            raise RuntimeError(
                f"Falha inesperada ao consultar tabela de output DynamoDB {table_name}: resposta vazia"
            )
        if not _output_dynamodb_table_has_expected_schema(response):
            raise _build_output_dynamodb_schema_error(table_name)
        return

    _log_event(
        "output.dynamodb.table.wait",
        table_name=table_name,
        status=table_status,
    )
    _wait_for_output_dynamodb_table_active(
        ddb_client,
        table_name=table_name,
    )
    response = ddb_client.describe_table(TableName=table_name)
    if not _output_dynamodb_table_has_expected_schema(response):
        raise _build_output_dynamodb_schema_error(table_name)


def _build_checkpoint_dynamodb_table_definition(table_name: str) -> Dict[str, Any]:
    return {
        "TableName": table_name,
        "AttributeDefinitions": [
            {"AttributeName": CHECKPOINT_DYNAMODB_PARTITION_KEY, "AttributeType": "S"},
            {"AttributeName": CHECKPOINT_DYNAMODB_SORT_KEY, "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": CHECKPOINT_DYNAMODB_PARTITION_KEY, "KeyType": "HASH"},
            {"AttributeName": CHECKPOINT_DYNAMODB_SORT_KEY, "KeyType": "RANGE"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }


def _resolve_checkpoint_dynamodb_table_attribute_types(table_description: Dict[str, Any]) -> Dict[str, str]:
    attribute_definitions = table_description.get("AttributeDefinitions")
    if not isinstance(attribute_definitions, list):
        return {}
    attribute_types: Dict[str, str] = {}
    for item in attribute_definitions:
        if not isinstance(item, dict):
            return {}
        attribute_name = _resolve_optional_text(item.get("AttributeName"))
        attribute_type = _resolve_optional_text(item.get("AttributeType"))
        if not attribute_name or not attribute_type:
            return {}
        attribute_types[attribute_name] = attribute_type.upper()
    return attribute_types


def _resolve_checkpoint_dynamodb_table_key_schema(table_description: Dict[str, Any]) -> Dict[str, str]:
    key_schema = table_description.get("KeySchema")
    if not isinstance(key_schema, list):
        return {}
    key_types: Dict[str, str] = {}
    for item in key_schema:
        if not isinstance(item, dict):
            return {}
        attribute_name = _resolve_optional_text(item.get("AttributeName"))
        key_type = _resolve_optional_text(item.get("KeyType"))
        if not attribute_name or not key_type:
            return {}
        key_types[attribute_name] = key_type.upper()
    return key_types


def _checkpoint_dynamodb_table_has_expected_schema(response: Dict[str, Any]) -> bool:
    table_description = _safe_dict_field(response.get("Table"), "DescribeTable.Table")
    attribute_types = _resolve_checkpoint_dynamodb_table_attribute_types(table_description)
    key_types = _resolve_checkpoint_dynamodb_table_key_schema(table_description)
    return (
        key_types
        == {
            CHECKPOINT_DYNAMODB_PARTITION_KEY: "HASH",
            CHECKPOINT_DYNAMODB_SORT_KEY: "RANGE",
        }
        and attribute_types.get(CHECKPOINT_DYNAMODB_PARTITION_KEY) == "S"
        and attribute_types.get(CHECKPOINT_DYNAMODB_SORT_KEY) == "S"
    )


def _resolve_checkpoint_dynamodb_table_status(response: Dict[str, Any]) -> str:
    table_description = _safe_dict_field(response.get("Table"), "DescribeTable.Table")
    return (
        _resolve_optional_text(table_description.get("TableStatus"), "ACTIVE")
        or "ACTIVE"
    ).upper()


def _wait_for_checkpoint_dynamodb_table_active(
    ddb_client: Any,
    *,
    table_name: str,
) -> Dict[str, Any]:
    deadline = time.time() + CHECKPOINT_DYNAMODB_TABLE_TIMEOUT_SECONDS
    while True:
        try:
            response = ddb_client.describe_table(TableName=table_name)
        except ClientError as exc:
            if _classify_aws_error(_client_error_code(exc)) == "not_found":
                if time.time() >= deadline:
                    raise TimeoutError(
                        f"Timeout aguardando tabela DynamoDB de checkpoint {table_name} ficar disponível"
                    ) from exc
                _log_event(
                    "checkpoint.dynamodb.table.wait",
                    table_name=table_name,
                    status="NOT_FOUND",
                )
                time.sleep(CHECKPOINT_DYNAMODB_TABLE_POLL_SECONDS)
                continue
            raise _build_aws_runtime_error("DynamoDB DescribeTable", exc, resource=table_name) from exc
        except Exception as exc:
            raise RuntimeError(
                f"Falha inesperada ao consultar tabela de checkpoint DynamoDB {table_name}: {exc}"
            ) from exc

        table_status = _resolve_checkpoint_dynamodb_table_status(response)
        if table_status == "ACTIVE":
            return response
        if time.time() >= deadline:
            raise TimeoutError(
                f"Timeout aguardando tabela DynamoDB de checkpoint {table_name} ficar ACTIVE"
            )
        _log_event(
            "checkpoint.dynamodb.table.wait",
            table_name=table_name,
            status=table_status,
        )
        time.sleep(CHECKPOINT_DYNAMODB_TABLE_POLL_SECONDS)


def _create_checkpoint_dynamodb_table(
    ddb_client: Any,
    *,
    table_name: str,
) -> None:
    try:
        ddb_client.create_table(**_build_checkpoint_dynamodb_table_definition(table_name))
        _log_event(
            "checkpoint.dynamodb.table.create.started",
            table_name=table_name,
            billing_mode="PAY_PER_REQUEST",
        )
    except ClientError as exc:
        if _classify_aws_error(_client_error_code(exc)) == "conflict":
            _log_event(
                "checkpoint.dynamodb.table.create.race",
                table_name=table_name,
            )
            return
        raise _build_aws_runtime_error("DynamoDB CreateTable", exc, resource=table_name) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Falha inesperada ao criar tabela de checkpoint DynamoDB {table_name}: {exc}"
        ) from exc


def _ensure_checkpoint_dynamodb_table_exists(
    ddb_client: Any,
    *,
    table_name: str,
) -> None:
    try:
        response = ddb_client.describe_table(TableName=table_name)
    except ClientError as exc:
        if _classify_aws_error(_client_error_code(exc)) != "not_found":
            raise _build_aws_runtime_error("DynamoDB DescribeTable", exc, resource=table_name) from exc
        _log_event(
            "checkpoint.dynamodb.table.missing",
            table_name=table_name,
        )
        _create_checkpoint_dynamodb_table(
            ddb_client,
            table_name=table_name,
        )
        response = _wait_for_checkpoint_dynamodb_table_active(
            ddb_client,
            table_name=table_name,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Falha inesperada ao consultar tabela de checkpoint DynamoDB {table_name}: {exc}"
        ) from exc
    else:
        table_status = _resolve_checkpoint_dynamodb_table_status(response)
        if table_status != "ACTIVE":
            _log_event(
                "checkpoint.dynamodb.table.wait",
                table_name=table_name,
                status=table_status,
            )
            response = _wait_for_checkpoint_dynamodb_table_active(
                ddb_client,
                table_name=table_name,
            )

    if not _checkpoint_dynamodb_table_has_expected_schema(response):
        raise _build_checkpoint_dynamodb_schema_error(table_name)


def _marshal_dynamodb_item(item: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        key: serializer.serialize(value)
        for key, value in snapshot_manager_compact_fields(item).items()
    }


def _build_output_dynamodb_items(
    payload: Any,
    *,
    snapshot_bucket: Optional[str],
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    raw_results = payload.get("results") if isinstance(payload, dict) else None
    if isinstance(raw_results, list):
        items.extend(
            item
            for result in raw_results
            if isinstance(result, dict)
            for item in [
                _build_output_dynamodb_table_item(
                    result,
                    snapshot_bucket=snapshot_bucket,
                )
            ]
            if item
        )
    return items


def _build_output_dynamodb_table_item(
    result: Dict[str, Any],
    *,
    snapshot_bucket: Optional[str],
) -> Dict[str, Any]:
    normalized_result = _normalize_output_result_item(
        result,
        snapshot_bucket=snapshot_bucket,
    )
    export_arn = _resolve_output_export_arn(normalized_result)
    table_name = _resolve_output_table_name(normalized_result)
    export_status = _format_output_dynamodb_status(
        _resolve_output_table_status(normalized_result)
    )
    export_started_at = _format_output_dynamodb_export_started_at(
        _resolve_output_export_started_at(normalized_result)
    )
    export_type = _format_output_dynamodb_export_type(
        _resolve_output_export_type(normalized_result)
    )
    destination_s3_bucket = _build_output_dynamodb_export_s3_destination(normalized_result)
    if not export_arn:
        return {}

    return snapshot_manager_compact_fields(
        {
            OUTPUT_DYNAMODB_PARTITION_KEY: export_arn,
            "Table name": table_name,
            "Destination S3 Bucket": destination_s3_bucket,
            "Status": export_status,
            "Export job start time (utc-03:00)": export_started_at,
            "Export Type": export_type,
        }
    )


def _write_output_dynamodb_items(
    ddb_client: Any,
    *,
    table_name: str,
    items: List[Dict[str, Any]],
) -> None:
    for item in items:
        ddb_client.put_item(
            TableName=table_name,
            Item=_marshal_dynamodb_item(item),
        )


def _resolve_output_dynamodb_session(manager: Optional[Dict[str, Any]] = None) -> Any:
    if isinstance(manager, dict):
        output_session = manager.get("_output_session")
        if output_session is not None:
            return output_session
        active_session = manager.get("_assume_session")
        if active_session is not None:
            return active_session
        session = manager.get("session")
        if session is not None:
            return session
    return _get_default_aws_session()


def _emit_output_to_dynamodb(
    source: str,
    payload: Any,
    *,
    config: Optional[SnapshotConfig] = None,
    event: Optional[Dict[str, Any]] = None,
    context: Any = None,
    manager: Optional[Dict[str, Any]] = None,
) -> None:
    destination: Dict[str, Optional[str] | bool] = {
        "enabled": False,
        "table_name": None,
        "region": None,
    }
    try:
        destination = _resolve_output_dynamodb_destination(config, event=event)
        if not destination.get("enabled"):
            return

        table_name = _safe_str_field(
            destination.get("table_name"),
            field_name="output_dynamodb_table",
        )
        region_name = _resolve_optional_text(destination.get("region"))
        session = _resolve_output_dynamodb_session(manager)
        ddb_client = _get_session_client(
            session,
            "dynamodb",
            region_name=region_name,
        )
        _ensure_output_dynamodb_table_exists(
            ddb_client,
            table_name=table_name,
        )
        snapshot_bucket = _resolve_output_snapshot_bucket(payload, config)
        items = _build_output_dynamodb_items(
            payload,
            snapshot_bucket=snapshot_bucket,
        )
        if not items:
            _log_event(
                "output.dynamodb.write.skip",
                table_name=table_name,
                region=region_name,
                source=source,
                reason="no_items",
            )
            return

        _write_output_dynamodb_items(
            ddb_client,
            table_name=table_name,
            items=items,
        )
        _log_event(
            "output.dynamodb.write.success",
            table_name=table_name,
            region=region_name,
            source=source,
            item_count=len(items),
        )
    except Exception as exc:
        logger.warning("Falha ao emitir output para DynamoDB: %s", exc)
        _log_event(
            "output.dynamodb.write.failed",
            level=logging.WARNING,
            table_name=destination.get("table_name"),
            region=destination.get("region"),
            source=source,
            error=str(exc),
        )


def _safe_str_field(value: Any, field_name: str, *, required: bool = True) -> str:
    if value is None:
        if required:
            raise ValueError(f"{field_name} é obrigatório")
        return ""
    cleaned = str(value).strip()
    if required and not cleaned:
        raise ValueError(f"{field_name} não pode ser vazio")
    return cleaned


def _safe_dict_field(value: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{field_name} deve ser um objeto (dict)")
    return value


def _safe_get_field(mapping: Any, key: str, *, field_name: str, required: bool = True) -> Any:
    source = _safe_dict_field(mapping, field_name)
    if key not in source:
        if required:
            raise RuntimeError(f"{field_name}.{key} ausente")
        return None
    value = source.get(key)
    if required and value is None:
        raise RuntimeError(f"{field_name}.{key} não pode ser None")
    return value


def _extract_entry_fields(entry: Any, *, source: str) -> Dict[str, str]:
    if not isinstance(entry, dict):
        raise ValueError(f"{source} inválida para processar tabela")
    table_name = _safe_str_field(entry.get("table_name"), field_name=f"{source} table_name")
    table_arn = _safe_str_field(entry.get("table_arn"), field_name=f"{source} table_arn")
    return {"table_name": table_name, "table_arn": table_arn}


def _to_decimal(v: Any) -> Any:
    if isinstance(v, Decimal):
        if v % 1 == 0:
            return int(v)
        return float(v)
    return v


def _safe_json(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_json(v) for v in obj]
    return _to_decimal(obj)


def _parse_updated_value(raw: Any, kind: str) -> Optional[datetime]:
    if raw is None:
        return None

    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)

    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)

    if isinstance(raw, Decimal):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            if kind == "number":
                return datetime.fromtimestamp(float(text), tz=timezone.utc)
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    return None


def _build_aws_session(config: SnapshotConfig):
    default_session = _get_default_aws_session()
    default_region = _resolve_runtime_region(default_session.region_name)

    assume_role_arn = _resolve_config_assume_role(config)
    if not assume_role_arn:
        _log_event(
            "aws.session.ready",
            mode="default_credentials",
            region=default_region,
            assume_role_template_enabled=False,
        )
        return default_session

    assume_role_template_enabled = _has_role_template_fields(assume_role_arn)
    if assume_role_template_enabled:
        _log_event(
            "aws.session.ready",
            mode="default_credentials_dynamic_role_template",
            region=default_region,
            assume_role_template_enabled=True,
        )
        return default_session

    assume_role_args: Dict[str, Any] = {
        "RoleArn": assume_role_arn,
        "RoleSessionName": config["assume_role_session_name"],
        "DurationSeconds": config["assume_role_duration_seconds"],
    }
    if config["assume_role_external_id"]:
        assume_role_args["ExternalId"] = config["assume_role_external_id"]

    _log_event(
        "aws.assume_role.bootstrap.start",
        role_arn=assume_role_arn,
        region=default_region,
        duration_seconds=config["assume_role_duration_seconds"],
        has_external_id=bool(config["assume_role_external_id"]),
        session_name=config["assume_role_session_name"],
    )
    try:
        sts_client = _get_session_client(default_session, "sts", region_name=default_region)
        response = sts_client.assume_role(**assume_role_args)
    except ClientError as exc:
        raise _build_aws_runtime_error("STS AssumeRole bootstrap", exc, resource=assume_role_arn) from exc
    except Exception as exc:
        raise RuntimeError(f"Erro inesperado ao executar assume role bootstrap {assume_role_arn}: {exc}") from exc

    credentials = response.get("Credentials") if isinstance(response, dict) else None
    if not isinstance(credentials, dict):
        raise RuntimeError("Resposta inválida do STS AssumeRole bootstrap: Credentials ausente")
    access_key_id = credentials.get("AccessKeyId")
    secret_access_key = credentials.get("SecretAccessKey")
    session_token = credentials.get("SessionToken")
    if not access_key_id or not secret_access_key or not session_token:
        raise RuntimeError("Resposta inválida do STS AssumeRole bootstrap: credenciais incompletas")

    assumed_session = boto3.session.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
        region_name=default_region,
    )
    _log_event(
        "aws.assume_role.bootstrap.success",
        role_arn=assume_role_arn,
        assumed_role_arn=(
            response.get("AssumedRoleUser", {}).get("Arn")
            if isinstance(response, dict)
            else None
        ),
        region=default_region,
        credentials_expiration=credentials.get("Expiration"),
    )
    _log_event(
        "aws.session.ready",
        mode="assumed_role_bootstrap",
        region=default_region,
        assume_role_template_enabled=False,
    )
    return assumed_session


def create_checkpoint_store(
    *,
    ddb_client: Any = None,
    checkpoint_dynamodb_table_arn: str,
) -> Dict[str, Any]:
    table_context = _extract_dynamodb_table_context(
        checkpoint_dynamodb_table_arn,
        field_name="checkpoint_dynamodb_table_arn",
    )
    resolved_table_name = _safe_str_field(
        table_context.get("table_name"),
        field_name="checkpoint_dynamodb_table_name",
    )
    if ddb_client is not None:
        _ensure_checkpoint_dynamodb_table_exists(
            ddb_client,
            table_name=resolved_table_name,
        )
    return {
        "backend": "dynamodb",
        "ddb": ddb_client,
        "table_arn": checkpoint_dynamodb_table_arn,
        "table_name": resolved_table_name,
        "region": table_context.get("region"),
    }


def build_checkpoint_store_for_session(session: Any, config: SnapshotConfig) -> Dict[str, Any]:
    checkpoint_dynamodb_table_arn = _safe_str_field(
        config.get("checkpoint_dynamodb_table_arn"),
        field_name="checkpoint_dynamodb_table_arn",
    )
    table_context = _extract_dynamodb_table_context(
        checkpoint_dynamodb_table_arn,
        field_name="checkpoint_dynamodb_table_arn",
    )
    return create_checkpoint_store(
        ddb_client=_get_session_client(
            session,
            "dynamodb",
            region_name=_safe_str_field(
                table_context.get("region"),
                field_name="checkpoint_dynamodb_table_region",
            ),
        ),
        checkpoint_dynamodb_table_arn=checkpoint_dynamodb_table_arn,
    )


def _build_empty_checkpoint_state(now: datetime) -> Dict[str, Any]:
    return {"version": 2, "tables": {}, "updated_at": _dt_to_iso(now)}


def _resolve_checkpoint_store_backend(store: Dict[str, Any]) -> str:
    backend = _resolve_optional_text(store.get("backend"), "dynamodb") or "dynamodb"
    if backend != "dynamodb":
        raise RuntimeError(f"Backend de checkpoint não suportado: {backend}")
    return backend


def _normalize_checkpoint_tables(raw_tables: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(raw_tables, dict):
        return {}

    normalized_tables: Dict[str, Dict[str, Any]] = {}
    for raw_state_key, raw_state in raw_tables.items():
        state_key = _safe_str_field(raw_state_key, field_name="checkpoint_state_key", required=False)
        if not state_key or not isinstance(raw_state, dict):
            continue
        table_name = _safe_str_field(
            raw_state.get("table_name"),
            field_name=f"{state_key}.table_name",
            required=False,
        )
        table_arn = _safe_str_field(
            raw_state.get("table_arn"),
            field_name=f"{state_key}.table_arn",
            required=False,
        )
        if not (table_name and table_arn):
            continue
        normalized_tables[state_key] = snapshot_manager_build_checkpoint_state_payload(
            table_name,
            table_arn,
            last_to=_safe_str_field(raw_state.get("last_to"), field_name=f"{state_key}.last_to", required=False),
            last_mode=_safe_str_field(raw_state.get("last_mode"), field_name=f"{state_key}.last_mode", required=False),
            source=_safe_str_field(raw_state.get("source"), field_name=f"{state_key}.source", required=False),
            pending_exports=snapshot_manager_normalize_pending_exports(raw_state.get("pending_exports")),
            history=snapshot_manager_normalize_checkpoint_history(raw_state.get("history")),
        )
    return normalized_tables


def _normalize_checkpoint_payload(payload: Dict[str, Any], *, now: datetime) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {}
    if "version" not in payload:
        defaults["version"] = 2
    if "tables" not in payload:
        defaults["tables"] = {}
    if "updated_at" not in payload:
        defaults["updated_at"] = _dt_to_iso(now)
    normalized_payload = {
        **payload,
        **defaults,
    }
    return {
        **normalized_payload,
        "tables": _normalize_checkpoint_tables(normalized_payload.get("tables")),
    }


def _build_checkpoint_payload_for_save(payload: Dict[str, Any], *, now: datetime) -> Dict[str, Any]:
    normalized_payload = _normalize_checkpoint_payload(payload, now=now)
    return {
        **normalized_payload,
        "updated_at": _dt_to_iso(now),
    }




def _extract_dynamodb_string_attribute(
    item: Dict[str, Any],
    attribute_name: str,
    *,
    required: bool = True,
) -> str:
    raw_value = item.get(attribute_name)
    if isinstance(raw_value, dict):
        if "S" in raw_value:
            return _safe_str_field(raw_value.get("S"), field_name=attribute_name)
        if raw_value.get("NULL") is True:
            return ""
    return _safe_str_field(raw_value, field_name=attribute_name, required=required)


def _extract_dynamodb_number_attribute(item: Dict[str, Any], attribute_name: str) -> int:
    raw_value = item.get(attribute_name)
    if isinstance(raw_value, dict) and "N" in raw_value:
        return int(str(raw_value.get("N")).strip())
    if raw_value is None:
        return 0
    return int(str(raw_value).strip())


def _deserialize_dynamodb_attribute(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    if "S" in value:
        return value.get("S")
    if "N" in value:
        number_text = str(value.get("N")).strip()
        if not number_text:
            return 0
        if "." in number_text:
            return float(number_text)
        return int(number_text)
    if "BOOL" in value:
        return bool(value.get("BOOL"))
    if value.get("NULL") is True:
        return None
    if "L" in value and isinstance(value.get("L"), list):
        return [
            _deserialize_dynamodb_attribute(item)
            for item in value.get("L", [])
        ]
    if "M" in value and isinstance(value.get("M"), dict):
        return {
            key: _deserialize_dynamodb_attribute(item)
            for key, item in value.get("M", {}).items()
        }
    return deserializer.deserialize(value)


def _deserialize_dynamodb_item(item: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(item, dict):
        return {}
    return _safe_json(
        {
            key: _deserialize_dynamodb_attribute(value)
            for key, value in item.items()
            if isinstance(value, dict)
        }
    )


def _checkpoint_dynamodb_is_current_item(item: Dict[str, Any]) -> bool:
    record_type = _extract_dynamodb_string_attribute(
        item,
        CHECKPOINT_DYNAMODB_SORT_KEY,
        required=False,
    )
    return not record_type or record_type == CHECKPOINT_DYNAMODB_CURRENT_RECORD


def _checkpoint_dynamodb_build_persisted_state(
    checkpoint_state: Dict[str, Any],
) -> Dict[str, Any]:
    state = dict(checkpoint_state) if isinstance(checkpoint_state, dict) else {}
    return snapshot_manager_build_checkpoint_state_payload(
        _safe_str_field(
            state.get("table_name"),
            field_name="checkpoint_state.table_name",
        ),
        _safe_str_field(
            state.get("table_arn"),
            field_name="checkpoint_state.table_arn",
        ),
        last_to=_safe_str_field(
            state.get("last_to"),
            field_name="checkpoint_state.last_to",
            required=False,
        ),
        last_mode=_safe_str_field(
            state.get("last_mode"),
            field_name="checkpoint_state.last_mode",
            required=False,
        ),
        source=_safe_str_field(
            state.get("source"),
            field_name="checkpoint_state.source",
            required=False,
        ),
        pending_exports=snapshot_manager_normalize_pending_exports(
            state.get("pending_exports")
        ),
    )


def _checkpoint_dynamodb_parse_current_item(item: Dict[str, Any]) -> Dict[str, Any]:
    if CHECKPOINT_DYNAMODB_PAYLOAD_ATTR in item:
        payload_text = _extract_dynamodb_string_attribute(item, CHECKPOINT_DYNAMODB_PAYLOAD_ATTR)
        payload = json.loads(payload_text)
        if not isinstance(payload, dict):
            raise ValueError("Checkpoint DynamoDB deve conter um payload JSON em formato de objeto")
        return {
            "revision": _extract_dynamodb_number_attribute(item, CHECKPOINT_DYNAMODB_REVISION_ATTR),
            "state_key": _extract_dynamodb_string_attribute(
                item,
                CHECKPOINT_DYNAMODB_STATE_KEY_ATTR,
                required=False,
            ),
            "payload": snapshot_manager_build_checkpoint_state_payload(
                _safe_str_field(payload.get("table_name"), field_name="checkpoint_dynamodb.table_name"),
                _safe_str_field(payload.get("table_arn"), field_name="checkpoint_dynamodb.table_arn"),
                last_to=_safe_str_field(payload.get("last_to"), field_name="checkpoint_dynamodb.last_to", required=False),
                last_mode=_safe_str_field(payload.get("last_mode"), field_name="checkpoint_dynamodb.last_mode", required=False),
                source=_safe_str_field(payload.get("source"), field_name="checkpoint_dynamodb.source", required=False),
                pending_exports=snapshot_manager_normalize_pending_exports(payload.get("pending_exports")),
            ),
        }

    parsed_item = _deserialize_dynamodb_item(item)
    table_name = _safe_str_field(
        parsed_item.get(CHECKPOINT_DYNAMODB_PARTITION_KEY),
        field_name="checkpoint_dynamodb.table_name",
    )
    table_arn = _safe_str_field(
        parsed_item.get(CHECKPOINT_DYNAMODB_TABLE_ARN_ATTR),
        field_name="checkpoint_dynamodb.table_arn",
    )
    return {
        "revision": _extract_dynamodb_number_attribute(
            item,
            CHECKPOINT_DYNAMODB_REVISION_ATTR,
        ),
        "state_key": _safe_str_field(
            parsed_item.get(CHECKPOINT_DYNAMODB_STATE_KEY_ATTR),
            field_name="checkpoint_dynamodb.state_key",
            required=False,
        ),
        "payload": snapshot_manager_build_checkpoint_state_payload(
            table_name,
            table_arn,
            last_to=_safe_str_field(
                parsed_item.get(CHECKPOINT_DYNAMODB_LAST_TO_ATTR),
                field_name="checkpoint_dynamodb.last_to",
                required=False,
            ),
            last_mode=_safe_str_field(
                parsed_item.get(CHECKPOINT_DYNAMODB_LAST_MODE_ATTR),
                field_name="checkpoint_dynamodb.last_mode",
                required=False,
            ),
            source=_safe_str_field(
                parsed_item.get(CHECKPOINT_DYNAMODB_SOURCE_ATTR),
                field_name="checkpoint_dynamodb.source",
                required=False,
            ),
            pending_exports=snapshot_manager_normalize_pending_exports(
                parsed_item.get(CHECKPOINT_DYNAMODB_PENDING_EXPORTS_ATTR)
            ),
        ),
    }


def _checkpoint_dynamodb_build_current_item(
    *,
    state_key: str,
    checkpoint_state: Dict[str, Any],
    revision: int,
    updated_at: str,
) -> Dict[str, Dict[str, Any]]:
    persisted_state = _checkpoint_dynamodb_build_persisted_state(checkpoint_state)
    return _marshal_dynamodb_item(
        {
            CHECKPOINT_DYNAMODB_PARTITION_KEY: _safe_str_field(
                persisted_state.get("table_name"),
                field_name="checkpoint_state.table_name",
            ),
            CHECKPOINT_DYNAMODB_SORT_KEY: CHECKPOINT_DYNAMODB_CURRENT_RECORD,
            CHECKPOINT_DYNAMODB_STATE_KEY_ATTR: _safe_str_field(
                state_key,
                field_name="checkpoint_state_key",
            ),
            CHECKPOINT_DYNAMODB_REVISION_ATTR: int(revision),
            CHECKPOINT_DYNAMODB_TABLE_ARN_ATTR: _safe_str_field(
                persisted_state.get("table_arn"),
                field_name="checkpoint_state.table_arn",
            ),
            CHECKPOINT_DYNAMODB_LAST_TO_ATTR: _safe_str_field(
                persisted_state.get("last_to"),
                field_name="checkpoint_state.last_to",
                required=False,
            ),
            CHECKPOINT_DYNAMODB_LAST_MODE_ATTR: _safe_str_field(
                persisted_state.get("last_mode"),
                field_name="checkpoint_state.last_mode",
                required=False,
            ),
            CHECKPOINT_DYNAMODB_SOURCE_ATTR: _safe_str_field(
                persisted_state.get("source"),
                field_name="checkpoint_state.source",
                required=False,
            ),
            CHECKPOINT_DYNAMODB_PENDING_EXPORTS_ATTR: snapshot_manager_normalize_pending_exports(
                persisted_state.get("pending_exports")
            ),
            CHECKPOINT_DYNAMODB_UPDATED_AT_ATTR: _safe_str_field(
                updated_at,
                field_name="updated_at",
            ),
        }
    )


def _checkpoint_dynamodb_build_snapshot_record_type(event: Dict[str, Any]) -> str:
    observed_at = _safe_str_field(
        event.get("observed_at"),
        field_name="checkpoint_history.observed_at",
        required=False,
    ) or _dt_to_iso(datetime.now(timezone.utc))
    event_id = _safe_str_field(
        event.get("event_id"),
        field_name="checkpoint_history.event_id",
    )
    return (
        f"{CHECKPOINT_DYNAMODB_SNAPSHOT_RECORD_PREFIX}"
        f"{observed_at}#{event_id}"
    )


def _checkpoint_dynamodb_build_snapshot_item(
    *,
    state_key: str,
    checkpoint_state: Dict[str, Any],
    event: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    persisted_state = _checkpoint_dynamodb_build_persisted_state(checkpoint_state)
    observed_at = _safe_str_field(
        event.get("observed_at"),
        field_name="checkpoint_history.observed_at",
        required=False,
    ) or _dt_to_iso(datetime.now(timezone.utc))
    event_id = _safe_str_field(
        event.get("event_id"),
        field_name="checkpoint_history.event_id",
    )
    return _marshal_dynamodb_item(
        {
            CHECKPOINT_DYNAMODB_PARTITION_KEY: _safe_str_field(
                persisted_state.get("table_name"),
                field_name="checkpoint_state.table_name",
            ),
            CHECKPOINT_DYNAMODB_SORT_KEY: _checkpoint_dynamodb_build_snapshot_record_type(event),
            CHECKPOINT_DYNAMODB_STATE_KEY_ATTR: _safe_str_field(
                state_key,
                field_name="checkpoint_state_key",
            ),
            CHECKPOINT_DYNAMODB_EVENT_ID_ATTR: event_id,
            CHECKPOINT_DYNAMODB_OBSERVED_AT_ATTR: observed_at,
            CHECKPOINT_DYNAMODB_TABLE_ARN_ATTR: _safe_str_field(
                persisted_state.get("table_arn"),
                field_name="checkpoint_state.table_arn",
            ),
            CHECKPOINT_DYNAMODB_LAST_TO_ATTR: _safe_str_field(
                persisted_state.get("last_to"),
                field_name="checkpoint_state.last_to",
                required=False,
            ),
            CHECKPOINT_DYNAMODB_LAST_MODE_ATTR: _safe_str_field(
                persisted_state.get("last_mode"),
                field_name="checkpoint_state.last_mode",
                required=False,
            ),
            CHECKPOINT_DYNAMODB_SOURCE_ATTR: _safe_str_field(
                persisted_state.get("source"),
                field_name="checkpoint_state.source",
                required=False,
            ),
            CHECKPOINT_DYNAMODB_PENDING_EXPORTS_ATTR: snapshot_manager_normalize_pending_exports(
                persisted_state.get("pending_exports")
            ),
            CHECKPOINT_DYNAMODB_CLEAR_STATE_ATTR: bool(event.get("clear_state")),
            CHECKPOINT_DYNAMODB_UPDATED_AT_ATTR: observed_at,
        }
    )


def _checkpoint_dynamodb_put_snapshot_item(
    ddb_client: Any,
    *,
    table_name: str,
    state_key: str,
    checkpoint_state: Dict[str, Any],
    event: Dict[str, Any],
) -> None:
    item = _checkpoint_dynamodb_build_snapshot_item(
        state_key=state_key,
        checkpoint_state=checkpoint_state,
        event=event,
    )
    try:
        ddb_client.put_item(
            TableName=table_name,
            Item=item,
            ConditionExpression="attribute_not_exists(#pk) AND attribute_not_exists(#sk)",
            ExpressionAttributeNames={
                "#pk": CHECKPOINT_DYNAMODB_PARTITION_KEY,
                "#sk": CHECKPOINT_DYNAMODB_SORT_KEY,
            },
        )
    except ClientError as exc:
        if _client_error_code(exc) == "ConditionalCheckFailedException":
            return
        record_type = _extract_dynamodb_string_attribute(
            item,
            CHECKPOINT_DYNAMODB_SORT_KEY,
        )
        raise _build_aws_runtime_error(
            "DynamoDB PutItem checkpoint snapshot",
            exc,
            resource=f"{table_name}:{record_type}",
        ) from exc


def _checkpoint_dynamodb_build_current_put_kwargs(
    *,
    table_name: str,
    item: Dict[str, Any],
    current_revision: int,
) -> Dict[str, Any]:
    base_kwargs: Dict[str, Any] = {
        "TableName": table_name,
        "Item": item,
    }
    if current_revision:
        return {
            **base_kwargs,
            "ConditionExpression": "#revision = :expected_revision",
            "ExpressionAttributeNames": {
                "#revision": CHECKPOINT_DYNAMODB_REVISION_ATTR,
            },
            "ExpressionAttributeValues": {
                ":expected_revision": {"N": str(current_revision)},
            },
        }
    return {
        **base_kwargs,
        "ConditionExpression": "attribute_not_exists(#pk) AND attribute_not_exists(#sk)",
        "ExpressionAttributeNames": {
            "#pk": CHECKPOINT_DYNAMODB_PARTITION_KEY,
            "#sk": CHECKPOINT_DYNAMODB_SORT_KEY,
        },
    }


def _build_checkpoint_dynamodb_schema_error(table_name: str) -> RuntimeError:
    return RuntimeError(
        "Tabela DynamoDB de checkpoint inválida. "
        f"{table_name} deve usar PK={CHECKPOINT_DYNAMODB_PARTITION_KEY} "
        f"e SK={CHECKPOINT_DYNAMODB_SORT_KEY}."
    )


def _load_checkpoint_dynamodb_item(
    ddb_client: Any,
    *,
    table_name: str,
    target_table_name: str,
) -> Optional[Dict[str, Any]]:
    try:
        response = ddb_client.get_item(
            TableName=table_name,
            Key={
                CHECKPOINT_DYNAMODB_PARTITION_KEY: {"S": target_table_name},
                CHECKPOINT_DYNAMODB_SORT_KEY: {"S": CHECKPOINT_DYNAMODB_CURRENT_RECORD},
            },
            ConsistentRead=True,
        )
    except ClientError as exc:
        if _classify_aws_error(_client_error_code(exc)) == "validation":
            raise _build_checkpoint_dynamodb_schema_error(table_name) from exc
        raise
    item = response.get("Item") if isinstance(response, dict) else None
    if not isinstance(item, dict) or not item:
        return None
    if not _checkpoint_dynamodb_is_current_item(item):
        return None
    return _checkpoint_dynamodb_parse_current_item(item)


def checkpoint_load_table_state(
    store: Dict[str, Any],
    *,
    target_table_name: str,
) -> Dict[str, Any]:
    _resolve_checkpoint_store_backend(store)
    table_name = _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name")
    current_item = _load_checkpoint_dynamodb_item(
        store["ddb"],
        table_name=table_name,
        target_table_name=_safe_str_field(
            target_table_name,
            field_name="target_table_name",
        ),
    )
    if not isinstance(current_item, dict):
        return {}
    return _safe_dict_field(current_item.get("payload"), "checkpoint_item.payload")


def _checkpoint_save_dynamodb_table_state(
    ddb_client: Any,
    *,
    table_name: str,
    state_key: str,
    candidate_state: Dict[str, Any],
    observed_at: str,
) -> None:
    target_table_name = _safe_str_field(
        candidate_state.get("table_name"),
        field_name="candidate_state.table_name",
    )

    for _attempt in range(CHECKPOINT_DYNAMODB_MAX_RETRIES):
        current_item = _load_checkpoint_dynamodb_item(
            ddb_client,
            table_name=table_name,
            target_table_name=target_table_name,
        )
        current_state = (
            _safe_dict_field(current_item.get("payload"), "checkpoint_item.payload")
            if isinstance(current_item, dict)
            else {}
        )
        current_revision = int(current_item.get("revision", 0)) if isinstance(current_item, dict) else 0
        events_to_persist = snapshot_manager_collect_unpersisted_history_events(
            current_state,
            candidate_state,
            table_key=state_key,
            observed_at=observed_at,
        )
        if not events_to_persist:
            return

        next_state = current_state
        for event in events_to_persist:
            next_state = snapshot_manager_apply_checkpoint_history_event(
                next_state,
                event,
            )
            _checkpoint_dynamodb_put_snapshot_item(
                ddb_client,
                table_name=table_name,
                state_key=state_key,
                checkpoint_state=next_state,
                event=event,
            )
        next_revision = current_revision + 1
        item = _checkpoint_dynamodb_build_current_item(
            state_key=state_key,
            checkpoint_state=next_state,
            revision=next_revision,
            updated_at=observed_at,
        )
        try:
            put_kwargs = _checkpoint_dynamodb_build_current_put_kwargs(
                table_name=table_name,
                item=item,
                current_revision=current_revision,
            )
            ddb_client.put_item(**put_kwargs)
            return
        except ClientError as exc:
            if _client_error_code(exc) == "ConditionalCheckFailedException":
                continue
            raise _build_aws_runtime_error(
                "DynamoDB PutItem checkpoint",
                exc,
                resource=f"{table_name}:{target_table_name}",
            ) from exc

    raise RuntimeError(
        f"Falha por concorrência ao salvar checkpoint DynamoDB {table_name}:{target_table_name}"
    )


def _checkpoint_save_dynamodb(store: Dict[str, Any], payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Payload de checkpoint deve ser dicionário")

    table_name = _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name")
    ddb_client = store["ddb"]
    payload_to_save = _build_checkpoint_payload_for_save(
        payload,
        now=datetime.now(timezone.utc),
    )
    _log_event(
        "checkpoint.save.start",
        backend="dynamodb",
        table_name=table_name,
        tables=(
            len(payload_to_save.get("tables", {}))
            if isinstance(payload_to_save.get("tables"), dict)
            else 0
        ),
    )
    try:
        observed_at = _safe_str_field(payload_to_save.get("updated_at"), field_name="updated_at")
        for state_key, candidate_state in _normalize_checkpoint_tables(payload_to_save.get("tables")).items():
            _checkpoint_save_dynamodb_table_state(
                ddb_client,
                table_name=table_name,
                state_key=state_key,
                candidate_state=candidate_state,
                observed_at=observed_at,
            )
    except ClientError as exc:
        raise _build_aws_runtime_error(
            "DynamoDB PutItem checkpoint",
            exc,
            resource=table_name,
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Falha inesperada ao salvar checkpoint DynamoDB {table_name}: {exc}"
        ) from exc
    _log_event(
        "checkpoint.save.success",
        backend="dynamodb",
        table_name=table_name,
    )


def checkpoint_save(store: Dict[str, Any], payload: Dict[str, Any]) -> None:
    _resolve_checkpoint_store_backend(store)
    _checkpoint_save_dynamodb(store, payload)


def _resolve_checkpoint_target(config: Dict[str, Any]) -> str:
    return _safe_str_field(
        config.get("checkpoint_dynamodb_table_arn"),
        field_name="checkpoint_dynamodb_table_arn",
    )


def create_snapshot_manager(config: SnapshotConfig) -> Dict[str, Any]:
    session = _build_aws_session(config)
    default_region = _resolve_runtime_region(session.region_name)
    configured_assume_role = _resolve_config_assume_role(config)
    assume_role_template_enabled = _has_role_template_fields(configured_assume_role)
    ddb_client = _get_session_client(session, "dynamodb", region_name=default_region)
    s3_client = _get_session_client(session, "s3")
    manager: Dict[str, Any] = {
        "config": config,
        "session": session,
        "_assume_session": session,
        "_output_session": session,
        "default_region": default_region,
        "_active_assume_role_arn": (
        configured_assume_role
        if configured_assume_role and not assume_role_template_enabled
        else None
        ),
        "assume_role_template_enabled": assume_role_template_enabled,
        "assume_role_enabled": bool(configured_assume_role),
        "_table_client_cache": {},
        "_table_client_lock": threading.Lock(),
        "_execution_context_cache": {},
        "_execution_context_lock": threading.Lock(),
        "_session_identity_cache": {},
        "_session_identity_lock": threading.Lock(),
        "ddb": ddb_client,
        "s3": s3_client,
        "checkpoint_store": build_checkpoint_store_for_session(session, config),
    }
    _log_event(
        "snapshot.manager.ready",
        run_id=config["run_id"],
        mode=config["mode"],
        dry_run=config["dry_run"],
        max_workers=config["max_workers"],
        targets=len(config["targets"]),
        ignore=len(config["ignore"]),
        targets_csv_enabled=bool(config["targets_csv"]),
        ignore_csv_enabled=bool(config["ignore_csv"]),
        checkpoint_backend=_resolve_checkpoint_store_backend(manager["checkpoint_store"]),
        assume_role_enabled=bool(configured_assume_role),
        assume_role_template_enabled=manager["assume_role_template_enabled"],
        default_region=manager["default_region"],
    )
    return manager


def snapshot_manager_build_active_session_fields(
    manager: Dict[str, Any],
    session: boto3.session.Session,
    *,
    assumed_role_arn: Optional[str],
    table_client_cache: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    resolved_region = _resolve_runtime_region(session.region_name)
    resolved_s3 = _get_session_client(session, "s3")
    checkpoint_store = manager.get("checkpoint_store")
    if not isinstance(checkpoint_store, dict):
        checkpoint_store = build_checkpoint_store_for_session(session, manager["config"])
    return {
        "session": session,
        "_assume_session": session,
        "_output_session": manager.get("_output_session", session),
        "_active_assume_role_arn": assumed_role_arn,
        "default_region": resolved_region,
        "ddb": _get_session_client(session, "dynamodb", region_name=resolved_region),
        "s3": resolved_s3,
        "checkpoint_store": checkpoint_store,
        "_table_client_cache": {} if table_client_cache is None else dict(table_client_cache),
        "_execution_context_cache": {},
        "_session_identity_cache": {},
    }


def snapshot_manager_set_active_session(
    manager: Dict[str, Any],
    session: boto3.session.Session,
    *,
    source: str,
    assumed_role_arn: Optional[str] = None,
) -> Dict[str, Any]:
    next_manager = {
        **manager,
        **snapshot_manager_build_active_session_fields(
            manager,
            session,
            assumed_role_arn=assumed_role_arn,
        ),
    }
    _log_event(
        "snapshot.aws_session.set_active",
        source=source,
        run_id=next_manager["config"]["run_id"],
        region=next_manager["default_region"],
        mode="active_session",
    )
    return next_manager


def snapshot_manager_build_table_client_entry(
    session: Any,
    *,
    region: str,
    session_mode: str,
    assume_role_arn: Optional[str],
    table_account_id: Optional[str],
) -> Dict[str, Any]:
    return {
        "session": session,
        "ddb": _get_session_client(session, "dynamodb", region_name=region),
        "s3": _get_session_client(session, "s3"),
        "session_mode": session_mode,
        "assume_role_arn": assume_role_arn,
        "table_account_id": table_account_id,
        "table_region": region,
    }


def snapshot_manager_cache_table_client_entry(
    manager: Dict[str, Any],
    cache_key: str,
    entry: Dict[str, Any],
    *,
    overwrite: bool,
) -> Dict[str, Any]:
    cache = _safe_dict_field(
        _safe_get_field(manager, "_table_client_cache", field_name="manager"),
        "manager._table_client_cache",
    )
    if not overwrite and cache_key in cache:
        return manager
    return {
        **manager,
        "_table_client_cache": {
            **cache,
            cache_key: entry,
        },
    }


def snapshot_manager_get_cached_execution_context(
    manager: Dict[str, Any],
    entry: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    cache = manager.get("_execution_context_cache")
    lock = manager.get("_execution_context_lock")
    if not isinstance(cache, dict) or lock is None:
        return None

    cache_key = snapshot_manager_entry_identity(manager, entry)
    with lock:
        cached = cache.get(cache_key)
    if not isinstance(cached, dict):
        return None
    return dict(cached)


def snapshot_manager_cache_execution_context(
    manager: Dict[str, Any],
    entry: Dict[str, str],
    execution_context: Dict[str, Any],
) -> Dict[str, Any]:
    cache = manager.get("_execution_context_cache")
    lock = manager.get("_execution_context_lock")
    if not isinstance(cache, dict) or lock is None:
        return manager

    cache_key = snapshot_manager_entry_identity(manager, entry)
    with lock:
        cache[cache_key] = dict(execution_context)
    return manager


def snapshot_manager_get_cached_session_identity(
    manager: Dict[str, Any],
    session: Any,
) -> Optional[str]:
    cache = manager.get("_session_identity_cache")
    lock = manager.get("_session_identity_lock")
    if not isinstance(cache, dict) or lock is None:
        return None

    with lock:
        cached = cache.get(id(session))
    return cached if isinstance(cached, str) else None


def snapshot_manager_cache_session_identity(
    manager: Dict[str, Any],
    session: Any,
    caller_arn: str,
) -> str:
    cache = manager.get("_session_identity_cache")
    lock = manager.get("_session_identity_lock")
    if not isinstance(cache, dict) or lock is None:
        return caller_arn

    with lock:
        cache[id(session)] = caller_arn
    return caller_arn


def snapshot_manager_coalesce_manager(candidate: Any, fallback: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(candidate, dict):
        return candidate
    return fallback


def snapshot_manager_unwrap_execution_context(
    candidate: Any,
    fallback_manager: Dict[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    if (
        isinstance(candidate, tuple)
        and len(candidate) == 2
        and isinstance(candidate[1], dict)
    ):
        return snapshot_manager_coalesce_manager(candidate[0], fallback_manager), candidate[1]
    return fallback_manager, _safe_dict_field(candidate, "execution_context")


def snapshot_manager_has_runtime_context(manager: Dict[str, Any]) -> bool:
    return isinstance(manager, dict) and "session" in manager and "default_region" in manager


def snapshot_manager_resolve_parallel_workers(
    manager: Dict[str, Any],
    item_count: int,
) -> int:
    if item_count < 2:
        return 1
    config = manager.get("config") if isinstance(manager, dict) else None
    configured_workers = config.get("max_workers", 1) if isinstance(config, dict) else 1
    try:
        resolved_workers = int(configured_workers)
    except (TypeError, ValueError):
        resolved_workers = 1
    return max(1, min(resolved_workers, item_count))


def snapshot_manager_parallel_map(
    manager: Dict[str, Any],
    items: List[Any],
    worker: Any,
    *,
    stage: str,
    allow_parallel: bool = True,
) -> List[Any]:
    if not items:
        return []

    worker_count = snapshot_manager_resolve_parallel_workers(manager, len(items))
    if not allow_parallel or worker_count <= 1:
        return [worker(item) for item in items]

    _log_event(
        "snapshot.parallel.start",
        stage=stage,
        items=len(items),
        workers=worker_count,
        level=logging.DEBUG,
    )
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        return list(executor.map(worker, items))


def snapshot_manager_can_parallelize_shared_context_stage(manager: Dict[str, Any]) -> bool:
    return not bool(manager.get("assume_role_template_enabled"))


def snapshot_manager_resolve_assume_role_for_table(manager: Dict[str, Any], table_arn: str) -> Optional[str]:
    configured_assume_role = _resolve_config_assume_role(manager["config"])
    if not configured_assume_role:
        return None
    return _render_role_arn_template(
        configured_assume_role,
        table_arn,
        field_name="assume_role",
        allowed_fields=ASSUME_ROLE_TEMPLATE_ALLOWED_FIELDS,
    )

def snapshot_manager_build_dynamic_assumed_session(manager: Dict[str, Any], *, role_arn: str, table_arn: str, region: str):
    dynamic_session_name = _sanitize_role_session_name(
        manager["config"]["assume_role_session_name"],
        manager["config"]["run_id"],
    )
    assume_role_args: Dict[str, Any] = {
        "RoleArn": role_arn,
        "RoleSessionName": dynamic_session_name,
        "DurationSeconds": manager["config"]["assume_role_duration_seconds"],
    }
    if manager["config"]["assume_role_external_id"]:
        assume_role_args["ExternalId"] = manager["config"]["assume_role_external_id"]

    _log_event(
        "aws.assume_role.dynamic.start",
        role_arn=role_arn,
        table_arn=table_arn,
        region=region,
        duration_seconds=manager["config"]["assume_role_duration_seconds"],
        has_external_id=bool(manager["config"]["assume_role_external_id"]),
        session_name=dynamic_session_name,
    )
    try:
        sts_region = region or manager["default_region"]
        sts_client = manager["_assume_session"].client("sts", region_name=sts_region)
        response = sts_client.assume_role(**assume_role_args)
    except ClientError as exc:
        raise _build_aws_runtime_error("STS AssumeRole dinâmico", exc, resource=role_arn) from exc
    except Exception as exc:
        raise RuntimeError(f"Erro inesperado ao executar assume role dinâmico {role_arn}: {exc}") from exc

    credentials = response.get("Credentials") if isinstance(response, dict) else None
    if not isinstance(credentials, dict):
        raise RuntimeError("Resposta inválida do STS AssumeRole dinâmico: Credentials ausente")
    access_key_id = credentials.get("AccessKeyId")
    secret_access_key = credentials.get("SecretAccessKey")
    session_token = credentials.get("SessionToken")
    if not access_key_id or not secret_access_key or not session_token:
        raise RuntimeError(
            "Resposta inválida do STS AssumeRole dinâmico: credenciais incompletas"
        )

    session = boto3.session.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
        region_name=region or manager["default_region"],
    )
    assumed_role_arn = (
        response.get("AssumedRoleUser", {}).get("Arn")
        if isinstance(response, dict)
        else None
    )
    _log_event(
        "aws.assume_role.dynamic.success",
        role_arn=role_arn,
        table_arn=table_arn,
        assumed_role_arn=assumed_role_arn,
        credentials_expiration=credentials.get("Expiration"),
        region=region,
        session_name=dynamic_session_name,
    )
    return session

def snapshot_manager_resolve_table_clients(
    manager: Dict[str, Any],
    table_arn: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    manager_obj = _safe_dict_field(manager, "manager")
    config = _safe_dict_field(_safe_get_field(manager_obj, "config", field_name="manager"), "manager.config")
    context = _extract_table_arn_context(table_arn)
    default_region = _safe_str_field(
        _safe_get_field(manager_obj, "default_region", field_name="manager"),
        field_name="default_region",
    )
    region = context.get("region") or default_region
    if not region:
        raise ValueError(f"Região não encontrada para tabela {table_arn}")

    assume_role_arn = snapshot_manager_resolve_assume_role_for_table(manager, table_arn)
    should_assume_dynamically = bool(
        assume_role_arn and bool(_safe_get_field(manager_obj, "assume_role_template_enabled", field_name="manager"))
    )
    configured_assume_role_arn = _safe_str_field(
        _resolve_config_assume_role(config),
        field_name="assume_role",
        required=False,
    )
    cache_role = assume_role_arn if should_assume_dynamically else "__shared__"
    cache_key = f"{cache_role}|{region}"
    table_client_lock = _safe_get_field(manager_obj, "_table_client_lock", field_name="manager")
    table_client_cache = _safe_dict_field(
        _safe_get_field(manager_obj, "_table_client_cache", field_name="manager"),
        "manager._table_client_cache",
    )
    with table_client_lock:
        cached = table_client_cache.get(cache_key)
    if cached:
        return manager, cached

    active_assume_role_arn = _safe_str_field(
        manager_obj.get("_active_assume_role_arn"),
        field_name="_active_assume_role_arn",
        required=False,
    )
    session = _safe_get_field(manager_obj, "session", field_name="manager")
    next_manager = manager

    if should_assume_dynamically:
        if active_assume_role_arn:
            if assume_role_arn != active_assume_role_arn:
                raise RuntimeError(
                    f"Assume role conflitante para tabela {table_arn}: "
                    f"esperado {active_assume_role_arn}, encontrado {assume_role_arn}"
                )
            session_mode = "assumed_session_reused"
        else:
            session = snapshot_manager_build_dynamic_assumed_session(manager,
                role_arn=assume_role_arn,
                table_arn=table_arn,
                region=region,
            )
            next_manager = snapshot_manager_set_active_session(
                manager,
                session,
                source="table_assume_role",
                assumed_role_arn=assume_role_arn,
            )
            session_mode = "assumed_role_by_table_arn"
    else:
        session_mode = (
            "shared_assumed_session_by_table_region"
            if configured_assume_role_arn
            else "shared_session_by_table_region"
        )

    entry = snapshot_manager_build_table_client_entry(
        session,
        region=region,
        session_mode=session_mode,
        assume_role_arn=(
            assume_role_arn if should_assume_dynamically else configured_assume_role_arn
        ),
        table_account_id=context.get("account_id"),
    )
    with table_client_lock:
        existing = _safe_dict_field(
            _safe_get_field(next_manager, "_table_client_cache", field_name="manager"),
            "manager._table_client_cache",
        ).get(cache_key)
    if existing:
        return next_manager, existing
    next_manager = snapshot_manager_cache_table_client_entry(
        next_manager,
        cache_key,
        entry,
        overwrite=False,
    )
    return next_manager, entry

def snapshot_manager_prime_assumed_session_from_targets(
    manager: Dict[str, Any],
    entries: List[Dict[str, str]],
) -> Dict[str, Any]:
    if not manager["assume_role_enabled"]:
        return manager
    if manager["_active_assume_role_arn"] and not manager["assume_role_template_enabled"]:
        _log_event(
            "snapshot.aws_session.bootstrap_skipped",
            reason="already_assumed_static_role",
            assume_role_arn=manager["_active_assume_role_arn"],
            level=logging.DEBUG,
        )
        return manager
    if not entries:
        return manager

    selected_assume_role_arn: Optional[str] = None
    selected_region: Optional[str] = None
    selected_table_arn: Optional[str] = None

    for entry in entries:
        fields = _extract_entry_fields(entry, source="entry de bootstrap")
        table_arn = _safe_str_field(fields.get("table_arn"), field_name="table_arn", required=False)
        try:
            assume_role_arn = snapshot_manager_resolve_assume_role_for_table(manager, table_arn)
        except ValueError as exc:
            if table_arn.startswith("arn:"):
                _log_event(
                    "snapshot.aws_session.bootstrap_error",
                    table_arn=table_arn,
                    error=str(exc),
                    level=logging.ERROR,
                )
                raise
            configured_assume_role = _resolve_config_assume_role(manager["config"])
            if configured_assume_role and _has_role_template_fields(configured_assume_role):
                _log_event(
                    "snapshot.aws_session.bootstrap_skipped",
                    reason="template_requires_arn",
                    table_ref=table_arn,
                    error=str(exc),
                    level=logging.WARNING,
                )
                continue
            assume_role_arn = configured_assume_role

        if not assume_role_arn:
            continue

        if selected_assume_role_arn is None:
            selected_assume_role_arn = assume_role_arn
        elif assume_role_arn != selected_assume_role_arn:
            _log_event(
                "snapshot.aws_session.bootstrap_conflict",
                reason="multiple_assume_roles_detected",
                selected_assume_role_arn=selected_assume_role_arn,
                table_assume_role_arn=assume_role_arn,
            )
            raise RuntimeError(
                "Não é possível usar um único assume_role para todas as tabelas. "
                "Existem targets com mappings diferentes para assume_role."
            )

        if table_arn.startswith("arn:"):
            selected_table_arn = table_arn
        elif not selected_table_arn:
            selected_table_arn = table_arn

        if table_arn.startswith("arn:") and selected_region is None:
            try:
                context = _extract_table_arn_context(table_arn)
            except Exception as exc:
                _log_event(
                    "snapshot.aws_session.bootstrap_error",
                    table_arn=table_arn,
                    error=str(exc),
                    level=logging.WARNING,
                )
                raise
            selected_region = context.get("region") or manager["default_region"]

        if selected_region is None:
            selected_region = manager["default_region"]

    if not selected_assume_role_arn:
        return manager

    if not selected_region:
        raise RuntimeError("Não foi possível determinar região para assumir role antes do processamento")
    if not selected_table_arn:
        raise RuntimeError("Não foi possível determinar table_arn de bootstrap para assume role")

    active_assume_role_arn = _safe_str_field(
        manager.get("_active_assume_role_arn"),
        field_name="_active_assume_role_arn",
        required=False,
    )
    if active_assume_role_arn:
        if active_assume_role_arn != selected_assume_role_arn:
            _log_event(
                "snapshot.aws_session.bootstrap_conflict",
                reason="active_assumed_role_differs_from_selected_role",
                active_assume_role_arn=active_assume_role_arn,
                selected_assume_role_arn=selected_assume_role_arn,
                level=logging.ERROR,
            )
            raise RuntimeError(
                "Já existe uma sessão assumida ativa para outra role. "
                "O processamento desta execução exige apenas uma assume_role consistente."
            )

        cache_key = f"{selected_assume_role_arn}|{selected_region}"
        manager = snapshot_manager_cache_table_client_entry(
            manager,
            cache_key,
            snapshot_manager_build_table_client_entry(
                manager["session"],
                region=selected_region,
                session_mode="assumed_session_bootstrap_reused",
                assume_role_arn=selected_assume_role_arn,
                table_account_id=(
                    _extract_table_arn_context(selected_table_arn).get("account_id")
                    if selected_table_arn.startswith("arn:")
                    else ""
                ),
            ),
            overwrite=False,
        )
        _log_event(
            "snapshot.aws_session.bootstrap_skipped",
            reason="already_assumed_matching_role",
            assume_role_arn=active_assume_role_arn,
            region=selected_region,
            template_mode=manager["assume_role_template_enabled"],
            level=logging.DEBUG,
        )
        return manager

    assumed_session = snapshot_manager_build_dynamic_assumed_session(manager, 
        role_arn=selected_assume_role_arn,
        table_arn=selected_table_arn,
        region=selected_region,
    )
    manager = snapshot_manager_set_active_session(manager, 
        assumed_session,
        source="targets_bootstrap",
        assumed_role_arn=selected_assume_role_arn,
    )
    cache_key = f"{selected_assume_role_arn}|{selected_region}"
    return snapshot_manager_cache_table_client_entry(
        manager,
        cache_key,
        snapshot_manager_build_table_client_entry(
            assumed_session,
            region=selected_region,
            session_mode="assumed_session_bootstrapped",
            assume_role_arn=selected_assume_role_arn,
            table_account_id=(
                _extract_table_arn_context(selected_table_arn).get("account_id")
                if selected_table_arn.startswith("arn:")
                else ""
            ),
        ),
        overwrite=True,
    )

def snapshot_manager_entry_identity(manager: Dict[str, Any], entry: Dict[str, str]) -> str:
    fields = _extract_entry_fields(entry, source="entry identity")
    table_arn = _safe_str_field(fields.get("table_arn"), field_name="table_arn", required=False)
    table_name = _safe_str_field(fields.get("table_name"), field_name="table_name")
    if table_arn.startswith("arn:"):
        return table_arn.lower()
    return table_name.lower()

def snapshot_manager_build_checkpoint_state_key(manager: Dict[str, Any], entry: Dict[str, str]) -> str:
    fields = _extract_entry_fields(entry, source="entry checkpoint")
    table_arn = _safe_str_field(fields.get("table_arn"), field_name="table_arn", required=False)
    if table_arn.startswith("arn:"):
        return table_arn
    return _safe_str_field(fields.get("table_name"), field_name="table_name")

def snapshot_manager_should_advance_checkpoint(result: Dict[str, Any]) -> bool:
    if not isinstance(result, dict):
        return False

    checkpoint_to = _safe_str_field(
        result.get("checkpoint_to"),
        field_name="checkpoint_to",
        required=False,
    )
    status = _safe_str_field(
        result.get("status"),
        field_name="status",
        required=False,
    ).upper()
    return bool(checkpoint_to and status == "COMPLETED")

def snapshot_manager_compact_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    def _should_include(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value)
        if isinstance(value, list):
            return bool(value)
        return True

    return {key: value for key, value in fields.items() if _should_include(value)}

def snapshot_manager_normalize_pending_export(raw_item: Any) -> Optional[Dict[str, str]]:
    if not isinstance(raw_item, dict):
        return None

    export_arn = _safe_str_field(
        raw_item.get("export_arn"),
        field_name="pending_export.export_arn",
        required=False,
    )
    checkpoint_to = _safe_str_field(
        raw_item.get("checkpoint_to"),
        field_name="pending_export.checkpoint_to",
        required=False,
    )
    if not export_arn or not checkpoint_to:
        return None

    mode = _safe_str_field(
        raw_item.get("mode"),
        field_name="pending_export.mode",
        required=False,
    ).upper() or "UNKNOWN"
    source = _safe_str_field(
        raw_item.get("source"),
        field_name="pending_export.source",
        required=False,
    ) or "native"
    checkpoint_from = _safe_str_field(
        raw_item.get("checkpoint_from"),
        field_name="pending_export.checkpoint_from",
        required=False,
    )
    started_at = _safe_str_field(
        raw_item.get("started_at"),
        field_name="pending_export.started_at",
        required=False,
    )

    return snapshot_manager_compact_fields(
        {
            "export_arn": export_arn,
            "checkpoint_to": checkpoint_to,
            "mode": mode,
            "source": source,
            "checkpoint_from": checkpoint_from,
            "started_at": started_at,
        }
    )

def snapshot_manager_normalize_pending_exports(raw_pending: Any) -> List[Dict[str, str]]:
    if not isinstance(raw_pending, list):
        return []
    return [
        normalized
        for normalized in (
            snapshot_manager_normalize_pending_export(raw_item)
            for raw_item in raw_pending
        )
        if normalized is not None
    ]

def snapshot_manager_build_pending_export_index(
    pending_exports: List[Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    return {
        export_arn: pending_export
        for pending_export in snapshot_manager_normalize_pending_exports(pending_exports)
        for export_arn in [
            _safe_str_field(
                pending_export.get("export_arn"),
                field_name="pending_export.export_arn",
                required=False,
            )
        ]
        if export_arn
    }

def snapshot_manager_resolve_checkpoint_table_key(table_name: str, table_arn: str) -> str:
    resolved_table_arn = _safe_str_field(table_arn, field_name="table_arn", required=False)
    if resolved_table_arn.startswith("arn:"):
        return resolved_table_arn
    return _safe_str_field(table_name, field_name="table_name")

def snapshot_manager_normalize_checkpoint_history_event(
    raw_event: Any,
) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_event, dict):
        return None

    event_id = _safe_str_field(
        raw_event.get("event_id"),
        field_name="checkpoint_history.event_id",
        required=False,
    )
    table_key = _safe_str_field(
        raw_event.get("table_key"),
        field_name="checkpoint_history.table_key",
        required=False,
    )
    table_name = _safe_str_field(
        raw_event.get("table_name"),
        field_name="checkpoint_history.table_name",
        required=False,
    )
    table_arn = _safe_str_field(
        raw_event.get("table_arn"),
        field_name="checkpoint_history.table_arn",
        required=False,
    )
    observed_at = _safe_str_field(
        raw_event.get("observed_at"),
        field_name="checkpoint_history.observed_at",
        required=False,
    )
    last_to = _safe_str_field(
        raw_event.get("last_to"),
        field_name="checkpoint_history.last_to",
        required=False,
    )
    last_mode = _safe_str_field(
        raw_event.get("last_mode"),
        field_name="checkpoint_history.last_mode",
        required=False,
    ).upper()
    source = _safe_str_field(
        raw_event.get("source"),
        field_name="checkpoint_history.source",
        required=False,
    )
    removed_pending_exports = _dedupe_values(
        [
            _safe_str_field(value, field_name="checkpoint_history.removed_pending_export", required=False)
            for value in (
                raw_event.get("removed_pending_exports")
                if isinstance(raw_event.get("removed_pending_exports"), list)
                else []
            )
            if _safe_str_field(value, field_name="checkpoint_history.removed_pending_export", required=False)
        ]
    )
    added_pending_exports = snapshot_manager_normalize_pending_exports(
        raw_event.get("added_pending_exports")
    )
    clear_state = bool(raw_event.get("clear_state"))

    if not (event_id and (table_key or table_name)):
        return None

    return snapshot_manager_compact_fields(
        {
            "event_id": event_id,
            "table_key": table_key or table_name,
            "table_name": table_name,
            "table_arn": table_arn,
            "observed_at": observed_at,
            "last_to": last_to,
            "last_mode": last_mode,
            "source": source,
            "added_pending_exports": added_pending_exports,
            "removed_pending_exports": removed_pending_exports,
            "clear_state": clear_state,
        }
    )

def snapshot_manager_normalize_checkpoint_history(
    raw_history: Any,
) -> List[Dict[str, Any]]:
    if not isinstance(raw_history, list):
        return []

    by_event_id: Dict[str, Dict[str, Any]] = {}
    for raw_event in raw_history:
        normalized_event = snapshot_manager_normalize_checkpoint_history_event(raw_event)
        if normalized_event is None:
            continue
        event_id = _safe_str_field(
            normalized_event.get("event_id"),
            field_name="checkpoint_history.event_id",
        )
        if event_id in by_event_id:
            continue
        by_event_id[event_id] = normalized_event

    return sorted(
        by_event_id.values(),
        key=lambda item: (
            _safe_str_field(item.get("observed_at"), field_name="checkpoint_history.observed_at", required=False),
            _safe_str_field(item.get("event_id"), field_name="checkpoint_history.event_id"),
        ),
    )

def snapshot_manager_collect_unpersisted_history_events(
    current_state: Dict[str, Any],
    candidate_state: Dict[str, Any],
    *,
    table_key: str,
    observed_at: str,
) -> List[Dict[str, Any]]:
    current_history = snapshot_manager_normalize_checkpoint_history(
        current_state.get("history")
    )
    candidate_history = snapshot_manager_normalize_checkpoint_history(
        candidate_state.get("history")
    )
    current_event_ids = {
        _safe_str_field(
            event.get("event_id"),
            field_name="checkpoint_history.event_id",
        )
        for event in current_history
    }

    if candidate_history:
        return [
            event
            for event in candidate_history
            if _safe_str_field(
                event.get("event_id"),
                field_name="checkpoint_history.event_id",
            ) not in current_event_ids
        ]

    if current_history:
        return []

    seed_event = snapshot_manager_build_checkpoint_history_event(
        {},
        candidate_state,
        table_key=table_key,
        observed_at=observed_at,
    )
    if seed_event is None:
        return []
    return [seed_event]

def snapshot_manager_build_checkpoint_history_event(
    current_state: Dict[str, Any],
    candidate_state: Dict[str, Any],
    *,
    table_key: str,
    observed_at: str,
) -> Optional[Dict[str, Any]]:
    current_pending_index = snapshot_manager_build_pending_export_index(
        snapshot_manager_normalize_pending_exports(current_state.get("pending_exports"))
    )
    candidate_pending = snapshot_manager_normalize_pending_exports(
        candidate_state.get("pending_exports")
    )
    candidate_pending_index = snapshot_manager_build_pending_export_index(candidate_pending)

    added_pending_exports = [
        pending_export
        for pending_export in candidate_pending
        if _safe_str_field(
            pending_export.get("export_arn"),
            field_name="pending_export.export_arn",
            required=False,
        ) not in current_pending_index
    ]
    removed_pending_exports = [
        export_arn
        for export_arn in current_pending_index
        if export_arn not in candidate_pending_index
    ]

    current_last_to = _safe_str_field(
        current_state.get("last_to"),
        field_name="current_state.last_to",
        required=False,
    )
    candidate_last_to = _safe_str_field(
        candidate_state.get("last_to"),
        field_name="candidate_state.last_to",
        required=False,
    )
    candidate_last_mode = _safe_str_field(
        candidate_state.get("last_mode"),
        field_name="candidate_state.last_mode",
        required=False,
    ).upper()
    candidate_source = _safe_str_field(
        candidate_state.get("source"),
        field_name="candidate_state.source",
        required=False,
    )
    current_has_state = bool(
        current_last_to
        or current_pending_index
    )
    candidate_has_state = bool(
        candidate_last_to
        or candidate_pending_index
    )
    clear_state = current_has_state and not candidate_has_state

    event_payload = snapshot_manager_compact_fields(
        {
            "table_key": _safe_str_field(table_key, field_name="table_key"),
            "table_name": _safe_str_field(
                candidate_state.get("table_name"),
                field_name="candidate_state.table_name",
                required=False,
            ) or _safe_str_field(
                current_state.get("table_name"),
                field_name="current_state.table_name",
                required=False,
            ),
            "table_arn": _safe_str_field(
                candidate_state.get("table_arn"),
                field_name="candidate_state.table_arn",
                required=False,
            ) or _safe_str_field(
                current_state.get("table_arn"),
                field_name="current_state.table_arn",
                required=False,
            ),
            "last_to": candidate_last_to,
            "last_mode": candidate_last_mode,
            "source": candidate_source,
            "added_pending_exports": added_pending_exports,
            "removed_pending_exports": removed_pending_exports,
            "clear_state": clear_state,
        }
    )

    if not event_payload.get("table_name") or (
        not event_payload.get("last_to")
        and not event_payload.get("added_pending_exports")
        and not event_payload.get("removed_pending_exports")
        and not event_payload.get("clear_state")
    ):
        return None

    event_signature = json.dumps(
        _safe_json(event_payload),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    event_id = hashlib.sha1(event_signature.encode("utf-8")).hexdigest()
    return {
        **event_payload,
        "event_id": event_id,
        "observed_at": _safe_str_field(observed_at, field_name="observed_at"),
    }

def snapshot_manager_apply_checkpoint_history_event(
    checkpoint_state: Dict[str, Any],
    event: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_event = snapshot_manager_normalize_checkpoint_history_event(event)
    state = dict(checkpoint_state) if isinstance(checkpoint_state, dict) else {}
    current_history = snapshot_manager_normalize_checkpoint_history(state.get("history"))

    if normalized_event is None:
        return snapshot_manager_build_checkpoint_state_payload(
            _safe_str_field(
                state.get("table_name"),
                field_name="checkpoint_state.table_name",
                required=False,
            ),
            _safe_str_field(
                state.get("table_arn"),
                field_name="checkpoint_state.table_arn",
                required=False,
            ),
            last_to=_safe_str_field(
                state.get("last_to"),
                field_name="checkpoint_state.last_to",
                required=False,
            ),
            last_mode=_safe_str_field(
                state.get("last_mode"),
                field_name="checkpoint_state.last_mode",
                required=False,
            ),
            source=_safe_str_field(
                state.get("source"),
                field_name="checkpoint_state.source",
                required=False,
            ),
            pending_exports=snapshot_manager_normalize_pending_exports(
                state.get("pending_exports")
            ),
            history=current_history,
        )

    table_name = _safe_str_field(
        normalized_event.get("table_name"),
        field_name="checkpoint_history.table_name",
        required=False,
    ) or _safe_str_field(
        state.get("table_name"),
        field_name="checkpoint_state.table_name",
    )
    table_arn = _safe_str_field(
        normalized_event.get("table_arn"),
        field_name="checkpoint_history.table_arn",
        required=False,
    ) or _safe_str_field(
        state.get("table_arn"),
        field_name="checkpoint_state.table_arn",
    )
    pending_index = snapshot_manager_build_pending_export_index(
        snapshot_manager_normalize_pending_exports(state.get("pending_exports"))
    )
    for export_arn in _dedupe_values(
        _safe_str_field(value, field_name="removed_pending_export", required=False)
        for value in normalized_event.get("removed_pending_exports", [])
    ):
        pending_index.pop(export_arn, None)
    for pending_export in snapshot_manager_normalize_pending_exports(
        normalized_event.get("added_pending_exports")
    ):
        export_arn = _safe_str_field(
            pending_export.get("export_arn"),
            field_name="pending_export.export_arn",
            required=False,
        )
        if export_arn:
            pending_index[export_arn] = pending_export

    next_last_to = _safe_str_field(
        state.get("last_to"),
        field_name="checkpoint_state.last_to",
        required=False,
    )
    next_last_mode = _safe_str_field(
        state.get("last_mode"),
        field_name="checkpoint_state.last_mode",
        required=False,
    )
    next_source = _safe_str_field(
        state.get("source"),
        field_name="checkpoint_state.source",
        required=False,
    )
    event_last_to = _safe_str_field(
        normalized_event.get("last_to"),
        field_name="checkpoint_history.last_to",
        required=False,
    )
    if snapshot_manager_should_replace_checkpoint(next_last_to, event_last_to):
        next_last_to = event_last_to
        next_last_mode = _safe_str_field(
            normalized_event.get("last_mode"),
            field_name="checkpoint_history.last_mode",
            required=False,
        ).upper() or next_last_mode
        next_source = _safe_str_field(
            normalized_event.get("source"),
            field_name="checkpoint_history.source",
            required=False,
        ) or next_source

    next_history = snapshot_manager_normalize_checkpoint_history(
        [*current_history, normalized_event]
    )
    if normalized_event.get("clear_state"):
        next_last_to = ""
        next_last_mode = ""
        next_source = ""
        pending_exports = []
    else:
        pending_exports = list(pending_index.values())

    return snapshot_manager_build_checkpoint_state_payload(
        table_name,
        table_arn,
        last_to=next_last_to,
        last_mode=next_last_mode,
        source=next_source,
        pending_exports=pending_exports,
        history=next_history,
    )

def snapshot_manager_build_checkpoint_state_payload(
    table_name: str,
    table_arn: str,
    *,
    last_to: str = "",
    last_mode: str = "",
    source: str = "",
    pending_exports: Optional[List[Dict[str, str]]] = None,
    history: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return snapshot_manager_compact_fields(
        {
            "table_name": _safe_str_field(table_name, field_name="table_name"),
            "table_arn": _safe_str_field(table_arn, field_name="table_arn"),
            "last_to": _safe_str_field(last_to, field_name="last_to", required=False),
            "last_mode": _safe_str_field(last_mode, field_name="last_mode", required=False).upper(),
            "source": _safe_str_field(source, field_name="source", required=False),
            "pending_exports": snapshot_manager_normalize_pending_exports(pending_exports),
            "history": snapshot_manager_normalize_checkpoint_history(history),
        }
    )

def snapshot_manager_build_checkpoint_state(
    entry: Dict[str, str],
    previous_state: Dict[str, Any],
) -> Dict[str, Any]:
    entry_fields = _extract_entry_fields(entry, source="entry checkpoint state")
    table_name = _safe_str_field(entry_fields.get("table_name"), field_name="table_name")
    table_arn = _safe_str_field(entry_fields.get("table_arn"), field_name="table_arn")
    safe_previous = previous_state if isinstance(previous_state, dict) else {}

    return snapshot_manager_build_checkpoint_state_payload(
        table_name,
        table_arn,
        last_to=_safe_str_field(
            safe_previous.get("last_to"),
            field_name="previous_state.last_to",
            required=False,
        ),
        last_mode=_safe_str_field(
            safe_previous.get("last_mode"),
            field_name="previous_state.last_mode",
            required=False,
        ).upper(),
        source=_safe_str_field(
            safe_previous.get("source"),
            field_name="previous_state.source",
            required=False,
        ),
        pending_exports=snapshot_manager_normalize_pending_exports(
            safe_previous.get("pending_exports")
        ),
        history=snapshot_manager_normalize_checkpoint_history(
            safe_previous.get("history")
        ),
    )

def snapshot_manager_should_replace_checkpoint(current: str, candidate: str) -> bool:
    current_value = _safe_str_field(current, field_name="current_checkpoint", required=False)
    candidate_value = _safe_str_field(candidate, field_name="candidate_checkpoint", required=False)
    if not candidate_value:
        return False
    if not current_value:
        return True

    try:
        return _parse_iso(candidate_value) > _parse_iso(current_value)
    except Exception:
        return candidate_value > current_value

def snapshot_manager_transition_pending_export_state(
    pending_export: Dict[str, str],
    *,
    export_status: str,
    current_last_to: str,
    current_last_mode: str,
    current_source: str,
) -> Dict[str, Any]:
    normalized_status = _safe_str_field(
        export_status,
        field_name="export_status",
        required=False,
    ).upper() or "UNKNOWN"
    checkpoint_to = _safe_str_field(
        pending_export.get("checkpoint_to"),
        field_name="pending_export.checkpoint_to",
        required=False,
    )
    pending_mode = _safe_str_field(
        pending_export.get("mode"),
        field_name="pending_export.mode",
        required=False,
    ).upper() or "UNKNOWN"
    pending_source = _safe_str_field(
        pending_export.get("source"),
        field_name="pending_export.source",
        required=False,
    ) or "native"

    if normalized_status == "COMPLETED":
        should_promote = snapshot_manager_should_replace_checkpoint(current_last_to, checkpoint_to)
        next_last_to = checkpoint_to if should_promote else current_last_to
        next_last_mode = pending_mode if should_promote else current_last_mode
        next_source = pending_source if should_promote else current_source
        return {
            "last_to": next_last_to,
            "last_mode": next_last_mode,
            "source": next_source,
            "keep_pending": False,
            "log_action": "checkpoint.pending.completed",
            "log_level": logging.INFO,
            "status": normalized_status,
        }

    if normalized_status in EXPORT_TERMINAL_FAILURE_STATUSES:
        return {
            "last_to": current_last_to,
            "last_mode": current_last_mode,
            "source": current_source,
            "keep_pending": False,
            "log_action": "checkpoint.pending.terminal",
            "log_level": logging.WARNING,
            "status": normalized_status,
        }

    return {
        "last_to": current_last_to,
        "last_mode": current_last_mode,
        "source": current_source,
        "keep_pending": True,
        "log_action": "checkpoint.pending.in_progress",
        "log_level": logging.INFO,
        "status": normalized_status,
    }

def snapshot_manager_reconcile_pending_exports(
    checkpoint_state: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    ddb_client: Any,
) -> Dict[str, Any]:
    state = dict(checkpoint_state) if isinstance(checkpoint_state, dict) else {}
    resolved_table_name = _safe_str_field(
        state.get("table_name"),
        field_name="checkpoint_state.table_name",
        required=False,
    ) or _safe_str_field(table_name, field_name="table_name")
    resolved_table_arn = _safe_str_field(
        state.get("table_arn"),
        field_name="checkpoint_state.table_arn",
        required=False,
    ) or _safe_str_field(table_arn, field_name="table_arn")
    pending_exports = snapshot_manager_normalize_pending_exports(state.get("pending_exports"))
    current_last_to = _safe_str_field(
        state.get("last_to"),
        field_name="checkpoint_state.last_to",
        required=False,
    )
    current_last_mode = _safe_str_field(
        state.get("last_mode"),
        field_name="checkpoint_state.last_mode",
        required=False,
    ).upper()
    current_source = _safe_str_field(
        state.get("source"),
        field_name="checkpoint_state.source",
        required=False,
    )
    current_history = snapshot_manager_normalize_checkpoint_history(state.get("history"))
    current_state_snapshot = snapshot_manager_build_checkpoint_state_payload(
        resolved_table_name,
        resolved_table_arn,
        last_to=current_last_to,
        last_mode=current_last_mode,
        source=current_source,
        pending_exports=pending_exports,
        history=current_history,
    )
    if not pending_exports:
        return current_state_snapshot
    next_pending: List[Dict[str, str]] = []

    for pending_export in pending_exports:
        export_arn = _safe_str_field(
            pending_export.get("export_arn"),
            field_name="pending_export.export_arn",
        )
        checkpoint_to = _safe_str_field(
            pending_export.get("checkpoint_to"),
            field_name="pending_export.checkpoint_to",
            required=False,
        )

        try:
            response = ddb_client.describe_export(ExportArn=export_arn)
            export_description = response.get("ExportDescription") if isinstance(response, dict) else None
            if not isinstance(export_description, dict):
                raise RuntimeError("DescribeExport sem ExportDescription")
            export_status = _safe_str_field(
                export_description.get("ExportStatus"),
                field_name=f"ExportStatus ({table_name})",
                required=False,
            ).upper() or "UNKNOWN"
        except ClientError as exc:
            _log_event(
                "checkpoint.pending.describe.failed",
                table_name=table_name,
                table_arn=table_arn,
                export_arn=export_arn,
                error_code=_client_error_code(exc),
                error_message=_client_error_message(exc),
                level=logging.WARNING,
            )
            next_pending.append(pending_export)
            continue
        except Exception as exc:
            _log_event(
                "checkpoint.pending.describe.failed",
                table_name=table_name,
                table_arn=table_arn,
                export_arn=export_arn,
                error=str(exc),
                level=logging.WARNING,
            )
            next_pending.append(pending_export)
            continue

        transition = snapshot_manager_transition_pending_export_state(
            pending_export,
            export_status=export_status,
            current_last_to=current_last_to,
            current_last_mode=current_last_mode,
            current_source=current_source,
        )
        current_last_to = _safe_str_field(
            transition.get("last_to"),
            field_name="transition.last_to",
            required=False,
        )
        current_last_mode = _safe_str_field(
            transition.get("last_mode"),
            field_name="transition.last_mode",
            required=False,
        )
        current_source = _safe_str_field(
            transition.get("source"),
            field_name="transition.source",
            required=False,
        )
        _log_event(
            _safe_str_field(
                transition.get("log_action"),
                field_name="transition.log_action",
            ),
            table_name=table_name,
            table_arn=table_arn,
            export_arn=export_arn,
            status=transition.get("status"),
            checkpoint_to=checkpoint_to,
            level=int(transition.get("log_level", logging.INFO)),
        )
        if bool(transition.get("keep_pending")):
            next_pending.append(pending_export)

    next_state = snapshot_manager_build_checkpoint_state_payload(
        resolved_table_name,
        resolved_table_arn,
        last_to=current_last_to,
        last_mode=current_last_mode,
        source=current_source,
        pending_exports=next_pending,
        history=current_history,
    )
    history_event = snapshot_manager_build_checkpoint_history_event(
        current_state_snapshot,
        next_state,
        table_key=snapshot_manager_resolve_checkpoint_table_key(
            resolved_table_name,
            resolved_table_arn,
        ),
        observed_at=_dt_to_iso(datetime.now(timezone.utc)),
    )
    if history_event is None:
        return next_state
    return snapshot_manager_apply_checkpoint_history_event(
        next_state,
        history_event,
    )

def snapshot_manager_has_pending_exports(checkpoint_state: Dict[str, Any]) -> bool:
    if not isinstance(checkpoint_state, dict):
        return False
    return bool(snapshot_manager_normalize_pending_exports(checkpoint_state.get("pending_exports")))

def snapshot_manager_remove_pending_export(
    pending_exports: List[Dict[str, str]],
    export_arn: str,
) -> List[Dict[str, str]]:
    target_export_arn = _safe_str_field(
        export_arn,
        field_name="export_arn",
        required=False,
    )
    if not target_export_arn:
        return list(pending_exports)
    return [
        item
        for item in pending_exports
        if _safe_str_field(
            item.get("export_arn"),
            field_name="pending_export.export_arn",
            required=False,
        ) != target_export_arn
    ]

def snapshot_manager_upsert_pending_export(
    pending_exports: List[Dict[str, str]],
    pending_export: Dict[str, str],
) -> List[Dict[str, str]]:
    export_arn = _safe_str_field(
        pending_export.get("export_arn"),
        field_name="pending_export.export_arn",
    )
    return [
        *snapshot_manager_remove_pending_export(
            snapshot_manager_normalize_pending_exports(pending_exports),
            export_arn,
        ),
        pending_export,
    ]

def snapshot_manager_build_pending_export_from_result(
    manager: Dict[str, Any],
    result: Dict[str, Any],
) -> Optional[Dict[str, str]]:
    if not isinstance(result, dict):
        return None
    status = _safe_str_field(result.get("status"), field_name="result.status", required=False).upper()
    if status not in EXPORT_PENDING_STATUSES:
        return None

    export_arn = _safe_str_field(result.get("export_arn"), field_name="result.export_arn", required=False)
    checkpoint_to = _safe_str_field(result.get("checkpoint_to"), field_name="result.checkpoint_to", required=False)
    if not export_arn or not checkpoint_to:
        return None

    mode = _safe_str_field(result.get("mode"), field_name="result.mode", required=False).upper() or "UNKNOWN"
    source = _safe_str_field(result.get("source"), field_name="result.source", required=False) or "native"
    checkpoint_from = _safe_str_field(
        result.get("checkpoint_from"),
        field_name="result.checkpoint_from",
        required=False,
    )

    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    run_time = _safe_get_field(config, "run_time", field_name="manager.config")
    started_at = _dt_to_iso(run_time if isinstance(run_time, datetime) else datetime.now(timezone.utc))
    return snapshot_manager_compact_fields(
        {
            "export_arn": export_arn,
            "checkpoint_to": checkpoint_to,
            "mode": mode,
            "source": source,
            "started_at": started_at,
            "checkpoint_from": checkpoint_from,
        }
    )

def snapshot_manager_apply_result_to_checkpoint_state(
    manager: Dict[str, Any],
    checkpoint_state: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    state = dict(checkpoint_state) if isinstance(checkpoint_state, dict) else {}
    table_name = _safe_str_field(
        state.get("table_name"),
        field_name="checkpoint_state.table_name",
        required=False,
    ) or _safe_str_field(
        result.get("table_name"),
        field_name="result.table_name",
    )
    table_arn = _safe_str_field(
        state.get("table_arn"),
        field_name="checkpoint_state.table_arn",
        required=False,
    ) or _safe_str_field(
        result.get("table_arn"),
        field_name="result.table_arn",
    )
    pending_exports = snapshot_manager_normalize_pending_exports(state.get("pending_exports"))
    result_export_arn = _safe_str_field(
        result.get("export_arn"),
        field_name="result.export_arn",
        required=False,
    )
    pending_without_current = snapshot_manager_remove_pending_export(
        pending_exports,
        result_export_arn,
    )

    checkpoint_to = _safe_str_field(
        result.get("checkpoint_to"),
        field_name="result.checkpoint_to",
        required=False,
    )
    status = _safe_str_field(
        result.get("status"),
        field_name="result.status",
        required=False,
    ).upper()
    mode = _safe_str_field(
        result.get("mode"),
        field_name="result.mode",
        required=False,
    ).upper()
    source = _safe_str_field(
        result.get("source"),
        field_name="result.source",
        required=False,
    )
    current_last_to = _safe_str_field(
        state.get("last_to"),
        field_name="checkpoint_state.last_to",
        required=False,
    )
    current_last_mode = _safe_str_field(
        state.get("last_mode"),
        field_name="checkpoint_state.last_mode",
        required=False,
    )
    current_source = _safe_str_field(
        state.get("source"),
        field_name="checkpoint_state.source",
        required=False,
    )
    current_history = snapshot_manager_normalize_checkpoint_history(state.get("history"))

    next_last_to = current_last_to
    next_last_mode = current_last_mode
    next_source = current_source
    if checkpoint_to and status == "COMPLETED":
        if snapshot_manager_should_replace_checkpoint(current_last_to, checkpoint_to):
            next_last_to = checkpoint_to
            if mode:
                next_last_mode = mode
            if source:
                next_source = source

    pending_export = snapshot_manager_build_pending_export_from_result(manager, result)
    next_pending = (
        snapshot_manager_upsert_pending_export(pending_without_current, pending_export)
        if pending_export
        else pending_without_current
    )

    next_state = snapshot_manager_build_checkpoint_state_payload(
        table_name,
        table_arn,
        last_to=next_last_to,
        last_mode=next_last_mode,
        source=next_source,
        pending_exports=next_pending,
        history=current_history,
    )
    history_event = snapshot_manager_build_checkpoint_history_event(
        state,
        next_state,
        table_key=snapshot_manager_resolve_checkpoint_table_key(table_name, table_arn),
        observed_at=_resolve_optional_text(
            result.get("started_at"),
            result.get("checkpoint_to"),
            _dt_to_iso(datetime.now(timezone.utc)),
        ) or _dt_to_iso(datetime.now(timezone.utc)),
    )
    if history_event is None:
        return next_state
    return snapshot_manager_apply_checkpoint_history_event(
        next_state,
        history_event,
    )

def snapshot_manager_assign_table_state(
    table_state: Dict[str, Any],
    state_key: str,
    state_value: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        **table_state,
        state_key: state_value,
    }

def snapshot_manager_remove_table_state_keys(
    table_state: Dict[str, Any],
    keys: List[str],
) -> Dict[str, Any]:
    normalized_keys = [
        _safe_str_field(key, field_name="table_state_key", required=False)
        for key in keys
    ]
    keys_to_remove = {key for key in normalized_keys if key}
    if not keys_to_remove:
        return dict(table_state)
    return {
        key: value
        for key, value in table_state.items()
        if key not in keys_to_remove
    }

def snapshot_manager_reduce_table_checkpoint_state(
    manager: Dict[str, Any],
    table_state: Dict[str, Any],
    entry: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    checkpoint_state = result.get("checkpoint_state")
    if isinstance(checkpoint_state, dict):
        state_key = snapshot_manager_build_checkpoint_state_key(manager, entry)
        table_name = _safe_str_field(
            entry.get("table_name"),
            field_name="table_name",
            required=False,
        )
        checkpoint_last_to = _safe_str_field(
            checkpoint_state.get("last_to"),
            field_name="checkpoint_state.last_to",
            required=False,
        )
        has_pending_exports = snapshot_manager_has_pending_exports(checkpoint_state)
        has_history = bool(
            snapshot_manager_normalize_checkpoint_history(checkpoint_state.get("history"))
        )
        if checkpoint_last_to or has_pending_exports or has_history:
            return snapshot_manager_assign_table_state(table_state, state_key, checkpoint_state)
        stale_keys = [state_key]
        if table_name and table_name != state_key:
            stale_keys.append(table_name)
        return snapshot_manager_remove_table_state_keys(table_state, stale_keys)

    if snapshot_manager_should_advance_checkpoint(result):
        table_name = _safe_str_field(entry.get("table_name"), field_name="table_name")
        table_arn = _safe_str_field(entry.get("table_arn"), field_name="table_arn")
        state_key = snapshot_manager_build_checkpoint_state_key(manager, entry)
        checkpoint_state_payload = snapshot_manager_build_checkpoint_state_payload(
            table_name,
            table_arn,
            last_to=_safe_str_field(result.get("checkpoint_to"), field_name="checkpoint_to"),
            last_mode=_safe_str_field(result.get("mode"), field_name="mode"),
            source=_safe_str_field(
                result.get("source"),
                field_name="source",
                required=False,
            ) or "native",
        )
        return snapshot_manager_assign_table_state(table_state, state_key, checkpoint_state_payload)

    if result.get("checkpoint_to"):
        _log_event(
            "snapshot.checkpoint.skipped",
            table_name=entry.get("table_name"),
            table_arn=entry.get("table_arn"),
            status=result.get("status"),
            reason="result_not_completed",
        )
    return dict(table_state)

def snapshot_manager_build_failed_table_result(
    manager: Dict[str, Any],
    entry: Dict[str, Any],
    error: BaseException,
) -> Dict[str, Any]:
    table_name = _safe_str_field(entry.get("table_name"), field_name="table_name", required=False)
    table_arn = _safe_str_field(entry.get("table_arn"), field_name="table_arn", required=False)
    _log_event(
        "snapshot.table.failed",
        table_name=table_name,
        table_arn=table_arn,
        error=str(error),
        level=logging.ERROR,
    )
    return _build_table_error_result(
        table_name=table_name,
        table_arn=table_arn,
        mode=manager["config"]["mode"].upper(),
        error=error,
        dry_run=manager["config"]["dry_run"],
    )

def snapshot_manager_resolve_table_future_result(
    manager: Dict[str, Any],
    entry: Dict[str, Any],
    future: Any,
) -> Dict[str, Any]:
    try:
        result = future.result()
        _log_event(
            "snapshot.table.completed",
            table_name=entry.get("table_name"),
            status=result.get("status"),
            mode=result.get("mode"),
            source=result.get("source"),
        )
        return result
    except Exception as exc:  # pragma: no cover
        logger.exception(
            "Falha processando tabela %s",
            _safe_str_field(entry.get("table_name"), field_name="table_name", required=False),
        )
        return snapshot_manager_build_failed_table_result(manager, entry, exc)

def snapshot_manager_load_previous_state(manager: Dict[str, Any], table_state: Dict[str, Any], entry: Dict[str, str]) -> Dict[str, Any]:
    if not isinstance(table_state, dict):
        return {}
    state_key = snapshot_manager_build_checkpoint_state_key(manager, entry)
    table_name = _safe_str_field(entry.get("table_name"), field_name="table_name", required=False)
    checkpoint_store = manager.get("checkpoint_store") if isinstance(manager, dict) else None
    state = table_state.get(state_key)
    if state is None and table_name:
        state = table_state.get(table_name)
    if state is None and table_name and isinstance(checkpoint_store, dict):
        _resolve_checkpoint_store_backend(checkpoint_store)
        state = checkpoint_load_table_state(
            checkpoint_store,
            target_table_name=table_name,
        )
        if isinstance(state, dict) and state:
            table_state[state_key] = state
    if not isinstance(state, dict):
        return {}
    return state


def snapshot_manager_resolve_table_storage_context(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    resolved_table_name = _safe_str_field(table_name, field_name="table_name")
    resolved_table_arn = _safe_str_field(table_arn, field_name="table_arn", required=False)
    account_id = ""
    region = ""

    if resolved_table_arn.startswith("arn:"):
        try:
            arn_context = _extract_table_arn_context(resolved_table_arn)
            account_id = _safe_str_field(
                arn_context.get("account_id"),
                field_name="table_account_id",
                required=False,
            )
            region = _safe_str_field(
                arn_context.get("region"),
                field_name="table_region",
                required=False,
            )
        except ValueError:
            account_id = ""
            region = ""

    if execution_context:
        if not account_id:
            account_id = _safe_str_field(
                execution_context.get("table_account_id"),
                field_name="execution_context.table_account_id",
                required=False,
            )
        if not region:
            region = _safe_str_field(
                execution_context.get("table_region"),
                field_name="execution_context.table_region",
                required=False,
            )

    if not region:
        region = _safe_str_field(
            manager.get("default_region"),
            field_name="default_region",
            required=False,
        )

    return {
        "table_name": resolved_table_name,
        "table_arn": resolved_table_arn,
        "account_id": account_id,
        "region": region,
    }


def snapshot_manager_build_table_storage_scope(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
    use_unknown: bool = True,
) -> Optional[str]:
    storage_context = snapshot_manager_resolve_table_storage_context(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    account_id = _safe_str_field(
        storage_context.get("account_id"),
        field_name="storage_context.account_id",
        required=False,
    )
    region = _safe_str_field(
        storage_context.get("region"),
        field_name="storage_context.region",
        required=False,
    )

    if use_unknown:
        account_id = account_id or "unknown"
        region = region or "unknown"

    if not account_id or not region:
        return None

    return f"account={account_id}/region={region}/table={storage_context['table_name']}"


def snapshot_manager_build_export_storage_scope(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
    use_unknown: bool = True,
) -> Optional[str]:
    storage_context = snapshot_manager_resolve_table_storage_context(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    account_id = _safe_str_field(
        storage_context.get("account_id"),
        field_name="storage_context.account_id",
        required=False,
    )
    if use_unknown:
        account_id = account_id or "unknown"
    if not account_id:
        return None
    return f"{account_id}/{storage_context['table_name']}"


def snapshot_manager_build_export_date_segment(manager: Dict[str, Any]) -> str:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    run_time = _safe_get_field(config, "run_time", field_name="manager.config")
    if not isinstance(run_time, datetime):
        raise RuntimeError("manager.config.run_time deve ser datetime")
    resolved_run_time = run_time if run_time.tzinfo else run_time.replace(tzinfo=timezone.utc)
    return resolved_run_time.astimezone(timezone.utc).strftime("%Y%m%d")


def snapshot_manager_build_export_base_prefix(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
    use_unknown: bool = True,
) -> str:
    export_date_segment = snapshot_manager_build_export_date_segment(manager)
    table_scope = snapshot_manager_build_export_storage_scope(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
        use_unknown=use_unknown,
    )
    if not table_scope:
        raise RuntimeError(
            f"Não foi possível montar o escopo de storage para a tabela {table_name}"
        )
    return f"{EXPORT_LAYOUT_PREFIX}/{export_date_segment}/{table_scope}"


def snapshot_manager_resolve_export_layout_s3_client(
    manager: Dict[str, Any],
    *,
    execution_context: Optional[Dict[str, Any]] = None,
) -> Optional[Any]:
    if execution_context:
        scoped_client = execution_context.get("s3")
        if scoped_client is not None:
            return scoped_client
    global_client = manager.get("s3")
    if global_client is not None:
        return global_client
    return None


def snapshot_manager_resolve_next_incremental_export_type(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
    use_unknown: bool = True,
) -> str:
    bucket = snapshot_manager_resolve_snapshot_bucket(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    base_prefix = snapshot_manager_build_export_base_prefix(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
        use_unknown=use_unknown,
    )
    s3_client = snapshot_manager_resolve_export_layout_s3_client(
        manager,
        execution_context=execution_context,
    )
    if s3_client is None:
        _log_event(
            "export.incremental.type.lookup.skip",
            table_name=table_name,
            table_arn=table_arn,
            prefix=base_prefix,
            reason="s3_client_not_available",
            level=logging.DEBUG,
        )
        return "INCR"

    max_incremental_index = 0
    lookup_prefix = f"{base_prefix}/INCR"
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=lookup_prefix,
            Delimiter="/",
        ):
            if not isinstance(page, dict):
                continue
            common_prefixes = page.get("CommonPrefixes")
            if not isinstance(common_prefixes, list):
                continue
            for raw_prefix in common_prefixes:
                if not isinstance(raw_prefix, dict):
                    continue
                prefix = _safe_str_field(
                    raw_prefix.get("Prefix"),
                    field_name="list_objects_v2.common_prefix",
                    required=False,
                ).strip("/")
                if not prefix:
                    continue
                prefix_segments = prefix.split("/")
                export_type = prefix_segments[-1] if prefix_segments else ""
                incremental_index = _parse_incremental_export_index(export_type)
                if incremental_index > max_incremental_index:
                    max_incremental_index = incremental_index
    except ClientError as exc:
        code = _client_error_code(exc)
        message = _client_error_message(exc)
        if _is_s3_access_denied_error(code):
            _log_event(
                "export.incremental.type.lookup.access_denied",
                table_name=table_name,
                table_arn=table_arn,
                bucket=bucket,
                prefix=lookup_prefix,
                error_code=code,
                error_message=message,
                level=logging.WARNING,
            )
            return "INCR"
        raise _build_aws_runtime_error(
            "S3 ListObjectsV2 incremental export type lookup",
            exc,
            resource=f"s3://{bucket}/{lookup_prefix}",
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Falha ao resolver próximo tipo incremental da tabela {table_name}: {exc}"
        ) from exc

    next_incremental_index = max_incremental_index + 1
    return "INCR" if next_incremental_index == 1 else f"INCR{next_incremental_index}"


def snapshot_manager_resolve_export_type_segment(
    manager: Dict[str, Any],
    mode_segment: str,
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
    use_unknown: bool = True,
) -> str:
    normalized_mode = _safe_str_field(mode_segment, field_name="mode_segment").lower()
    if normalized_mode == "full":
        return "FULL"
    if normalized_mode.startswith("incremental"):
        return snapshot_manager_resolve_next_incremental_export_type(
            manager,
            table_name,
            table_arn,
            execution_context=execution_context,
            use_unknown=use_unknown,
        )
    raise ValueError(f"mode_segment inválido para path de export: {mode_segment}")


def snapshot_manager_build_mode_prefix(
    manager: Dict[str, Any],
    mode_segment: str,
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
    use_unknown: bool = True,
) -> str:
    export_type = snapshot_manager_resolve_export_type_segment(
        manager,
        mode_segment,
        table_name,
        table_arn,
        execution_context=execution_context,
        use_unknown=use_unknown,
    )
    base_prefix = snapshot_manager_build_export_base_prefix(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
        use_unknown=use_unknown,
    )
    return f"{base_prefix}/{export_type}"


def snapshot_manager_build_full_lookup_prefixes(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    execution_context: Optional[Dict[str, Any]] = None,
) -> List[str]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    s3_base_prefix = _safe_str_field(config.get("s3_prefix"), field_name="s3_prefix")
    prefixes: List[str] = [f"{EXPORT_LAYOUT_PREFIX}/"]

    scoped_table_scope = snapshot_manager_build_table_storage_scope(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
        use_unknown=False,
    )
    if scoped_table_scope:
        prefixes.append(f"{s3_base_prefix}/mode=full/{scoped_table_scope}/")

    prefixes.append(f"{s3_base_prefix}/mode=full/table={table_name}/")
    return _dedupe_values(prefixes)


def snapshot_manager_find_latest_full_export_checkpoint(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    s3_client: Any,
    execution_context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    bucket = snapshot_manager_resolve_snapshot_bucket(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    lookup_prefixes = snapshot_manager_build_full_lookup_prefixes(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    storage_context = snapshot_manager_resolve_table_storage_context(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    target_account_id = _safe_str_field(
        storage_context.get("account_id"),
        field_name="storage_context.account_id",
        required=False,
    )

    _log_event(
        "snapshot.incremental.bootstrap.lookup.start",
        table_name=table_name,
        bucket=bucket,
        prefixes=lookup_prefixes,
    )

    latest_reference_id: Optional[str] = None
    latest_checkpoint_from: Optional[str] = None
    latest_full_run_id: Optional[str] = None
    latest_key: Optional[str] = None
    latest_prefix: Optional[str] = None

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for lookup_prefix in lookup_prefixes:
            for page in paginator.paginate(Bucket=bucket, Prefix=lookup_prefix):
                if not isinstance(page, dict):
                    continue
                contents = page.get("Contents")
                if not isinstance(contents, list):
                    continue
                for item in contents:
                    if not isinstance(item, dict):
                        continue
                    key = _safe_str_field(item.get("Key"), field_name="list_objects_v2.key", required=False)
                    if not _is_full_export_completion_key(key):
                        continue
                    if key.startswith(f"{EXPORT_LAYOUT_PREFIX}/"):
                        if not _is_new_layout_full_completion_key_for_table(
                            key,
                            table_name=table_name,
                            account_id=target_account_id,
                        ):
                            continue
                    reference = _resolve_full_export_reference_from_item(item, key)
                    if not reference:
                        continue
                    reference_id = _safe_str_field(reference.get("order_key"), field_name="full_reference.order_key")
                    if latest_reference_id is None or reference_id > latest_reference_id:
                        latest_reference_id = reference_id
                        latest_checkpoint_from = _safe_str_field(
                            reference.get("checkpoint_from"),
                            field_name="full_reference.checkpoint_from",
                        )
                        latest_full_run_id = _safe_str_field(
                            reference.get("full_run_id"),
                            field_name="full_reference.full_run_id",
                        )
                        latest_key = key
                        latest_prefix = lookup_prefix
    except ClientError as exc:
        code = _client_error_code(exc)
        message = _client_error_message(exc)
        if _is_s3_access_denied_error(code):
            logger.warning(
                "Sem permissão para listar exports FULL anteriores em s3://%s (%s). Seguindo sem bootstrap automático.",
                bucket,
                ", ".join(lookup_prefixes),
            )
            _log_event(
                "snapshot.incremental.bootstrap.lookup.access_denied",
                table_name=table_name,
                bucket=bucket,
                prefixes=lookup_prefixes,
                error_code=code,
                error_message=message,
                level=logging.WARNING,
            )
            return None
        raise _build_aws_runtime_error(
            "S3 ListObjectsV2 full export lookup",
            exc,
            resource=f"s3://{bucket}/{' | '.join(lookup_prefixes)}",
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Falha ao buscar FULL anterior para bootstrap incremental da tabela {table_name}: {exc}"
        ) from exc

    if not latest_reference_id:
        _log_event(
            "snapshot.incremental.bootstrap.lookup.not_found",
            table_name=table_name,
            bucket=bucket,
            prefixes=lookup_prefixes,
            level=logging.INFO,
        )
        return None

    checkpoint_from = _safe_str_field(
        latest_checkpoint_from,
        field_name="latest_checkpoint_from",
        required=False,
    )
    if not checkpoint_from:
        run_id_for_checkpoint = _safe_str_field(
            latest_full_run_id or latest_reference_id,
            field_name="latest_full_run_id",
        )
        checkpoint_from = _dt_to_iso(_parse_run_id(run_id_for_checkpoint))
    resolved_prefix = _extract_export_prefix_from_key(latest_key or "")
    if not resolved_prefix and latest_prefix:
        if latest_full_run_id:
            resolved_prefix = f"{latest_prefix}run_id={latest_full_run_id}"
        else:
            resolved_prefix = latest_prefix.rstrip("/")
    _log_event(
        "snapshot.incremental.bootstrap.lookup.found",
        table_name=table_name,
        bucket=bucket,
        prefixes=lookup_prefixes,
        matched_prefix=latest_prefix,
        full_run_id=latest_full_run_id,
        full_export_s3_prefix=resolved_prefix,
        matched_key=latest_key,
        checkpoint_from=checkpoint_from,
    )
    return {
        "checkpoint_from": checkpoint_from,
        "checkpoint_source": "full_export_s3",
        "full_run_id": latest_full_run_id,
        "full_export_s3_prefix": resolved_prefix,
    }


def snapshot_manager_resolve_incremental_reference(
    manager: Dict[str, Any],
    entry: Dict[str, str],
    previous_state: Dict[str, Any],
    *,
    s3_client: Any,
    execution_context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    entry_fields = _extract_entry_fields(entry, source="entry incremental")
    table_name = _safe_str_field(entry_fields.get("table_name"), field_name="table_name")
    table_arn = _safe_str_field(entry_fields.get("table_arn"), field_name="table_arn")
    if not isinstance(previous_state, dict):
        previous_state = {}

    checkpoint_from = _safe_str_field(
        previous_state.get("last_to"),
        field_name="previous_state.last_to",
        required=False,
    )
    if checkpoint_from:
        return {
            "checkpoint_from": checkpoint_from,
            "checkpoint_source": "checkpoint",
            "full_run_id": None,
            "full_export_s3_prefix": None,
        }

    return snapshot_manager_find_latest_full_export_checkpoint(
        manager,
        table_name,
        table_arn,
        s3_client=s3_client,
        execution_context=execution_context,
    )

def snapshot_manager_load_csv_source(manager: Dict[str, Any], raw_source: str, *, source_name: str) -> str:
    manager_obj = _safe_dict_field(manager, "manager")
    source = _safe_str_field(raw_source, field_name=source_name)
    if source.startswith("s3://"):
        bucket, key = _parse_s3_uri(source)
        _log_event("csv.source.load.start", source=source_name, mode="s3", bucket=bucket, key=key)
        try:
            s3_client = _safe_get_field(manager_obj, "s3", field_name="manager")
            obj = s3_client.get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = _client_error_code(exc)
            message = _client_error_message(exc)
            _log_event(
                "csv.source.load.failed",
                source=source_name,
                mode="s3",
                bucket=bucket,
                key=key,
                error_code=code,
                error_message=message,
                level=logging.ERROR,
            )
            if _is_s3_access_denied_error(code):
                raise PermissionError(
                    f"Sem permissão para GetObject no CSV {source_name} ({source}). "
                    f"Conceda s3:GetObject para o objeto (code={code})."
                ) from exc
            if _is_s3_missing_object_error(code):
                raise FileNotFoundError(f"Objeto CSV não encontrado para {source_name}: {source}") from exc
            raise RuntimeError(f"Falha ao ler CSV {source_name} em {source}: {code} - {message}") from exc
        except Exception as exc:
            raise RuntimeError(f"Falha inesperada ao ler CSV {source_name} em {source}: {exc}") from exc

        body = obj.get("Body") if isinstance(obj, dict) else None
        if body is None:
            raise RuntimeError(f"Resposta de get_object sem Body para CSV {source_name} ({source})")
        try:
            payload = body.read().decode("utf-8")
        except Exception as exc:
            raise RuntimeError(f"Falha ao decodificar CSV {source_name} ({source}) como UTF-8: {exc}") from exc
        _log_event(
            "csv.source.load.success",
            source=source_name,
            mode="s3",
            bucket=bucket,
            key=key,
            bytes=len(payload.encode("utf-8")),
        )
        return payload

    local_path = _extract_local_file_path(source)
    if local_path:
        _log_event("csv.source.load.start", source=source_name, mode="local_file", path=local_path)
        try:
            with open(local_path, "r", encoding="utf-8") as file_handle:
                payload = file_handle.read()
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"Arquivo CSV não encontrado para {source_name}: {local_path}") from exc
        except PermissionError as exc:
            raise PermissionError(f"Sem permissão para ler arquivo CSV {source_name}: {local_path}") from exc
        except Exception as exc:
            raise RuntimeError(f"Falha ao ler arquivo CSV {source_name} ({local_path}): {exc}") from exc

        _log_event(
            "csv.source.load.success",
            source=source_name,
            mode="local_file",
            path=local_path,
            bytes=len(payload.encode("utf-8")),
        )
        return payload

    _log_event(
        "csv.source.load.success",
        source=source_name,
        mode="inline",
        bytes=len(source.encode("utf-8")),
    )
    return source

def snapshot_manager_resolve_targets(manager: Dict[str, Any]) -> List[str]:
    merged_targets = list(manager["config"]["targets"])
    csv_targets = []
    if manager["config"]["targets_csv"]:
        try:
            csv_payload = snapshot_manager_load_csv_source(manager, manager["config"]["targets_csv"], source_name="targets_csv")
            csv_targets = _parse_targets_csv(csv_payload)
            _log_event(
                "csv.parse.targets",
                source="targets_csv",
                parsed_count=len(csv_targets),
            )
        except PermissionError as exc:
            _log_event(
                "csv.parse.targets.access_denied",
                source="targets_csv",
                direct_targets_available=bool(merged_targets),
                error=str(exc),
                level=logging.WARNING,
            )
            if merged_targets:
                logger.warning("%s Seguindo com targets diretos.", exc)
            else:
                raise ValueError(str(exc)) from exc
        except FileNotFoundError as exc:
            _log_event(
                "csv.parse.targets.not_found",
                source="targets_csv",
                direct_targets_available=bool(merged_targets),
                error=str(exc),
                level=logging.WARNING,
            )
            if merged_targets:
                logger.warning("%s Seguindo com targets diretos.", exc)
            else:
                raise ValueError(str(exc)) from exc
    merged_targets.extend(csv_targets)
    resolved = _dedupe_values(merged_targets)
    _log_event(
        "snapshot.targets.resolved",
        direct_count=len(manager["config"]["targets"]),
        csv_count=len(csv_targets),
        final_count=len(resolved),
    )
    return resolved

def snapshot_manager_resolve_ignore(manager: Dict[str, Any]) -> List[str]:
    merged_ignore = list(manager["config"]["ignore"])
    csv_ignore = []
    if manager["config"]["ignore_csv"]:
        try:
            csv_payload = snapshot_manager_load_csv_source(manager, manager["config"]["ignore_csv"], source_name="ignore_csv")
            csv_ignore = _parse_ignore_csv(csv_payload)
            _log_event(
                "csv.parse.ignore",
                source="ignore_csv",
                parsed_count=len(csv_ignore),
            )
        except PermissionError as exc:
            logger.warning("%s Seguindo sem ignore_csv.", exc)
            _log_event(
                "csv.parse.ignore.access_denied",
                source="ignore_csv",
                error=str(exc),
                level=logging.WARNING,
            )
        except FileNotFoundError as exc:
            logger.warning("%s Seguindo sem ignore_csv.", exc)
            _log_event(
                "csv.parse.ignore.not_found",
                source="ignore_csv",
                error=str(exc),
                level=logging.WARNING,
            )
    merged_ignore.extend(csv_ignore)
    resolved = _dedupe_values(merged_ignore, case_insensitive=True)
    _log_event(
        "snapshot.ignore.resolved",
        direct_count=len(manager["config"]["ignore"]),
        csv_count=len(csv_ignore),
        final_count=len(resolved),
    )
    return resolved

def snapshot_manager_prime_assumed_session_from_direct_targets(manager: Dict[str, Any]) -> Dict[str, Any]:
    if not manager["assume_role_enabled"]:
        return manager
    if manager["_active_assume_role_arn"]:
        return manager

    direct_targets = manager["config"]["targets"]
    if not direct_targets:
        return manager

    bootstrap_entries: List[Dict[str, str]] = []
    for ref in direct_targets:
        table_name = _extract_table_name(ref)
        if not table_name:
            continue
        bootstrap_entries.append({"table_name": table_name, "table_arn": ref})

    if not bootstrap_entries:
        return manager

    _log_event(
        "snapshot.aws_session.bootstrap.pre_resolution",
        direct_target_count=len(bootstrap_entries),
        template_mode=manager["assume_role_template_enabled"],
    )
    return snapshot_manager_prime_assumed_session_from_targets(manager, bootstrap_entries)

def snapshot_manager_build_preflight_target_failure(
    manager: Dict[str, Any],
    target_ref: str,
    error: BaseException,
) -> Dict[str, Any]:
    return _build_table_error_result(
        table_name=str(target_ref),
        table_arn=str(target_ref),
        mode=manager["config"]["mode"].upper(),
        error=error,
        dry_run=manager["config"]["dry_run"],
    )

def snapshot_manager_build_preflight_entry_failure(
    manager: Dict[str, Any],
    error: BaseException,
) -> Dict[str, Any]:
    return _build_table_error_result(
        table_name="desconhecida",
        table_arn="desconhecido",
        mode=manager["config"]["mode"].upper(),
        error=error,
        dry_run=manager["config"]["dry_run"],
    )

def snapshot_manager_map_target_ref_to_entry(
    manager: Dict[str, Any],
    target_ref: str,
) -> Dict[str, Any]:
    table_name = _extract_table_name(target_ref)
    if not table_name:
        return {
            "entry": None,
            "failure": snapshot_manager_build_preflight_target_failure(
                manager,
                target_ref,
                ValueError("Tabela inválida na lista de targets"),
            ),
        }

    _log_event(
        "snapshot.preflight.target",
        table_ref=target_ref,
        table_name=table_name,
        dry_run=manager["config"]["dry_run"],
        is_arn=target_ref.startswith("arn:"),
        level=logging.DEBUG,
    )
    entry = {
        "table_name": table_name,
        "table_arn": (
            table_name
            if (manager["config"]["dry_run"] and not target_ref.startswith("arn:"))
            else target_ref
        ),
    }
    return {"entry": entry, "failure": None}

def snapshot_manager_collect_preflight_entries(
    manager: Dict[str, Any],
    resolved_targets: List[str],
) -> tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    mapped = [
        snapshot_manager_map_target_ref_to_entry(manager, target_ref)
        for target_ref in resolved_targets
    ]
    entries = [
        _safe_dict_field(item.get("entry"), "preflight.entry")
        for item in mapped
        if isinstance(item.get("entry"), dict)
    ]
    failures = [
        _safe_dict_field(item.get("failure"), "preflight.failure")
        for item in mapped
        if isinstance(item.get("failure"), dict)
    ]
    return entries, failures

def snapshot_manager_build_ignore_set(resolved_ignore: List[str]) -> set[str]:
    return {
        _safe_str_field(item, field_name="ignore_item", required=False).strip().lower()
        for item in resolved_ignore
        if _safe_str_field(item, field_name="ignore_item", required=False).strip()
    }

def snapshot_manager_filter_entry_by_ignore(
    manager: Dict[str, Any],
    entry: Dict[str, Any],
    ignore_set: set[str],
) -> Dict[str, Any]:
    try:
        values = _extract_entry_fields(entry, source="entry do snapshot")
    except ValueError as exc:
        logger.warning("Entrada inválida ignorada: %s", exc)
        return {
            "entry": None,
            "failure": snapshot_manager_build_preflight_entry_failure(manager, exc),
        }

    table_arn = _safe_str_field(
        values.get("table_arn"),
        field_name="table_arn",
        required=False,
    ).lower()
    table_name = _safe_str_field(
        values.get("table_name"),
        field_name="table_name",
        required=False,
    ).lower()
    if table_arn in ignore_set or table_name in ignore_set:
        return {"entry": None, "failure": None}
    return {"entry": values, "failure": None}

def snapshot_manager_filter_entries_by_ignore(
    manager: Dict[str, Any],
    entries: List[Dict[str, Any]],
    resolved_ignore: List[str],
) -> tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    ignore_set = snapshot_manager_build_ignore_set(resolved_ignore)
    mapped = [
        snapshot_manager_filter_entry_by_ignore(manager, entry, ignore_set)
        for entry in entries
    ]
    filtered_entries = [
        _safe_dict_field(item.get("entry"), "filtered.entry")
        for item in mapped
        if isinstance(item.get("entry"), dict)
    ]
    failures = [
        _safe_dict_field(item.get("failure"), "filtered.failure")
        for item in mapped
        if isinstance(item.get("failure"), dict)
    ]
    return filtered_entries, failures

def snapshot_manager_resolve_entry_arn_for_execution(
    manager: Dict[str, Any],
    entry: Dict[str, str],
) -> Dict[str, Any]:
    table_name = _safe_str_field(entry.get("table_name"), field_name="table_name", required=False)
    table_arn = _safe_str_field(entry.get("table_arn"), field_name="table_arn")
    if table_arn.startswith("arn:"):
        return {"entry": entry, "failure": None}

    try:
        return {
            "entry": {
                "table_name": table_name,
                "table_arn": snapshot_manager_resolve_table_arn(manager, table_arn),
            },
            "failure": None,
        }
    except Exception as exc:
        logger.exception("Falha ao resolver ARN da tabela %s", table_arn)
        return {
            "entry": None,
            "failure": _build_table_error_result(
                table_name=table_name or table_arn,
                table_arn=table_arn,
                mode=manager["config"]["mode"].upper(),
                error=exc,
                dry_run=manager["config"]["dry_run"],
            ),
        }

def snapshot_manager_resolve_entries_to_arns(
    manager: Dict[str, Any],
    filtered_entries: List[Dict[str, str]],
) -> tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    if manager["config"]["dry_run"]:
        return filtered_entries, []

    def resolve_entry(entry: Dict[str, str]) -> Dict[str, Any]:
        return snapshot_manager_resolve_entry_arn_for_execution(manager, entry)

    mapped = snapshot_manager_parallel_map(
        manager,
        filtered_entries,
        resolve_entry,
        stage="resolve_entries_to_arns",
    )
    resolved_entries = [
        _safe_dict_field(item.get("entry"), "resolved.entry")
        for item in mapped
        if isinstance(item.get("entry"), dict)
    ]
    failures = [
        _safe_dict_field(item.get("failure"), "resolved.failure")
        for item in mapped
        if isinstance(item.get("failure"), dict)
    ]
    return resolved_entries, failures

def snapshot_manager_dedupe_entries(
    manager: Dict[str, Any],
    entries: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    deduped = {
        snapshot_manager_entry_identity(manager, entry): entry
        for entry in entries
    }
    return list(deduped.values())

def snapshot_manager_partition_by_permission_precheck(
    manager: Dict[str, Any],
    entries: List[Dict[str, str]],
) -> tuple[Dict[str, Any], List[Dict[str, str]], List[Dict[str, Any]]]:
    if not manager["config"]["permission_precheck_enabled"]:
        return manager, entries, []

    _log_event("snapshot.permissions.start", table_count=len(entries))
    next_manager = manager
    if snapshot_manager_can_parallelize_shared_context_stage(manager):
        def validate_entry(entry: Dict[str, str]) -> Dict[str, Any]:
            _, failure = snapshot_manager_preflight_entry_permissions(manager, entry)
            return {
                "entry": entry,
                "failure": failure,
            }

        validations = snapshot_manager_parallel_map(
            manager,
            entries,
            validate_entry,
            stage="permission_precheck",
        )
    else:
        validations = []
        for entry in entries:
            next_manager, failure = snapshot_manager_preflight_entry_permissions(next_manager, entry)
            validations.append(
                {
                    "entry": entry,
                    "failure": failure,
                }
            )
    allowed_entries = [
        _safe_dict_field(item.get("entry"), "permission.entry")
        for item in validations
        if item.get("failure") is None
    ]
    permission_failures = [
        _safe_dict_field(item.get("failure"), "permission.failure")
        for item in validations
        if isinstance(item.get("failure"), dict)
    ]
    if permission_failures:
        _log_event(
            "snapshot.permissions.failed",
            table_count=len(permission_failures),
            errors=[failure.get("error", "") for failure in permission_failures],
        )
    return next_manager, allowed_entries, permission_failures

def snapshot_manager_load_checkpoint_state(manager: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    _resolve_checkpoint_store_backend(manager["checkpoint_store"])
    return _build_empty_checkpoint_state(datetime.now(timezone.utc)), {}

def snapshot_manager_build_no_tables_response(
    manager: Dict[str, Any],
    preflight_failures: List[Dict[str, Any]],
) -> Dict[str, Any]:
    status = "partial_ok" if preflight_failures else "ok"
    snapshot_bucket = _resolve_optional_text(manager["config"].get("bucket"))
    _log_event(
        "snapshot.run.no_tables",
        preflight_failures=len(preflight_failures),
        status=status,
    )
    return {
        "status": status,
        "message": "nenhuma tabela selecionada",
        "dry_run": manager["config"]["dry_run"],
        "results": _normalize_output_results(
            preflight_failures,
            snapshot_bucket=snapshot_bucket,
        ),
        "updated_checkpoint": _resolve_checkpoint_target(manager["config"]),
    }

def snapshot_manager_build_dry_run_results(
    manager: Dict[str, Any],
    filtered_entries: List[Dict[str, str]],
    table_state: Dict[str, Any],
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    next_manager = manager
    results: List[Dict[str, Any]] = []
    for entry in filtered_entries:
        next_manager, plan = snapshot_manager_build_dry_run_plan(
            next_manager,
            entry,
            snapshot_manager_load_previous_state(next_manager, table_state, entry),
        )
        results.append(plan)
    return next_manager, results


def snapshot_manager_prepare_execution_entries(
    manager: Dict[str, Any],
    filtered_entries: List[Dict[str, str]],
    table_state: Dict[str, Any],
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if snapshot_manager_can_parallelize_shared_context_stage(manager):
        def prepare_entry(entry: Dict[str, str]) -> Dict[str, Any]:
            entry_fields = _extract_entry_fields(entry, source="entry de execucao")
            table_name = _safe_str_field(entry_fields.get("table_name"), field_name="table_name")
            table_arn = _safe_str_field(entry_fields.get("table_arn"), field_name="table_arn")
            execution_context = snapshot_manager_get_cached_execution_context(manager, entry)
            if execution_context is None:
                _, execution_context = snapshot_manager_unwrap_execution_context(
                    snapshot_manager_resolve_execution_context(
                        manager,
                        table_name,
                        table_arn,
                    ),
                    manager,
                )
                snapshot_manager_cache_execution_context(
                    manager,
                    entry,
                    execution_context,
                )
            return {
                "entry": entry,
                "previous_state": snapshot_manager_load_previous_state(manager, table_state, entry),
                "execution_context": execution_context,
            }

        prepared_entries = snapshot_manager_parallel_map(
            manager,
            filtered_entries,
            prepare_entry,
            stage="prepare_execution_entries",
        )
        return manager, prepared_entries

    next_manager = manager
    prepared_entries: List[Dict[str, Any]] = []
    for entry in filtered_entries:
        entry_fields = _extract_entry_fields(entry, source="entry de execucao")
        table_name = _safe_str_field(entry_fields.get("table_name"), field_name="table_name")
        table_arn = _safe_str_field(entry_fields.get("table_arn"), field_name="table_arn")
        execution_context = snapshot_manager_get_cached_execution_context(next_manager, entry)
        if execution_context is None:
            next_manager, execution_context = snapshot_manager_unwrap_execution_context(
                snapshot_manager_resolve_execution_context(
                    next_manager,
                    table_name,
                    table_arn,
                ),
                next_manager,
            )
            next_manager = snapshot_manager_cache_execution_context(
                next_manager,
                entry,
                execution_context,
            )
        prepared_entries.append(
            {
                "entry": entry,
                "previous_state": snapshot_manager_load_previous_state(next_manager, table_state, entry),
                "execution_context": execution_context,
            }
        )
    return next_manager, prepared_entries

def snapshot_manager_build_dry_run_response(
    manager: Dict[str, Any],
    results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    failed_count = sum(item.get("status") == "FAILED" for item in results)
    status = "partial_ok" if failed_count else "ok"
    snapshot_bucket = _resolve_optional_text(manager["config"].get("bucket"))
    _log_event(
        "snapshot.run.dry_run.complete",
        results=len(results),
        failed=failed_count,
        status=status,
    )
    return {
        "status": status,
        "run_id": manager["config"]["run_id"],
        "mode": manager["config"]["mode"],
        "dry_run": True,
        "results": _normalize_output_results(
            results,
            snapshot_bucket=snapshot_bucket,
        ),
    }

def snapshot_manager_execute_entries(
    manager: Dict[str, Any],
    filtered_entries: List[Dict[str, str]],
    table_state: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    logger.info("Iniciando snapshot para %s tabela(s)", len(filtered_entries))
    _log_event("snapshot.run.execute", tables=len(filtered_entries))

    results: List[Dict[str, Any]] = []
    new_table_state = dict(table_state)
    prepared_entries: List[Dict[str, Any]]
    if snapshot_manager_has_runtime_context(manager):
        manager, prepared_entries = snapshot_manager_prepare_execution_entries(
            manager,
            filtered_entries,
            new_table_state,
        )
    else:
        prepared_entries = [
            {
                "entry": entry,
                "previous_state": snapshot_manager_load_previous_state(manager, new_table_state, entry),
                "execution_context": None,
            }
            for entry in filtered_entries
        ]
    with ThreadPoolExecutor(max_workers=min(manager["config"]["max_workers"], len(filtered_entries))) as executor:
        future_map = {}
        for prepared_entry in prepared_entries:
            entry = prepared_entry["entry"]
            previous_state = prepared_entry["previous_state"]
            execution_context = prepared_entry["execution_context"]
            submit_args = (
                snapshot_manager_snapshot_table,
                manager,
                entry,
                previous_state,
            )
            future = executor.submit(
                snapshot_manager_snapshot_table,
                manager,
                entry,
                previous_state,
                execution_context=execution_context,
            ) if execution_context is not None else executor.submit(*submit_args)
            future_map[future] = entry

        for future in as_completed(future_map):
            entry = future_map[future]
            result = snapshot_manager_resolve_table_future_result(
                manager,
                entry,
                future,
            )
            new_table_state = snapshot_manager_reduce_table_checkpoint_state(
                manager,
                new_table_state,
                entry,
                result,
            )
            results.append(result)

    return results, new_table_state

def snapshot_manager_try_save_checkpoint(
    manager: Dict[str, Any],
    snapshot_state: Dict[str, Any],
) -> tuple[Optional[str], Dict[str, Any]]:
    if manager["config"]["dry_run"]:
        return None, {}
    try:
        checkpoint_save(manager["checkpoint_store"], snapshot_state)
        return None, {}
    except Exception as exc:
        checkpoint_error = str(exc)
        checkpoint_error_feedback = _build_error_response_fields(exc)
        logger.exception("Falha ao salvar checkpoint")
        _log_event("snapshot.checkpoint.failed", error=checkpoint_error, level=logging.ERROR)
        return checkpoint_error, checkpoint_error_feedback

def snapshot_manager_build_run_response(
    manager: Dict[str, Any],
    results: List[Dict[str, Any]],
    checkpoint_error: Optional[str],
    checkpoint_error_feedback: Dict[str, Any],
) -> Dict[str, Any]:
    failed_count = sum(item.get("status") == "FAILED" for item in results)
    status = "partial_ok" if (checkpoint_error or failed_count) else "ok"
    snapshot_bucket = _resolve_optional_text(manager["config"].get("bucket"))
    _log_event(
        "snapshot.run.complete",
        run_id=manager["config"]["run_id"],
        status=status,
        total_results=len(results),
        failed=failed_count,
        checkpoint_error=checkpoint_error,
    )

    response: Dict[str, Any] = {
        "status": status,
        "run_id": manager["config"]["run_id"],
        "mode": manager["config"]["mode"],
        "dry_run": False,
        "checkpoint_error": checkpoint_error,
        "results": _normalize_output_results(
            results,
            snapshot_bucket=snapshot_bucket,
        ),
        "updated_checkpoint": _resolve_checkpoint_target(manager["config"]),
    }
    if checkpoint_error_feedback:
        response = {
            **response,
            "checkpoint_error_detail": checkpoint_error_feedback.get("error_detail"),
            "checkpoint_user_message": checkpoint_error_feedback.get("user_message"),
            "checkpoint_resolution": checkpoint_error_feedback.get("resolution"),
        }
    return response

def snapshot_manager_run(manager: Dict[str, Any], event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    manager = snapshot_manager_coalesce_manager(
        snapshot_manager_prime_assumed_session_from_direct_targets(manager),
        manager,
    )
    resolved_targets = snapshot_manager_resolve_targets(manager)
    resolved_ignore = snapshot_manager_resolve_ignore(manager)
    if not resolved_targets:
        raise ValueError("Nenhum target válido após processar targets/targets_csv")

    _log_event(
        "snapshot.run.start",
        run_id=manager["config"]["run_id"],
        mode=manager["config"]["mode"],
        dry_run=manager["config"]["dry_run"],
        wait_for_completion=manager["config"]["wait_for_completion"],
        max_workers=manager["config"]["max_workers"],
        target_count=len(resolved_targets),
        ignore_count=len(resolved_ignore),
    )
    entries, preflight_failures = snapshot_manager_collect_preflight_entries(
        manager,
        resolved_targets,
    )

    _log_event(
        "snapshot.preflight.summary",
        total_targets=len(resolved_targets),
        valid_entries=len(entries),
        preflight_failures=len(preflight_failures),
    )

    filtered_entries, ignore_failures = snapshot_manager_filter_entries_by_ignore(
        manager,
        entries,
        resolved_ignore,
    )
    preflight_failures.extend(ignore_failures)

    manager = snapshot_manager_coalesce_manager(
        snapshot_manager_prime_assumed_session_from_targets(manager, filtered_entries),
        manager,
    )

    # Dedup antecipado evita DescribeTable duplicado quando o input repete nomes.
    filtered_entries = snapshot_manager_dedupe_entries(manager, filtered_entries)

    filtered_entries, arn_resolution_failures = snapshot_manager_resolve_entries_to_arns(
        manager,
        filtered_entries,
    )
    preflight_failures.extend(arn_resolution_failures)

    # Dedup final colapsa misto nome+ARN da mesma tabela para um único disparo.
    filtered_entries = snapshot_manager_dedupe_entries(manager, filtered_entries)
    _log_event(
        "snapshot.preflight.filtered",
        ignored=len(entries) - len(filtered_entries),
        selected=len(filtered_entries),
    )

    snapshot_state, table_state = snapshot_manager_load_checkpoint_state(manager)
    _log_event("snapshot.run.checkpoint_loaded", known_tables=len(table_state))

    manager, filtered_entries, permission_failures = snapshot_manager_partition_by_permission_precheck(
        manager,
        filtered_entries,
    )
    preflight_failures.extend(permission_failures)

    if not filtered_entries:
        logger.warning("Nenhuma tabela para processar após filtros.")
        return snapshot_manager_build_no_tables_response(manager, preflight_failures)

    if manager["config"]["dry_run"]:
        _, results = snapshot_manager_build_dry_run_results(manager, filtered_entries, table_state)
        results.extend(preflight_failures)
        return snapshot_manager_build_dry_run_response(manager, results)

    results, new_table_state = snapshot_manager_execute_entries(
        manager,
        filtered_entries,
        table_state,
    )

    results.extend(preflight_failures)
    next_snapshot_state = {
        **snapshot_state,
        "tables": new_table_state,
    }
    checkpoint_error, checkpoint_error_feedback = snapshot_manager_try_save_checkpoint(
        manager,
        next_snapshot_state,
    )
    return snapshot_manager_build_run_response(
        manager,
        results,
        checkpoint_error,
        checkpoint_error_feedback,
    )

def snapshot_manager_build_dry_run_plan(
    manager: Dict[str, Any],
    entry: Dict[str, str],
    previous_state: Dict[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    entry_fields = _extract_entry_fields(entry, source="entry de dry-run")
    table_name = _safe_str_field(entry_fields.get("table_name"), field_name="table_name")
    table_arn = _safe_str_field(entry_fields.get("table_arn"), field_name="table_arn")
    if not isinstance(previous_state, dict):
        previous_state = {}

    def with_snapshot_bucket(result: Dict[str, Any], *, execution_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return snapshot_manager_attach_snapshot_bucket_to_result(
            manager,
            table_name,
            table_arn,
            result,
            execution_context=execution_context,
        )

    next_manager = manager
    if manager["config"]["mode"] == "incremental":
        next_manager, execution_context = snapshot_manager_unwrap_execution_context(
            snapshot_manager_resolve_execution_context(
                next_manager,
                table_name,
                table_arn,
            ),
            next_manager,
        )
        incremental_reference = snapshot_manager_resolve_incremental_reference(
            next_manager,
            entry,
            previous_state,
            s3_client=execution_context.get("s3"),
            execution_context=execution_context,
        )
        if incremental_reference:
            from_checkpoint = _safe_str_field(
                incremental_reference.get("checkpoint_from"),
                field_name="incremental_reference.checkpoint_from",
            )
            s3_prefix = snapshot_manager_build_mode_prefix(
                next_manager,
                "incremental",
                table_name,
                table_arn,
                execution_context=execution_context,
            )
            return next_manager, with_snapshot_bucket(
                {
                    "table_name": table_name,
                    "table_arn": table_arn,
                    "mode": "INCREMENTAL",
                    "status": "DRY_RUN",
                    "source": "native_incremental (with scan fallback if unsupported)",
                    "s3_prefix": s3_prefix,
                    "checkpoint_from": from_checkpoint,
                    "checkpoint_source": incremental_reference.get("checkpoint_source"),
                    "full_run_id": incremental_reference.get("full_run_id"),
                    "full_export_s3_prefix": incremental_reference.get("full_export_s3_prefix"),
                    "checkpoint_to": _dt_to_iso(next_manager["config"]["run_time"]),
                    "dry_run": True,
                },
                execution_context=execution_context,
            )

        return next_manager, with_snapshot_bucket(
            {
                "table_name": table_name,
                "table_arn": table_arn,
                "mode": "FULL",
                "status": "DRY_RUN",
                "source": "full (no checkpoint or previous full export found)",
                "s3_prefix": snapshot_manager_build_mode_prefix(
                    next_manager,
                    "full",
                    table_name,
                    table_arn,
                    execution_context=execution_context,
                ),
                "checkpoint_to": _dt_to_iso(next_manager["config"]["run_time"]),
                "dry_run": True,
            },
            execution_context=execution_context,
        )

    return next_manager, with_snapshot_bucket(
        {
            "table_name": table_name,
            "table_arn": table_arn,
            "mode": "FULL",
            "status": "DRY_RUN",
            "source": "full",
            "s3_prefix": snapshot_manager_build_mode_prefix(
                next_manager,
                "full",
                table_name,
                table_arn,
            ),
            "checkpoint_to": _dt_to_iso(next_manager["config"]["run_time"]),
            "dry_run": True,
        }
    )

def snapshot_manager_resolve_table_arn(manager: Dict[str, Any], table_ref: str) -> str:
    table_ref = _safe_str_field(table_ref, field_name="table_ref")
    if table_ref.startswith("arn:"):
        _log_event("table.resolve_arn.skip", table_ref=table_ref, reason="already_arn", level=logging.DEBUG)
        return table_ref
    _log_event("table.resolve_arn.start", table_ref=table_ref)
    try:
        response = manager["ddb"].describe_table(TableName=table_ref)
        arn = response.get("Table", {}).get("TableArn")
        if not arn:
            raise RuntimeError(f"Resposta de DescribeTable sem TableArn para {table_ref}")
        _log_event("table.resolve_arn.success", table_ref=table_ref, table_arn=arn)
        return arn
    except ClientError as exc:
        raise _build_aws_runtime_error("DynamoDB DescribeTable", exc, resource=table_ref) from exc
    except Exception as exc:
        raise RuntimeError(f"Falha inesperada ao resolver ARN para {table_ref}: {exc}") from exc

def snapshot_manager_resolve_execution_context(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    if not table_arn.startswith("arn:"):
        manager_obj = _safe_dict_field(manager, "manager")
        session = _safe_get_field(manager_obj, "session", field_name="manager")
        default_region = _safe_str_field(
            _safe_get_field(manager_obj, "default_region", field_name="manager"),
            field_name="default_region",
        )
        config = _safe_dict_field(
            _safe_get_field(manager_obj, "config", field_name="manager"),
            "manager.config",
        )
        assume_role_arn = _safe_str_field(
            _resolve_config_assume_role(config),
            field_name="assume_role",
            required=False,
        )
        return manager, {
            "session": session,
            "ddb": _safe_get_field(manager_obj, "ddb", field_name="manager"),
            "s3": _safe_get_field(manager_obj, "s3", field_name="manager"),
            "session_mode": "shared_session_without_arn",
            "assume_role_arn": assume_role_arn,
            "table_account_id": None,
            "table_region": default_region,
        }

    next_manager, clients = snapshot_manager_resolve_table_clients(manager, table_arn)
    context = {
        **clients,
        "table_name": table_name,
        "table_arn": table_arn,
    }
    return next_manager, context

def snapshot_manager_extract_account_id_from_arn(
    manager: Dict[str, Any],
    arn: Optional[str],
    *,
    field_name: str,
) -> Optional[str]:
    if not isinstance(arn, str):
        return None
    try:
        parsed = _parse_arn(arn, field_name=field_name)
    except ValueError:
        return None
    return parsed.get("account_id") or None

def snapshot_manager_extract_session_identity(manager: Dict[str, Any], session: Any) -> str:
    cache = manager.get("_session_identity_cache")
    lock = manager.get("_session_identity_lock")
    if not isinstance(cache, dict) or lock is None:
        try:
            sts_client = _get_session_client(session, "sts")
            response = sts_client.get_caller_identity()
            return _safe_str_field(response.get("Arn"), field_name="sts caller arn", required=False)
        except Exception:
            return ""

    session_key = id(session)
    with lock:
        cached_identity = cache.get(session_key)
        if isinstance(cached_identity, str):
            return cached_identity
        try:
            sts_client = _get_session_client(session, "sts")
            response = sts_client.get_caller_identity()
            caller_arn = _safe_str_field(response.get("Arn"), field_name="sts caller arn", required=False)
        except Exception:
            caller_arn = ""
        cache[session_key] = caller_arn
        return caller_arn

def snapshot_manager_validate_export_session_account(manager: Dict[str, Any], table_name: str, table_arn: str, context: Dict[str, Any]) -> None:
    table_account_id = _safe_str_field(context.get("table_account_id"), field_name="table_account_id", required=False)
    if not table_account_id:
        return

    session = context.get("session")
    caller_arn = _safe_str_field(context.get("caller_arn"), field_name="caller_arn", required=False)
    caller_account_id = _safe_str_field(
        context.get("caller_account_id"),
        field_name="caller_account_id",
        required=False,
    )
    if not caller_arn and session:
        caller_arn = snapshot_manager_extract_session_identity(manager, session)
        caller_account_id = snapshot_manager_extract_account_id_from_arn(
            manager,
            caller_arn,
            field_name="caller_arn",
        )

    mismatches = []
    if caller_account_id and caller_account_id != table_account_id:
        mismatches.append("caller")
    if not mismatches:
        _log_event(
            "snapshot.table.account_validation",
            table_name=table_name,
            table_arn=table_arn,
            table_account_id=table_account_id,
            caller_account_id=caller_account_id,
            status="ok",
            level=logging.DEBUG,
        )
        return

    message = (
        f"TableArn {table_arn} pertence à conta {table_account_id}, mas "
        f"caller={caller_account_id or 'indefinido'}. "
        "A operação ExportTableToPointInTime exige sessão da conta dona da tabela."
    )
    _log_event(
        "snapshot.table.account_validation",
        table_name=table_name,
        table_arn=table_arn,
        table_account_id=table_account_id,
        caller_account_id=caller_account_id,
        mismatches=",".join(mismatches),
        status="blocked",
        level=logging.ERROR,
    )
    raise RuntimeError(message)


def snapshot_manager_describe_point_in_time_recovery(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    ddb_client: Any,
) -> Dict[str, str]:
    try:
        response = ddb_client.describe_continuous_backups(TableName=table_name)
    except ClientError as exc:
        raise _build_aws_runtime_error(
            "DynamoDB DescribeContinuousBackups",
            exc,
            resource=table_arn or table_name,
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Falha inesperada ao consultar Point-in-Time Recovery da tabela {table_name}: {exc}"
        ) from exc

    description = response.get("ContinuousBackupsDescription") if isinstance(response, dict) else None
    if not isinstance(description, dict):
        raise RuntimeError(
            f"Resposta inválida de DescribeContinuousBackups para a tabela {table_name}"
        )
    point_in_time_description = description.get("PointInTimeRecoveryDescription")
    if not isinstance(point_in_time_description, dict):
        point_in_time_description = {}

    continuous_backups_status = _safe_str_field(
        description.get("ContinuousBackupsStatus"),
        field_name="ContinuousBackupsStatus",
        required=False,
    ).upper()
    point_in_time_recovery_status = _safe_str_field(
        point_in_time_description.get("PointInTimeRecoveryStatus"),
        field_name="PointInTimeRecoveryStatus",
        required=False,
    ).upper()

    return {
        "continuous_backups_status": continuous_backups_status,
        "point_in_time_recovery_status": point_in_time_recovery_status,
    }


def snapshot_manager_wait_point_in_time_recovery_enabled(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    ddb_client: Any,
    timeout_seconds: int = PITR_ENABLE_TIMEOUT_SECONDS,
    poll_seconds: int = PITR_ENABLE_POLL_SECONDS,
) -> Dict[str, str]:
    waited_seconds = 0

    while waited_seconds <= timeout_seconds:
        status = snapshot_manager_describe_point_in_time_recovery(
            manager,
            table_name,
            table_arn,
            ddb_client=ddb_client,
        )
        point_in_time_recovery_status = status.get("point_in_time_recovery_status", "")
        _log_event(
            "table.pitr.wait.poll",
            table_name=table_name,
            table_arn=table_arn,
            point_in_time_recovery_status=point_in_time_recovery_status,
            continuous_backups_status=status.get("continuous_backups_status"),
            waited_seconds=waited_seconds,
            timeout_seconds=timeout_seconds,
            level=logging.DEBUG,
        )
        if point_in_time_recovery_status == "ENABLED":
            return status
        if waited_seconds == timeout_seconds:
            break
        time.sleep(poll_seconds)
        waited_seconds = min(waited_seconds + poll_seconds, timeout_seconds)

    raise TimeoutError(
        f"Timeout aguardando Point-in-Time Recovery ENABLED na tabela {table_name}"
    )


def snapshot_manager_ensure_point_in_time_recovery(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    ddb_client: Any,
) -> Dict[str, Any]:
    try:
        status = snapshot_manager_describe_point_in_time_recovery(
            manager,
            table_name,
            table_arn,
            ddb_client=ddb_client,
        )
    except RuntimeError as exc:
        cause = exc.__cause__
        if isinstance(cause, ClientError) and _classify_aws_error(_client_error_code(cause)) == "access_denied":
            _log_event(
                "table.pitr.ensure.skipped",
                table_name=table_name,
                table_arn=table_arn,
                reason="describe_access_denied",
                error=str(exc),
                level=logging.WARNING,
            )
            return {
                "continuous_backups_status": "",
                "point_in_time_recovery_status": "UNKNOWN",
                "changed": False,
                "skipped": True,
            }
        raise

    point_in_time_recovery_status = status.get("point_in_time_recovery_status", "")
    if point_in_time_recovery_status == "ENABLED":
        _log_event(
            "table.pitr.ensure.skip",
            table_name=table_name,
            table_arn=table_arn,
            point_in_time_recovery_status=point_in_time_recovery_status,
            continuous_backups_status=status.get("continuous_backups_status"),
            changed=False,
            level=logging.DEBUG,
        )
        return {
            **status,
            "changed": False,
            "skipped": False,
        }

    _log_event(
        "table.pitr.enable.start",
        table_name=table_name,
        table_arn=table_arn,
        point_in_time_recovery_status=point_in_time_recovery_status or "UNKNOWN",
        continuous_backups_status=status.get("continuous_backups_status"),
        timeout_seconds=PITR_ENABLE_TIMEOUT_SECONDS,
    )

    try:
        ddb_client.update_continuous_backups(
            TableName=table_name,
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
        )
    except ClientError as exc:
        raise _build_aws_runtime_error(
            "DynamoDB UpdateContinuousBackups",
            exc,
            resource=table_arn or table_name,
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Falha inesperada ao habilitar Point-in-Time Recovery da tabela {table_name}: {exc}"
        ) from exc

    enabled_status = snapshot_manager_wait_point_in_time_recovery_enabled(
        manager,
        table_name,
        table_arn,
        ddb_client=ddb_client,
    )
    _log_event(
        "table.pitr.enable.success",
        table_name=table_name,
        table_arn=table_arn,
        point_in_time_recovery_status=enabled_status.get("point_in_time_recovery_status"),
        continuous_backups_status=enabled_status.get("continuous_backups_status"),
        changed=True,
    )
    return {
        **enabled_status,
        "changed": True,
        "skipped": False,
    }

def snapshot_manager_preflight_entry_permissions(
    manager: Dict[str, Any],
    entry: Dict[str, str],
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    mode = _safe_str_field(config.get("mode"), field_name="mode").upper()
    dry_run = bool(config.get("dry_run"))

    fields = _extract_entry_fields(entry, source="entry de preflight")
    table_name = _safe_str_field(fields.get("table_name"), field_name="table_name")
    table_arn = _safe_str_field(fields.get("table_arn"), field_name="table_arn")

    try:
        next_manager, context = snapshot_manager_unwrap_execution_context(
            snapshot_manager_resolve_execution_context(
                manager,
                table_name,
                table_arn,
            ),
            manager,
        )
        session = context.get("session")
        assume_role_arn = _safe_str_field(context.get("assume_role_arn"), field_name="assume_role_arn", required=False)
        session_mode = context.get("session_mode")
        table_account_id = context.get("table_account_id")
        table_region = context.get("table_region")

        caller_arn = snapshot_manager_extract_session_identity(next_manager, session) if session else ""
        caller_account_id = snapshot_manager_extract_account_id_from_arn(
            manager,
            caller_arn,
            field_name="caller_arn",
        )
        execution_context = {
            **context,
            "caller_arn": caller_arn,
            "caller_account_id": caller_account_id,
        }

        _log_event(
            "snapshot.permissions.entry.start",
            table_name=table_name,
            table_arn=table_arn,
            session_mode=session_mode,
            assume_role_arn=assume_role_arn,
            caller_arn=caller_arn,
            table_account_id=table_account_id,
            table_region=table_region,
        )

        _log_event("snapshot.permissions.entry.success", table_name=table_name, table_arn=table_arn)
        next_manager = snapshot_manager_cache_execution_context(
            next_manager,
            entry,
            execution_context,
        )
        return next_manager, None
    except Exception as exc:
        return manager, _build_table_error_result(
            table_name=table_name,
            table_arn=table_arn,
            mode=mode,
            error=exc,
            dry_run=dry_run,
            overrides={"error": f"Falha no preflight de permissões: {exc}"},
        )

def snapshot_manager_attach_checkpoint_state_to_result(
    manager: Dict[str, Any],
    checkpoint_state: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result
    return {
        **result,
        "checkpoint_state": snapshot_manager_apply_result_to_checkpoint_state(
            manager,
            checkpoint_state,
            result,
        ),
    }

def snapshot_manager_build_pending_tracking_result(
    table_name: str,
    table_arn: str,
    mode: str,
    checkpoint_state: Dict[str, Any],
    execution_context: Dict[str, Any],
    pending_exports: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "table_name": table_name,
        "table_arn": table_arn,
        "mode": mode.upper(),
        "status": "PENDING",
        "source": "pending_export_tracking",
        "pending_exports": pending_exports,
        "checkpoint_state": checkpoint_state,
        "assume_role_arn": execution_context.get("assume_role_arn"),
        "table_account_id": execution_context.get("table_account_id"),
        "table_region": execution_context.get("table_region"),
    }


def snapshot_manager_with_config_overrides(
    manager: Dict[str, Any],
    **config_overrides: Any,
) -> Dict[str, Any]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    return {
        **manager,
        "config": {
            **config,
            **config_overrides,
        },
    }


def snapshot_manager_build_catch_up_run_result(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    return snapshot_manager_compact_fields(
        {
            key: value
            for key, value in result.items()
            if key != "checkpoint_state"
        }
    )


def snapshot_manager_build_catch_up_result(
    result: Dict[str, Any],
    *,
    catch_up_runs: List[Dict[str, Any]],
    requested_export_to: datetime,
    forced_wait_for_completion: bool,
) -> Dict[str, Any]:
    return snapshot_manager_compact_fields(
        {
            **result,
            "catch_up_enabled": True,
            "catch_up_chunks_executed": len(catch_up_runs),
            "catch_up_requested_to": _dt_to_iso(requested_export_to),
            "catch_up_forced_wait_for_completion": forced_wait_for_completion,
            "catch_up_runs": [
                snapshot_manager_build_catch_up_run_result(run_result)
                for run_result in catch_up_runs
                if isinstance(run_result, dict)
            ],
        }
    )


def snapshot_manager_build_incremental_window_skip_result(
    table_name: str,
    table_arn: str,
    *,
    execution_context: Dict[str, Any],
    incremental_reference: Dict[str, Any],
    export_from: datetime,
    requested_export_to: datetime,
) -> Dict[str, Any]:
    return snapshot_manager_compact_fields(
        {
            "table_name": table_name,
            "table_arn": table_arn,
            "mode": "INCREMENTAL",
            "status": "SKIPPED",
            "source": "incremental_window_guard",
            "message": (
                "Janela incremental menor que 15 minutos; "
                "aguardando a próxima execução."
            ),
            "checkpoint_from": _safe_str_field(
                incremental_reference.get("checkpoint_from"),
                field_name="incremental_reference.checkpoint_from",
            ),
            "checkpoint_source": incremental_reference.get("checkpoint_source"),
            "full_run_id": incremental_reference.get("full_run_id"),
            "full_export_s3_prefix": incremental_reference.get("full_export_s3_prefix"),
            "requested_export_from": _dt_to_iso(export_from),
            "requested_export_to": _dt_to_iso(requested_export_to),
            "assume_role_arn": execution_context.get("assume_role_arn"),
            "table_account_id": execution_context.get("table_account_id"),
            "table_region": execution_context.get("table_region"),
        }
    )


def snapshot_manager_execute_incremental_catch_up(
    manager: Dict[str, Any],
    entry: Dict[str, str],
    checkpoint_state: Dict[str, Any],
    *,
    table_name: str,
    table_arn: str,
    run_time: datetime,
    dry_run: bool,
    ddb_client: Any,
    s3_client: Any,
    execution_context: Dict[str, Any],
) -> Dict[str, Any]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    forced_wait_for_completion = not bool(config.get("wait_for_completion"))
    catch_up_manager = (
        snapshot_manager_with_config_overrides(manager, wait_for_completion=True)
        if forced_wait_for_completion
        else manager
    )
    current_checkpoint_state = dict(checkpoint_state)
    catch_up_results: List[Dict[str, Any]] = []

    while True:
        incremental_reference = snapshot_manager_resolve_incremental_reference(
            catch_up_manager,
            entry,
            current_checkpoint_state,
            s3_client=s3_client,
            execution_context=execution_context,
        )
        if not incremental_reference:
            logger.info(
                "Tabela %s sem baseline incremental durante catch-up; executando FULL",
                table_name,
            )
            full_result = snapshot_manager_attach_checkpoint_state_to_result(
                catch_up_manager,
                current_checkpoint_state,
                snapshot_manager_start_full_export(
                    catch_up_manager,
                    table_name,
                    table_arn,
                    ddb_client=ddb_client,
                    execution_context=execution_context,
                ),
            )
            return snapshot_manager_build_catch_up_result(
                full_result,
                catch_up_runs=catch_up_results + [full_result],
                requested_export_to=run_time,
                forced_wait_for_completion=forced_wait_for_completion,
            )

        try:
            export_from = _parse_iso(
                _safe_str_field(
                    incremental_reference.get("checkpoint_from"),
                    field_name="incremental_reference.checkpoint_from",
                )
            )
        except Exception as exc:
            error_result = _build_table_error_result(
                table_name=table_name,
                table_arn=table_arn,
                mode="INCREMENTAL",
                error=exc,
                dry_run=dry_run,
                overrides={
                    "error": f"Baseline incremental inválido para tabela {table_name}: {exc}",
                    "checkpoint_raw": incremental_reference.get("checkpoint_from"),
                    "checkpoint_source": incremental_reference.get("checkpoint_source"),
                },
            )
            return {
                **error_result,
                "checkpoint_state": current_checkpoint_state,
            }

        incremental_window = snapshot_manager_resolve_incremental_window(
            export_from,
            run_time,
        )
        if incremental_window["window_is_invalid"]:
            invalid_window_error = ValueError(
                "checkpoint_from deve ser anterior ao momento atual da execução"
            )
            error_result = _build_table_error_result(
                table_name=table_name,
                table_arn=table_arn,
                mode="INCREMENTAL",
                error=invalid_window_error,
                dry_run=dry_run,
                overrides={
                    "error": f"Janela incremental inválida para tabela {table_name}: {invalid_window_error}",
                    "checkpoint_raw": incremental_reference.get("checkpoint_from"),
                    "checkpoint_source": incremental_reference.get("checkpoint_source"),
                },
            )
            return {
                **error_result,
                "checkpoint_state": current_checkpoint_state,
            }

        if incremental_window["window_too_small"]:
            _log_event(
                "export.incremental.window_skipped",
                table_name=table_name,
                table_arn=table_arn,
                export_from=_dt_to_iso(incremental_window["export_from"]),
                requested_export_to=_dt_to_iso(incremental_window["requested_export_to"]),
                requested_window_seconds=incremental_window["requested_window_seconds"],
                minimum_window_seconds=int(INCREMENTAL_EXPORT_MIN_WINDOW.total_seconds()),
                catch_up_enabled=True,
            )
            skipped_result = snapshot_manager_attach_checkpoint_state_to_result(
                catch_up_manager,
                current_checkpoint_state,
                snapshot_manager_build_incremental_window_skip_result(
                    table_name,
                    table_arn,
                    execution_context=execution_context,
                    incremental_reference=incremental_reference,
                    export_from=incremental_window["export_from"],
                    requested_export_to=incremental_window["requested_export_to"],
                ),
            )
            if not catch_up_results:
                return snapshot_manager_build_catch_up_result(
                    skipped_result,
                    catch_up_runs=[],
                    requested_export_to=run_time,
                    forced_wait_for_completion=forced_wait_for_completion,
                )
            return snapshot_manager_build_catch_up_result(
                {
                    **catch_up_results[-1],
                    "checkpoint_state": current_checkpoint_state,
                },
                catch_up_runs=catch_up_results,
                requested_export_to=run_time,
                forced_wait_for_completion=forced_wait_for_completion,
            )

        export_to = incremental_window["export_to"]
        if incremental_window["window_truncated"]:
            _log_event(
                "export.incremental.window_truncated",
                table_name=table_name,
                table_arn=table_arn,
                export_from=_dt_to_iso(incremental_window["export_from"]),
                requested_export_to=_dt_to_iso(incremental_window["requested_export_to"]),
                export_to=_dt_to_iso(export_to),
                requested_window_seconds=incremental_window["requested_window_seconds"],
                effective_window_seconds=incremental_window["effective_window_seconds"],
                max_window_seconds=int(INCREMENTAL_EXPORT_MAX_WINDOW.total_seconds()),
                catch_up_enabled=True,
            )

        chunk_result = snapshot_manager_attach_checkpoint_state_to_result(
            catch_up_manager,
            current_checkpoint_state,
            snapshot_manager_start_incremental_export(
                catch_up_manager,
                table_name,
                table_arn,
                incremental_window["export_from"],
                export_to,
                ddb_client=ddb_client,
                s3_client=s3_client,
                execution_context=execution_context,
                incremental_reference=incremental_reference,
            ),
        )
        catch_up_results.append(chunk_result)

        if _safe_str_field(
            chunk_result.get("status"),
            field_name="chunk_result.status",
            required=False,
        ).upper() != "COMPLETED":
            return snapshot_manager_build_catch_up_result(
                chunk_result,
                catch_up_runs=catch_up_results,
                requested_export_to=run_time,
                forced_wait_for_completion=forced_wait_for_completion,
            )

        next_checkpoint_state = _safe_dict_field(
            chunk_result.get("checkpoint_state"),
            "chunk_result.checkpoint_state",
        )
        current_last_to = _safe_str_field(
            next_checkpoint_state.get("last_to"),
            field_name="chunk_result.checkpoint_state.last_to",
            required=False,
        )
        if not current_last_to:
            raise RuntimeError(
                f"Catch-up incremental não avançou checkpoint para a tabela {table_name}"
            )

        current_checkpoint_state = next_checkpoint_state
        remaining_window = run_time - _parse_iso(current_last_to)
        if remaining_window < INCREMENTAL_EXPORT_MIN_WINDOW:
            return snapshot_manager_build_catch_up_result(
                {
                    **chunk_result,
                    "checkpoint_state": current_checkpoint_state,
                },
                catch_up_runs=catch_up_results,
                requested_export_to=run_time,
                forced_wait_for_completion=forced_wait_for_completion,
            )

def snapshot_manager_snapshot_table(
    manager: Dict[str, Any],
    entry: Dict[str, str],
    previous_state: Dict[str, Any],
    *,
    execution_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    mode = _safe_str_field(config.get("mode"), field_name="mode")
    dry_run = bool(config.get("dry_run"))
    run_time = _safe_get_field(config, "run_time", field_name="manager.config")
    if not isinstance(run_time, datetime):
        raise RuntimeError("manager.config.run_time deve ser datetime")

    entry_fields = _extract_entry_fields(entry, source="entry de snapshot")
    table_name = _safe_str_field(entry_fields.get("table_name"), field_name="table_name")
    table_arn = _safe_str_field(entry_fields.get("table_arn"), field_name="table_arn")
    checkpoint_state = snapshot_manager_build_checkpoint_state(entry, previous_state)
    if execution_context is None:
        _, execution_context = snapshot_manager_unwrap_execution_context(
            snapshot_manager_resolve_execution_context(manager, table_name, table_arn),
            manager,
        )

    def with_snapshot_bucket(result: Dict[str, Any]) -> Dict[str, Any]:
        return snapshot_manager_attach_snapshot_bucket_to_result(
            manager,
            table_name,
            table_arn,
            result,
            execution_context=execution_context,
        )

    try:
        snapshot_manager_validate_export_session_account(manager, table_name, table_arn, execution_context)
    except RuntimeError as exc:
        _log_event(
            "snapshot.table.account_validation.failed",
            table_name=table_name,
            table_arn=table_arn,
            mode=mode,
            session_mode=execution_context.get("session_mode"),
            assume_role_arn=execution_context.get("assume_role_arn"),
            table_account_id=execution_context.get("table_account_id"),
            table_region=execution_context.get("table_region"),
            reason=str(exc),
            level=logging.WARNING,
        )
        return with_snapshot_bucket(_build_table_error_result(
            table_name=table_name,
            table_arn=table_arn,
            mode=mode.upper(),
            error=exc,
            dry_run=dry_run,
            overrides={"error": f"Falha na validação de conta de execução: {exc}"},
        ))
    ddb_client = execution_context.get("ddb")
    s3_client = execution_context.get("s3")
    if ddb_client is None:
        raise RuntimeError(f"Cliente DynamoDB ausente no contexto da tabela {table_name}")
    if s3_client is None:
        raise RuntimeError(f"Cliente S3 ausente no contexto da tabela {table_name}")
    assume_role_arn = execution_context.get("assume_role_arn")
    table_account_id = execution_context.get("table_account_id")
    table_region = execution_context.get("table_region")
    session_mode = execution_context.get("session_mode")
    _log_event(
        "snapshot.table.start",
        table_name=table_name,
        table_arn=table_arn,
        mode=mode,
        has_previous_checkpoint=bool(checkpoint_state.get("last_to")),
        session_mode=session_mode,
        assume_role_arn=assume_role_arn,
        table_account_id=table_account_id,
        table_region=table_region,
    )

    checkpoint_state = snapshot_manager_reconcile_pending_exports(
        checkpoint_state,
        table_name,
        table_arn,
        ddb_client=ddb_client,
    )

    if snapshot_manager_has_pending_exports(checkpoint_state):
        pending_exports = snapshot_manager_normalize_pending_exports(
            checkpoint_state.get("pending_exports")
        )
        _log_event(
            "snapshot.table.pending_export_block",
            table_name=table_name,
            table_arn=table_arn,
            pending_exports=len(pending_exports),
        )
        return with_snapshot_bucket(snapshot_manager_build_pending_tracking_result(
            table_name,
            table_arn,
            mode,
            checkpoint_state,
            execution_context,
            pending_exports,
        ))

    if mode == "incremental":
        if bool(config.get("catch_up")):
            return with_snapshot_bucket(snapshot_manager_execute_incremental_catch_up(
                manager,
                entry,
                checkpoint_state,
                table_name=table_name,
                table_arn=table_arn,
                run_time=run_time,
                dry_run=dry_run,
                ddb_client=ddb_client,
                s3_client=s3_client,
                execution_context=execution_context,
            ))

        incremental_reference = snapshot_manager_resolve_incremental_reference(
            manager,
            entry,
            checkpoint_state,
            s3_client=s3_client,
            execution_context=execution_context,
        )
        if not incremental_reference:
            logger.info(
                "Tabela %s sem baseline incremental; executando FULL",
                table_name,
            )
            return with_snapshot_bucket(snapshot_manager_attach_checkpoint_state_to_result(
                manager,
                checkpoint_state,
                snapshot_manager_start_full_export(
                    manager,
                    table_name,
                    table_arn,
                    ddb_client=ddb_client,
                    execution_context=execution_context,
                ),
            ))

        try:
            export_from = _parse_iso(
                _safe_str_field(
                    incremental_reference.get("checkpoint_from"),
                    field_name="incremental_reference.checkpoint_from",
                )
            )
        except Exception as exc:
            error_result = _build_table_error_result(
                table_name=table_name,
                table_arn=table_arn,
                mode="INCREMENTAL",
                error=exc,
                dry_run=dry_run,
                overrides={
                    "error": f"Baseline incremental inválido para tabela {table_name}: {exc}",
                    "checkpoint_raw": incremental_reference.get("checkpoint_from"),
                    "checkpoint_source": incremental_reference.get("checkpoint_source"),
                },
            )
            return with_snapshot_bucket({
                **error_result,
                "checkpoint_state": checkpoint_state,
            })

        incremental_window = snapshot_manager_resolve_incremental_window(
            export_from,
            run_time,
        )
        if incremental_window["window_is_invalid"]:
            invalid_window_error = ValueError(
                "checkpoint_from deve ser anterior ao momento atual da execução"
            )
            error_result = _build_table_error_result(
                table_name=table_name,
                table_arn=table_arn,
                mode="INCREMENTAL",
                error=invalid_window_error,
                dry_run=dry_run,
                overrides={
                    "error": f"Janela incremental inválida para tabela {table_name}: {invalid_window_error}",
                    "checkpoint_raw": incremental_reference.get("checkpoint_from"),
                    "checkpoint_source": incremental_reference.get("checkpoint_source"),
                },
            )
            return with_snapshot_bucket({
                **error_result,
                "checkpoint_state": checkpoint_state,
            })

        if incremental_window["window_too_small"]:
            _log_event(
                "export.incremental.window_skipped",
                table_name=table_name,
                table_arn=table_arn,
                export_from=_dt_to_iso(incremental_window["export_from"]),
                requested_export_to=_dt_to_iso(incremental_window["requested_export_to"]),
                requested_window_seconds=incremental_window["requested_window_seconds"],
                minimum_window_seconds=int(INCREMENTAL_EXPORT_MIN_WINDOW.total_seconds()),
            )
            return with_snapshot_bucket(snapshot_manager_attach_checkpoint_state_to_result(
                manager,
                checkpoint_state,
                snapshot_manager_build_incremental_window_skip_result(
                    table_name,
                    table_arn,
                    execution_context=execution_context,
                    incremental_reference=incremental_reference,
                    export_from=incremental_window["export_from"],
                    requested_export_to=incremental_window["requested_export_to"],
                ),
            ))

        export_to = incremental_window["export_to"]
        if incremental_window["window_truncated"]:
            _log_event(
                "export.incremental.window_truncated",
                table_name=table_name,
                table_arn=table_arn,
                export_from=_dt_to_iso(incremental_window["export_from"]),
                requested_export_to=_dt_to_iso(incremental_window["requested_export_to"]),
                export_to=_dt_to_iso(export_to),
                requested_window_seconds=incremental_window["requested_window_seconds"],
                effective_window_seconds=incremental_window["effective_window_seconds"],
                max_window_seconds=int(INCREMENTAL_EXPORT_MAX_WINDOW.total_seconds()),
            )

        return with_snapshot_bucket(snapshot_manager_attach_checkpoint_state_to_result(
            manager,
            checkpoint_state,
            snapshot_manager_start_incremental_export(
                manager,
                table_name,
                table_arn,
                export_from,
                export_to,
                ddb_client=ddb_client,
                s3_client=s3_client,
                execution_context=execution_context,
                incremental_reference=incremental_reference,
            ),
        ))

    return with_snapshot_bucket(snapshot_manager_attach_checkpoint_state_to_result(
        manager,
        checkpoint_state,
        snapshot_manager_start_full_export(
            manager,
            table_name,
            table_arn,
            ddb_client=ddb_client,
            execution_context=execution_context,
        ),
    ))

def snapshot_manager_start_full_export(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    ddb_client: Any,
    execution_context: Dict[str, Any],
) -> Dict[str, Any]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    snapshot_bucket = snapshot_manager_resolve_snapshot_bucket(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    export_bucket_params = _build_export_bucket_params(
        config,
        bucket_name=snapshot_bucket,
    )
    bucket = _safe_str_field(export_bucket_params.get("S3Bucket"), field_name="S3Bucket")
    bucket_owner = _resolve_optional_text(export_bucket_params.get("S3BucketOwner"))
    wait_for_completion = bool(config.get("wait_for_completion"))
    run_time = _safe_get_field(config, "run_time", field_name="manager.config")
    if not isinstance(run_time, datetime):
        raise RuntimeError("manager.config.run_time deve ser datetime")

    s3_prefix = snapshot_manager_build_mode_prefix(
        manager,
        "full",
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    params = {
        "TableArn": table_arn,
        **export_bucket_params,
        "S3Prefix": s3_prefix,
        "ExportFormat": "DYNAMODB_JSON",
        "ExportType": "FULL_EXPORT",
        "ClientToken": snapshot_manager_build_export_client_token(
            table_arn=table_arn,
            export_bucket_params=export_bucket_params,
            s3_prefix=s3_prefix,
            export_type="FULL_EXPORT",
            export_format="DYNAMODB_JSON",
        ),
    }

    snapshot_manager_ensure_point_in_time_recovery(
        manager,
        table_name,
        table_arn,
        ddb_client=ddb_client,
    )
    logger.info("Iniciando FULL export: %s", table_name)
    _log_event(
        "export.full.start",
        table_name=table_name,
        table_arn=table_arn,
        s3_bucket=bucket,
        s3_bucket_owner=bucket_owner,
        s3_prefix=s3_prefix,
        wait_for_completion=wait_for_completion,
        session_mode=execution_context.get("session_mode"),
        assume_role_arn=execution_context.get("assume_role_arn"),
        table_account_id=execution_context.get("table_account_id"),
        table_region=execution_context.get("table_region"),
    )
    try:
        response = ddb_client.export_table_to_point_in_time(**params)
    except ClientError as exc:
        raise _build_aws_runtime_error("DynamoDB FULL Export", exc, resource=table_name) from exc
    except Exception as exc:
        raise RuntimeError(f"Falha inesperada no FULL export da tabela {table_name}: {exc}") from exc
    export_desc = response.get("ExportDescription") if isinstance(response, dict) else None
    if not isinstance(export_desc, dict):
        raise RuntimeError(f"Resposta inesperada do FULL export da tabela {table_name}")
    export_ref = _safe_str_field(export_desc.get("ExportArn"), field_name=f"ExportArn ({table_name})")
    export_log_fields = _build_export_fields(export_ref, field_name="export_ref")
    export_result_fields = _build_export_fields(export_ref, field_name="export_arn")
    status = "STARTED"
    if wait_for_completion:
        status = snapshot_manager_wait_export(manager, export_ref, ddb_client=ddb_client)
    _log_event(
        "export.full.started",
        table_name=table_name,
        status=status,
        **export_log_fields,
    )

    return {
        "table_name": table_name,
        "table_arn": table_arn,
        "mode": "FULL",
        "status": status,
        "source": "native",
        **export_result_fields,
        "snapshot_bucket": snapshot_bucket,
        "started_at": _dt_to_iso(run_time),
        "s3_prefix": s3_prefix,
        "checkpoint_to": _dt_to_iso(run_time),
        "assume_role_arn": execution_context.get("assume_role_arn"),
        "table_account_id": execution_context.get("table_account_id"),
        "table_region": execution_context.get("table_region"),
    }

def snapshot_manager_start_incremental_export(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    export_from: datetime,
    export_to: datetime,
    *,
    ddb_client: Any,
    s3_client: Any,
    execution_context: Dict[str, Any],
    incremental_reference: Dict[str, Any],
) -> Dict[str, Any]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    snapshot_bucket = snapshot_manager_resolve_snapshot_bucket(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    export_bucket_params = _build_export_bucket_params(
        config,
        bucket_name=snapshot_bucket,
    )
    bucket = _safe_str_field(export_bucket_params.get("S3Bucket"), field_name="S3Bucket")
    bucket_owner = _resolve_optional_text(export_bucket_params.get("S3BucketOwner"))
    wait_for_completion = bool(config.get("wait_for_completion"))
    fallback_enabled = bool(config.get("fallback_enabled"))

    s3_prefix = snapshot_manager_build_mode_prefix(
        manager,
        "incremental",
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    params = {
        "TableArn": table_arn,
        **export_bucket_params,
        "S3Prefix": s3_prefix,
        "ExportFormat": "DYNAMODB_JSON",
        "ExportType": "INCREMENTAL_EXPORT",
        "IncrementalExportSpecification": {
            "ExportFromTime": export_from,
            "ExportToTime": export_to,
        },
        "ClientToken": snapshot_manager_build_export_client_token(
            table_arn=table_arn,
            export_bucket_params=export_bucket_params,
            s3_prefix=s3_prefix,
            export_type="INCREMENTAL_EXPORT",
            export_format="DYNAMODB_JSON",
            export_from=export_from,
            export_to=export_to,
        ),
    }

    snapshot_manager_ensure_point_in_time_recovery(
        manager,
        table_name,
        table_arn,
        ddb_client=ddb_client,
    )
    logger.info("Iniciando INCREMENTAL export: %s (%s -> %s)", table_name, export_from, export_to)
    _log_event(
        "export.incremental.start",
        table_name=table_name,
        table_arn=table_arn,
        s3_bucket=bucket,
        s3_bucket_owner=bucket_owner,
        s3_prefix=s3_prefix,
        export_from=_dt_to_iso(export_from),
        export_to=_dt_to_iso(export_to),
        wait_for_completion=wait_for_completion,
        session_mode=execution_context.get("session_mode"),
        assume_role_arn=execution_context.get("assume_role_arn"),
        table_account_id=execution_context.get("table_account_id"),
        table_region=execution_context.get("table_region"),
    )

    try:
        response = ddb_client.export_table_to_point_in_time(**params)
        export_desc = response.get("ExportDescription") if isinstance(response, dict) else None
        if not isinstance(export_desc, dict):
            raise RuntimeError(f"Resposta inesperada do INCREMENTAL export da tabela {table_name}")
        export_ref = _safe_str_field(export_desc.get("ExportArn"), field_name=f"ExportArn ({table_name})")
        export_log_fields = _build_export_fields(export_ref, field_name="export_ref")
        export_result_fields = _build_export_fields(export_ref, field_name="export_arn")
        status = "STARTED"
        if wait_for_completion:
            status = snapshot_manager_wait_export(manager, export_ref, ddb_client=ddb_client)
        _log_event(
            "export.incremental.started",
            table_name=table_name,
            status=status,
            **export_log_fields,
        )

        return {
            "table_name": table_name,
            "table_arn": table_arn,
            "mode": "INCREMENTAL",
            "status": status,
            "source": "native",
            **export_result_fields,
            "snapshot_bucket": snapshot_bucket,
            "started_at": _dt_to_iso(export_to),
            "s3_prefix": s3_prefix,
            "checkpoint_from": _safe_str_field(
                incremental_reference.get("checkpoint_from"),
                field_name="incremental_reference.checkpoint_from",
            ),
            "checkpoint_source": incremental_reference.get("checkpoint_source"),
            "full_run_id": incremental_reference.get("full_run_id"),
            "full_export_s3_prefix": incremental_reference.get("full_export_s3_prefix"),
            "checkpoint_to": _dt_to_iso(export_to),
            "assume_role_arn": execution_context.get("assume_role_arn"),
            "table_account_id": execution_context.get("table_account_id"),
            "table_region": execution_context.get("table_region"),
        }

    except ClientError as exc:
        error_msg = _client_error_message(exc)
        if fallback_enabled and snapshot_manager_should_fallback_incremental(manager, exc, error_msg):
            logger.warning(
                "Fallback de incremental ativado para tabela %s. Motivo: %s",
                table_name,
                error_msg,
            )
            _log_event(
                "export.incremental.fallback",
                table_name=table_name,
                table_arn=table_arn,
                reason=error_msg,
                level=logging.WARNING,
            )
            return snapshot_manager_start_incremental_scan_fallback(manager, 
                table_name,
                table_arn,
                export_from,
                export_to,
                ddb_client=ddb_client,
                s3_client=s3_client,
                execution_context=execution_context,
                incremental_reference=incremental_reference,
            )
        raise _build_aws_runtime_error("DynamoDB INCREMENTAL Export", exc, resource=table_name) from exc
    except Exception as exc:
        raise RuntimeError(f"Erro inesperado no INCREMENTAL export da tabela {table_name}: {exc}") from exc

def snapshot_manager_start_incremental_scan_fallback(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    export_from: datetime,
    export_to: datetime,
    *,
    ddb_client: Any,
    s3_client: Any,
    execution_context: Dict[str, Any],
    incremental_reference: Dict[str, Any],
) -> Dict[str, Any]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    bucket = snapshot_manager_resolve_snapshot_bucket(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )
    fallback_updated_attr = _safe_str_field(
        config.get("fallback_updated_attr"),
        field_name="fallback_updated_attr",
    )
    fallback_updated_attr_type = _safe_str_field(
        config.get("fallback_updated_attr_type"),
        field_name="fallback_updated_attr_type",
    )
    fallback_partition_size_raw = _safe_get_field(
        config,
        "fallback_partition_size",
        field_name="manager.config",
    )
    fallback_partition_size = int(fallback_partition_size_raw)
    fallback_compress = bool(config.get("fallback_compress"))

    prefix = snapshot_manager_build_mode_prefix(
        manager,
        "incremental-fallback",
        table_name,
        table_arn,
        execution_context=execution_context,
    )

    logger.info("Iniciando fallback por Scan em %s", table_name)
    _log_event(
        "fallback.scan.start",
        table_name=table_name,
        table_arn=table_arn,
        s3_bucket=bucket,
        s3_prefix=prefix,
        export_from=_dt_to_iso(export_from),
        export_to=_dt_to_iso(export_to),
        updated_attr=fallback_updated_attr,
        updated_attr_type=fallback_updated_attr_type,
        partition_size=fallback_partition_size,
        compress=fallback_compress,
        session_mode=execution_context.get("session_mode"),
        assume_role_arn=execution_context.get("assume_role_arn"),
        table_account_id=execution_context.get("table_account_id"),
        table_region=execution_context.get("table_region"),
    )
    result = snapshot_manager_scan_to_s3_partitioned(manager, 
        table_name=table_name,
        table_arn=table_arn,
        export_from=export_from,
        export_to=export_to,
        prefix=prefix,
        ddb_client=ddb_client,
        s3_client=s3_client,
        execution_context=execution_context,
    )
    _log_event(
        "fallback.scan.completed",
        table_name=table_name,
        files_written=result.get("files_written"),
        items_written=result.get("items_written"),
        manifest=result.get("manifest"),
    )

    return {
        "table_name": table_name,
        "table_arn": table_arn,
        "mode": "INCREMENTAL",
        "status": "COMPLETED",
        "source": "scan_fallback",
        "snapshot_bucket": bucket,
        "s3_prefix": prefix,
        "checkpoint_from": _safe_str_field(
            incremental_reference.get("checkpoint_from"),
            field_name="incremental_reference.checkpoint_from",
        ),
        "checkpoint_source": incremental_reference.get("checkpoint_source"),
        "full_run_id": incremental_reference.get("full_run_id"),
        "full_export_s3_prefix": incremental_reference.get("full_export_s3_prefix"),
        "checkpoint_to": _dt_to_iso(export_to),
        "assume_role_arn": execution_context.get("assume_role_arn"),
        "table_account_id": execution_context.get("table_account_id"),
        "table_region": execution_context.get("table_region"),
        **result,
    }

def snapshot_manager_scan_to_s3_partitioned(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    export_from: datetime,
    export_to: datetime,
    prefix: str,
    *,
    ddb_client: Any,
    s3_client: Any,
    execution_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    fallback_partition_size_raw = _safe_get_field(
        config,
        "fallback_partition_size",
        field_name="manager.config",
    )
    fallback_partition_size = int(fallback_partition_size_raw)
    fallback_updated_attr = _safe_str_field(
        config.get("fallback_updated_attr"),
        field_name="fallback_updated_attr",
    )
    fallback_updated_attr_type = _safe_str_field(
        config.get("fallback_updated_attr_type"),
        field_name="fallback_updated_attr_type",
    )
    fallback_compress = bool(config.get("fallback_compress"))
    bucket = snapshot_manager_resolve_snapshot_bucket(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )

    scan_expression, expr_names, expr_values = snapshot_manager_build_scan_filter(manager, export_from, export_to)
    kwargs = {
        "TableName": table_name,
    }
    if scan_expression:
        kwargs["FilterExpression"] = scan_expression
        kwargs["ExpressionAttributeNames"] = expr_names
        kwargs["ExpressionAttributeValues"] = expr_values

    paginator = ddb_client.get_paginator("scan")
    partition = []
    total_items = 0
    total_files = 0
    total_pages = 0
    part_files: List[str] = []
    partition_size = max(1, fallback_partition_size)

    try:
        for page in paginator.paginate(**kwargs):
            total_pages += 1
            if not isinstance(page, dict):
                raise RuntimeError(f"Página inválida no scan da tabela {table_name}")
            items = page.get("Items")
            if not isinstance(items, list):
                logger.warning("Página sem Items válido na tabela %s; seguindo", table_name)
                continue
            _log_event(
                "fallback.scan.page",
                table_name=table_name,
                page=total_pages,
                page_items=len(items),
                accumulated_items=total_items,
                level=logging.DEBUG,
            )
            for raw_item in items:
                if not isinstance(raw_item, dict):
                    logger.warning("Item inválido durante scan da tabela %s; ignorando", table_name)
                    continue
                item = _safe_json({
                    key: deserializer.deserialize(value) for key, value in raw_item.items()
                })

                updated_value = item.get(fallback_updated_attr)
                parsed = _parse_updated_value(updated_value, fallback_updated_attr_type)
                if parsed is None or not (export_from <= parsed <= export_to):
                    continue

                partition.append(item)
                total_items += 1

                if len(partition) == partition_size:
                    total_files += 1
                    part_key = snapshot_manager_write_partition(manager, 
                        prefix,
                        table_name,
                        table_arn,
                        total_files,
                        partition,
                        s3_client=s3_client,
                        execution_context=execution_context,
                    )
                    part_files.append(part_key)
                    partition = []
    except ClientError as exc:
        raise _build_aws_runtime_error("DynamoDB Scan fallback", exc, resource=table_name) from exc
    except Exception as exc:
        raise RuntimeError(f"Falha durante scan fallback da tabela {table_name}: {exc}") from exc

    if partition:
        total_files += 1
        part_key = snapshot_manager_write_partition(manager, 
            prefix,
            table_name,
            table_arn,
            total_files,
            partition,
            s3_client=s3_client,
            execution_context=execution_context,
        )
        part_files.append(part_key)

    manifest_key = f"{prefix}/manifest.json"
    manifest = {
        "table": table_name,
        "table_arn": table_arn,
        "mode": "incremental_scan_fallback",
        "source": "scan_fallback",
        "total_items": total_items,
        "total_parts": total_files,
        "compress": fallback_compress,
        "partition_size": partition_size,
        "from": _dt_to_iso(export_from),
        "to": _dt_to_iso(export_to),
        "files": part_files,
    }
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError as exc:
        raise _build_aws_runtime_error(
            "S3 PutObject manifest fallback",
            exc,
            resource=f"s3://{bucket}/{manifest_key}",
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Falha ao gravar manifest fallback da tabela {table_name}: {exc}") from exc
    _log_event(
        "fallback.manifest.write.success",
        table_name=table_name,
        manifest_key=manifest_key,
        files=total_files,
        items=total_items,
    )

    return {
        "files_written": total_files,
        "items_written": total_items,
        "manifest": manifest_key,
        "pages_scanned": total_pages,
    }

def snapshot_manager_build_scan_filter(manager: Dict[str, Any], export_from: datetime, export_to: datetime) -> tuple[str, Dict[str, str], Dict[str, dict]]:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    fallback_updated_attr_type = _safe_str_field(
        config.get("fallback_updated_attr_type"),
        field_name="fallback_updated_attr_type",
    )
    fallback_updated_attr = _safe_str_field(
        config.get("fallback_updated_attr"),
        field_name="fallback_updated_attr",
    )
    attr_alias = "#u"
    if fallback_updated_attr_type == "number":
        return (
            f"{attr_alias} BETWEEN :from AND :to",
            {attr_alias: fallback_updated_attr},
            {
                ":from": {"N": str(export_from.timestamp())},
                ":to": {"N": str(export_to.timestamp())},
            },
        )

    return (
        f"{attr_alias} BETWEEN :from AND :to",
        {attr_alias: fallback_updated_attr},
        {
            ":from": {"S": _dt_to_iso(export_from)},
            ":to": {"S": _dt_to_iso(export_to)},
        },
    )

def snapshot_manager_write_partition(
    manager: Dict[str, Any],
    prefix: str,
    table_name: str,
    table_arn: str,
    index: int,
    batch: List[Dict[str, Any]],
    *,
    s3_client: Any,
    execution_context: Optional[Dict[str, Any]] = None,
) -> str:
    config = _safe_dict_field(
        _safe_get_field(manager, "config", field_name="manager"),
        "manager.config",
    )
    fallback_compress = bool(config.get("fallback_compress"))
    bucket = snapshot_manager_resolve_snapshot_bucket(
        manager,
        table_name,
        table_arn,
        execution_context=execution_context,
    )

    ext = ".jsonl.gz" if fallback_compress else ".jsonl"
    key = f"{prefix}/part={index:05d}{ext}"
    buffer = io.BytesIO()
    _log_event(
        "fallback.partition.write.start",
        table_name=table_name,
        table_arn=table_arn,
        part=index,
        records=len(batch),
        key=key,
        compressed=fallback_compress,
        level=logging.DEBUG,
    )

    if fallback_compress:
        try:
            with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
                for item in batch:
                    gz.write(json.dumps(item, default=str).encode("utf-8"))
                    gz.write(b"\n")
        except Exception as exc:
            raise RuntimeError(f"Falha ao serializar partição compactada {index} da tabela {table_name}: {exc}") from exc
        content_type = "application/gzip"
        metadata = {
            "table": table_name,
            "table_arn": table_arn,
            "mode": "incremental_scan_fallback",
            "compressed": "true",
            "records": str(len(batch)),
            "part": str(index),
        }
    else:
        try:
            content = "\n".join(json.dumps(item, default=str) for item in batch) + "\n"
            buffer.write(content.encode("utf-8"))
        except Exception as exc:
            raise RuntimeError(f"Falha ao serializar partição {index} da tabela {table_name}: {exc}") from exc
        content_type = "application/jsonl"
        metadata = {
            "table": table_name,
            "table_arn": table_arn,
            "mode": "incremental_scan_fallback",
            "compressed": "false",
            "records": str(len(batch)),
            "part": str(index),
        }

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType=content_type,
            Metadata=metadata,
        )
    except ClientError as exc:
        raise _build_aws_runtime_error(
            "S3 PutObject partição fallback",
            exc,
            resource=f"s3://{bucket}/{key}",
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Falha ao gravar partição {index} da tabela {table_name} em S3: {exc}") from exc
    _log_event(
        "fallback.partition.write.success",
        table_name=table_name,
        part=index,
        records=len(batch),
        key=key,
        level=logging.DEBUG,
    )
    return key

def snapshot_manager_should_fallback_incremental(manager: Dict[str, Any], exc: ClientError, message: str) -> bool:
    code = _client_error_code(exc)
    lower_msg = message.lower()
    hints = {
        "incremental", "incremental_export", "not supported", "not enabled",
        "invalid exporttype", "exporttype", "pitr", "point in time",
        "24 hour", "24 hours", "15 minute", "15 minutes", "exportfromtime", "exporttotime",
    }
    if code == "ValidationException" and any(h in lower_msg for h in hints):
        return True
    if code in {"KMSAccessDeniedException", "ExportNotCompleted", "InvalidExportTypeException"}:
        return True
    return False

def snapshot_manager_wait_export(manager: Dict[str, Any], export_ref: str, *, ddb_client: Any, timeout_minutes: int = 120) -> str:
    poll_secs = 10
    max_seconds = timeout_minutes * 60
    elapsed = 0
    export_log_fields = _build_export_fields(export_ref, field_name="export_ref")
    _log_event(
        "export.wait.start",
        timeout_minutes=timeout_minutes,
        poll_seconds=poll_secs,
        **export_log_fields,
    )

    while elapsed < max_seconds:
        try:
            response = ddb_client.describe_export(ExportArn=export_ref)
        except ClientError as exc:
            raise _build_aws_runtime_error("DynamoDB DescribeExport", exc, resource=export_ref) from exc
        except Exception as exc:
            raise RuntimeError(f"Erro inesperado ao consultar status do export {export_ref}: {exc}") from exc
        description = response.get("ExportDescription") if isinstance(response, dict) else None
        if not isinstance(description, dict):
            raise RuntimeError(f"Resposta inválida de describe_export para {export_ref}")
        status = _safe_str_field(description.get("ExportStatus"), field_name="ExportStatus")
        _log_event(
            "export.wait.poll",
            status=status,
            elapsed_seconds=elapsed,
            level=logging.DEBUG,
            **export_log_fields,
        )

        if status in {"COMPLETED", "FAILED", "CANCELLED"}:
            if status != "COMPLETED":
                reason = description.get("FailureMessage", "- sem mensagem")
                _log_event(
                    "export.wait.failed",
                    status=status,
                    failure_message=reason,
                    level=logging.ERROR,
                    **export_log_fields,
                )
                raise RuntimeError(f"Export {status}: {reason}")
            _log_event("export.wait.completed", elapsed_seconds=elapsed, **export_log_fields)
            return status

        import time

        time.sleep(poll_secs)
        elapsed += poll_secs

    _log_event("export.wait.timeout", timeout_minutes=timeout_minutes, level=logging.ERROR, **export_log_fields)
    raise TimeoutError(f"Timeout aguardando export {export_ref}")

def snapshot_manager_build_client_token(*parts: str) -> str:
    payload = "|".join(_resolve_optional_text(part, "") or "" for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:32]

def snapshot_manager_build_export_client_token(
    *,
    table_arn: str,
    export_bucket_params: Dict[str, str],
    s3_prefix: str,
    export_type: str,
    export_format: str,
    export_from: Optional[datetime] = None,
    export_to: Optional[datetime] = None,
) -> str:
    token_parts = [
        _safe_str_field(table_arn, field_name="TableArn"),
        _safe_str_field(export_bucket_params.get("S3Bucket"), field_name="S3Bucket"),
        _resolve_optional_text(export_bucket_params.get("S3BucketOwner"), "") or "",
        _safe_str_field(s3_prefix, field_name="S3Prefix"),
        _safe_str_field(export_type, field_name="ExportType"),
        _safe_str_field(export_format, field_name="ExportFormat"),
    ]
    if isinstance(export_from, datetime):
        token_parts.append(export_from.astimezone(timezone.utc).isoformat())
    if isinstance(export_to, datetime):
        token_parts.append(export_to.astimezone(timezone.utc).isoformat())
    return snapshot_manager_build_client_token(*token_parts)

def lambda_handler(
    event: Optional[Dict[str, Any]] = None,
    context: Any = None,
    *,
    emit_cloudwatch_output: bool = True,
) -> Dict[str, Any]:
    event_keys = sorted(event.keys()) if isinstance(event, dict) else []
    config: Optional[SnapshotConfig] = None
    manager: Optional[Dict[str, Any]] = None
    _log_event(
        "handler.start",
        event_keys=event_keys,
        has_event=event is not None,
        aws_request_id=getattr(context, "aws_request_id", None),
    )
    try:
        config = build_snapshot_config(event)
        manager = create_snapshot_manager(config)
        result = {
            "ok": True,
            "snapshot_bucket": config["bucket"],
            **snapshot_manager_run(manager, event),
        }
        _log_event(
            "handler.success",
            status=result.get("status"),
            run_id=result.get("run_id"),
            result_count=len(result.get("results", [])) if isinstance(result.get("results"), list) else 0,
            checkpoint_error=result.get("checkpoint_error"),
            snapshot_bucket=result.get("snapshot_bucket"),
        )
        if emit_cloudwatch_output:
            _emit_output_to_cloudwatch(
                "lambda_handler.success",
                result,
                config=config,
                context=context,
            )
        _emit_output_to_dynamodb(
            "lambda_handler.success",
            result,
            config=config,
            event=event,
            context=context,
            manager=manager,
        )
        return result
    except ValueError as exc:
        error_info = _normalize_exception(exc)
        error_fields = _build_error_response_fields(exc, info=error_info)
        logger.exception("Erro de configuração")
        _log_event("handler.config_error", error=str(exc), level=logging.ERROR)
        response = {
            "ok": False,
            "status": "error",
            "error_type": "config",
            "error_category": error_info.get("error_category"),
            "error_code": error_info.get("error_code"),
            **error_fields,
        }
        snapshot_bucket = _resolve_optional_text(config.get("bucket")) if isinstance(config, dict) else None
        if snapshot_bucket:
            response["snapshot_bucket"] = snapshot_bucket
        if emit_cloudwatch_output:
            _emit_output_to_cloudwatch(
                "lambda_handler.config_error",
                response,
                config=config,
                context=context,
            )
        _emit_output_to_dynamodb(
            "lambda_handler.config_error",
            response,
            config=config,
            event=event,
            context=context,
            manager=manager,
        )
        return response
    except Exception as exc:
        error_info = _normalize_exception(exc)
        error_fields = _build_error_response_fields(exc, info=error_info)
        error_type = str(error_info.get("error_type", "runtime"))
        logger.exception("Erro não tratado no handler")
        _log_event(
            "handler.runtime_error",
            error=str(exc),
            error_type=error_type,
            error_code=error_info.get("error_code"),
            retryable=error_info.get("retryable", False),
            level=logging.ERROR,
        )
        response = {
            "ok": False,
            "status": "error",
            "error_type": error_type,
            "error_code": error_info.get("error_code"),
            "error_category": error_info.get("error_category"),
            **error_fields,
        }
        snapshot_bucket = _resolve_optional_text(config.get("bucket")) if isinstance(config, dict) else None
        if snapshot_bucket:
            response["snapshot_bucket"] = snapshot_bucket
        if emit_cloudwatch_output:
            _emit_output_to_cloudwatch(
                "lambda_handler.runtime_error",
                response,
                config=config,
                context=context,
            )
        _emit_output_to_dynamodb(
            "lambda_handler.runtime_error",
            response,
            config=config,
            event=event,
            context=context,
            manager=manager,
        )
        return response
