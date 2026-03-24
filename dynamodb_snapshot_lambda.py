"""AWS Lambda para export de snapshots de tabelas DynamoDB para S3.

Implementação reescrita com foco em previsibilidade operacional:
- resolve config com suporte snake_case + camelCase
- resolve targets por lista e CSV (inline, arquivo local ou s3://)
- suporte a ignore por lista e CSV
- export FULL e INCREMENTAL com checkpoint em DynamoDB
- robustez no cálculo de bucket por região (evita falso positivo de sufixo)
- output opcional em CloudWatch (logs) e DynamoDB
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import re
import threading
import time
import weakref
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
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

logger = logging.getLogger()
if not logger.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

serializer = TypeSerializer()
deserializer = TypeDeserializer()

ROLE_TEMPLATE_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
AWS_ACCOUNT_ID_PATTERN = re.compile(r"^\d{12}$")
AWS_REGION_TEXT_PATTERN = re.compile(
    r"^[a-z]{2}(?:-(?:gov|iso|isob|isof|isofb))?-[a-z]+(?:-[a-z]+)?-\d$"
)

AWS_NOT_FOUND_ERROR_CODES = {"ResourceNotFoundException", "NoSuchKey", "NotFound", "404"}
AWS_ACCESS_DENIED_ERROR_CODES = {
    "AccessDenied",
    "AccessDeniedException",
    "AllAccessDisabled",
    "UnauthorizedOperation",
    "UnrecognizedClientException",
    "InvalidClientTokenId",
    "SignatureDoesNotMatch",
}

EXPORT_PENDING_STATUSES = {"STARTED", "IN_PROGRESS", "PENDING"}
EXPORT_TERMINAL_FAILURE_STATUSES = {"FAILED", "CANCELLED"}

CHECKPOINT_DYNAMODB_PARTITION_KEY = "TableName"
CHECKPOINT_DYNAMODB_SORT_KEY = "RecordType"
CHECKPOINT_DYNAMODB_CURRENT_RECORD = "CURRENT"
CHECKPOINT_DYNAMODB_TABLE_POLL_SECONDS = 2
CHECKPOINT_DYNAMODB_TABLE_TIMEOUT_SECONDS = 60

OUTPUT_DYNAMODB_PARTITION_KEY = "Export ARN"
OUTPUT_DYNAMODB_TABLE_POLL_SECONDS = 2
OUTPUT_DYNAMODB_TABLE_TIMEOUT_SECONDS = 60
OUTPUT_DYNAMODB_LOCAL_TIMEZONE = timezone(timedelta(hours=-3))

PITR_ENABLE_POLL_SECONDS = 5
PITR_ENABLE_TIMEOUT_SECONDS = 300
EXPORT_WAIT_TIMEOUT_SECONDS = 900
EXPORT_WAIT_POLL_SECONDS = 5

INCREMENTAL_EXPORT_MIN_WINDOW = timedelta(minutes=15)
INCREMENTAL_EXPORT_MAX_WINDOW = timedelta(hours=24)
MAX_INCREMENTAL_EXPORTS_PER_CYCLE = 6
INCREMENTAL_EXPORT_VIEW_TYPES = frozenset({"NEW_IMAGE", "NEW_AND_OLD_IMAGES"})
DEFAULT_INCREMENTAL_EXPORT_VIEW_TYPE = "NEW_IMAGE"

CLOUDWATCH_OUTPUT_MAX_BYTES = 240000


_DEFAULT_AWS_SESSION_LOCK = threading.Lock()
_DEFAULT_AWS_SESSION: Any = None
_SESSION_CLIENT_CACHE_LOCK = threading.Lock()
_SESSION_CLIENT_CACHE: weakref.WeakKeyDictionary[Any, Dict[tuple[str, str], Any]] = (
    weakref.WeakKeyDictionary()
)


@dataclass(frozen=True)
class TableTarget:
    raw_ref: str
    table_name: str
    table_arn: str
    account_id: str
    region: str


def _resolve_optional_text(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _resolve_incremental_export_view_type(*values: Any) -> str:
    view_type = (_resolve_optional_text(*values, DEFAULT_INCREMENTAL_EXPORT_VIEW_TYPE) or DEFAULT_INCREMENTAL_EXPORT_VIEW_TYPE).strip().upper()
    if view_type not in INCREMENTAL_EXPORT_VIEW_TYPES:
        allowed_values = ", ".join(sorted(INCREMENTAL_EXPORT_VIEW_TYPES))
        raise ValueError(
            f"INCREMENTAL_EXPORT_VIEW_TYPE inválido: {view_type}. Valores permitidos: {allowed_values}"
        )
    return view_type


def _safe_str_field(value: Any, *, field_name: str, required: bool = True) -> str:
    if value is None:
        if required:
            raise ValueError(f"{field_name} é obrigatório")
        return ""
    text = str(value).strip()
    if not text and required:
        raise ValueError(f"{field_name} é obrigatório")
    return text


def _safe_dict_field(value: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} deve ser um objeto JSON")
    return value


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    text = _resolve_optional_text(value)
    if not text:
        return None
    normalized = text
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_datetime_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return _parse_iso_datetime(value)


def _extract_pitr_window(pitr_desc: Dict[str, Any]) -> Dict[str, Optional[datetime]]:
    return {
        "earliest_restorable": _coerce_datetime_utc(pitr_desc.get("EarliestRestorableDateTime")),
        "latest_restorable": _coerce_datetime_utc(pitr_desc.get("LatestRestorableDateTime")),
    }


def _clamp_incremental_export_window_to_pitr(
    *,
    export_from: datetime,
    export_to: datetime,
    pitr_window: Dict[str, Optional[datetime]],
    table_name: str,
    table_arn: str,
) -> Tuple[datetime, datetime]:
    earliest = pitr_window.get("earliest_restorable")
    latest = pitr_window.get("latest_restorable")
    resolved_export_from = max(export_from, earliest) if isinstance(earliest, datetime) else export_from
    resolved_export_to = min(export_to, latest) if isinstance(latest, datetime) else export_to

    if resolved_export_from != export_from or resolved_export_to != export_to:
        _log_event(
            "export.incremental.window.adjusted_to_pitr",
            table_name=table_name,
            table_arn=table_arn,
            requested_export_from=_dt_to_iso(export_from),
            requested_export_to=_dt_to_iso(export_to),
            export_from=_dt_to_iso(resolved_export_from),
            export_to=_dt_to_iso(resolved_export_to),
            earliest_restorable=_dt_to_iso(earliest) if isinstance(earliest, datetime) else None,
            latest_restorable=_dt_to_iso(latest) if isinstance(latest, datetime) else None,
        )

    return resolved_export_from, resolved_export_to


def _coerce_non_negative_int(value: Any, *, field_name: str, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        parsed = int(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} deve ser um inteiro válido") from exc
    return parsed if parsed >= 0 else 0


def _coerce_optional_non_negative_int(value: Any, *, field_name: str) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} deve ser um inteiro válido") from exc
    return parsed if parsed >= 0 else 0


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _to_json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return _dt_to_iso(value)
    return value


def _log_event(action: str, *, level: int = logging.INFO, **fields: Any) -> None:
    if not logger.isEnabledFor(level):
        return
    payload = {"action": action, **fields}
    logger.log(level, "%s", json.dumps(_to_json_safe(payload), ensure_ascii=False, default=str))


def _client_error_code(exc: ClientError) -> str:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    error = response.get("Error")
    if not isinstance(error, dict):
        return ""
    return _safe_str_field(error.get("Code"), field_name="Error.Code", required=False)


def _client_error_message(exc: ClientError) -> str:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    error = response.get("Error")
    if not isinstance(error, dict):
        return str(exc)
    message = _safe_str_field(error.get("Message"), field_name="Error.Message", required=False)
    return message or str(exc)


def _build_aws_runtime_error(operation: str, exc: ClientError, *, resource: str) -> RuntimeError:
    code = _client_error_code(exc)
    message = _client_error_message(exc)
    detail = f"{operation} falhou para {resource}. code={code} message={message}"
    return RuntimeError(detail)


def _is_localstack_endpoint(endpoint_url: Optional[str]) -> bool:
    url = _resolve_optional_text(endpoint_url)
    if not url:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return False
    if host in {"localhost", "127.0.0.1", "::1", "localstack", "localstack-main"}:
        return True
    return host.endswith(".localstack.cloud")


def _is_export_operation_unsupported(exc: ClientError) -> bool:
    code = _client_error_code(exc)
    if code in {"UnknownOperationException", "NotImplementedException"}:
        return True
    message = _client_error_message(exc).lower()
    return "unknown operation" in message or "not implemented" in message


def _can_use_scan_fallback(config: Dict[str, Any], ddb_client: Any, exc: ClientError) -> bool:
    if not bool(config.get("scan_fallback_enabled")):
        return False
    if not _is_export_operation_unsupported(exc):
        return False
    endpoint_url = _resolve_optional_text(getattr(getattr(ddb_client, "meta", None), "endpoint_url", ""))
    return _is_localstack_endpoint(endpoint_url)


def _resolve_runtime_region(session_region: Optional[str] = None) -> Optional[str]:
    return _resolve_optional_text(session_region, os.getenv("AWS_REGION"), os.getenv("AWS_DEFAULT_REGION"))


def _get_default_aws_session() -> Any:
    global _DEFAULT_AWS_SESSION
    with _DEFAULT_AWS_SESSION_LOCK:
        if _DEFAULT_AWS_SESSION is None:
            _DEFAULT_AWS_SESSION = boto3.session.Session()
        return _DEFAULT_AWS_SESSION


def _get_session_client(session: Any, service_name: str, *, region_name: Optional[str] = None) -> Any:
    cache_key = (service_name, region_name or "")

    try:
        with _SESSION_CLIENT_CACHE_LOCK:
            session_cache = _SESSION_CLIENT_CACHE.get(session)
            if session_cache is None:
                session_cache = {}
                _SESSION_CLIENT_CACHE[session] = session_cache
            cached = session_cache.get(cache_key)
        if cached is not None:
            return cached
    except TypeError:
        session_cache = None

    client = session.client(service_name, region_name=region_name) if region_name else session.client(service_name)
    if session_cache is None:
        return client

    with _SESSION_CLIENT_CACHE_LOCK:
        existing_cache = _SESSION_CLIENT_CACHE.get(session)
        if existing_cache is None:
            existing_cache = {}
            _SESSION_CLIENT_CACHE[session] = existing_cache
        existing_cache[cache_key] = client
    return client


def _extract_event_payload(event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        return {}
    payload = dict(event)
    raw_body = payload.get("body")
    if isinstance(raw_body, dict):
        payload.update(raw_body)
    elif isinstance(raw_body, str):
        body_text = raw_body.strip()
        if body_text:
            try:
                parsed = json.loads(body_text)
                if isinstance(parsed, dict):
                    payload.update(parsed)
            except json.JSONDecodeError:
                _log_event(
                    "event.body.invalid_json",
                    level=logging.WARNING,
                )
    return payload


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return _normalize_list(parsed)
            except json.JSONDecodeError:
                pass
        return [item.strip() for item in text.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        values: List[str] = []
        for item in value:
            item_text = _resolve_optional_text(item)
            if item_text:
                values.append(item_text)
        return values
    fallback = _resolve_optional_text(value)
    return [fallback] if fallback else []


def _dedupe_values(values: Iterable[str], *, case_insensitive: bool = False) -> List[str]:
    seen: set[str] = set()
    deduped: List[str] = []
    for value in values:
        raw = _resolve_optional_text(value)
        if not raw:
            continue
        key = raw.lower() if case_insensitive else raw
        if key in seen:
            continue
        seen.add(key)
        deduped.append(raw)
    return deduped


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _resolve_env_first_bool(event_value: Any, env_name: str, default: bool) -> bool:
    env_value = os.getenv(env_name)
    if env_value is not None and str(env_value).strip():
        return _parse_bool(env_value, default=default)
    return _parse_bool(event_value, default=default)


def _parse_arn(arn: str, *, field_name: str) -> Dict[str, str]:
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


def _extract_table_arn_context(table_arn: str, *, field_name: str = "table_arn") -> Dict[str, str]:
    parsed = _parse_arn(table_arn, field_name=field_name)
    if parsed.get("service") != "dynamodb":
        raise ValueError(f"ARN não é DynamoDB: {table_arn}")
    resource = _safe_str_field(parsed.get("resource"), field_name=f"{field_name}.resource")
    if not resource.startswith("table/"):
        raise ValueError(f"ARN de tabela DynamoDB inválido: {table_arn}")
    suffix = resource.split("table/", 1)[1]
    table_name = suffix.split("/", 1)[0]
    if not table_name:
        raise ValueError(f"ARN de tabela DynamoDB inválido: {table_arn}")
    region = _safe_str_field(parsed.get("region"), field_name=f"{field_name}.region")
    account_id = _safe_str_field(parsed.get("account_id"), field_name=f"{field_name}.account_id")
    return {
        "table_name": table_name,
        "region": region,
        "account_id": account_id,
        "table_arn": table_arn,
    }


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    text = _safe_str_field(uri, field_name="s3_uri")
    if not text.startswith("s3://"):
        raise ValueError(f"URI S3 inválida: {uri}")
    remainder = text[5:]
    bucket, sep, key = remainder.partition("/")
    if not bucket or not sep or not key:
        raise ValueError(f"URI S3 inválida: {uri}")
    return bucket, key


def _extract_local_file_path(source: str) -> Optional[str]:
    raw = _safe_str_field(source, field_name="csv_source")
    if raw.startswith("file://"):
        path = raw[7:]
        return path if path else None
    if raw.startswith("/") or raw.startswith("./") or raw.startswith("../"):
        return raw
    if os.path.exists(raw):
        return raw
    packaged = os.path.join(os.path.dirname(__file__), raw)
    if os.path.exists(packaged):
        return packaged
    return None


def _load_text_from_csv_source(
    source_name: str,
    source: str,
    *,
    session: Any,
    bucket_hint: Optional[str],
) -> str:
    text_source = _safe_str_field(source, field_name=source_name)

    if text_source.startswith("s3://"):
        bucket, key = _parse_s3_uri(text_source)
        s3_client = _get_session_client(session, "s3")
        _log_event("csv.source.load.start", source=source_name, mode="s3", bucket=bucket, key=key)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response.get("Body")
        if body is None:
            raise RuntimeError(f"Objeto S3 vazio para {text_source}")
        content = body.read().decode("utf-8")
        _log_event(
            "csv.source.load.success",
            source=source_name,
            mode="s3",
            bucket=bucket,
            key=key,
            bytes=len(content.encode("utf-8")),
        )
        return content

    local_path = _extract_local_file_path(text_source)
    if local_path:
        _log_event("csv.source.load.start", source=source_name, mode="file", path=local_path)
        with open(local_path, "r", encoding="utf-8") as handle:
            content = handle.read()
        _log_event(
            "csv.source.load.success",
            source=source_name,
            mode="file",
            path=local_path,
            bytes=len(content.encode("utf-8")),
        )
        return content

    looks_like_path = text_source.endswith(".csv") and "," not in text_source and "\n" not in text_source
    if looks_like_path:
        bucket = _resolve_optional_text(bucket_hint)
        if bucket:
            s3_client = _get_session_client(session, "s3")
            _log_event("csv.source.load.start", source=source_name, mode="s3_key", bucket=bucket, key=text_source)
            response = s3_client.get_object(Bucket=bucket, Key=text_source)
            body = response.get("Body")
            if body is None:
                raise RuntimeError(f"Objeto S3 vazio para s3://{bucket}/{text_source}")
            content = body.read().decode("utf-8")
            _log_event(
                "csv.source.load.success",
                source=source_name,
                mode="s3_key",
                bucket=bucket,
                key=text_source,
                bytes=len(content.encode("utf-8")),
            )
            return content
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {text_source}")

    _log_event("csv.source.load.inline", source=source_name, bytes=len(text_source.encode("utf-8")), level=logging.DEBUG)
    return text_source


def _parse_values_from_csv_text(text: str) -> List[str]:
    if not text.strip():
        return []

    reader = csv.reader(io.StringIO(text))
    rows = [row for row in reader if row]
    if not rows:
        return []

    header = [col.strip().lower() for col in rows[0]]
    header_indexes = {
        "target",
        "targets",
        "table",
        "table_name",
        "table_arn",
        "ignore",
        "ignore_target",
        "ignore_targets",
    }

    if any(col in header_indexes for col in header):
        selected_index = 0
        for idx, col in enumerate(header):
            if col in header_indexes:
                selected_index = idx
                break
        content_rows = rows[1:]
        return [
            _safe_str_field(row[selected_index], field_name="csv_value", required=False)
            for row in content_rows
            if len(row) > selected_index and _safe_str_field(row[selected_index], field_name="csv_value", required=False)
        ]

    values: List[str] = []
    for row in rows:
        for cell in row:
            cell_text = _safe_str_field(cell, field_name="csv_cell", required=False)
            if cell_text:
                values.append(cell_text)
    return values


def _load_values_from_csv_source(
    source_name: str,
    source: Optional[str],
    *,
    session: Any,
    bucket_hint: Optional[str],
    optional: bool,
) -> List[str]:
    if not source:
        return []
    try:
        text = _load_text_from_csv_source(
            source_name,
            source,
            session=session,
            bucket_hint=bucket_hint,
        )
        return _dedupe_values(_parse_values_from_csv_text(text), case_insensitive=True)
    except FileNotFoundError as exc:
        if optional:
            _log_event("csv.parse.not_found", source=source_name, error=str(exc), level=logging.WARNING)
            return []
        raise
    except ClientError as exc:
        if optional:
            _log_event(
                "csv.parse.s3_error",
                source=source_name,
                code=_client_error_code(exc),
                message=_client_error_message(exc),
                level=logging.WARNING,
            )
            return []
        raise


def _extract_bucket_region_suffix(bucket: str) -> Optional[str]:
    bucket_name = _safe_str_field(bucket, field_name="bucket")
    parts = [part for part in bucket_name.split("-") if part]
    if len(parts) < 3:
        return None
    candidates = ["-".join(parts[-3:])]
    if len(parts) >= 4:
        candidates.append("-".join(parts[-4:]))
    for candidate in candidates:
        if AWS_REGION_TEXT_PATTERN.fullmatch(candidate):
            return candidate
    return None


def snapshot_manager_build_bucket_name(base_bucket: str, region: Optional[str], *, exact: bool = False) -> str:
    bucket = _safe_str_field(base_bucket, field_name="bucket")
    if exact:
        return bucket
    target_region = _resolve_optional_text(region)
    if not target_region:
        return bucket
    suffix = f"-{target_region}"
    if bucket.endswith(suffix):
        return bucket
    existing = _extract_bucket_region_suffix(bucket)
    if existing:
        _log_event(
            "snapshot.bucket.region_suffix.detected",
            bucket=bucket,
            bucket_region=existing,
            target_region=target_region,
            mismatch=existing != target_region,
            level=(logging.WARNING if existing != target_region else logging.DEBUG),
        )
        return bucket
    return f"{bucket}{suffix}"


def _build_export_prefix(run_time: datetime, target: TableTarget, export_type: str, *, incremental_index: int = 1) -> str:
    date_part = run_time.astimezone(timezone.utc).strftime("%Y%m%d")
    export_segment = "FULL" if export_type == "FULL_EXPORT" else ("INCR" if incremental_index <= 1 else f"INCR{incremental_index}")
    return f"DDB/{date_part}/{target.account_id}/{target.table_name}/{export_segment}"


def _build_client_token(*parts: str) -> str:
    payload = "|".join(_resolve_optional_text(part, "") or "" for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:32]


def _build_export_client_token(
    *,
    table_arn: str,
    bucket: str,
    bucket_owner: Optional[str],
    s3_prefix: str,
    export_type: str,
    export_from: Optional[datetime] = None,
    export_to: Optional[datetime] = None,
    token_salt: Optional[str] = None,
) -> str:
    parts = [
        table_arn,
        bucket,
        _resolve_optional_text(bucket_owner, "") or "",
        s3_prefix,
        export_type,
        "DYNAMODB_JSON",
        _resolve_optional_text(token_salt, "") or "",
    ]
    if isinstance(export_from, datetime):
        parts.append(_dt_to_iso(export_from))
    if isinstance(export_to, datetime):
        parts.append(_dt_to_iso(export_to))
    return _build_client_token(*parts)


def _extract_export_fields(export_arn: str) -> Dict[str, str]:
    value = _safe_str_field(export_arn, field_name="ExportArn")
    return {
        "export_arn": value,
        "export_job_id": value.rsplit("/", 1)[-1] if "/" in value else value,
    }


def _describe_export_description(ddb_client: Any, *, export_arn: str) -> Dict[str, Any]:
    response = ddb_client.describe_export(ExportArn=export_arn)
    return _safe_dict_field(response.get("ExportDescription"), "ExportDescription")


def _extract_export_item_count(export_description: Dict[str, Any]) -> Optional[int]:
    return _coerce_optional_non_negative_int(
        export_description.get("ItemCount"),
        field_name="ExportDescription.ItemCount",
    )


def _resolve_completed_result_item_count(result: Dict[str, Any]) -> Optional[int]:
    direct_item_count = _coerce_optional_non_negative_int(
        result.get("item_count"),
        field_name="result.item_count",
    )
    if direct_item_count is not None:
        return direct_item_count
    return _coerce_optional_non_negative_int(
        result.get("items_written"),
        field_name="result.items_written",
    )


def _normalize_pending_exports(raw_pending: Any) -> List[Dict[str, str]]:
    if isinstance(raw_pending, str):
        text = raw_pending.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        return _normalize_pending_exports(parsed)

    if not isinstance(raw_pending, list):
        return []

    normalized: List[Dict[str, str]] = []
    for item in raw_pending:
        if not isinstance(item, dict):
            continue
        export_arn = _safe_str_field(item.get("export_arn"), field_name="pending.export_arn", required=False)
        checkpoint_to = _safe_str_field(item.get("checkpoint_to"), field_name="pending.checkpoint_to", required=False)
        mode = _safe_str_field(item.get("mode"), field_name="pending.mode", required=False).upper() or "INCREMENTAL"
        source = _safe_str_field(item.get("source"), field_name="pending.source", required=False) or "native"
        if not export_arn:
            continue
        normalized.append(
            {
                "export_arn": export_arn,
                "checkpoint_to": checkpoint_to,
                "mode": mode,
                "source": source,
            }
        )
    return normalized


def _normalize_checkpoint_state(state: Dict[str, Any], *, table_name: str, table_arn: str) -> Dict[str, Any]:
    last_mode = _safe_str_field(state.get("last_mode"), field_name="state.last_mode", required=False).upper()
    incremental_seq = _coerce_non_negative_int(
        state.get("incremental_seq"),
        field_name="state.incremental_seq",
        default=0,
    )
    if last_mode == "FULL":
        incremental_seq = 0
    return {
        "table_name": _safe_str_field(state.get("table_name"), field_name="state.table_name", required=False) or table_name,
        "table_arn": _safe_str_field(state.get("table_arn"), field_name="state.table_arn", required=False) or table_arn,
        "table_created_at": _safe_str_field(state.get("table_created_at"), field_name="state.table_created_at", required=False),
        "last_to": _safe_str_field(state.get("last_to"), field_name="state.last_to", required=False),
        "last_mode": last_mode,
        "source": _safe_str_field(state.get("source"), field_name="state.source", required=False),
        "last_export_arn": _safe_str_field(state.get("last_export_arn"), field_name="state.last_export_arn", required=False),
        "last_export_item_count": _coerce_optional_non_negative_int(
            state.get("last_export_item_count"),
            field_name="state.last_export_item_count",
        ),
        "pending_exports": _normalize_pending_exports(state.get("pending_exports")),
        "incremental_seq": incremental_seq,
    }


def _ddb_encode_item(python_item: Dict[str, Any]) -> Dict[str, Any]:
    encoded: Dict[str, Any] = {}
    for key, value in python_item.items():
        if value is None:
            continue
        encoded[key] = serializer.serialize(value)
    return encoded


def _ddb_decode_item(ddb_item: Dict[str, Any]) -> Dict[str, Any]:
    decoded: Dict[str, Any] = {}
    for key, value in ddb_item.items():
        decoded[key] = deserializer.deserialize(value)
    return decoded


def _validate_checkpoint_table_schema(table_name: str, description: Dict[str, Any]) -> None:
    key_schema = description.get("KeySchema") if isinstance(description, dict) else None
    attr_defs = description.get("AttributeDefinitions") if isinstance(description, dict) else None
    if not isinstance(key_schema, list) or not isinstance(attr_defs, list):
        raise RuntimeError(
            f"Tabela DynamoDB de checkpoint inválida: {table_name}. Schema ausente."
        )

    key_map = {
        item.get("KeyType"): item.get("AttributeName")
        for item in key_schema
        if isinstance(item, dict)
    }
    if key_map.get("HASH") != CHECKPOINT_DYNAMODB_PARTITION_KEY or key_map.get("RANGE") != CHECKPOINT_DYNAMODB_SORT_KEY:
        raise RuntimeError(
            f"Tabela DynamoDB de checkpoint inválida. {table_name} deve usar PK={CHECKPOINT_DYNAMODB_PARTITION_KEY} e SK={CHECKPOINT_DYNAMODB_SORT_KEY}."
        )

    attr_type_map = {
        item.get("AttributeName"): item.get("AttributeType")
        for item in attr_defs
        if isinstance(item, dict)
    }
    if attr_type_map.get(CHECKPOINT_DYNAMODB_PARTITION_KEY) != "S" or attr_type_map.get(CHECKPOINT_DYNAMODB_SORT_KEY) != "S":
        raise RuntimeError(
            f"Tabela DynamoDB de checkpoint inválida. {table_name} deve ter PK/SK do tipo String."
        )


def _wait_dynamodb_table_active(
    ddb_client: Any,
    *,
    table_name: str,
    poll_seconds: int,
    timeout_seconds: int,
) -> None:
    elapsed = 0
    while elapsed < timeout_seconds:
        response = ddb_client.describe_table(TableName=table_name)
        table = response.get("Table") if isinstance(response, dict) else None
        status = _safe_str_field(table.get("TableStatus"), field_name="TableStatus", required=False) if isinstance(table, dict) else ""
        if status == "ACTIVE":
            return
        _log_event("dynamodb.table.wait", table_name=table_name, status=status)
        time.sleep(poll_seconds)
        elapsed += poll_seconds
    raise TimeoutError(f"Timeout aguardando tabela DynamoDB {table_name} ficar ACTIVE")


def _ensure_checkpoint_dynamodb_table_exists(ddb_client: Any, *, table_name: str) -> None:
    try:
        response = ddb_client.describe_table(TableName=table_name)
        table = response.get("Table") if isinstance(response, dict) else None
        _validate_checkpoint_table_schema(table_name, _safe_dict_field(table, "table"))
        return
    except ClientError as exc:
        code = _client_error_code(exc)
        if code not in AWS_NOT_FOUND_ERROR_CODES:
            raise _build_aws_runtime_error("DynamoDB DescribeTable checkpoint", exc, resource=table_name) from exc

    _log_event("checkpoint.dynamodb.table.missing", table_name=table_name)
    ddb_client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": CHECKPOINT_DYNAMODB_PARTITION_KEY, "KeyType": "HASH"},
            {"AttributeName": CHECKPOINT_DYNAMODB_SORT_KEY, "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": CHECKPOINT_DYNAMODB_PARTITION_KEY, "AttributeType": "S"},
            {"AttributeName": CHECKPOINT_DYNAMODB_SORT_KEY, "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    _wait_dynamodb_table_active(
        ddb_client,
        table_name=table_name,
        poll_seconds=CHECKPOINT_DYNAMODB_TABLE_POLL_SECONDS,
        timeout_seconds=CHECKPOINT_DYNAMODB_TABLE_TIMEOUT_SECONDS,
    )


def create_checkpoint_store(*, session: Any, checkpoint_dynamodb_table_arn: str) -> Dict[str, Any]:
    table_context = _extract_table_arn_context(
        checkpoint_dynamodb_table_arn,
        field_name="checkpoint_dynamodb_table_arn",
    )
    table_region = _safe_str_field(table_context.get("region"), field_name="checkpoint.region")
    table_name = _safe_str_field(table_context.get("table_name"), field_name="checkpoint.table_name")
    ddb_client = _get_session_client(session, "dynamodb", region_name=table_region)
    _ensure_checkpoint_dynamodb_table_exists(ddb_client, table_name=table_name)
    return {
        "backend": "dynamodb",
        "ddb": ddb_client,
        "table_name": table_name,
        "table_region": table_region,
        "table_arn": checkpoint_dynamodb_table_arn,
    }


def _checkpoint_get_current_item(
    ddb_client: Any,
    *,
    table_name: str,
    partition_value: str,
) -> Dict[str, Any]:
    response = ddb_client.get_item(
        TableName=table_name,
        Key={
            CHECKPOINT_DYNAMODB_PARTITION_KEY: {"S": partition_value},
            CHECKPOINT_DYNAMODB_SORT_KEY: {"S": CHECKPOINT_DYNAMODB_CURRENT_RECORD},
        },
        ConsistentRead=True,
    )
    item = response.get("Item") if isinstance(response, dict) else None
    return item if isinstance(item, dict) else {}


def checkpoint_load_table_state(
    store: Dict[str, Any],
    *,
    target_table_name: str,
    target_table_arn: Optional[str] = None,
) -> Dict[str, Any]:
    table_name = _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name")
    ddb_client = store.get("ddb")
    if ddb_client is None:
        raise RuntimeError("checkpoint_store.ddb ausente")

    requested_table_name = _safe_str_field(target_table_name, field_name="target_table_name")
    requested_table_arn = _safe_str_field(target_table_arn, field_name="target_table_arn", required=False)

    item: Dict[str, Any] = {}
    decoded: Optional[Dict[str, Any]] = None
    if requested_table_arn:
        item = _checkpoint_get_current_item(
            ddb_client,
            table_name=table_name,
            partition_value=requested_table_arn,
        )
        if item:
            decoded = _ddb_decode_item(item)

    if not item:
        item = _checkpoint_get_current_item(
            ddb_client,
            table_name=table_name,
            partition_value=requested_table_name,
        )
        if item and requested_table_arn:
            decoded_legacy = _ddb_decode_item(item)
            legacy_table_arn = _safe_str_field(decoded_legacy.get("TableArn"), field_name="checkpoint.TableArn", required=False)
            if legacy_table_arn != requested_table_arn:
                _log_event(
                    "checkpoint.load.legacy_partition_mismatch",
                    requested_table_name=requested_table_name,
                    requested_table_arn=requested_table_arn,
                    legacy_table_arn=legacy_table_arn,
                    level=logging.WARNING,
                )
                return {}
            decoded = decoded_legacy

    if not item:
        return {}

    if decoded is None:
        decoded = _ddb_decode_item(item)
    pending_exports = _normalize_pending_exports(decoded.get("PendingExports"))
    state = {
        "table_name": _safe_str_field(
            decoded.get("TargetTableName"),
            field_name="checkpoint.TargetTableName",
            required=False,
        ) or _safe_str_field(decoded.get("TableName"), field_name="checkpoint.TableName", required=False),
        "table_arn": _safe_str_field(decoded.get("TableArn"), field_name="checkpoint.TableArn", required=False),
        "table_created_at": _safe_str_field(decoded.get("TableCreatedAt"), field_name="checkpoint.TableCreatedAt", required=False),
        "last_to": _safe_str_field(decoded.get("LastTo"), field_name="checkpoint.LastTo", required=False),
        "last_mode": _safe_str_field(decoded.get("LastMode"), field_name="checkpoint.LastMode", required=False),
        "source": _safe_str_field(decoded.get("Source"), field_name="checkpoint.Source", required=False),
        "last_export_arn": _safe_str_field(decoded.get("LastExportArn"), field_name="checkpoint.LastExportArn", required=False),
        "last_export_item_count": _coerce_optional_non_negative_int(
            decoded.get("LastExportItemCount"),
            field_name="checkpoint.LastExportItemCount",
        ),
        "pending_exports": pending_exports,
        "incremental_seq": _coerce_non_negative_int(
            decoded.get("IncrementalSeq"),
            field_name="checkpoint.IncrementalSeq",
            default=0,
        ),
    }
    return {
        key: value
        for key, value in state.items()
        if value not in (None, "") or key in {"pending_exports", "incremental_seq"}
    }


def checkpoint_save(store: Dict[str, Any], payload: Dict[str, Any]) -> None:
    table_name = _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name")
    ddb_client = store.get("ddb")
    if ddb_client is None:
        raise RuntimeError("checkpoint_store.ddb ausente")

    if not isinstance(payload, dict):
        raise ValueError("Payload de checkpoint inválido")
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        return

    observed_at = _dt_to_iso(_now_utc())
    for _, raw_state in tables.items():
        if not isinstance(raw_state, dict):
            continue
        state = _normalize_checkpoint_state(
            raw_state,
            table_name=_safe_str_field(raw_state.get("table_name"), field_name="state.table_name"),
            table_arn=_safe_str_field(raw_state.get("table_arn"), field_name="state.table_arn"),
        )
        partition_value = _safe_str_field(state.get("table_arn"), field_name="state.table_arn", required=False) or _safe_str_field(
            state.get("table_name"),
            field_name="state.table_name",
        )
        item = {
            CHECKPOINT_DYNAMODB_PARTITION_KEY: partition_value,
            CHECKPOINT_DYNAMODB_SORT_KEY: CHECKPOINT_DYNAMODB_CURRENT_RECORD,
            "TargetTableName": state["table_name"],
            "TableArn": state["table_arn"],
            "TableCreatedAt": state.get("table_created_at"),
            "LastTo": state.get("last_to"),
            "LastMode": state.get("last_mode"),
            "Source": state.get("source"),
            "LastExportArn": state.get("last_export_arn"),
            "LastExportItemCount": state.get("last_export_item_count"),
            "PendingExports": state.get("pending_exports", []),
            "IncrementalSeq": _coerce_non_negative_int(
                state.get("incremental_seq"),
                field_name="state.incremental_seq",
                default=0,
            ),
            "UpdatedAt": observed_at,
        }
        ddb_client.put_item(TableName=table_name, Item=_ddb_encode_item(item))


def _validate_output_table_schema(table_name: str, description: Dict[str, Any]) -> None:
    key_schema = description.get("KeySchema") if isinstance(description, dict) else None
    attr_defs = description.get("AttributeDefinitions") if isinstance(description, dict) else None
    if not isinstance(key_schema, list) or not isinstance(attr_defs, list):
        raise RuntimeError(f"Tabela DynamoDB de output inválida: {table_name}")

    key_map = {
        item.get("KeyType"): item.get("AttributeName")
        for item in key_schema
        if isinstance(item, dict)
    }
    if key_map.get("HASH") != OUTPUT_DYNAMODB_PARTITION_KEY or "RANGE" in key_map:
        raise RuntimeError(
            f"Esquema inválido para tabela DynamoDB de output {table_name}. Use partition key {OUTPUT_DYNAMODB_PARTITION_KEY} do tipo String e sem chave secundária."
        )

    attr_map = {
        item.get("AttributeName"): item.get("AttributeType")
        for item in attr_defs
        if isinstance(item, dict)
    }
    if attr_map.get(OUTPUT_DYNAMODB_PARTITION_KEY) != "S":
        raise RuntimeError(
            f"Esquema inválido para tabela DynamoDB de output {table_name}. Use partition key {OUTPUT_DYNAMODB_PARTITION_KEY} do tipo String e sem chave secundária."
        )


def _ensure_output_dynamodb_table_exists(ddb_client: Any, *, table_name: str) -> None:
    try:
        response = ddb_client.describe_table(TableName=table_name)
        table = response.get("Table") if isinstance(response, dict) else None
        _validate_output_table_schema(table_name, _safe_dict_field(table, "output_table"))
        return
    except ClientError as exc:
        code = _client_error_code(exc)
        if code not in AWS_NOT_FOUND_ERROR_CODES:
            raise _build_aws_runtime_error("DynamoDB DescribeTable output", exc, resource=table_name) from exc

    _log_event("output.dynamodb.table.missing", table_name=table_name)
    ddb_client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": OUTPUT_DYNAMODB_PARTITION_KEY, "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": OUTPUT_DYNAMODB_PARTITION_KEY, "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    _wait_dynamodb_table_active(
        ddb_client,
        table_name=table_name,
        poll_seconds=OUTPUT_DYNAMODB_TABLE_POLL_SECONDS,
        timeout_seconds=OUTPUT_DYNAMODB_TABLE_TIMEOUT_SECONDS,
    )


def _build_error_response_fields(exc: BaseException) -> Dict[str, str]:
    message = str(exc)
    message_l = message.lower()

    if "target" in message_l or "tabela alvo" in message_l:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "Nenhuma tabela alvo foi informada para a execução.",
            "resolution": "Informe targets no payload (targets) ou configure TARGET_TABLE_ARNS/TARGET_TABLES.",
        }

    if "checkpoint_dynamodb_table_arn" in message_l:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A configuração de checkpoint não foi informada.",
            "resolution": "Defina CHECKPOINT_DYNAMODB_TABLE_ARN no ambiente ou checkpoint_dynamodb_table_arn/checkpointDynamodbTableArn no payload.",
        }

    if "accessdenied" in message_l or "acesso" in message_l:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A execução não tem permissão para acessar um recurso AWS necessário.",
            "resolution": "Revise IAM role/policies para DynamoDB Export, S3 e DynamoDB de checkpoint/output.",
        }

    if "timeout" in message_l:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A operação excedeu o tempo limite.",
            "resolution": "Use WAIT_FOR_COMPLETION=false para não bloquear a invocação e valide latência/permissões na conta AWS.",
        }

    return {
        "error": message,
        "error_detail": message,
        "user_message": "A execução falhou por erro interno.",
        "resolution": "Verifique os logs da Lambda e os parâmetros enviados no payload/variáveis de ambiente.",
    }


def _build_table_error_result(table_name: str, table_arn: str, mode: str, error: BaseException, *, dry_run: bool) -> Dict[str, Any]:
    fields = _build_error_response_fields(error)
    return {
        "table_name": table_name,
        "table_arn": table_arn,
        "mode": mode,
        "status": "FAILED",
        "source": "runtime",
        "dry_run": dry_run,
        **fields,
    }


def _build_aws_session(config: Dict[str, Any]) -> Any:
    session = _get_default_aws_session()
    _ = config
    return session


def _assume_role_session(
    base_session: Any,
    *,
    role_arn: str,
    external_id: Optional[str],
    session_name: str,
    duration_seconds: int,
) -> Any:
    sts_client = _get_session_client(base_session, "sts")
    request: Dict[str, Any] = {
        "RoleArn": role_arn,
        "RoleSessionName": session_name,
        "DurationSeconds": duration_seconds,
    }
    if external_id:
        request["ExternalId"] = external_id

    response = sts_client.assume_role(**request)
    credentials = _safe_dict_field(response.get("Credentials"), "assume_role.Credentials")
    return boto3.session.Session(
        aws_access_key_id=_safe_str_field(credentials.get("AccessKeyId"), field_name="AccessKeyId"),
        aws_secret_access_key=_safe_str_field(credentials.get("SecretAccessKey"), field_name="SecretAccessKey"),
        aws_session_token=_safe_str_field(credentials.get("SessionToken"), field_name="SessionToken"),
    )


def _render_assume_role_template(role_template: str, *, account_id: str) -> str:
    allowed = {"account_id": account_id}

    def replacer(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in allowed:
            raise ValueError(f"Campo de template de ASSUME_ROLE não suportado: {key}")
        return allowed[key]

    return ROLE_TEMPLATE_PATTERN.sub(replacer, role_template)


def _resolve_assumed_session_for_target(
    *,
    base_session: Any,
    config: Dict[str, Any],
    table_account_id: str,
    cache: Dict[str, Any],
) -> Tuple[Any, Optional[str]]:
    assume_role = _resolve_optional_text(config.get("assume_role"))
    if not assume_role:
        return base_session, None

    role_arn = assume_role
    if ROLE_TEMPLATE_PATTERN.search(assume_role):
        role_arn = _render_assume_role_template(assume_role, account_id=table_account_id)

    cached = cache.get(role_arn)
    if cached is not None:
        return cached, role_arn

    assumed = _assume_role_session(
        base_session,
        role_arn=role_arn,
        external_id=_resolve_optional_text(config.get("assume_role_external_id")),
        session_name=_safe_str_field(config.get("assume_role_session_name"), field_name="assume_role_session_name"),
        duration_seconds=int(config.get("assume_role_duration_seconds", 3600)),
    )
    cache[role_arn] = assumed
    return assumed, role_arn


def _resolve_table_target(target_ref: str, *, session: Any, runtime_region: Optional[str]) -> TableTarget:
    ref = _safe_str_field(target_ref, field_name="target")
    if ref.startswith("arn:"):
        context = _extract_table_arn_context(ref, field_name="target")
        return TableTarget(
            raw_ref=ref,
            table_name=_safe_str_field(context.get("table_name"), field_name="table_name"),
            table_arn=ref,
            account_id=_safe_str_field(context.get("account_id"), field_name="account_id"),
            region=_safe_str_field(context.get("region"), field_name="region"),
        )

    resolved_region = _resolve_optional_text(runtime_region)
    if not resolved_region:
        raise ValueError(
            f"Não foi possível resolver região para target '{ref}'. Defina AWS_REGION/AWS_DEFAULT_REGION."
        )

    ddb_client = _get_session_client(session, "dynamodb", region_name=resolved_region)
    response = ddb_client.describe_table(TableName=ref)
    table = _safe_dict_field(response.get("Table"), "DescribeTable.Table")
    table_arn = _safe_str_field(table.get("TableArn"), field_name="TableArn")
    context = _extract_table_arn_context(table_arn, field_name="TableArn")
    return TableTarget(
        raw_ref=ref,
        table_name=_safe_str_field(context.get("table_name"), field_name="table_name"),
        table_arn=table_arn,
        account_id=_safe_str_field(context.get("account_id"), field_name="account_id"),
        region=_safe_str_field(context.get("region"), field_name="region"),
    )


def _ensure_point_in_time_recovery(
    ddb_client: Any,
    *,
    table_name: str,
    table_arn: str,
) -> Dict[str, Optional[datetime]]:
    response = ddb_client.describe_continuous_backups(TableName=table_name)
    desc = _safe_dict_field(response.get("ContinuousBackupsDescription"), "ContinuousBackupsDescription")
    pitr_desc = _safe_dict_field(desc.get("PointInTimeRecoveryDescription"), "PointInTimeRecoveryDescription")
    pitr_status = _safe_str_field(pitr_desc.get("PointInTimeRecoveryStatus"), field_name="PointInTimeRecoveryStatus", required=False)

    if pitr_status == "ENABLED":
        return _extract_pitr_window(pitr_desc)

    _log_event(
        "table.pitr.enable.start",
        table_name=table_name,
        table_arn=table_arn,
        point_in_time_recovery_status=pitr_status,
        timeout_seconds=PITR_ENABLE_TIMEOUT_SECONDS,
    )
    ddb_client.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )

    elapsed = 0
    while elapsed < PITR_ENABLE_TIMEOUT_SECONDS:
        response = ddb_client.describe_continuous_backups(TableName=table_name)
        desc = _safe_dict_field(response.get("ContinuousBackupsDescription"), "ContinuousBackupsDescription")
        pitr_desc = _safe_dict_field(desc.get("PointInTimeRecoveryDescription"), "PointInTimeRecoveryDescription")
        status = _safe_str_field(pitr_desc.get("PointInTimeRecoveryStatus"), field_name="PointInTimeRecoveryStatus", required=False)
        if status == "ENABLED":
            _log_event(
                "table.pitr.enable.success",
                table_name=table_name,
                table_arn=table_arn,
                point_in_time_recovery_status=status,
                changed=True,
            )
            return _extract_pitr_window(pitr_desc)
        time.sleep(PITR_ENABLE_POLL_SECONDS)
        elapsed += PITR_ENABLE_POLL_SECONDS

    raise TimeoutError(f"Timeout aguardando PITR habilitar para tabela {table_name}")


def snapshot_manager_validate_export_request(
    params: Dict[str, Any],
    *,
    export_type: str,
    table_name: str,
    table_region: Optional[str],
) -> None:
    required = {"TableArn", "S3Bucket", "S3Prefix", "ExportFormat", "ExportType", "ClientToken"}
    if export_type == "INCREMENTAL_EXPORT":
        required = {*required, "IncrementalExportSpecification"}

    missing = sorted(key for key in required if key not in params)
    if missing:
        raise ValueError(
            f"Parâmetros obrigatórios ausentes no request de export da tabela {table_name}: {', '.join(missing)}"
        )

    bucket = _safe_str_field(params.get("S3Bucket"), field_name="S3Bucket")
    bucket_owner = _safe_str_field(params.get("S3BucketOwner"), field_name="S3BucketOwner", required=False)
    if bucket_owner and not AWS_ACCOUNT_ID_PATTERN.fullmatch(bucket_owner):
        raise ValueError(
            f"S3BucketOwner inválido para {table_name}: {bucket_owner}. Use account id AWS de 12 dígitos."
        )

    resolved_export_type = _safe_str_field(params.get("ExportType"), field_name="ExportType")
    if resolved_export_type != export_type:
        raise ValueError(
            f"ExportType inválido para {table_name}: esperado {export_type}, recebido {resolved_export_type}"
        )

    table_region_text = _resolve_optional_text(table_region)
    bucket_region = _extract_bucket_region_suffix(bucket)
    if bucket_region and table_region_text and bucket_region != table_region_text:
        raise ValueError(
            f"Bucket {bucket} indica região {bucket_region}, mas a tabela {table_name} está em {table_region_text}."
        )

    if export_type == "INCREMENTAL_EXPORT":
        incremental_spec = _safe_dict_field(params.get("IncrementalExportSpecification"), "IncrementalExportSpecification")
        export_from = incremental_spec.get("ExportFromTime")
        export_to = incremental_spec.get("ExportToTime")
        if not isinstance(export_from, datetime) or not isinstance(export_to, datetime):
            raise ValueError(
                f"IncrementalExportSpecification inválido para tabela {table_name}: ExportFromTime/ExportToTime devem ser datetime."
            )
        if export_from >= export_to:
            raise ValueError(
                f"IncrementalExportSpecification inválido para tabela {table_name}: ExportFromTime deve ser menor que ExportToTime."
            )
        export_view_type = _safe_str_field(
            incremental_spec.get("ExportViewType"),
            field_name="IncrementalExportSpecification.ExportViewType",
            required=False,
        )
        if export_view_type:
            _resolve_incremental_export_view_type(export_view_type)


def _wait_export_completion(ddb_client: Any, *, export_arn: str) -> str:
    elapsed = 0
    while elapsed < EXPORT_WAIT_TIMEOUT_SECONDS:
        description = _describe_export_description(ddb_client, export_arn=export_arn)
        status = _safe_str_field(description.get("ExportStatus"), field_name="ExportStatus")
        if status == "COMPLETED":
            return status
        if status in EXPORT_TERMINAL_FAILURE_STATUSES:
            raise RuntimeError(f"Export {export_arn} terminou com status {status}")
        time.sleep(EXPORT_WAIT_POLL_SECONDS)
        elapsed += EXPORT_WAIT_POLL_SECONDS
    raise TimeoutError(f"Timeout aguardando export {export_arn}")


def _read_table_created_at_iso(ddb_client: Any, *, table_name: str, table_arn: str) -> str:
    try:
        response = ddb_client.describe_table(TableName=table_name)
    except ClientError as exc:
        _log_event(
            "table.describe.failed",
            table_name=table_name,
            table_arn=table_arn,
            code=_client_error_code(exc),
            message=_client_error_message(exc),
            level=logging.WARNING,
        )
        return ""
    table = _safe_dict_field(response.get("Table"), "DescribeTable.Table")
    created_at = table.get("CreationDateTime")
    if isinstance(created_at, datetime):
        return _dt_to_iso(created_at)
    parsed = _parse_iso_datetime(created_at)
    if isinstance(parsed, datetime):
        return _dt_to_iso(parsed)
    return ""


def _start_full_export(
    *,
    config: Dict[str, Any],
    target: TableTarget,
    ddb_client: Any,
    bucket: str,
    bucket_owner: Optional[str],
    assume_role_arn: Optional[str],
) -> Dict[str, Any]:
    run_time = config["run_time"]
    run_token_salt = (
        run_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if isinstance(run_time, datetime)
        else _safe_str_field(config.get("run_id"), field_name="run_id", required=False)
    )
    s3_prefix = _build_export_prefix(run_time, target, "FULL_EXPORT")

    params: Dict[str, Any] = {
        "TableArn": target.table_arn,
        "S3Bucket": bucket,
        "S3Prefix": s3_prefix,
        "ExportFormat": "DYNAMODB_JSON",
        "ExportType": "FULL_EXPORT",
        "ClientToken": _build_export_client_token(
            table_arn=target.table_arn,
            bucket=bucket,
            bucket_owner=bucket_owner,
            s3_prefix=s3_prefix,
            export_type="FULL_EXPORT",
            token_salt=run_token_salt,
        ),
    }
    if bucket_owner:
        params["S3BucketOwner"] = bucket_owner

    snapshot_manager_validate_export_request(
        params,
        export_type="FULL_EXPORT",
        table_name=target.table_name,
        table_region=target.region,
    )

    _ensure_point_in_time_recovery(
        ddb_client,
        table_name=target.table_name,
        table_arn=target.table_arn,
    )

    _log_event(
        "export.full.attempt",
        table_name=target.table_name,
        table_arn=target.table_arn,
        s3_bucket=bucket,
        s3_bucket_owner=bucket_owner,
        s3_prefix=s3_prefix,
        wait_for_completion=bool(config.get("wait_for_completion")),
        assume_role_arn=assume_role_arn,
        table_account_id=target.account_id,
        table_region=target.region,
        client_token=params.get("ClientToken"),
    )
    response = ddb_client.export_table_to_point_in_time(**params)
    description = _safe_dict_field(response.get("ExportDescription"), "ExportDescription")
    export_arn = _safe_str_field(description.get("ExportArn"), field_name="ExportArn")
    export_status = _safe_str_field(description.get("ExportStatus"), field_name="ExportStatus", required=False)
    response_metadata = response.get("ResponseMetadata") if isinstance(response.get("ResponseMetadata"), dict) else {}
    request_id = _safe_str_field(response_metadata.get("RequestId"), field_name="ResponseMetadata.RequestId", required=False)
    _log_event(
        "export.full.started",
        table_name=target.table_name,
        table_arn=target.table_arn,
        export_arn=export_arn,
        export_status=export_status,
        request_id=request_id,
    )
    status = "STARTED"
    export_item_count: Optional[int] = None
    if bool(config.get("wait_for_completion")):
        status = _wait_export_completion(ddb_client, export_arn=export_arn)
        if status == "COMPLETED":
            export_item_count = _extract_export_item_count(
                _describe_export_description(ddb_client, export_arn=export_arn)
            )

    export_fields = _extract_export_fields(export_arn)
    result = {
        "table_name": target.table_name,
        "table_arn": target.table_arn,
        "mode": "FULL",
        "status": status,
        "source": "native",
        "snapshot_bucket": bucket,
        "s3_prefix": s3_prefix,
        "started_at": _dt_to_iso(run_time),
        "checkpoint_to": _dt_to_iso(run_time),
        "assume_role_arn": assume_role_arn,
        "table_account_id": target.account_id,
        "table_region": target.region,
        "export_request_id": request_id,
        **export_fields,
    }
    if export_item_count is not None:
        result["item_count"] = export_item_count
    return result


def _start_incremental_export(
    *,
    config: Dict[str, Any],
    target: TableTarget,
    ddb_client: Any,
    bucket: str,
    bucket_owner: Optional[str],
    export_from: datetime,
    export_to: datetime,
    incremental_index: int,
    checkpoint_source: str,
    assume_role_arn: Optional[str],
) -> Dict[str, Any]:
    export_view_type = _resolve_incremental_export_view_type(config.get("incremental_export_view_type"))
    run_time = config["run_time"]
    run_token_salt = (
        run_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if isinstance(run_time, datetime)
        else _safe_str_field(config.get("run_id"), field_name="run_id", required=False)
    )
    s3_prefix = _build_export_prefix(
        run_time,
        target,
        "INCREMENTAL_EXPORT",
        incremental_index=incremental_index,
    )

    pitr_window = _ensure_point_in_time_recovery(
        ddb_client,
        table_name=target.table_name,
        table_arn=target.table_arn,
    )
    resolved_export_from, resolved_export_to = _clamp_incremental_export_window_to_pitr(
        export_from=export_from,
        export_to=export_to,
        pitr_window=pitr_window,
        table_name=target.table_name,
        table_arn=target.table_arn,
    )
    latest_restorable = pitr_window.get("latest_restorable")
    if resolved_export_to <= resolved_export_from and resolved_export_from > export_from:
        max_allowed_export_to = run_time if isinstance(run_time, datetime) else resolved_export_to
        if isinstance(latest_restorable, datetime):
            max_allowed_export_to = min(max_allowed_export_to, latest_restorable)
        recomputed_export_to = min(resolved_export_from + INCREMENTAL_EXPORT_MAX_WINDOW, max_allowed_export_to)
        if recomputed_export_to > resolved_export_from:
            _log_event(
                "export.incremental.window.recomputed_after_pitr_adjustment",
                table_name=target.table_name,
                table_arn=target.table_arn,
                requested_export_from=_dt_to_iso(export_from),
                requested_export_to=_dt_to_iso(export_to),
                export_from=_dt_to_iso(resolved_export_from),
                previous_export_to=_dt_to_iso(resolved_export_to),
                export_to=_dt_to_iso(recomputed_export_to),
                latest_restorable=_dt_to_iso(latest_restorable) if isinstance(latest_restorable, datetime) else None,
                max_window_seconds=int(INCREMENTAL_EXPORT_MAX_WINDOW.total_seconds()),
            )
            resolved_export_to = recomputed_export_to

    if resolved_export_from >= resolved_export_to:
        return {
            "table_name": target.table_name,
            "table_arn": target.table_arn,
            "mode": "INCREMENTAL",
            "status": "PENDING",
            "source": "window_outside_pitr",
            "message": (
                f"Janela incremental inválida após ajuste ao PITR para tabela {target.table_name}: "
                f"checkpoint_from={_dt_to_iso(resolved_export_from)} "
                f"checkpoint_to={_dt_to_iso(resolved_export_to)}."
            ),
            "snapshot_bucket": bucket,
            "s3_prefix": s3_prefix,
            "checkpoint_from": _dt_to_iso(resolved_export_from),
            "checkpoint_to": _dt_to_iso(resolved_export_to),
            "checkpoint_source": checkpoint_source,
            "assume_role_arn": assume_role_arn,
            "table_account_id": target.account_id,
            "table_region": target.region,
        }
    if (resolved_export_to - resolved_export_from) < INCREMENTAL_EXPORT_MIN_WINDOW:
        return {
            "table_name": target.table_name,
            "table_arn": target.table_arn,
            "mode": "INCREMENTAL",
            "status": "PENDING",
            "source": "window_outside_pitr",
            "message": (
                f"Janela incremental ajustada ao PITR ficou menor que 15 minutos para tabela {target.table_name}: "
                f"checkpoint_from={_dt_to_iso(resolved_export_from)} "
                f"checkpoint_to={_dt_to_iso(resolved_export_to)}."
            ),
            "snapshot_bucket": bucket,
            "s3_prefix": s3_prefix,
            "checkpoint_from": _dt_to_iso(resolved_export_from),
            "checkpoint_to": _dt_to_iso(resolved_export_to),
            "checkpoint_source": checkpoint_source,
            "assume_role_arn": assume_role_arn,
            "table_account_id": target.account_id,
            "table_region": target.region,
        }

    params: Dict[str, Any] = {
        "TableArn": target.table_arn,
        "S3Bucket": bucket,
        "S3Prefix": s3_prefix,
        "ExportFormat": "DYNAMODB_JSON",
        "ExportType": "INCREMENTAL_EXPORT",
        "IncrementalExportSpecification": {
            "ExportFromTime": resolved_export_from,
            "ExportToTime": resolved_export_to,
            "ExportViewType": export_view_type,
        },
        "ClientToken": _build_export_client_token(
            table_arn=target.table_arn,
            bucket=bucket,
            bucket_owner=bucket_owner,
            s3_prefix=s3_prefix,
            export_type="INCREMENTAL_EXPORT",
            export_from=resolved_export_from,
            export_to=resolved_export_to,
            token_salt=run_token_salt,
        ),
    }
    if bucket_owner:
        params["S3BucketOwner"] = bucket_owner

    snapshot_manager_validate_export_request(
        params,
        export_type="INCREMENTAL_EXPORT",
        table_name=target.table_name,
        table_region=target.region,
    )

    _log_event(
        "export.incremental.attempt",
        table_name=target.table_name,
        table_arn=target.table_arn,
        s3_bucket=bucket,
        s3_bucket_owner=bucket_owner,
        s3_prefix=s3_prefix,
        export_from=_dt_to_iso(resolved_export_from),
        export_to=_dt_to_iso(resolved_export_to),
        export_view_type=export_view_type,
        wait_for_completion=bool(config.get("wait_for_completion")),
        assume_role_arn=assume_role_arn,
        table_account_id=target.account_id,
        table_region=target.region,
        client_token=params.get("ClientToken"),
    )

    response = ddb_client.export_table_to_point_in_time(**params)
    description = _safe_dict_field(response.get("ExportDescription"), "ExportDescription")
    export_arn = _safe_str_field(description.get("ExportArn"), field_name="ExportArn")
    export_status = _safe_str_field(description.get("ExportStatus"), field_name="ExportStatus", required=False)
    response_metadata = response.get("ResponseMetadata") if isinstance(response.get("ResponseMetadata"), dict) else {}
    request_id = _safe_str_field(response_metadata.get("RequestId"), field_name="ResponseMetadata.RequestId", required=False)
    _log_event(
        "export.incremental.started",
        table_name=target.table_name,
        table_arn=target.table_arn,
        export_arn=export_arn,
        export_status=export_status,
        request_id=request_id,
    )

    status = "STARTED"
    export_item_count: Optional[int] = None
    if bool(config.get("wait_for_completion")):
        status = _wait_export_completion(ddb_client, export_arn=export_arn)
        if status == "COMPLETED":
            export_item_count = _extract_export_item_count(
                _describe_export_description(ddb_client, export_arn=export_arn)
            )

    export_fields = _extract_export_fields(export_arn)
    result = {
        "table_name": target.table_name,
        "table_arn": target.table_arn,
        "mode": "INCREMENTAL",
        "status": status,
        "source": "native",
        "snapshot_bucket": bucket,
        "s3_prefix": s3_prefix,
        "started_at": _dt_to_iso(resolved_export_to),
        "checkpoint_from": _dt_to_iso(resolved_export_from),
        "checkpoint_to": _dt_to_iso(resolved_export_to),
        "checkpoint_source": checkpoint_source,
        "assume_role_arn": assume_role_arn,
        "table_account_id": target.account_id,
        "table_region": target.region,
        "export_request_id": request_id,
        **export_fields,
    }
    if export_item_count is not None:
        result["item_count"] = export_item_count
    return result


def _run_scan_snapshot_fallback(
    *,
    config: Dict[str, Any],
    target: TableTarget,
    ddb_client: Any,
    s3_client: Any,
    bucket: str,
    s3_prefix: str,
    mode: str,
    assume_role_arn: Optional[str],
    fallback_error_code: str,
    fallback_error_message: str,
    checkpoint_from: Optional[datetime] = None,
    checkpoint_to: Optional[datetime] = None,
    checkpoint_source: Optional[str] = None,
) -> Dict[str, Any]:
    paginator = ddb_client.get_paginator("scan")
    rows: List[str] = []
    items_written = 0
    pages_scanned = 0

    for page in paginator.paginate(TableName=target.table_name):
        pages_scanned += 1
        page_items = page.get("Items")
        if not isinstance(page_items, list):
            continue
        valid_items = [item for item in page_items if isinstance(item, dict)]
        items_written += len(valid_items)
        rows.extend(json.dumps(_to_json_safe(item), ensure_ascii=False, default=str) for item in valid_items)

    file_key = f"{s3_prefix}/scan_fallback/part-00001.jsonl"
    body_text = "\n".join(rows)
    body_bytes = (f"{body_text}\n" if body_text else "").encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=file_key,
        Body=body_bytes,
        ContentType="application/x-ndjson",
    )

    manifest_payload: Dict[str, Any] = {
        "table_name": target.table_name,
        "table_arn": target.table_arn,
        "mode": mode,
        "source": "scan_fallback",
        "run_id": _safe_str_field(config.get("run_id"), field_name="run_id"),
        "started_at": _dt_to_iso(config["run_time"]),
        "files": [f"s3://{bucket}/{file_key}"],
        "total_items": items_written,
        "total_parts": 1,
        "pages_scanned": pages_scanned,
        "fallback_error_code": fallback_error_code,
        "fallback_error_message": fallback_error_message,
    }
    if isinstance(checkpoint_from, datetime):
        manifest_payload["from"] = _dt_to_iso(checkpoint_from)
    if isinstance(checkpoint_to, datetime):
        manifest_payload["to"] = _dt_to_iso(checkpoint_to)

    manifest_key = f"{s3_prefix}/scan_fallback/manifest.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(_to_json_safe(manifest_payload), ensure_ascii=False, default=str).encode("utf-8"),
        ContentType="application/json",
    )

    _log_event(
        "export.scan_fallback.completed",
        table_name=target.table_name,
        table_arn=target.table_arn,
        mode=mode,
        s3_bucket=bucket,
        s3_prefix=s3_prefix,
        items_written=items_written,
        pages_scanned=pages_scanned,
        fallback_error_code=fallback_error_code,
    )

    result: Dict[str, Any] = {
        "table_name": target.table_name,
        "table_arn": target.table_arn,
        "mode": mode,
        "status": "COMPLETED",
        "source": "scan_fallback",
        "snapshot_bucket": bucket,
        "s3_prefix": s3_prefix,
        "started_at": _dt_to_iso(config["run_time"]),
        "checkpoint_to": _dt_to_iso(checkpoint_to if isinstance(checkpoint_to, datetime) else config["run_time"]),
        "assume_role_arn": assume_role_arn,
        "table_account_id": target.account_id,
        "table_region": target.region,
        "files_written": 2,
        "items_written": items_written,
        "pages_scanned": pages_scanned,
        "manifest": f"s3://{bucket}/{manifest_key}",
        "message": "Export nativo indisponível no endpoint atual; fallback por Scan aplicado.",
        "fallback_error_code": fallback_error_code,
        "fallback_error_message": fallback_error_message,
    }
    if isinstance(checkpoint_from, datetime):
        result["checkpoint_from"] = _dt_to_iso(checkpoint_from)
    if isinstance(checkpoint_to, datetime):
        result["checkpoint_to"] = _dt_to_iso(checkpoint_to)
    if checkpoint_source:
        result["checkpoint_source"] = checkpoint_source
    return result


def _reconcile_pending_exports(
    *,
    ddb_client: Any,
    state: Dict[str, Any],
    table_name: str,
    table_arn: str,
) -> Dict[str, Any]:
    pending = _normalize_pending_exports(state.get("pending_exports"))
    if not pending:
        return state

    next_pending: List[Dict[str, str]] = []
    next_state = dict(state)

    for pending_export in pending:
        export_arn = _safe_str_field(pending_export.get("export_arn"), field_name="pending.export_arn")
        try:
            desc = _describe_export_description(ddb_client, export_arn=export_arn)
            status = _safe_str_field(desc.get("ExportStatus"), field_name="ExportStatus")
        except ClientError as exc:
            _log_event(
                "checkpoint.pending.describe_error",
                table_name=table_name,
                table_arn=table_arn,
                export_arn=export_arn,
                code=_client_error_code(exc),
                message=_client_error_message(exc),
                level=logging.WARNING,
            )
            next_pending.append(pending_export)
            continue

        if status == "COMPLETED":
            checkpoint_to = _safe_str_field(
                pending_export.get("checkpoint_to"),
                field_name="pending.checkpoint_to",
                required=False,
            )
            if checkpoint_to:
                next_state["last_to"] = checkpoint_to
            next_state["last_mode"] = _safe_str_field(
                pending_export.get("mode"),
                field_name="pending.mode",
                required=False,
            ) or "INCREMENTAL"
            next_state["source"] = _safe_str_field(
                pending_export.get("source"),
                field_name="pending.source",
                required=False,
            ) or "native"
            next_state["last_export_arn"] = export_arn
            next_state["last_export_item_count"] = _extract_export_item_count(desc)
            _log_event(
                "checkpoint.pending.completed",
                table_name=table_name,
                table_arn=table_arn,
                export_arn=export_arn,
                status=status,
                checkpoint_to=checkpoint_to,
                item_count=next_state.get("last_export_item_count"),
            )
            continue

        if status in EXPORT_TERMINAL_FAILURE_STATUSES:
            _log_event(
                "checkpoint.pending.terminal_failure",
                table_name=table_name,
                table_arn=table_arn,
                export_arn=export_arn,
                status=status,
                level=logging.WARNING,
            )
            continue

        next_pending.append(pending_export)
        _log_event(
            "checkpoint.pending.in_progress",
            table_name=table_name,
            table_arn=table_arn,
            export_arn=export_arn,
            status=status,
            checkpoint_to=pending_export.get("checkpoint_to"),
        )

    next_state["pending_exports"] = next_pending
    return next_state


def _refresh_last_incremental_export_metadata(
    *,
    ddb_client: Any,
    checkpoint_state: Dict[str, Any],
    table_name: str,
    table_arn: str,
) -> Dict[str, Any]:
    incremental_seq = _coerce_non_negative_int(
        checkpoint_state.get("incremental_seq"),
        field_name="checkpoint_state.incremental_seq",
        default=0,
    )
    last_mode = _safe_str_field(
        checkpoint_state.get("last_mode"),
        field_name="checkpoint_state.last_mode",
        required=False,
    ).upper()
    if incremental_seq <= 0 or last_mode != "INCREMENTAL":
        return checkpoint_state

    last_export_arn = _safe_str_field(
        checkpoint_state.get("last_export_arn"),
        field_name="checkpoint_state.last_export_arn",
        required=False,
    )
    if not last_export_arn:
        _log_event(
            "checkpoint.incremental.previous_export_missing",
            table_name=table_name,
            table_arn=table_arn,
            incremental_seq=incremental_seq,
            level=logging.WARNING,
        )
        return checkpoint_state

    current_item_count = checkpoint_state.get("last_export_item_count")
    if current_item_count is not None:
        return checkpoint_state

    try:
        export_description = _describe_export_description(ddb_client, export_arn=last_export_arn)
    except ClientError as exc:
        _log_event(
            "checkpoint.incremental.previous_export_describe_error",
            table_name=table_name,
            table_arn=table_arn,
            export_arn=last_export_arn,
            code=_client_error_code(exc),
            message=_client_error_message(exc),
            level=logging.WARNING,
        )
        return checkpoint_state

    refreshed_item_count = _extract_export_item_count(export_description)
    return {
        **checkpoint_state,
        "last_export_item_count": refreshed_item_count,
    }


def _resolve_automatic_export_plan(
    *,
    checkpoint_record_exists: bool,
    checkpoint_state: Dict[str, Any],
    ddb_client: Any,
    table_name: str,
    table_arn: str,
) -> Dict[str, Any]:
    refreshed_checkpoint_state = _refresh_last_incremental_export_metadata(
        ddb_client=ddb_client,
        checkpoint_state=checkpoint_state,
        table_name=table_name,
        table_arn=table_arn,
    )
    last_to_candidate = _parse_iso_datetime(refreshed_checkpoint_state.get("last_to"))
    if not checkpoint_record_exists:
        return {
            "mode": "FULL",
            "reason": "checkpoint_record_missing",
            "checkpoint_state": refreshed_checkpoint_state,
            "last_to": last_to_candidate,
        }
    if not isinstance(last_to_candidate, datetime):
        return {
            "mode": "FULL",
            "reason": "checkpoint_last_to_missing_or_invalid",
            "checkpoint_state": refreshed_checkpoint_state,
            "last_to": last_to_candidate,
        }

    current_incremental_seq = _coerce_non_negative_int(
        refreshed_checkpoint_state.get("incremental_seq"),
        field_name="checkpoint_state.incremental_seq",
        default=0,
    )
    if current_incremental_seq >= MAX_INCREMENTAL_EXPORTS_PER_CYCLE:
        return {
            "mode": "FULL",
            "reason": "incremental_cycle_limit_reached",
            "checkpoint_state": refreshed_checkpoint_state,
            "last_to": last_to_candidate,
        }

    if current_incremental_seq <= 0:
        next_incremental_index = 1
        reason = "initial_incremental_after_full"
    else:
        previous_item_count = refreshed_checkpoint_state.get("last_export_item_count")
        if previous_item_count is None:
            next_incremental_index = current_incremental_seq
            reason = "previous_incremental_item_count_unknown"
        elif previous_item_count > 0:
            next_incremental_index = current_incremental_seq + 1
            reason = "previous_incremental_had_items"
        else:
            next_incremental_index = current_incremental_seq
            reason = "previous_incremental_without_items"

    return {
        "mode": "INCREMENTAL",
        "reason": reason,
        "next_incremental_index": next_incremental_index,
        "checkpoint_state": refreshed_checkpoint_state,
        "last_to": last_to_candidate,
    }


def _build_pending_result(
    *,
    target: TableTarget,
    mode: str,
    source: str,
    message: str,
    bucket: str,
    assume_role_arn: Optional[str],
    checkpoint_state: Dict[str, Any],
    checkpoint_from: Optional[datetime] = None,
    checkpoint_to: Optional[datetime] = None,
) -> Dict[str, Any]:
    return {
        "table_name": target.table_name,
        "table_arn": target.table_arn,
        "mode": mode,
        "status": "PENDING",
        "source": source,
        "message": message,
        "snapshot_bucket": bucket,
        "checkpoint_from": _dt_to_iso(checkpoint_from) if isinstance(checkpoint_from, datetime) else None,
        "checkpoint_to": _dt_to_iso(checkpoint_to) if isinstance(checkpoint_to, datetime) else None,
        "pending_exports": _normalize_pending_exports(checkpoint_state.get("pending_exports")),
        "assume_role_arn": assume_role_arn,
        "table_account_id": target.account_id,
        "table_region": target.region,
    }


def _resolve_snapshot_bucket(config: Dict[str, Any], target: TableTarget) -> str:
    base_bucket = _safe_str_field(config.get("bucket"), field_name="bucket")
    return snapshot_manager_build_bucket_name(
        base_bucket,
        target.region,
        exact=bool(config.get("snapshot_bucket_exact")),
    )


def _process_table(
    *,
    config: Dict[str, Any],
    base_session: Any,
    checkpoint_store: Dict[str, Any],
    raw_target_ref: str,
    ignore_set: set[str],
    assume_session_cache: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    runtime_region = _resolve_runtime_region(base_session.region_name)
    target = _resolve_table_target(raw_target_ref, session=base_session, runtime_region=runtime_region)

    if target.raw_ref.lower() in ignore_set or target.table_name.lower() in ignore_set or target.table_arn.lower() in ignore_set:
        _log_event(
            "snapshot.preflight.target.ignored",
            table_name=target.table_name,
            table_arn=target.table_arn,
        )
        return None

    execution_session, assume_role_arn = _resolve_assumed_session_for_target(
        base_session=base_session,
        config=config,
        table_account_id=target.account_id,
        cache=assume_session_cache,
    )

    ddb_client = _get_session_client(execution_session, "dynamodb", region_name=target.region)
    s3_client = _get_session_client(execution_session, "s3")

    raw_checkpoint_state = checkpoint_load_table_state(
        checkpoint_store,
        target_table_name=target.table_name,
        target_table_arn=target.table_arn,
    )
    checkpoint_record_exists = bool(raw_checkpoint_state)
    checkpoint_state = _normalize_checkpoint_state(
        raw_checkpoint_state,
        table_name=target.table_name,
        table_arn=target.table_arn,
    )
    table_created_at = _read_table_created_at_iso(
        ddb_client,
        table_name=target.table_name,
        table_arn=target.table_arn,
    )
    checkpoint_created_at = _safe_str_field(
        checkpoint_state.get("table_created_at"),
        field_name="checkpoint_state.table_created_at",
        required=False,
    )
    if checkpoint_record_exists and table_created_at and checkpoint_created_at and checkpoint_created_at != table_created_at:
        _log_event(
            "checkpoint.table_recreated.detected",
            table_name=target.table_name,
            table_arn=target.table_arn,
            checkpoint_table_created_at=checkpoint_created_at,
            runtime_table_created_at=table_created_at,
            level=logging.WARNING,
        )
        checkpoint_record_exists = False
        checkpoint_state = _normalize_checkpoint_state(
            {},
            table_name=target.table_name,
            table_arn=target.table_arn,
        )
    if table_created_at:
        checkpoint_state = {
            **checkpoint_state,
            "table_created_at": table_created_at,
        }

    bucket = _resolve_snapshot_bucket(config, target)
    bucket_owner = _resolve_optional_text(config.get("bucket_owner"))

    checkpoint_state = _reconcile_pending_exports(
        ddb_client=ddb_client,
        state=checkpoint_state,
        table_name=target.table_name,
        table_arn=target.table_arn,
    )

    automatic_plan = _resolve_automatic_export_plan(
        checkpoint_record_exists=checkpoint_record_exists,
        checkpoint_state=checkpoint_state,
        ddb_client=ddb_client,
        table_name=target.table_name,
        table_arn=target.table_arn,
    )
    checkpoint_state = automatic_plan["checkpoint_state"]

    if bool(config.get("dry_run")):
        planned_mode = _safe_str_field(automatic_plan.get("mode"), field_name="automatic_plan.mode").upper()
        return {
            "table_name": target.table_name,
            "table_arn": target.table_arn,
            "mode": planned_mode,
            "status": "PLANNED",
            "source": "dry_run",
            "snapshot_bucket": bucket,
            "checkpoint_state": checkpoint_state,
            "assume_role_arn": assume_role_arn,
            "table_account_id": target.account_id,
            "table_region": target.region,
            "mode_selection_reason": automatic_plan.get("reason"),
            "message": "Execução em dry_run: nenhum export foi iniciado.",
        }

    if _normalize_pending_exports(checkpoint_state.get("pending_exports")):
        result = _build_pending_result(
            target=target,
            mode="INCREMENTAL",
            source="pending_export_tracking",
            message="Já existe export em andamento para esta tabela.",
            bucket=bucket,
            assume_role_arn=assume_role_arn,
            checkpoint_state=checkpoint_state,
        )
        result["checkpoint_state"] = checkpoint_state
        result["mode_selection_reason"] = "pending_export_tracking"
        return result

    selected_mode = _safe_str_field(automatic_plan.get("mode"), field_name="automatic_plan.mode")

    if selected_mode == "FULL":
        try:
            result = _start_full_export(
                config=config,
                target=target,
                ddb_client=ddb_client,
                bucket=bucket,
                bucket_owner=bucket_owner,
                assume_role_arn=assume_role_arn,
            )
        except ClientError as exc:
            if not _can_use_scan_fallback(config, ddb_client, exc):
                raise
            _log_event(
                "export.full.scan_fallback.start",
                table_name=target.table_name,
                table_arn=target.table_arn,
                code=_client_error_code(exc),
                message=_client_error_message(exc),
                endpoint_url=_resolve_optional_text(getattr(getattr(ddb_client, "meta", None), "endpoint_url", "")),
                level=logging.WARNING,
            )
            result = _run_scan_snapshot_fallback(
                config=config,
                target=target,
                ddb_client=ddb_client,
                s3_client=s3_client,
                bucket=bucket,
                s3_prefix=_build_export_prefix(config["run_time"], target, "FULL_EXPORT"),
                mode="FULL",
                assume_role_arn=assume_role_arn,
                fallback_error_code=_client_error_code(exc),
                fallback_error_message=_client_error_message(exc),
                checkpoint_to=config["run_time"],
            )

        if result.get("status") in EXPORT_PENDING_STATUSES:
            pending = _normalize_pending_exports(checkpoint_state.get("pending_exports"))
            pending.append(
                {
                    "export_arn": _safe_str_field(result.get("export_arn"), field_name="export_arn"),
                    "checkpoint_to": _safe_str_field(result.get("checkpoint_to"), field_name="checkpoint_to", required=False),
                    "mode": "FULL",
                    "source": "native",
                }
            )
            checkpoint_state = {
                **checkpoint_state,
                "pending_exports": _dedupe_pending_exports(pending),
                "incremental_seq": 0,
            }
        elif result.get("status") == "COMPLETED":
            result_source = _safe_str_field(result.get("source"), field_name="source", required=False) or "native"
            checkpoint_state = {
                **checkpoint_state,
                "last_to": _safe_str_field(result.get("checkpoint_to"), field_name="checkpoint_to", required=False),
                "last_mode": "FULL",
                "source": result_source,
                "last_export_arn": _safe_str_field(result.get("export_arn"), field_name="export_arn", required=False),
                "last_export_item_count": _resolve_completed_result_item_count(result),
                "pending_exports": [],
                "incremental_seq": 0,
            }

        result["checkpoint_state"] = checkpoint_state
        result["mode_selection_reason"] = automatic_plan.get("reason")
        return result

    last_to_candidate = automatic_plan.get("last_to")
    if not isinstance(last_to_candidate, datetime):
        raise RuntimeError("checkpoint_state.last_to inválido para execução incremental")
    last_to = last_to_candidate

    run_time = _safe_dict_field(config, "config").get("run_time")
    if not isinstance(run_time, datetime):
        raise RuntimeError("config.run_time inválido")

    export_from = last_to
    export_to = run_time

    if export_from >= export_to:
        result = _build_pending_result(
            target=target,
            mode="INCREMENTAL",
            source="window_invalid",
            message="Checkpoint já está atualizado para o horário atual.",
            bucket=bucket,
            assume_role_arn=assume_role_arn,
            checkpoint_state=checkpoint_state,
            checkpoint_from=export_from,
            checkpoint_to=export_to,
        )
        result["checkpoint_state"] = checkpoint_state
        return result

    if (export_to - export_from) < INCREMENTAL_EXPORT_MIN_WINDOW:
        result = _build_pending_result(
            target=target,
            mode="INCREMENTAL",
            source="window_too_small",
            message="Janela incremental menor que 15 minutos; nada para exportar agora.",
            bucket=bucket,
            assume_role_arn=assume_role_arn,
            checkpoint_state=checkpoint_state,
            checkpoint_from=export_from,
            checkpoint_to=export_to,
        )
        result["checkpoint_state"] = checkpoint_state
        return result

    if (export_to - export_from) > INCREMENTAL_EXPORT_MAX_WINDOW:
        export_to = export_from + INCREMENTAL_EXPORT_MAX_WINDOW
        _log_event(
            "export.incremental.window_truncated",
            table_name=target.table_name,
            table_arn=target.table_arn,
            export_from=_dt_to_iso(export_from),
            requested_export_to=_dt_to_iso(run_time),
            export_to=_dt_to_iso(export_to),
            max_window_seconds=int(INCREMENTAL_EXPORT_MAX_WINDOW.total_seconds()),
        )

    next_incremental_index = _coerce_non_negative_int(
        automatic_plan.get("next_incremental_index"),
        field_name="automatic_plan.next_incremental_index",
        default=1,
    )
    try:
        incremental_result = _start_incremental_export(
            config=config,
            target=target,
            ddb_client=ddb_client,
            bucket=bucket,
            bucket_owner=bucket_owner,
            export_from=export_from,
            export_to=export_to,
            incremental_index=next_incremental_index,
            checkpoint_source="checkpoint_last_to",
            assume_role_arn=assume_role_arn,
        )
    except ClientError as exc:
        if not _can_use_scan_fallback(config, ddb_client, exc):
            raise
        _log_event(
            "export.incremental.scan_fallback.start",
            table_name=target.table_name,
            table_arn=target.table_arn,
            code=_client_error_code(exc),
            message=_client_error_message(exc),
            endpoint_url=_resolve_optional_text(getattr(getattr(ddb_client, "meta", None), "endpoint_url", "")),
            level=logging.WARNING,
        )
        incremental_result = _run_scan_snapshot_fallback(
            config=config,
            target=target,
            ddb_client=ddb_client,
            s3_client=s3_client,
            bucket=bucket,
            s3_prefix=_build_export_prefix(
                config["run_time"],
                target,
                "INCREMENTAL_EXPORT",
                incremental_index=next_incremental_index,
            ),
            mode="INCREMENTAL",
            assume_role_arn=assume_role_arn,
            fallback_error_code=_client_error_code(exc),
            fallback_error_message=_client_error_message(exc),
            checkpoint_from=export_from,
            checkpoint_to=export_to,
            checkpoint_source="checkpoint_last_to",
        )

    if incremental_result.get("status") in EXPORT_PENDING_STATUSES:
        export_arn = _safe_str_field(incremental_result.get("export_arn"), field_name="export_arn", required=False)
        checkpoint_to = _safe_str_field(
            incremental_result.get("checkpoint_to"),
            field_name="checkpoint_to",
            required=False,
        )
        if export_arn and checkpoint_to:
            pending = _normalize_pending_exports(checkpoint_state.get("pending_exports"))
            pending.append(
                {
                    "export_arn": export_arn,
                    "checkpoint_to": checkpoint_to,
                    "mode": "INCREMENTAL",
                    "source": "native",
                }
            )
            checkpoint_state = {
                **checkpoint_state,
                "pending_exports": _dedupe_pending_exports(pending),
                "incremental_seq": next_incremental_index,
            }
    elif incremental_result.get("status") == "COMPLETED":
        result_source = _safe_str_field(incremental_result.get("source"), field_name="source", required=False) or "native"
        checkpoint_state = {
            **checkpoint_state,
            "last_to": _safe_str_field(incremental_result.get("checkpoint_to"), field_name="checkpoint_to"),
            "last_mode": "INCREMENTAL",
            "source": result_source,
            "last_export_arn": _safe_str_field(incremental_result.get("export_arn"), field_name="export_arn", required=False),
            "last_export_item_count": _resolve_completed_result_item_count(incremental_result),
            "pending_exports": [],
            "incremental_seq": next_incremental_index,
        }

    incremental_result["checkpoint_state"] = checkpoint_state
    incremental_result["mode_selection_reason"] = automatic_plan.get("reason")
    return incremental_result


def _dedupe_pending_exports(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: set[str] = set()
    deduped: List[Dict[str, str]] = []
    for item in items:
        export_arn = _safe_str_field(item.get("export_arn"), field_name="pending.export_arn", required=False)
        if not export_arn or export_arn in seen:
            continue
        seen.add(export_arn)
        deduped.append(item)
    return deduped


def _resolve_targets(config: Dict[str, Any], *, session: Any) -> List[str]:
    direct = _normalize_list(config.get("targets"))
    csv_targets = _load_values_from_csv_source(
        "targets_csv",
        _resolve_optional_text(config.get("targets_csv")),
        session=session,
        bucket_hint=_resolve_optional_text(config.get("bucket")),
        optional=True,
    )
    return _dedupe_values([*direct, *csv_targets], case_insensitive=True)


def _resolve_ignore(config: Dict[str, Any], *, session: Any) -> List[str]:
    direct = _normalize_list(config.get("ignore"))
    csv_ignore = _load_values_from_csv_source(
        "ignore_csv",
        _resolve_optional_text(config.get("ignore_csv")),
        session=session,
        bucket_hint=_resolve_optional_text(config.get("bucket")),
        optional=True,
    )
    return _dedupe_values([*direct, *csv_ignore], case_insensitive=True)


def _normalize_output_results(results: List[Dict[str, Any]], *, snapshot_bucket: Optional[str]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if snapshot_bucket and not _resolve_optional_text(row.get("snapshot_bucket")):
            row["snapshot_bucket"] = snapshot_bucket
        normalized.append(row)
    return normalized


def _resolve_checkpoint_target(config: Dict[str, Any]) -> str:
    return _safe_str_field(config.get("checkpoint_dynamodb_table_arn"), field_name="checkpoint_dynamodb_table_arn")


def _run_snapshot(config: Dict[str, Any], event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    _ = event
    base_session = _build_aws_session(config)
    checkpoint_store = create_checkpoint_store(
        session=base_session,
        checkpoint_dynamodb_table_arn=_safe_str_field(
            config.get("checkpoint_dynamodb_table_arn"),
            field_name="checkpoint_dynamodb_table_arn",
        ),
    )

    targets = _resolve_targets(config, session=base_session)
    if not targets:
        raise ValueError("Informe ao menos um target em 'targets', TARGET_TABLE_ARNS ou targets_csv/TARGETS_CSV")
    ignore = _resolve_ignore(config, session=base_session)
    ignore_set = {value.lower() for value in ignore}

    _log_event(
        "snapshot.run.start",
        run_id=config["run_id"],
        mode=config["mode"],
        dry_run=config["dry_run"],
        wait_for_completion=config["wait_for_completion"],
        max_workers=config["max_workers"],
        target_count=len(targets),
        ignore_count=len(ignore),
    )

    assume_session_cache: Dict[str, Any] = {}
    results: List[Dict[str, Any]] = []

    def execute_target(target_ref: str) -> Optional[Dict[str, Any]]:
        try:
            return _process_table(
                config=config,
                base_session=base_session,
                checkpoint_store=checkpoint_store,
                raw_target_ref=target_ref,
                ignore_set=ignore_set,
                assume_session_cache=assume_session_cache,
            )
        except Exception as exc:
            fallback_name = _extract_table_arn_context(target_ref).get("table_name") if target_ref.startswith("arn:") else target_ref
            fallback_arn = target_ref if target_ref.startswith("arn:") else target_ref
            return _build_table_error_result(
                table_name=fallback_name,
                table_arn=fallback_arn,
                mode=_safe_str_field(config.get("mode"), field_name="mode").upper(),
                error=exc,
                dry_run=bool(config.get("dry_run")),
            )

    selected_targets = [target for target in targets if target.lower() not in ignore_set]
    if not selected_targets:
        return {
            "status": "ok",
            "message": "nenhuma tabela selecionada",
            "dry_run": bool(config.get("dry_run")),
            "results": [],
            "updated_checkpoint": _resolve_checkpoint_target(config),
        }

    max_workers = max(1, int(config.get("max_workers", 1)))
    if len(selected_targets) == 1:
        result = execute_target(selected_targets[0])
        if isinstance(result, dict):
            results.append(result)
    else:
        worker_count = min(max_workers, len(selected_targets))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(execute_target, target): target for target in selected_targets}
            for future in as_completed(future_map):
                resolved = future.result()
                if isinstance(resolved, dict):
                    results.append(resolved)

    checkpoint_error: Optional[str] = None
    checkpoint_error_feedback: Dict[str, str] = {}

    if not bool(config.get("dry_run")):
        tables_payload: Dict[str, Dict[str, Any]] = {}
        for result in results:
            checkpoint_state = result.get("checkpoint_state")
            if not isinstance(checkpoint_state, dict):
                continue
            state = _normalize_checkpoint_state(
                checkpoint_state,
                table_name=_safe_str_field(result.get("table_name"), field_name="table_name"),
                table_arn=_safe_str_field(result.get("table_arn"), field_name="table_arn"),
            )
            state_key = _safe_str_field(state.get("table_arn"), field_name="state.table_arn", required=False) or _safe_str_field(state.get("table_name"), field_name="state.table_name")
            tables_payload[state_key] = state

        if tables_payload:
            try:
                checkpoint_save(
                    checkpoint_store,
                    {
                        "version": 2,
                        "tables": tables_payload,
                    },
                )
            except Exception as exc:
                checkpoint_error = str(exc)
                checkpoint_error_feedback = _build_error_response_fields(exc)
                _log_event("snapshot.checkpoint.failed", error=checkpoint_error, level=logging.ERROR)

    failed_count = sum(
        1
        for item in results
        if _safe_str_field(item.get("status"), field_name="result.status", required=False).upper() == "FAILED"
    )
    status = "partial_ok" if (failed_count or checkpoint_error) else "ok"

    response: Dict[str, Any] = {
        "status": status,
        "run_id": config["run_id"],
        "mode": config["mode"],
        "dry_run": bool(config.get("dry_run")),
        "checkpoint_error": checkpoint_error,
        "results": _normalize_output_results(results, snapshot_bucket=_resolve_optional_text(config.get("bucket"))),
        "updated_checkpoint": _resolve_checkpoint_target(config),
    }
    if checkpoint_error_feedback:
        response = {
            **response,
            "checkpoint_error_detail": checkpoint_error_feedback.get("error_detail"),
            "checkpoint_user_message": checkpoint_error_feedback.get("user_message"),
            "checkpoint_resolution": checkpoint_error_feedback.get("resolution"),
        }
    return response


def _emit_output_to_cloudwatch(
    source: str,
    payload: Dict[str, Any],
    *,
    config: Dict[str, Any],
    context: Any,
) -> None:
    if not bool(config.get("output_cloudwatch_enabled")):
        return
    envelope = {
        "source": source,
        "aws_request_id": getattr(context, "aws_request_id", None),
        "run_id": payload.get("run_id"),
        "payload": payload,
    }
    text = json.dumps(_to_json_safe(envelope), ensure_ascii=False, default=str)
    if len(text.encode("utf-8")) > CLOUDWATCH_OUTPUT_MAX_BYTES:
        text = text.encode("utf-8")[:CLOUDWATCH_OUTPUT_MAX_BYTES].decode("utf-8", errors="ignore")
    logger.info("%s", text)


def _resolve_output_row_export_arn(row: Dict[str, Any]) -> str:
    direct_export_arn = _safe_str_field(
        row.get("export_arn"),
        field_name="result.export_arn",
        required=False,
    )
    if direct_export_arn:
        return direct_export_arn

    pending_exports = row.get("pending_exports")
    if not isinstance(pending_exports, list):
        return ""

    for item in pending_exports:
        if not isinstance(item, dict):
            continue
        pending_export_arn = _safe_str_field(
            item.get("export_arn"),
            field_name="pending.export_arn",
            required=False,
        )
        if pending_export_arn:
            return pending_export_arn
    return ""


def _resolve_output_row_started_at(row: Dict[str, Any]) -> str:
    direct_started_at = _safe_str_field(
        row.get("started_at"),
        field_name="result.started_at",
        required=False,
    )
    if direct_started_at:
        return direct_started_at

    pending_exports = row.get("pending_exports")
    if not isinstance(pending_exports, list):
        return ""

    for item in pending_exports:
        if not isinstance(item, dict):
            continue
        pending_started_at = _safe_str_field(
            item.get("started_at"),
            field_name="pending.started_at",
            required=False,
        )
        if pending_started_at:
            return pending_started_at
    return ""


def _format_output_status_label(status: str) -> str:
    normalized = _safe_str_field(status, field_name="result.status", required=False).upper()
    if normalized in {"STARTED", "IN_PROGRESS", "PENDING"}:
        return "In progress"
    if normalized == "COMPLETED":
        return "Completed"
    if normalized == "FAILED":
        return "Failed"
    if normalized == "CANCELLED":
        return "Cancelled"
    if not normalized:
        return "Unknown"
    return normalized.replace("_", " ").capitalize()


def _format_output_export_type_label(mode: str) -> str:
    normalized = _safe_str_field(mode, field_name="result.mode", required=False).upper()
    if normalized == "FULL":
        return "Full export"
    if normalized == "INCREMENTAL":
        return "Incremental export"
    return "Unknown"


def _format_output_started_at_local(started_at: str) -> str:
    parsed = _parse_iso_datetime(started_at)
    if not isinstance(parsed, datetime):
        return ""
    return parsed.astimezone(OUTPUT_DYNAMODB_LOCAL_TIMEZONE).isoformat(timespec="seconds")


def _resolve_output_destination_uri(row: Dict[str, Any], *, payload: Dict[str, Any]) -> str:
    bucket = _resolve_optional_text(
        row.get("snapshot_bucket"),
        payload.get("snapshot_bucket"),
    )
    s3_prefix = _safe_str_field(row.get("s3_prefix"), field_name="result.s3_prefix", required=False).lstrip("/")
    if bucket and s3_prefix:
        return f"s3://{bucket}/{s3_prefix}"
    if bucket:
        return f"s3://{bucket}"
    return ""


def _build_output_dynamodb_item(row: Dict[str, Any], *, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    export_arn = _resolve_output_row_export_arn(row)
    if not export_arn:
        return None

    started_at = _resolve_output_row_started_at(row)
    return {
        OUTPUT_DYNAMODB_PARTITION_KEY: export_arn,
        "Table name": _safe_str_field(row.get("table_name"), field_name="result.table_name", required=False),
        "Destination S3 Bucket": _resolve_output_destination_uri(row, payload=payload),
        "Status": _format_output_status_label(_safe_str_field(row.get("status"), field_name="result.status", required=False)),
        "Export job start time (utc-03:00)": _format_output_started_at_local(started_at),
        "Export Type": _format_output_export_type_label(_safe_str_field(row.get("mode"), field_name="result.mode", required=False)),
    }


def _emit_output_to_dynamodb(
    source: str,
    payload: Dict[str, Any],
    *,
    config: Dict[str, Any],
    event: Optional[Dict[str, Any]],
    context: Any,
    session: Any,
) -> None:
    _ = (event, source, context)
    if not bool(config.get("output_dynamodb_enabled")):
        return

    table_name = _resolve_optional_text(config.get("output_dynamodb_table"))
    if not table_name:
        return

    region = _resolve_optional_text(config.get("output_dynamodb_region"), _resolve_runtime_region(session.region_name))
    if not region:
        _log_event("output.dynamodb.write.skipped", reason="region_not_resolved", table_name=table_name, level=logging.WARNING)
        return

    ddb_client = _get_session_client(session, "dynamodb", region_name=region)
    try:
        _ensure_output_dynamodb_table_exists(ddb_client, table_name=table_name)
        results = payload.get("results") if isinstance(payload.get("results"), list) else []
        written_rows = 0
        for row in results:
            if not isinstance(row, dict):
                continue
            item = _build_output_dynamodb_item(row, payload=payload)
            if not isinstance(item, dict):
                continue
            ddb_client.put_item(TableName=table_name, Item=_ddb_encode_item(item))
            written_rows += 1
        _log_event(
            "output.dynamodb.write.success",
            table_name=table_name,
            region=region,
            rows=written_rows,
        )
    except Exception as exc:
        logger.warning("Falha ao emitir output para DynamoDB: %s", exc)
        _log_event(
            "output.dynamodb.write.failed",
            table_name=table_name,
            region=region,
            source=source,
            error=str(exc),
            level=logging.WARNING,
        )


def build_snapshot_config(event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = _extract_event_payload(event)

    targets = _normalize_list(
        os.getenv("TARGET_TABLE_ARNS")
        or os.getenv("TARGET_TABLES")
        or payload.get("targets")
        or payload.get("target")
        or ""
    )
    targets_csv = _resolve_optional_text(
        os.getenv("TARGETS_CSV", ""),
        payload.get("targets_csv"),
        payload.get("target_csv"),
        payload.get("targetsCsv"),
        payload.get("targetCsv"),
    )

    ignore = _normalize_list(
        os.getenv("IGNORE_TARGETS")
        or os.getenv("IGNORE_TABLES")
        or payload.get("ignore")
        or payload.get("ignore_targets")
        or ""
    )
    ignore_csv = _resolve_optional_text(
        os.getenv("IGNORE_CSV", ""),
        payload.get("ignore_csv"),
        payload.get("ignore_targets_csv"),
        payload.get("ignoreCsv"),
        payload.get("ignoreTargetsCsv"),
    )

    if not targets and not targets_csv:
        raise ValueError("Informe ao menos um target em 'targets', TARGET_TABLE_ARNS ou targets_csv/TARGETS_CSV")

    mode = "automatic"

    try:
        max_workers = int(
            _resolve_optional_text(
                os.getenv("MAX_WORKERS"),
                payload.get("max_workers"),
                payload.get("maxWorkers"),
                "4",
            ) or "4"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("MAX_WORKERS deve ser um inteiro válido") from exc
    max_workers = max(1, max_workers)

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
        payload.get("bucketOwner"),
        payload.get("snapshot_bucket_owner"),
        payload.get("snapshotBucketOwner"),
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
        _resolve_optional_text(payload.get("wait_for_completion"), payload.get("waitForCompletion")),
        "WAIT_FOR_COMPLETION",
        False,
    )

    snapshot_bucket_exact = _resolve_env_first_bool(
        _resolve_optional_text(payload.get("snapshot_bucket_exact"), payload.get("snapshotBucketExact")),
        "SNAPSHOT_BUCKET_EXACT",
        False,
    )

    dry_run = _resolve_env_first_bool(
        _resolve_optional_text(payload.get("dry_run"), payload.get("dryRun")),
        "DRY_RUN",
        False,
    )

    scan_fallback_enabled = _resolve_env_first_bool(
        _resolve_optional_text(payload.get("scan_fallback_enabled"), payload.get("scanFallbackEnabled")),
        "SCAN_FALLBACK_ENABLED",
        True,
    )

    incremental_export_view_type = _resolve_incremental_export_view_type(
        os.getenv("INCREMENTAL_EXPORT_VIEW_TYPE"),
        payload.get("incremental_export_view_type"),
        payload.get("incrementalExportViewType"),
    )

    checkpoint_dynamodb_table_arn = _resolve_optional_text(
        os.getenv("CHECKPOINT_DYNAMODB_TABLE_ARN", ""),
        payload.get("checkpoint_dynamodb_table_arn"),
        payload.get("checkpointDynamodbTableArn"),
    )
    if not checkpoint_dynamodb_table_arn:
        raise ValueError("CHECKPOINT_DYNAMODB_TABLE_ARN não definido")
    _extract_table_arn_context(checkpoint_dynamodb_table_arn, field_name="checkpoint_dynamodb_table_arn")

    output_cloudwatch_enabled = _resolve_env_first_bool(
        _resolve_optional_text(payload.get("output_cloudwatch_enabled"), payload.get("outputCloudwatchEnabled")),
        "OUTPUT_CLOUDWATCH_ENABLED",
        False,
    )

    output_dynamodb_enabled = _resolve_env_first_bool(
        _resolve_optional_text(payload.get("output_dynamodb_enabled"), payload.get("outputDynamodbEnabled")),
        "OUTPUT_DYNAMODB_ENABLED",
        False,
    )

    output_dynamodb_table = _resolve_optional_text(
        os.getenv("OUTPUT_DYNAMODB_TABLE", ""),
        payload.get("output_dynamodb_table"),
        payload.get("outputDynamodbTable"),
    )
    if output_dynamodb_enabled and not output_dynamodb_table:
        raise ValueError("OUTPUT_DYNAMODB_TABLE deve ser informado quando OUTPUT_DYNAMODB_ENABLED=true")

    output_dynamodb_region = _resolve_optional_text(
        os.getenv("OUTPUT_DYNAMODB_REGION", ""),
        payload.get("output_dynamodb_region"),
        payload.get("outputDynamodbRegion"),
        _resolve_runtime_region(),
    )

    run_time = _now_utc()
    run_id = run_time.strftime("%Y%m%dT%H%M%SZ")

    assume_role = _resolve_optional_text(
        os.getenv("ASSUME_ROLE", ""),
        payload.get("assume_role"),
        payload.get("assume_role_arn"),
        payload.get("assumeRole"),
        payload.get("assumeRoleArn"),
    )

    assume_role_external_id = _resolve_optional_text(
        os.getenv("ASSUME_ROLE_EXTERNAL_ID", ""),
        payload.get("assume_role_external_id"),
        payload.get("assumeRoleExternalId"),
    )

    assume_role_session_name = _resolve_optional_text(
        os.getenv("ASSUME_ROLE_SESSION_NAME", ""),
        payload.get("assume_role_session_name"),
        payload.get("assumeRoleSessionName"),
        f"dynamodb-snapshot-{run_id}",
    ) or f"dynamodb-snapshot-{run_id}"

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

    if assume_role_duration_seconds < 900 or assume_role_duration_seconds > 43200:
        raise ValueError("ASSUME_ROLE_DURATION_SECONDS deve estar entre 900 e 43200")

    config: Dict[str, Any] = {
        "run_id": run_id,
        "run_time": run_time,
        "targets": targets,
        "targets_csv": targets_csv,
        "ignore": ignore,
        "ignore_csv": ignore_csv,
        "mode": mode,
        "max_workers": max_workers,
        "bucket": bucket,
        "bucket_owner": bucket_owner,
        "s3_prefix": s3_prefix,
        "wait_for_completion": wait_for_completion,
        "snapshot_bucket_exact": snapshot_bucket_exact,
        "dry_run": dry_run,
        "scan_fallback_enabled": scan_fallback_enabled,
        "incremental_export_view_type": incremental_export_view_type,
        "checkpoint_dynamodb_table_arn": checkpoint_dynamodb_table_arn,
        "output_cloudwatch_enabled": output_cloudwatch_enabled,
        "output_dynamodb_enabled": output_dynamodb_enabled,
        "output_dynamodb_table": output_dynamodb_table,
        "output_dynamodb_region": output_dynamodb_region,
        "assume_role": assume_role,
        "assume_role_external_id": assume_role_external_id,
        "assume_role_session_name": assume_role_session_name,
        "assume_role_duration_seconds": assume_role_duration_seconds,
    }

    _log_event(
        "config.targets.resolved",
        targets_count=len(targets),
        targets_csv_configured=bool(targets_csv),
        ignore_count=len(ignore),
        ignore_csv_configured=bool(ignore_csv),
        mode=mode,
        max_workers=max_workers,
        wait_for_completion=wait_for_completion,
        snapshot_bucket_exact=snapshot_bucket_exact,
        scan_fallback_enabled=scan_fallback_enabled,
        incremental_export_view_type=incremental_export_view_type,
    )
    _log_event(
        "config.assume_role.resolved",
        assume_role_enabled=bool(assume_role),
        assume_role_template_enabled=bool(assume_role and ROLE_TEMPLATE_PATTERN.search(assume_role)),
        assume_role_source=("env_or_event" if assume_role else "unset"),
        has_external_id=bool(assume_role_external_id),
        session_name=assume_role_session_name,
        duration_seconds=assume_role_duration_seconds,
    )

    return config


def lambda_handler(
    event: Optional[Dict[str, Any]] = None,
    context: Any = None,
    *,
    emit_cloudwatch_output: bool = True,
) -> Dict[str, Any]:
    event_keys = sorted(event.keys()) if isinstance(event, dict) else []
    config: Optional[Dict[str, Any]] = None

    _log_event(
        "handler.start",
        event_keys=event_keys,
        has_event=event is not None,
        aws_request_id=getattr(context, "aws_request_id", None),
    )

    try:
        config = build_snapshot_config(event)
        run_result = _run_snapshot(config, event)

        run_status = _safe_str_field(run_result.get("status"), field_name="status", required=False).lower()
        result_ok = run_status == "ok"

        result: Dict[str, Any] = {
            "ok": result_ok,
            "snapshot_bucket": config["bucket"],
            **run_result,
        }

        log_action = "handler.success" if result_ok else "handler.partial_failure"
        output_source = "lambda_handler.success" if result_ok else "lambda_handler.partial_failure"

        failed_count = sum(
            _safe_str_field(row.get("status"), field_name="result.status", required=False).upper() == "FAILED"
            for row in result.get("results", [])
            if isinstance(row, dict)
        )

        _log_event(
            log_action,
            status=result.get("status"),
            run_id=result.get("run_id"),
            result_count=len(result.get("results", [])) if isinstance(result.get("results"), list) else 0,
            failed_count=failed_count,
            checkpoint_error=result.get("checkpoint_error"),
            snapshot_bucket=result.get("snapshot_bucket"),
            level=(logging.INFO if result_ok else logging.WARNING),
        )

        if emit_cloudwatch_output:
            _emit_output_to_cloudwatch(
                output_source,
                result,
                config=config,
                context=context,
            )

        _emit_output_to_dynamodb(
            output_source,
            result,
            config=config,
            event=event,
            context=context,
            session=_build_aws_session(config),
        )
        return result

    except ValueError as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.config_error", error=str(exc), level=logging.ERROR)
        response = {
            "ok": False,
            "status": "error",
            "error_type": "config",
            **fields,
        }
        if emit_cloudwatch_output and isinstance(config, dict):
            _emit_output_to_cloudwatch("lambda_handler.config_error", response, config=config, context=context)
        return response

    except (BotoCoreError, ClientError, ConnectTimeoutError, EndpointConnectionError, NoCredentialsError, NoRegionError,
            PartialCredentialsError, ProxyConnectionError, ReadTimeoutError, TimeoutError) as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.aws_error", error=str(exc), level=logging.ERROR)
        response = {
            "ok": False,
            "status": "error",
            "error_type": "aws",
            **fields,
        }
        if emit_cloudwatch_output and isinstance(config, dict):
            _emit_output_to_cloudwatch("lambda_handler.aws_error", response, config=config, context=context)
        return response

    except Exception as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.runtime_error", error=str(exc), level=logging.ERROR)
        response = {
            "ok": False,
            "status": "error",
            "error_type": "runtime",
            **fields,
        }
        if emit_cloudwatch_output and isinstance(config, dict):
            _emit_output_to_cloudwatch("lambda_handler.runtime_error", response, config=config, context=context)
        return response
