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
CHECKPOINT_DYNAMODB_LOCK_RECORD = "LOCK"
CHECKPOINT_DYNAMODB_REVISION_ATTR = "Revision"
CHECKPOINT_DYNAMODB_LOCK_OWNER_ATTR = "OwnerToken"
CHECKPOINT_DYNAMODB_LOCK_EXPIRES_AT_ATTR = "ExpiresAt"
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
CHECKPOINT_DYNAMODB_LOCK_TIMEOUT_SECONDS = EXPORT_WAIT_TIMEOUT_SECONDS + 60

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
_CHECKPOINT_DDB_COMMAND_SEQUENCE_LOCK = threading.Lock()
_CHECKPOINT_DDB_COMMAND_SEQUENCE = 0


class CheckpointStateError(RuntimeError):
    pass


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


def _resolve_max_incremental_exports_per_cycle(*values: Any) -> int:
    resolved_text = _resolve_optional_text(*values, str(MAX_INCREMENTAL_EXPORTS_PER_CYCLE)) or str(MAX_INCREMENTAL_EXPORTS_PER_CYCLE)
    try:
        resolved_value = int(resolved_text)
    except (TypeError, ValueError) as exc:
        raise ValueError("MAX_INCREMENTAL_EXPORTS_PER_CYCLE deve ser um inteiro válido") from exc
    if resolved_value <= 0:
        raise ValueError("MAX_INCREMENTAL_EXPORTS_PER_CYCLE deve ser maior que zero")
    return resolved_value


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


def _dt_to_iso_with_milliseconds(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


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


def _resolve_checkpoint_last_to_timestamp(
    *,
    raw_last_to: Any,
    table_name: str,
    table_arn: str,
) -> Optional[datetime]:
    parsed = _parse_iso_datetime(raw_last_to)
    if isinstance(parsed, datetime):
        return parsed

    parsed = _coerce_checkpoint_timestamp_to_utc(raw_last_to)
    if isinstance(parsed, datetime):
        _log_event(
            "checkpoint.last_to.recovered_from_legacy_format",
            table_name=table_name,
            table_arn=table_arn,
            raw_last_to=_safe_str_field(raw_last_to, field_name="checkpoint_state.last_to", required=False),
            last_to=_dt_to_iso(parsed),
            level=logging.INFO,
        )
    return parsed


def _coerce_checkpoint_timestamp_to_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    text = _resolve_optional_text(value)
    if not text:
        return None
    if text.isdigit():
        try:
            epoch_seconds = int(text)
            if epoch_seconds > 10**12:
                epoch_seconds = epoch_seconds / 1000
            return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if text.endswith("Z"):
        normalized = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return _parse_iso_datetime(text)
    return _parse_iso_datetime(text)


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


def _safe_coerce_non_negative_int(
    value: Any,
    *,
    field_name: str,
    table_name: str,
    table_arn: str,
    default: int = 0,
) -> int:
    try:
        return _coerce_non_negative_int(value, field_name=field_name, default=default)
    except ValueError:
        _log_event(
            "checkpoint.state.invalid_numeric",
            table_name=table_name,
            table_arn=table_arn,
            field=field_name,
            raw_value=_safe_str_field(value, field_name=field_name, required=False),
            level=logging.WARNING,
        )
        return default


def _safe_coerce_optional_non_negative_int(
    value: Any,
    *,
    field_name: str,
    table_name: str,
    table_arn: str,
) -> Optional[int]:
    try:
        return _coerce_optional_non_negative_int(value, field_name=field_name)
    except ValueError:
        _log_event(
            "checkpoint.state.invalid_numeric",
            table_name=table_name,
            table_arn=table_arn,
            field=field_name,
            raw_value=_safe_str_field(value, field_name=field_name, required=False),
            level=logging.WARNING,
        )
        return None


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
    payload = {
        "action": action,
        "eventTime": _dt_to_iso_with_milliseconds(_now_utc()),
        **fields,
    }
    logger.log(level, "%s", json.dumps(_to_json_safe(payload), ensure_ascii=False, default=str))


def _next_checkpoint_dynamodb_command_sequence() -> int:
    global _CHECKPOINT_DDB_COMMAND_SEQUENCE
    with _CHECKPOINT_DDB_COMMAND_SEQUENCE_LOCK:
        _CHECKPOINT_DDB_COMMAND_SEQUENCE += 1
        return _CHECKPOINT_DDB_COMMAND_SEQUENCE


def _extract_checkpoint_ddb_scalar_string(value: Any) -> str:
    if isinstance(value, dict):
        if "S" in value:
            return _safe_str_field(value.get("S"), field_name="ddb.S", required=False)
        if "N" in value:
            return _safe_str_field(value.get("N"), field_name="ddb.N", required=False)
    return _safe_str_field(value, field_name="ddb.scalar", required=False)


def _extract_checkpoint_ddb_identity(item_or_key: Any) -> Dict[str, str]:
    if not isinstance(item_or_key, dict):
        return {"partition_value": "", "record_type": ""}
    return {
        "partition_value": _extract_checkpoint_ddb_scalar_string(item_or_key.get(CHECKPOINT_DYNAMODB_PARTITION_KEY)),
        "record_type": _extract_checkpoint_ddb_scalar_string(item_or_key.get(CHECKPOINT_DYNAMODB_SORT_KEY)),
    }


def _log_checkpoint_dynamodb_command(
    *,
    command: str,
    table_name: str,
    reason: str,
    partition_value: Optional[str] = None,
    record_type: Optional[str] = None,
    consistent_read: Optional[bool] = None,
    condition_expression: Optional[str] = None,
    attempt: Optional[int] = None,
    level: int = logging.INFO,
) -> Dict[str, Any]:
    command_time = _dt_to_iso_with_milliseconds(_now_utc())
    payload = {
        "command": _safe_str_field(command, field_name="command"),
        "commandTime": command_time,
        "commandSequence": _next_checkpoint_dynamodb_command_sequence(),
        "reason": _safe_str_field(reason, field_name="reason"),
        "checkpoint_table_name": _safe_str_field(table_name, field_name="checkpoint_table_name"),
    }
    if partition_value:
        payload["partition_value"] = _safe_str_field(partition_value, field_name="partition_value", required=False)
    if record_type:
        payload["record_type"] = _safe_str_field(record_type, field_name="record_type", required=False)
    if consistent_read is not None:
        payload["consistent_read"] = bool(consistent_read)
    if condition_expression:
        payload["condition_expression"] = _safe_str_field(
            condition_expression,
            field_name="condition_expression",
            required=False,
        )
    if attempt is not None:
        payload["attempt"] = int(attempt)
    _log_event("checkpoint.dynamodb.command", level=level, **payload)
    return payload


def _checkpoint_execute_dynamodb_command(
    ddb_client: Any,
    *,
    command: str,
    method_name: str,
    table_name: str,
    reason: str,
    partition_value: Optional[str] = None,
    record_type: Optional[str] = None,
    consistent_read: Optional[bool] = None,
    condition_expression: Optional[str] = None,
    attempt: Optional[int] = None,
    **kwargs: Any,
) -> Any:
    trace_payload = _log_checkpoint_dynamodb_command(
        command=command,
        table_name=table_name,
        reason=reason,
        partition_value=partition_value,
        record_type=record_type,
        consistent_read=consistent_read,
        condition_expression=condition_expression,
        attempt=attempt,
    )
    try:
        method = getattr(ddb_client, method_name)
        return method(**kwargs)
    except ClientError as exc:
        _log_event(
            "checkpoint.dynamodb.command.failed",
            level=logging.WARNING,
            command=trace_payload["command"],
            commandTime=trace_payload["commandTime"],
            commandSequence=trace_payload["commandSequence"],
            reason=trace_payload["reason"],
            checkpoint_table_name=trace_payload["checkpoint_table_name"],
            partition_value=partition_value,
            record_type=record_type,
            error_code=_client_error_code(exc),
            error_detail=str(exc),
        )
        raise


def _checkpoint_describe_table(
    ddb_client: Any,
    *,
    table_name: str,
    reason: str,
    attempt: Optional[int] = None,
) -> Dict[str, Any]:
    response = _checkpoint_execute_dynamodb_command(
        ddb_client,
        command="DescribeTable",
        method_name="describe_table",
        table_name=table_name,
        reason=reason,
        attempt=attempt,
        TableName=table_name,
    )
    return response if isinstance(response, dict) else {}


def _checkpoint_create_table(
    ddb_client: Any,
    *,
    table_name: str,
    reason: str,
    **kwargs: Any,
) -> Any:
    return _checkpoint_execute_dynamodb_command(
        ddb_client,
        command="CreateTable",
        method_name="create_table",
        table_name=table_name,
        reason=reason,
        **kwargs,
    )


def _checkpoint_scan_table(
    ddb_client: Any,
    *,
    table_name: str,
    reason: str,
) -> Dict[str, Any]:
    response = _checkpoint_execute_dynamodb_command(
        ddb_client,
        command="Scan",
        method_name="scan",
        table_name=table_name,
        reason=reason,
        TableName=table_name,
    )
    return response if isinstance(response, dict) else {}


def _checkpoint_put_item(
    ddb_client: Any,
    *,
    table_name: str,
    reason: str,
    item: Optional[Dict[str, Any]] = None,
    attempt: Optional[int] = None,
    **kwargs: Any,
) -> Any:
    identity = _extract_checkpoint_ddb_identity(item)
    condition_expression = _safe_str_field(
        kwargs.get("ConditionExpression"),
        field_name="ConditionExpression",
        required=False,
    )
    return _checkpoint_execute_dynamodb_command(
        ddb_client,
        command="PutItem",
        method_name="put_item",
        table_name=table_name,
        reason=reason,
        partition_value=identity.get("partition_value"),
        record_type=identity.get("record_type"),
        condition_expression=condition_expression,
        attempt=attempt,
        **kwargs,
    )


def _checkpoint_delete_item(
    ddb_client: Any,
    *,
    table_name: str,
    reason: str,
    key: Dict[str, Any],
    **kwargs: Any,
) -> Any:
    identity = _extract_checkpoint_ddb_identity(key)
    condition_expression = _safe_str_field(
        kwargs.get("ConditionExpression"),
        field_name="ConditionExpression",
        required=False,
    )
    return _checkpoint_execute_dynamodb_command(
        ddb_client,
        command="DeleteItem",
        method_name="delete_item",
        table_name=table_name,
        reason=reason,
        partition_value=identity.get("partition_value"),
        record_type=identity.get("record_type"),
        condition_expression=condition_expression,
        **kwargs,
    )


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
    run_id_part = run_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    export_segment = "FULL" if export_type == "FULL_EXPORT" else "INCR"
    return f"DDB/{target.account_id}/{target.table_name}/{export_segment}/run_id={run_id_part}"


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
    raw_last_mode = _safe_str_field(state.get("last_mode"), field_name="state.last_mode", required=False).upper()
    last_mode = raw_last_mode
    if last_mode and last_mode not in {"FULL", "INCREMENTAL"}:
        _log_event(
            "checkpoint.state.invalid_last_mode",
            table_name=table_name,
            table_arn=table_arn,
            raw_value=raw_last_mode,
            level=logging.WARNING,
        )
        last_mode = ""
    normalized_table_name = _safe_str_field(state.get("table_name"), field_name="state.table_name", required=False) or table_name
    normalized_table_arn = _safe_str_field(state.get("table_arn"), field_name="state.table_arn", required=False) or table_arn
    incremental_seq = _safe_coerce_non_negative_int(
        state.get("incremental_seq"),
        field_name="state.incremental_seq",
        default=0,
        table_name=normalized_table_name,
        table_arn=normalized_table_arn,
    )
    pending_exports = _normalize_pending_exports(state.get("pending_exports"))
    if last_mode == "FULL" and not pending_exports:
        incremental_seq = 0
    return {
        "table_name": normalized_table_name,
        "table_arn": normalized_table_arn,
        "table_created_at": _safe_str_field(state.get("table_created_at"), field_name="state.table_created_at", required=False),
        "last_to": _safe_str_field(state.get("last_to"), field_name="state.last_to", required=False),
        "last_mode": last_mode,
        "source": _safe_str_field(state.get("source"), field_name="state.source", required=False),
        "last_export_arn": _safe_str_field(state.get("last_export_arn"), field_name="state.last_export_arn", required=False),
        "last_export_item_count": _safe_coerce_optional_non_negative_int(
            state.get("last_export_item_count"),
            field_name="state.last_export_item_count",
            table_name=normalized_table_name,
            table_arn=normalized_table_arn,
        ),
        "pending_exports": pending_exports,
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
    checkpoint_command_reason: str = "",
) -> None:
    elapsed = 0
    while elapsed < timeout_seconds:
        try:
            if checkpoint_command_reason:
                response = _checkpoint_describe_table(
                    ddb_client,
                    table_name=table_name,
                    reason=checkpoint_command_reason,
                    attempt=(elapsed // poll_seconds) + 1,
                )
            else:
                response = ddb_client.describe_table(TableName=table_name)
        except ClientError as exc:
            if _client_error_code(exc) not in AWS_NOT_FOUND_ERROR_CODES:
                raise
            _log_event("dynamodb.table.wait", table_name=table_name, status="NOT_FOUND")
            time.sleep(poll_seconds)
            elapsed += poll_seconds
            continue
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
        response = _checkpoint_describe_table(
            ddb_client,
            table_name=table_name,
            reason="ensure checkpoint table exists before using checkpoint backend",
        )
        table = response.get("Table") if isinstance(response, dict) else None
        _validate_checkpoint_table_schema(table_name, _safe_dict_field(table, "table"))
        return
    except ClientError as exc:
        code = _client_error_code(exc)
        if code not in AWS_NOT_FOUND_ERROR_CODES:
            raise _build_aws_runtime_error("DynamoDB DescribeTable checkpoint", exc, resource=table_name) from exc

    _log_event("checkpoint.dynamodb.table.missing", table_name=table_name)
    try:
        _checkpoint_create_table(
            ddb_client,
            table_name=table_name,
            reason="create checkpoint table because it was missing",
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
    except ClientError as exc:
        if _client_error_code(exc) != "ResourceInUseException":
            raise
        _log_event(
            "checkpoint.dynamodb.table.create_race_detected",
            table_name=table_name,
            resolution="wait_for_table_active",
            level=logging.WARNING,
        )
    _wait_dynamodb_table_active(
        ddb_client,
        table_name=table_name,
        poll_seconds=CHECKPOINT_DYNAMODB_TABLE_POLL_SECONDS,
        timeout_seconds=CHECKPOINT_DYNAMODB_TABLE_TIMEOUT_SECONDS,
        checkpoint_command_reason="wait for checkpoint table to become ACTIVE after creation",
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
    _log_event(
        "checkpoint.dynamodb.table.validated",
        table_name=table_name,
        table_region=table_region,
        table_arn=checkpoint_dynamodb_table_arn,
        level=logging.INFO,
    )
    return {
        "backend": "dynamodb",
        "ddb": ddb_client,
        "table_name": table_name,
        "table_region": table_region,
        "table_arn": checkpoint_dynamodb_table_arn,
    }


def build_checkpoint_store_for_session(session: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    checkpoint_table_arn = _safe_str_field(
        config.get("checkpoint_dynamodb_table_arn"),
        field_name="checkpoint_dynamodb_table_arn",
        required=False,
    )
    if checkpoint_table_arn:
        return create_checkpoint_store(
            session=session,
            checkpoint_dynamodb_table_arn=checkpoint_table_arn,
        )

    checkpoint_bucket = _safe_str_field(
        config.get("checkpoint_bucket"),
        field_name="checkpoint_bucket",
        required=False,
    ) or _safe_str_field(config.get("bucket"), field_name="bucket")
    checkpoint_key = _safe_str_field(config.get("checkpoint_key"), field_name="checkpoint_key")
    return {
        "backend": "s3",
        "s3": _get_session_client(session, "s3"),
        "bucket": checkpoint_bucket,
        "key": checkpoint_key,
    }


def _checkpoint_supports_execution_lock(store: Dict[str, Any]) -> bool:
    backend = _safe_str_field(store.get("backend"), field_name="checkpoint_store.backend", required=False).lower()
    if backend == "s3" or store.get("s3") is not None:
        return False
    return store.get("ddb") is not None and bool(
        _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name", required=False)
    )


def _checkpoint_resolve_partition_value(
    *,
    target_table_name: str,
    target_table_arn: Optional[str] = None,
) -> str:
    return _safe_str_field(target_table_arn, field_name="target_table_arn", required=False) or _safe_str_field(
        target_table_name,
        field_name="target_table_name",
    )


def _checkpoint_get_record_item(
    ddb_client: Any,
    *,
    table_name: str,
    partition_value: str,
    record_type: str,
    reason: str,
) -> Dict[str, Any]:
    response = _checkpoint_execute_dynamodb_command(
        ddb_client,
        command="GetItem",
        method_name="get_item",
        table_name=table_name,
        reason=reason,
        partition_value=partition_value,
        record_type=record_type,
        consistent_read=True,
        TableName=table_name,
        Key={
            CHECKPOINT_DYNAMODB_PARTITION_KEY: {"S": partition_value},
            CHECKPOINT_DYNAMODB_SORT_KEY: {"S": record_type},
        },
        ConsistentRead=True,
    )
    item = response.get("Item") if isinstance(response, dict) else None
    return item if isinstance(item, dict) else {}


def _checkpoint_get_current_item(
    ddb_client: Any,
    *,
    table_name: str,
    partition_value: str,
    reason: str = "load checkpoint CURRENT record",
) -> Dict[str, Any]:
    return _checkpoint_get_record_item(
        ddb_client,
        table_name=table_name,
        partition_value=partition_value,
        record_type=CHECKPOINT_DYNAMODB_CURRENT_RECORD,
        reason=reason,
    )


def _checkpoint_get_lock_item(
    ddb_client: Any,
    *,
    table_name: str,
    partition_value: str,
    reason: str = "load checkpoint LOCK record",
) -> Dict[str, Any]:
    return _checkpoint_get_record_item(
        ddb_client,
        table_name=table_name,
        partition_value=partition_value,
        record_type=CHECKPOINT_DYNAMODB_LOCK_RECORD,
        reason=reason,
    )


def _decode_checkpoint_execution_lock(item: Dict[str, Any]) -> Dict[str, str]:
    if not item:
        return {}
    decoded = _ddb_decode_item(item)
    return {
        "owner_token": _safe_str_field(
            decoded.get(CHECKPOINT_DYNAMODB_LOCK_OWNER_ATTR),
            field_name="checkpoint.OwnerToken",
            required=False,
        ),
        "observed_at": _safe_str_field(decoded.get("ObservedAt"), field_name="checkpoint.ObservedAt", required=False),
        "expires_at": _safe_str_field(
            decoded.get(CHECKPOINT_DYNAMODB_LOCK_EXPIRES_AT_ATTR),
            field_name="checkpoint.ExpiresAt",
            required=False,
        ),
        "mode": _safe_str_field(decoded.get("Mode"), field_name="checkpoint.Mode", required=False).upper(),
    }


def _build_checkpoint_execution_lock_owner_token(
    *,
    config: Dict[str, Any],
    target: TableTarget,
) -> str:
    run_id = _safe_str_field(config.get("run_id"), field_name="config.run_id", required=False)
    return _build_client_token(
        target.table_arn,
        run_id or _dt_to_iso(_now_utc()),
        str(time.time_ns()),
        str(threading.get_ident()),
    )


def _checkpoint_try_acquire_execution_lock(
    store: Dict[str, Any],
    *,
    target_table_name: str,
    target_table_arn: Optional[str],
    mode_hint: str,
    owner_token: str,
) -> Dict[str, Any]:
    if not _checkpoint_supports_execution_lock(store):
        return {"acquired": True, "owner_token": owner_token}

    table_name = _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name")
    ddb_client = store.get("ddb")
    if ddb_client is None:
        raise RuntimeError("checkpoint_store.ddb ausente")

    partition_value = _checkpoint_resolve_partition_value(
        target_table_name=target_table_name,
        target_table_arn=target_table_arn,
    )
    current_item_before_lock = _checkpoint_get_current_item(
        ddb_client,
        table_name=table_name,
        partition_value=partition_value,
        reason="load checkpoint CURRENT record before acquiring execution lock",
    )
    active_lock_before_put = _checkpoint_get_lock_item(
        ddb_client,
        table_name=table_name,
        partition_value=partition_value,
        reason="load checkpoint LOCK record before acquiring execution lock",
    )
    _log_event(
        "checkpoint.lock.preflight_consulted",
        target_table_name=target_table_name,
        target_table_arn=target_table_arn,
        partition_value=partition_value,
        checkpoint_record_exists=bool(current_item_before_lock),
        active_lock_exists=bool(active_lock_before_put),
        level=logging.INFO,
    )
    observed_at = _now_utc()
    expires_at = observed_at + timedelta(seconds=CHECKPOINT_DYNAMODB_LOCK_TIMEOUT_SECONDS)
    observed_at_iso = _dt_to_iso(observed_at)
    expires_at_iso = _dt_to_iso(expires_at)
    lock_item = {
        CHECKPOINT_DYNAMODB_PARTITION_KEY: partition_value,
        CHECKPOINT_DYNAMODB_SORT_KEY: CHECKPOINT_DYNAMODB_LOCK_RECORD,
        "TargetTableName": _safe_str_field(target_table_name, field_name="target_table_name"),
        "TableArn": _safe_str_field(target_table_arn, field_name="target_table_arn", required=False),
        "Mode": _safe_str_field(mode_hint, field_name="mode_hint", required=False).upper(),
        "ObservedAt": observed_at_iso,
        CHECKPOINT_DYNAMODB_LOCK_OWNER_ATTR: owner_token,
        CHECKPOINT_DYNAMODB_LOCK_EXPIRES_AT_ATTR: expires_at_iso,
    }

    try:
        _checkpoint_put_item(
            ddb_client,
            table_name=table_name,
            reason="lock checkpoint table for concurrency control",
            item=_ddb_encode_item(lock_item),
            TableName=table_name,
            Item=_ddb_encode_item(lock_item),
            ConditionExpression=(
                "attribute_not_exists(#pk) OR "
                "attribute_not_exists(#expires_at) OR "
                "#expires_at < :now OR "
                "#owner_token = :owner_token"
            ),
            ExpressionAttributeNames={
                "#pk": CHECKPOINT_DYNAMODB_PARTITION_KEY,
                "#expires_at": CHECKPOINT_DYNAMODB_LOCK_EXPIRES_AT_ATTR,
                "#owner_token": CHECKPOINT_DYNAMODB_LOCK_OWNER_ATTR,
            },
            ExpressionAttributeValues={
                ":now": {"S": observed_at_iso},
                ":owner_token": {"S": owner_token},
            },
        )
        _log_event(
            "checkpoint.lock.acquired",
            target_table_name=target_table_name,
            target_table_arn=target_table_arn,
            owner_token=owner_token,
            lock_expires_at=expires_at_iso,
            level=logging.INFO,
        )
        return {
            "acquired": True,
            "owner_token": owner_token,
            "observed_at": observed_at_iso,
            "expires_at": expires_at_iso,
        }
    except ClientError as exc:
        if _client_error_code(exc) != "ConditionalCheckFailedException":
            raise

    active_lock = _decode_checkpoint_execution_lock(
        _checkpoint_get_lock_item(
            ddb_client,
            table_name=table_name,
            partition_value=partition_value,
            reason="reload checkpoint LOCK record after lock contention",
        )
    )
    _log_event(
        "checkpoint.lock.busy",
        target_table_name=target_table_name,
        target_table_arn=target_table_arn,
        owner_token=owner_token,
        current_owner_token=_safe_str_field(active_lock.get("owner_token"), field_name="lock.owner_token", required=False),
        current_lock_expires_at=_safe_str_field(active_lock.get("expires_at"), field_name="lock.expires_at", required=False),
        level=logging.INFO,
    )
    return {
        "acquired": False,
        "owner_token": owner_token,
        "expires_at": _safe_str_field(active_lock.get("expires_at"), field_name="lock.expires_at", required=False),
        "lock_state": active_lock,
    }


def _checkpoint_release_execution_lock(
    store: Dict[str, Any],
    *,
    target_table_name: str,
    target_table_arn: Optional[str],
    owner_token: str,
) -> bool:
    if not _checkpoint_supports_execution_lock(store):
        return True

    table_name = _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name")
    ddb_client = store.get("ddb")
    if ddb_client is None:
        raise RuntimeError("checkpoint_store.ddb ausente")

    partition_value = _checkpoint_resolve_partition_value(
        target_table_name=target_table_name,
        target_table_arn=target_table_arn,
    )
    lock_key = {
        CHECKPOINT_DYNAMODB_PARTITION_KEY: {"S": partition_value},
        CHECKPOINT_DYNAMODB_SORT_KEY: {"S": CHECKPOINT_DYNAMODB_LOCK_RECORD},
    }
    try:
        _checkpoint_delete_item(
            ddb_client,
            table_name=table_name,
            reason="release checkpoint execution lock after table processing",
            key=lock_key,
            TableName=table_name,
            Key=lock_key,
            ConditionExpression="#owner_token = :owner_token",
            ExpressionAttributeNames={
                "#owner_token": CHECKPOINT_DYNAMODB_LOCK_OWNER_ATTR,
            },
            ExpressionAttributeValues={
                ":owner_token": {"S": owner_token},
            },
        )
        _log_event(
            "checkpoint.lock.released",
            target_table_name=target_table_name,
            target_table_arn=target_table_arn,
            owner_token=owner_token,
            level=logging.INFO,
        )
        return True
    except ClientError as exc:
        if _client_error_code(exc) != "ConditionalCheckFailedException":
            raise
        _log_event(
            "checkpoint.lock.release_skipped",
            target_table_name=target_table_name,
            target_table_arn=target_table_arn,
            owner_token=owner_token,
            level=logging.WARNING,
        )
        return False


def checkpoint_load_table_state(
    store: Dict[str, Any],
    *,
    target_table_name: str,
    target_table_arn: Optional[str] = None,
) -> Dict[str, Any]:
    state, _diagnostics = _checkpoint_load_table_state_with_diagnostics(
        store,
        target_table_name=target_table_name,
        target_table_arn=target_table_arn,
    )
    return state


def _build_checkpoint_lookup_attempt(*, lookup_key: str, partition_value: str, item_found: bool) -> Dict[str, Any]:
    return {
        "lookup_key": lookup_key,
        "partition_value": partition_value,
        "item_found": item_found,
    }


def _evaluate_checkpoint_last_mode_field(value: Any) -> Dict[str, Any]:
    raw_value = _safe_str_field(value, field_name="checkpoint.LastMode", required=False)
    normalized_value = raw_value.upper()
    if not raw_value:
        return {
            "raw_value": "",
            "normalized_value": "",
            "is_present": False,
            "is_valid": False,
            "reason": "missing",
        }
    if normalized_value not in {"FULL", "INCREMENTAL"}:
        return {
            "raw_value": raw_value,
            "normalized_value": normalized_value,
            "is_present": True,
            "is_valid": False,
            "reason": "unsupported_value",
        }
    return {
        "raw_value": raw_value,
        "normalized_value": normalized_value,
        "is_present": True,
        "is_valid": True,
        "reason": "ok",
    }


def _evaluate_checkpoint_timestamp_field(value: Any, *, field_name: str, required: bool) -> Dict[str, Any]:
    raw_value = _safe_str_field(value, field_name=field_name, required=False)
    if not raw_value:
        return {
            "raw_value": "",
            "normalized_value": "",
            "is_present": False,
            "is_valid": not required,
            "format": "",
            "reason": "missing" if required else "missing_optional",
        }
    parsed_iso_value = _parse_iso_datetime(raw_value)
    if isinstance(parsed_iso_value, datetime):
        return {
            "raw_value": raw_value,
            "normalized_value": _dt_to_iso(parsed_iso_value),
            "is_present": True,
            "is_valid": True,
            "format": "iso8601",
            "reason": "ok",
        }
    recovered_value = _coerce_checkpoint_timestamp_to_utc(raw_value)
    if isinstance(recovered_value, datetime):
        return {
            "raw_value": raw_value,
            "normalized_value": _dt_to_iso(recovered_value),
            "is_present": True,
            "is_valid": True,
            "format": "legacy_recovered",
            "reason": "legacy_format_recovered",
        }
    return {
        "raw_value": raw_value,
        "normalized_value": "",
        "is_present": True,
        "is_valid": False,
        "format": "",
        "reason": "invalid_timestamp",
    }


def _evaluate_checkpoint_int_field(
    value: Any,
    *,
    field_name: str,
    default_when_missing: Optional[int],
    optional: bool,
) -> Dict[str, Any]:
    raw_value = _safe_str_field(value, field_name=field_name, required=False)
    if not raw_value:
        return {
            "raw_value": "",
            "normalized_value": None if optional else default_when_missing,
            "is_present": False,
            "is_valid": True,
            "reason": "missing_optional" if optional else "missing_default_applied",
        }
    try:
        normalized_value = (
            _coerce_optional_non_negative_int(value, field_name=field_name)
            if optional
            else _coerce_non_negative_int(value, field_name=field_name, default=default_when_missing or 0)
        )
    except ValueError:
        return {
            "raw_value": raw_value,
            "normalized_value": None if optional else default_when_missing,
            "is_present": True,
            "is_valid": False,
            "reason": "invalid_integer",
        }
    return {
        "raw_value": raw_value,
        "normalized_value": normalized_value,
        "is_present": True,
        "is_valid": True,
        "reason": "ok",
    }


def _build_checkpoint_field_integrity(
    decoded_item: Dict[str, Any],
    *,
    requested_table_name: str,
    requested_table_arn: str,
) -> Dict[str, Any]:
    raw_partition_value = _safe_str_field(
        decoded_item.get(CHECKPOINT_DYNAMODB_PARTITION_KEY),
        field_name=f"checkpoint.{CHECKPOINT_DYNAMODB_PARTITION_KEY}",
        required=False,
    )
    stored_target_table_name = _safe_str_field(decoded_item.get("TargetTableName"), field_name="checkpoint.TargetTableName", required=False)
    resolved_table_name = stored_target_table_name or ("" if raw_partition_value.startswith("arn:") else raw_partition_value)
    stored_table_arn = _safe_str_field(decoded_item.get("TableArn"), field_name="checkpoint.TableArn", required=False)
    raw_pending_exports = decoded_item.get("PendingExports")
    normalized_pending_exports = _normalize_pending_exports(raw_pending_exports)
    pending_entries_filtered = isinstance(raw_pending_exports, list) and len(normalized_pending_exports) != len(raw_pending_exports)
    pending_string_rejected = (
        isinstance(raw_pending_exports, str)
        and bool(raw_pending_exports.strip())
        and not normalized_pending_exports
    )
    if pending_entries_filtered:
        pending_reason = "entries_filtered_during_normalization"
    elif pending_string_rejected:
        pending_reason = "string_payload_rejected_or_empty_after_normalization"
    elif raw_pending_exports is None:
        pending_reason = "missing_optional"
    elif isinstance(raw_pending_exports, (list, str)):
        pending_reason = "ok"
    else:
        pending_reason = "unsupported_type"

    return {
        "target_table_name": {
            "stored_value": stored_target_table_name,
            "resolved_value": resolved_table_name,
            "source": "TargetTableName" if stored_target_table_name else ("partition_key" if resolved_table_name else "missing"),
            "is_present": bool(resolved_table_name),
            "matches_requested": resolved_table_name.lower() == requested_table_name.lower() if resolved_table_name else False,
        },
        "table_arn": {
            "stored_value": stored_table_arn,
            "is_present": bool(stored_table_arn),
            "matches_requested": stored_table_arn == requested_table_arn if stored_table_arn and requested_table_arn else None,
            "reason": "ok" if stored_table_arn else "missing_optional",
        },
        "last_mode": _evaluate_checkpoint_last_mode_field(decoded_item.get("LastMode")),
        "last_to": _evaluate_checkpoint_timestamp_field(
            decoded_item.get("LastTo"),
            field_name="checkpoint.LastTo",
            required=True,
        ),
        "table_created_at": _evaluate_checkpoint_timestamp_field(
            decoded_item.get("TableCreatedAt"),
            field_name="checkpoint.TableCreatedAt",
            required=False,
        ),
        "incremental_seq": _evaluate_checkpoint_int_field(
            decoded_item.get("IncrementalSeq"),
            field_name="checkpoint.IncrementalSeq",
            default_when_missing=0,
            optional=False,
        ),
        "last_export_item_count": _evaluate_checkpoint_int_field(
            decoded_item.get("LastExportItemCount"),
            field_name="checkpoint.LastExportItemCount",
            default_when_missing=None,
            optional=True,
        ),
        "pending_exports": {
            "raw_type": type(raw_pending_exports).__name__ if raw_pending_exports is not None else "missing",
            "raw_count": len(raw_pending_exports) if isinstance(raw_pending_exports, list) else None,
            "normalized_count": len(normalized_pending_exports),
            "is_valid": pending_reason in {"ok", "missing_optional"},
            "reason": pending_reason,
        },
    }


def _checkpoint_load_table_state_with_diagnostics(
    store: Dict[str, Any],
    *,
    target_table_name: str,
    target_table_arn: Optional[str] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    table_name = _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name")
    ddb_client = store.get("ddb")
    if ddb_client is None:
        raise RuntimeError("checkpoint_store.ddb ausente")

    requested_table_name = _safe_str_field(target_table_name, field_name="target_table_name")
    requested_table_arn = _safe_str_field(target_table_arn, field_name="target_table_arn", required=False)
    diagnostics: Dict[str, Any] = {
        "requested_table_name": requested_table_name,
        "requested_table_arn": requested_table_arn,
        "record_found": False,
        "lookup_key": "",
        "lookup_attempts": [],
        "used_legacy_partition": False,
        "rejected": False,
        "rejected_reason": "",
        "notes": [],
        "field_integrity": {},
    }

    item: Dict[str, Any] = {}
    decoded: Optional[Dict[str, Any]] = None
    if requested_table_arn:
        item = _checkpoint_get_current_item(
            ddb_client,
            table_name=table_name,
            partition_value=requested_table_arn,
        )
        diagnostics["lookup_attempts"].append(
            _build_checkpoint_lookup_attempt(
                lookup_key="arn",
                partition_value=requested_table_arn,
                item_found=bool(item),
            )
        )
        if item:
            decoded = _ddb_decode_item(item)
            diagnostics["record_found"] = True
            diagnostics["lookup_key"] = "arn"
            _log_event(
                "checkpoint.load.record.found",
                lookup_key="arn",
                target_table_name=requested_table_name,
                target_table_arn=requested_table_arn,
                level=logging.INFO,
            )

    if not item:
        item = _checkpoint_get_current_item(
            ddb_client,
            table_name=table_name,
            partition_value=requested_table_name,
        )
        diagnostics["lookup_attempts"].append(
            _build_checkpoint_lookup_attempt(
                lookup_key="table_name",
                partition_value=requested_table_name,
                item_found=bool(item),
            )
        )
        if item and requested_table_arn:
            diagnostics["record_found"] = True
            diagnostics["lookup_key"] = "table_name"
            diagnostics["used_legacy_partition"] = True
            decoded_legacy = _ddb_decode_item(item)
            legacy_table_arn = _safe_str_field(decoded_legacy.get("TableArn"), field_name="checkpoint.TableArn", required=False)
            legacy_target_name = (
                _safe_str_field(decoded_legacy.get("TargetTableName"), field_name="checkpoint.TargetTableName", required=False)
                or _safe_str_field(decoded_legacy.get("TableName"), field_name="checkpoint.TableName", required=False)
            )
            if legacy_target_name and legacy_target_name.lower() != requested_table_name.lower():
                diagnostics["rejected"] = True
                diagnostics["rejected_reason"] = "legacy_partition_table_name_mismatch"
                diagnostics["notes"].append("Registro legado encontrado por nome, mas o TargetTableName não corresponde à tabela solicitada.")
                _log_event(
                    "checkpoint.load.legacy_partition_name_mismatch",
                    requested_table_name=requested_table_name,
                    requested_table_arn=requested_table_arn,
                    legacy_table_name=legacy_target_name,
                    level=logging.WARNING,
                )
                return {}, diagnostics
            if legacy_table_arn and legacy_table_arn != requested_table_arn:
                diagnostics["rejected"] = True
                diagnostics["rejected_reason"] = "legacy_partition_table_arn_mismatch"
                diagnostics["notes"].append("Registro legado encontrado por nome, mas o TableArn aponta para outra tabela.")
                _log_event(
                    "checkpoint.load.legacy_partition_mismatch",
                    requested_table_name=requested_table_name,
                    requested_table_arn=requested_table_arn,
                    legacy_table_arn=legacy_table_arn,
                    level=logging.WARNING,
                )
                return {}, diagnostics
            if not legacy_table_arn and requested_table_arn:
                diagnostics["notes"].append("Registro legado recuperado sem TableArn persistido; a identidade foi validada apenas pelo nome da tabela.")
                _log_event(
                    "checkpoint.load.legacy_partition_missing_table_arn_recovered_by_request",
                    requested_table_name=requested_table_name,
                    requested_table_arn=requested_table_arn,
                    level=logging.WARNING,
                )
            _log_event(
                "checkpoint.load.legacy_partition_recovered",
                requested_table_name=requested_table_name,
                requested_table_arn=requested_table_arn,
                legacy_table_name=legacy_target_name,
                level=logging.INFO,
            )
            if not legacy_table_arn:
                _log_event(
                    "checkpoint.load.legacy_partition_missing_table_arn",
                    requested_table_name=requested_table_name,
                    requested_table_arn=requested_table_arn,
                    level=logging.WARNING,
                )
            decoded = decoded_legacy
            _log_event(
                "checkpoint.load.record.found",
                lookup_key="table_name",
                target_table_name=requested_table_name,
                target_table_arn=requested_table_arn,
                level=logging.INFO,
            )
        elif item:
            decoded = _ddb_decode_item(item)
            diagnostics["record_found"] = True
            diagnostics["lookup_key"] = "table_name"
            _log_event(
                "checkpoint.load.record.found",
                lookup_key="table_name",
                target_table_name=requested_table_name,
                target_table_arn=requested_table_arn,
                level=logging.INFO,
            )

    if not item:
        _log_event(
            "checkpoint.load.record.missing",
            target_table_name=requested_table_name,
            target_table_arn=requested_table_arn,
            level=logging.INFO,
        )
        diagnostics["notes"].append("Nenhum registro CURRENT foi encontrado para a tabela solicitada.")
        return {}, diagnostics

    if decoded is None:
        decoded = _ddb_decode_item(item)
    diagnostics["field_integrity"] = _build_checkpoint_field_integrity(
        decoded,
        requested_table_name=requested_table_name,
        requested_table_arn=requested_table_arn,
    )
    normalized_table_name = (
        _safe_str_field(
            decoded.get("TargetTableName"),
            field_name="checkpoint.TargetTableName",
            required=False,
        )
        or _safe_str_field(decoded.get("TableName"), field_name="checkpoint.TableName", required=False)
        or requested_table_name
    )
    normalized_table_arn = _safe_str_field(decoded.get("TableArn"), field_name="checkpoint.TableArn", required=False)
    pending_exports = _normalize_pending_exports(decoded.get("PendingExports"))
    state = {
        "table_name": normalized_table_name,
        "table_arn": normalized_table_arn,
        "table_created_at": _safe_str_field(decoded.get("TableCreatedAt"), field_name="checkpoint.TableCreatedAt", required=False),
        "last_to": _safe_str_field(decoded.get("LastTo"), field_name="checkpoint.LastTo", required=False),
        "last_mode": _safe_str_field(decoded.get("LastMode"), field_name="checkpoint.LastMode", required=False),
        "source": _safe_str_field(decoded.get("Source"), field_name="checkpoint.Source", required=False),
        "last_export_arn": _safe_str_field(decoded.get("LastExportArn"), field_name="checkpoint.LastExportArn", required=False),
        "last_export_item_count": _safe_coerce_optional_non_negative_int(
            decoded.get("LastExportItemCount"),
            field_name="checkpoint.LastExportItemCount",
            table_name=normalized_table_name,
            table_arn=normalized_table_arn or _safe_str_field(requested_table_arn, field_name="target_table_arn", required=False),
        ),
        "pending_exports": pending_exports,
        "incremental_seq": _safe_coerce_non_negative_int(
            decoded.get("IncrementalSeq"),
            field_name="checkpoint.IncrementalSeq",
            default=0,
            table_name=normalized_table_name,
            table_arn=normalized_table_arn or _safe_str_field(requested_table_arn, field_name="target_table_arn", required=False),
        ),
    }
    filtered_state = {
        key: value
        for key, value in state.items()
        if value not in (None, "") or key in {"pending_exports", "incremental_seq"}
    }
    diagnostics["resolved_state"] = filtered_state
    return filtered_state, diagnostics


def _marshal_dynamodb_item(python_item: Dict[str, Any]) -> Dict[str, Any]:
    return _ddb_encode_item(python_item)


def _deserialize_dynamodb_value(value: Any) -> Any:
    if not isinstance(value, dict) or len(value) != 1:
        return value
    if "S" in value:
        return value["S"]
    if "N" in value:
        number_text = _safe_str_field(value.get("N"), field_name="dynamodb.N")
        if re.fullmatch(r"-?\d+", number_text):
            return int(number_text)
        try:
            return float(number_text)
        except ValueError:
            return number_text
    if "BOOL" in value:
        return bool(value["BOOL"])
    if "NULL" in value:
        return None
    if "L" in value:
        raw_items = value.get("L")
        if not isinstance(raw_items, list):
            return []
        return [_deserialize_dynamodb_value(item) for item in raw_items]
    if "M" in value:
        raw_mapping = value.get("M")
        if not isinstance(raw_mapping, dict):
            return {}
        return {
            item_key: _deserialize_dynamodb_value(item_value)
            for item_key, item_value in raw_mapping.items()
        }
    if "SS" in value:
        raw_items = value.get("SS")
        return list(raw_items) if isinstance(raw_items, list) else []
    if "NS" in value:
        raw_items = value.get("NS")
        if not isinstance(raw_items, list):
            return []
        return [
            _deserialize_dynamodb_value({"N": item})
            for item in raw_items
        ]
    return value


def _deserialize_dynamodb_item(ddb_item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: _deserialize_dynamodb_value(value)
        for key, value in ddb_item.items()
    }


def _empty_checkpoint_payload(*, version: int = 1) -> Dict[str, Any]:
    return {
        "version": version,
        "updated_at": _dt_to_iso(_now_utc()),
        "tables": {},
    }


def _compact_legacy_checkpoint_state(state: Dict[str, Any], *, state_key: str) -> Dict[str, Any]:
    normalized = _normalize_checkpoint_state(
        state,
        table_name=_safe_str_field(state.get("table_name"), field_name="state.table_name", required=False) or state_key,
        table_arn=_safe_str_field(state.get("table_arn"), field_name="state.table_arn", required=False) or state_key,
    )
    history = state.get("history") if isinstance(state.get("history"), list) else []
    compact: Dict[str, Any] = {**normalized}
    compact_history = [item for item in history if isinstance(item, dict)]
    if compact_history:
        compact["history"] = compact_history
    return {
        key: value
        for key, value in compact.items()
        if value not in (None, "")
        and not (key == "pending_exports" and not value)
        and not (key == "incremental_seq" and value == 0)
        and not (key == "history" and not value)
    }


def _build_checkpoint_history_event(state: Dict[str, Any]) -> Dict[str, Any]:
    pending_exports = _normalize_pending_exports(state.get("pending_exports"))
    return {
        "table_name": _safe_str_field(state.get("table_name"), field_name="state.table_name", required=False),
        "table_arn": _safe_str_field(state.get("table_arn"), field_name="state.table_arn", required=False),
        "last_to": _safe_str_field(state.get("last_to"), field_name="state.last_to", required=False),
        "last_mode": _safe_str_field(state.get("last_mode"), field_name="state.last_mode", required=False),
        "source": _safe_str_field(state.get("source"), field_name="state.source", required=False),
        "pending_exports": pending_exports,
    }


def _merge_checkpoint_histories(existing: Any, candidate: Any) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for collection in (existing, candidate):
        if not isinstance(collection, list):
            continue
        for item in collection:
            if not isinstance(item, dict):
                continue
            key = json.dumps(_to_json_safe(item), sort_keys=True, ensure_ascii=False, default=str)
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
    return merged


def _merge_legacy_checkpoint_states(existing: Dict[str, Any], candidate: Dict[str, Any], *, state_key: str) -> Dict[str, Any]:
    existing_state = _compact_legacy_checkpoint_state(existing, state_key=state_key) if isinstance(existing, dict) else {}
    candidate_state = _compact_legacy_checkpoint_state(candidate, state_key=state_key)
    merged_pending_exports = _dedupe_pending_exports(
        [
            *(_normalize_pending_exports(existing_state.get("pending_exports"))),
            *(_normalize_pending_exports(candidate_state.get("pending_exports"))),
        ]
    )
    merged_history = _merge_checkpoint_histories(
        existing_state.get("history"),
        candidate_state.get("history"),
    )
    if not merged_history:
        synthesized_history: List[Dict[str, Any]] = []
        if _normalize_pending_exports(existing_state.get("pending_exports")):
            synthesized_history.append(_build_checkpoint_history_event(existing_state))
        if _normalize_pending_exports(candidate_state.get("pending_exports")):
            synthesized_history.append(_build_checkpoint_history_event(candidate_state))
        merged_history = _merge_checkpoint_histories([], synthesized_history)
    merged = {
        "table_name": _safe_str_field(candidate_state.get("table_name"), field_name="state.table_name", required=False)
        or _safe_str_field(existing_state.get("table_name"), field_name="state.table_name", required=False)
        or state_key,
        "table_arn": _safe_str_field(candidate_state.get("table_arn"), field_name="state.table_arn", required=False)
        or _safe_str_field(existing_state.get("table_arn"), field_name="state.table_arn", required=False),
        "table_created_at": _safe_str_field(candidate_state.get("table_created_at"), field_name="state.table_created_at", required=False)
        or _safe_str_field(existing_state.get("table_created_at"), field_name="state.table_created_at", required=False),
        "last_to": _safe_str_field(candidate_state.get("last_to"), field_name="state.last_to", required=False)
        or _safe_str_field(existing_state.get("last_to"), field_name="state.last_to", required=False),
        "last_mode": _safe_str_field(candidate_state.get("last_mode"), field_name="state.last_mode", required=False)
        or _safe_str_field(existing_state.get("last_mode"), field_name="state.last_mode", required=False),
        "source": _safe_str_field(candidate_state.get("source"), field_name="state.source", required=False)
        or _safe_str_field(existing_state.get("source"), field_name="state.source", required=False),
        "last_export_arn": _safe_str_field(candidate_state.get("last_export_arn"), field_name="state.last_export_arn", required=False)
        or _safe_str_field(existing_state.get("last_export_arn"), field_name="state.last_export_arn", required=False),
        "last_export_item_count": candidate_state.get("last_export_item_count", existing_state.get("last_export_item_count")),
        "pending_exports": merged_pending_exports,
        "incremental_seq": max(
            _coerce_non_negative_int(existing_state.get("incremental_seq"), field_name="state.incremental_seq", default=0),
            _coerce_non_negative_int(candidate_state.get("incremental_seq"), field_name="state.incremental_seq", default=0),
        ),
    }
    if merged_history:
        merged["history"] = merged_history
    if merged_pending_exports and not _safe_str_field(candidate_state.get("last_to"), field_name="state.last_to", required=False):
        merged["last_to"] = _safe_str_field(existing_state.get("last_to"), field_name="state.last_to", required=False)
        merged["last_mode"] = _safe_str_field(existing_state.get("last_mode"), field_name="state.last_mode", required=False)
        merged["source"] = _safe_str_field(existing_state.get("source"), field_name="state.source", required=False)
    return _compact_legacy_checkpoint_state(merged, state_key=state_key)


def _checkpoint_load_legacy_dynamodb(store: Dict[str, Any]) -> Dict[str, Any]:
    ddb_client = store.get("ddb")
    if ddb_client is None:
        raise RuntimeError("checkpoint_store.ddb ausente")
    table_name = _safe_str_field(store.get("table_name"), field_name="checkpoint_store.table_name")
    response = _checkpoint_scan_table(
        ddb_client,
        table_name=table_name,
        reason="scan checkpoint table to rebuild legacy checkpoint payload",
    )
    items = response.get("Items") if isinstance(response, dict) else None
    tables: Dict[str, Any] = {}
    updated_at = ""
    for raw_item in items or []:
        if not isinstance(raw_item, dict):
            continue
        item = _deserialize_dynamodb_item(raw_item)
        if _safe_str_field(item.get("RecordType"), field_name="RecordType", required=False) != CHECKPOINT_DYNAMODB_CURRENT_RECORD:
            continue
        state_key = _safe_str_field(item.get("StateKey"), field_name="StateKey", required=False) or _safe_str_field(
            item.get("TableArn"),
            field_name="TableArn",
            required=False,
        ) or _safe_str_field(item.get("TableName"), field_name="TableName")
        tables[state_key] = _compact_legacy_checkpoint_state(
            {
                "table_name": _safe_str_field(item.get("TargetTableName"), field_name="TargetTableName", required=False)
                or _safe_str_field(item.get("TableName"), field_name="TableName", required=False),
                "table_arn": _safe_str_field(item.get("TableArn"), field_name="TableArn", required=False),
                "table_created_at": _safe_str_field(item.get("TableCreatedAt"), field_name="TableCreatedAt", required=False),
                "last_to": _safe_str_field(item.get("LastTo"), field_name="LastTo", required=False),
                "last_mode": _safe_str_field(item.get("LastMode"), field_name="LastMode", required=False),
                "source": _safe_str_field(item.get("Source"), field_name="Source", required=False),
                "last_export_arn": _safe_str_field(item.get("LastExportArn"), field_name="LastExportArn", required=False),
                "last_export_item_count": item.get("LastExportItemCount"),
                "pending_exports": _normalize_pending_exports(item.get("PendingExports")),
                "incremental_seq": item.get("IncrementalSeq"),
            },
            state_key=state_key,
        )
        item_updated_at = _safe_str_field(item.get("UpdatedAt"), field_name="UpdatedAt", required=False)
        if item_updated_at and item_updated_at > updated_at:
            updated_at = item_updated_at
    payload = _empty_checkpoint_payload(version=2)
    payload["tables"] = tables
    payload["updated_at"] = updated_at or payload["updated_at"]
    return payload


def _checkpoint_load_legacy_s3(store: Dict[str, Any]) -> Dict[str, Any]:
    s3_client = store.get("s3")
    if s3_client is None:
        raise RuntimeError("checkpoint_store.s3 ausente")
    bucket = _safe_str_field(store.get("bucket"), field_name="checkpoint_store.bucket")
    key = _safe_str_field(store.get("key"), field_name="checkpoint_store.key")
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except KeyError:
        return _empty_checkpoint_payload()
    except ClientError as exc:
        if _client_error_code(exc) in AWS_NOT_FOUND_ERROR_CODES:
            return _empty_checkpoint_payload()
        raise
    body = response.get("Body")
    if body is None:
        return _empty_checkpoint_payload()
    payload = json.loads(body.read().decode("utf-8"))
    if not isinstance(payload, dict):
        return _empty_checkpoint_payload()
    tables = payload.get("tables") if isinstance(payload.get("tables"), dict) else {}
    return {
        "version": payload.get("version", 1),
        "updated_at": payload.get("updated_at") or _dt_to_iso(_now_utc()),
        "tables": tables,
    }


def checkpoint_load(store: Dict[str, Any]) -> Dict[str, Any]:
    backend = _safe_str_field(store.get("backend"), field_name="checkpoint_store.backend", required=False).lower()
    if backend == "dynamodb":
        return _checkpoint_load_legacy_dynamodb(store)
    if backend == "s3" or store.get("s3") is not None:
        return _checkpoint_load_legacy_s3(store)
    return _empty_checkpoint_payload()


def _build_checkpoint_history_s3_key(key: str, observed_at: str) -> str:
    if "/" in key:
        prefix, file_name = key.rsplit("/", 1)
    else:
        prefix, file_name = "", key
    stem = file_name[:-5] if file_name.endswith(".json") else file_name
    history_prefix = f"{prefix}/{stem}.history" if prefix else f"{stem}.history"
    return f"{history_prefix}/{observed_at}.json"


def _resolve_checkpoint_revision_write_plan(
    *,
    checkpoint_table_name: str,
    partition_value: str,
    record_type: str,
    current_item_exists: bool,
    raw_revision: Any,
) -> Dict[str, Any]:
    if not current_item_exists:
        return {
            "current_revision": 0,
            "next_revision": 1,
            "condition_expression": "attribute_not_exists(#pk) AND attribute_not_exists(#sk)",
            "expression_attribute_names": {
                "#pk": CHECKPOINT_DYNAMODB_PARTITION_KEY,
                "#sk": CHECKPOINT_DYNAMODB_SORT_KEY,
            },
            "expression_attribute_values": None,
            "revision_integrity": "new_item",
        }

    try:
        current_revision = _coerce_non_negative_int(
            raw_revision,
            field_name=f"checkpoint.{CHECKPOINT_DYNAMODB_REVISION_ATTR}",
            default=0,
        )
    except ValueError:
        _log_event(
            "checkpoint.state.invalid_revision",
            checkpoint_table_name=checkpoint_table_name,
            partition_value=partition_value,
            record_type=record_type,
            raw_revision=_safe_str_field(raw_revision, field_name="checkpoint.Revision", required=False),
            resolution="overwrite_existing_item_and_reset_revision",
            level=logging.WARNING,
        )
        return {
            "current_revision": 0,
            "next_revision": 1,
            "condition_expression": "attribute_exists(#pk) AND attribute_exists(#sk)",
            "expression_attribute_names": {
                "#pk": CHECKPOINT_DYNAMODB_PARTITION_KEY,
                "#sk": CHECKPOINT_DYNAMODB_SORT_KEY,
            },
            "expression_attribute_values": None,
            "revision_integrity": "invalid_existing_item",
        }

    if current_revision > 0:
        return {
            "current_revision": current_revision,
            "next_revision": current_revision + 1,
            "condition_expression": "#revision = :expected_revision",
            "expression_attribute_names": {
                "#revision": CHECKPOINT_DYNAMODB_REVISION_ATTR,
            },
            "expression_attribute_values": {
                ":expected_revision": {"N": str(current_revision)},
            },
            "revision_integrity": "valid_existing_item",
        }

    return {
        "current_revision": 0,
        "next_revision": 1,
        "condition_expression": "attribute_not_exists(#revision)",
        "expression_attribute_names": {
            "#revision": CHECKPOINT_DYNAMODB_REVISION_ATTR,
        },
        "expression_attribute_values": None,
        "revision_integrity": "missing_revision",
    }


def _build_legacy_checkpoint_snapshot_item(state: Dict[str, Any], *, state_key: str, observed_at: str, salt: str) -> Dict[str, Any]:
    table_name = _safe_str_field(state.get("table_name"), field_name="state.table_name")
    event_id = _build_client_token(state_key, observed_at, salt)
    return {
        "TableName": table_name,
        "RecordType": f"SNAPSHOT#{observed_at}#{event_id}",
        "StateKey": state_key,
        "EventId": event_id,
        "ObservedAt": observed_at,
        "TargetTableName": table_name,
        "TableArn": _safe_str_field(state.get("table_arn"), field_name="state.table_arn", required=False),
        "LastTo": _safe_str_field(state.get("last_to"), field_name="state.last_to", required=False),
        "LastMode": _safe_str_field(state.get("last_mode"), field_name="state.last_mode", required=False),
        "Source": _safe_str_field(state.get("source"), field_name="state.source", required=False),
        "PendingExports": _normalize_pending_exports(state.get("pending_exports")),
        "IncrementalSeq": _coerce_non_negative_int(state.get("incremental_seq"), field_name="state.incremental_seq", default=0),
    }


def _build_legacy_checkpoint_current_item(state: Dict[str, Any], *, state_key: str, observed_at: str, revision: int) -> Dict[str, Any]:
    return {
        "TableName": _safe_str_field(state.get("table_name"), field_name="state.table_name"),
        "RecordType": CHECKPOINT_DYNAMODB_CURRENT_RECORD,
        "StateKey": state_key,
        "Revision": revision,
        "TargetTableName": _safe_str_field(state.get("table_name"), field_name="state.table_name"),
        "TableArn": _safe_str_field(state.get("table_arn"), field_name="state.table_arn", required=False),
        "TableCreatedAt": _safe_str_field(state.get("table_created_at"), field_name="state.table_created_at", required=False),
        "LastTo": _safe_str_field(state.get("last_to"), field_name="state.last_to", required=False),
        "LastMode": _safe_str_field(state.get("last_mode"), field_name="state.last_mode", required=False),
        "Source": _safe_str_field(state.get("source"), field_name="state.source", required=False),
        "LastExportArn": _safe_str_field(state.get("last_export_arn"), field_name="state.last_export_arn", required=False),
        "LastExportItemCount": state.get("last_export_item_count"),
        "PendingExports": _normalize_pending_exports(state.get("pending_exports")),
        "IncrementalSeq": _coerce_non_negative_int(state.get("incremental_seq"), field_name="state.incremental_seq", default=0),
        "UpdatedAt": observed_at,
    }


def _checkpoint_save_legacy_dynamodb(store: Dict[str, Any], payload: Dict[str, Any]) -> None:
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
    for state_key, raw_state in tables.items():
        if not isinstance(raw_state, dict):
            continue
        candidate_state = _compact_legacy_checkpoint_state(raw_state, state_key=state_key)
        snapshot_salt = json.dumps(
            _to_json_safe(candidate_state),
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        snapshot_item = _build_legacy_checkpoint_snapshot_item(
            candidate_state,
            state_key=state_key,
            observed_at=observed_at,
            salt=snapshot_salt,
        )
        current_key = {
            CHECKPOINT_DYNAMODB_PARTITION_KEY: {"S": _safe_str_field(candidate_state.get("table_name"), field_name="state.table_name")},
            CHECKPOINT_DYNAMODB_SORT_KEY: {"S": CHECKPOINT_DYNAMODB_CURRENT_RECORD},
        }

        for attempt in range(2):
            current_response = _checkpoint_execute_dynamodb_command(
                ddb_client,
                command="GetItem",
                method_name="get_item",
                table_name=table_name,
                reason="load legacy checkpoint CURRENT record before merge",
                partition_value=_safe_str_field(candidate_state.get("table_name"), field_name="state.table_name"),
                record_type=CHECKPOINT_DYNAMODB_CURRENT_RECORD,
                attempt=attempt + 1,
                TableName=table_name,
                Key=current_key,
            )
            current_item = current_response.get("Item") if isinstance(current_response, dict) else None
            current_payload = _deserialize_dynamodb_item(current_item) if isinstance(current_item, dict) and current_item else {}
            current_state = _compact_legacy_checkpoint_state(
                {
                    "table_name": _safe_str_field(current_payload.get("TargetTableName"), field_name="TargetTableName", required=False)
                    or _safe_str_field(current_payload.get("TableName"), field_name="TableName", required=False)
                    or candidate_state["table_name"],
                    "table_arn": _safe_str_field(current_payload.get("TableArn"), field_name="TableArn", required=False)
                    or candidate_state.get("table_arn"),
                    "table_created_at": _safe_str_field(current_payload.get("TableCreatedAt"), field_name="TableCreatedAt", required=False),
                    "last_to": _safe_str_field(current_payload.get("LastTo"), field_name="LastTo", required=False),
                    "last_mode": _safe_str_field(current_payload.get("LastMode"), field_name="LastMode", required=False),
                    "source": _safe_str_field(current_payload.get("Source"), field_name="Source", required=False),
                    "last_export_arn": _safe_str_field(current_payload.get("LastExportArn"), field_name="LastExportArn", required=False),
                    "last_export_item_count": current_payload.get("LastExportItemCount"),
                    "pending_exports": _normalize_pending_exports(current_payload.get("PendingExports")),
                    "incremental_seq": current_payload.get("IncrementalSeq"),
                },
                state_key=state_key,
            ) if current_payload else {}
            merged_state = _merge_legacy_checkpoint_states(current_state, candidate_state, state_key=state_key)
            revision_write_plan = _resolve_checkpoint_revision_write_plan(
                checkpoint_table_name=table_name,
                partition_value=_safe_str_field(candidate_state.get("table_name"), field_name="state.table_name"),
                record_type=CHECKPOINT_DYNAMODB_CURRENT_RECORD,
                current_item_exists=bool(current_item),
                raw_revision=current_payload.get("Revision") if current_payload else None,
            )
            next_revision = int(revision_write_plan.get("next_revision") or 1)

            try:
                _checkpoint_put_item(
                    ddb_client,
                    table_name=table_name,
                    reason="persist legacy checkpoint snapshot history",
                    item=_marshal_dynamodb_item(snapshot_item),
                    attempt=attempt + 1,
                    TableName=table_name,
                    Item=_marshal_dynamodb_item(snapshot_item),
                )
            except ClientError as exc:
                if _client_error_code(exc) != "ConditionalCheckFailedException":
                    raise

            current_item_payload = _build_legacy_checkpoint_current_item(
                merged_state,
                state_key=state_key,
                observed_at=observed_at,
                revision=next_revision,
            )
            put_kwargs: Dict[str, Any] = {
                "TableName": table_name,
                "Item": _marshal_dynamodb_item(current_item_payload),
                "ConditionExpression": _safe_str_field(
                    revision_write_plan.get("condition_expression"),
                    field_name="revision_write_plan.condition_expression",
                ),
                "ExpressionAttributeNames": _safe_dict_field(
                    revision_write_plan.get("expression_attribute_names"),
                    "revision_write_plan.expression_attribute_names",
                ),
            }
            expression_attribute_values = revision_write_plan.get("expression_attribute_values")
            if isinstance(expression_attribute_values, dict) and expression_attribute_values:
                put_kwargs["ExpressionAttributeValues"] = expression_attribute_values
            try:
                _checkpoint_put_item(
                    ddb_client,
                    table_name=table_name,
                    reason="persist legacy checkpoint CURRENT record after merge",
                    item=put_kwargs.get("Item"),
                    attempt=attempt + 1,
                    **put_kwargs,
                )
                break
            except ClientError as exc:
                if _client_error_code(exc) != "ConditionalCheckFailedException" or attempt >= 1:
                    raise


def _checkpoint_save_legacy_s3(store: Dict[str, Any], payload: Dict[str, Any]) -> None:
    s3_client = store.get("s3")
    if s3_client is None:
        raise RuntimeError("checkpoint_store.s3 ausente")
    bucket = _safe_str_field(store.get("bucket"), field_name="checkpoint_store.bucket")
    key = _safe_str_field(store.get("key"), field_name="checkpoint_store.key")
    if not isinstance(payload, dict):
        raise ValueError("Payload de checkpoint inválido")
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        return

    existing_payload = checkpoint_load(store)
    merged_tables = dict(existing_payload.get("tables", {}))
    for state_key, raw_state in tables.items():
        if not isinstance(raw_state, dict):
            continue
        merged_tables[state_key] = _merge_legacy_checkpoint_states(
            merged_tables.get(state_key, {}),
            raw_state,
            state_key=state_key,
        )

    observed_at = _dt_to_iso(_now_utc())
    final_payload = {
        "version": max(int(existing_payload.get("version", 1)), int(payload.get("version", 1)), 2),
        "updated_at": observed_at,
        "tables": merged_tables,
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=_build_checkpoint_history_s3_key(key, observed_at),
        Body=json.dumps(_to_json_safe(payload), ensure_ascii=False, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(_to_json_safe(final_payload), ensure_ascii=False, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def checkpoint_save(store: Dict[str, Any], payload: Dict[str, Any]) -> None:
    backend = _safe_str_field(store.get("backend"), field_name="checkpoint_store.backend", required=False).lower()
    if backend == "s3" or store.get("s3") is not None:
        _checkpoint_save_legacy_s3(store, payload)
        return
    if store.get("key"):
        _checkpoint_save_legacy_dynamodb(store, payload)
        return

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
        candidate_state = _normalize_checkpoint_state(
            raw_state,
            table_name=_safe_str_field(raw_state.get("table_name"), field_name="state.table_name"),
            table_arn=_safe_str_field(raw_state.get("table_arn"), field_name="state.table_arn"),
        )
        partition_value = _safe_str_field(candidate_state.get("table_arn"), field_name="state.table_arn", required=False) or _safe_str_field(
            candidate_state.get("table_name"),
            field_name="state.table_name",
        )

        for attempt in range(2):
            current_item = _checkpoint_get_current_item(
                ddb_client,
                table_name=table_name,
                partition_value=partition_value,
                reason="load checkpoint CURRENT record before persisting merged state",
            )
            _log_event(
                "checkpoint.save.preflight_consulted",
                checkpoint_table_name=table_name,
                partition_value=partition_value,
                checkpoint_record_exists=bool(current_item),
                attempt=attempt + 1,
                level=logging.INFO,
            )
            current_payload = _ddb_decode_item(current_item) if current_item else {}
            current_state = _normalize_checkpoint_state(
                {
                    "table_name": _safe_str_field(current_payload.get("TargetTableName"), field_name="TargetTableName", required=False)
                    or _safe_str_field(current_payload.get("TableName"), field_name="TableName", required=False)
                    or candidate_state["table_name"],
                    "table_arn": _safe_str_field(current_payload.get("TableArn"), field_name="TableArn", required=False)
                    or candidate_state.get("table_arn"),
                    "table_created_at": _safe_str_field(current_payload.get("TableCreatedAt"), field_name="TableCreatedAt", required=False),
                    "last_to": _safe_str_field(current_payload.get("LastTo"), field_name="LastTo", required=False),
                    "last_mode": _safe_str_field(current_payload.get("LastMode"), field_name="LastMode", required=False),
                    "source": _safe_str_field(current_payload.get("Source"), field_name="Source", required=False),
                    "last_export_arn": _safe_str_field(current_payload.get("LastExportArn"), field_name="LastExportArn", required=False),
                    "last_export_item_count": current_payload.get("LastExportItemCount"),
                    "pending_exports": _normalize_pending_exports(current_payload.get("PendingExports")),
                    "incremental_seq": current_payload.get("IncrementalSeq"),
                } if current_payload else candidate_state,
                table_name=candidate_state["table_name"],
                table_arn=_safe_str_field(candidate_state.get("table_arn"), field_name="state.table_arn"),
            ) if current_payload else candidate_state
            merged_state = _merge_legacy_checkpoint_states(
                current_state,
                candidate_state,
                state_key=partition_value,
            )
            revision_write_plan = _resolve_checkpoint_revision_write_plan(
                checkpoint_table_name=table_name,
                partition_value=partition_value,
                record_type=CHECKPOINT_DYNAMODB_CURRENT_RECORD,
                current_item_exists=bool(current_item),
                raw_revision=current_payload.get(CHECKPOINT_DYNAMODB_REVISION_ATTR) if current_payload else None,
            )
            next_revision = int(revision_write_plan.get("next_revision") or 1)
            item = {
                CHECKPOINT_DYNAMODB_PARTITION_KEY: partition_value,
                CHECKPOINT_DYNAMODB_SORT_KEY: CHECKPOINT_DYNAMODB_CURRENT_RECORD,
                CHECKPOINT_DYNAMODB_REVISION_ATTR: next_revision,
                "TargetTableName": merged_state["table_name"],
                "TableArn": merged_state["table_arn"],
                "TableCreatedAt": merged_state.get("table_created_at"),
                "LastTo": merged_state.get("last_to"),
                "LastMode": merged_state.get("last_mode"),
                "Source": merged_state.get("source"),
                "LastExportArn": merged_state.get("last_export_arn"),
                "LastExportItemCount": merged_state.get("last_export_item_count"),
                "PendingExports": merged_state.get("pending_exports", []),
                "IncrementalSeq": _coerce_non_negative_int(
                    merged_state.get("incremental_seq"),
                    field_name="state.incremental_seq",
                    default=0,
                ),
                "UpdatedAt": observed_at,
            }
            put_kwargs: Dict[str, Any] = {
                "TableName": table_name,
                "Item": _ddb_encode_item(item),
                "ConditionExpression": _safe_str_field(
                    revision_write_plan.get("condition_expression"),
                    field_name="revision_write_plan.condition_expression",
                ),
                "ExpressionAttributeNames": _safe_dict_field(
                    revision_write_plan.get("expression_attribute_names"),
                    "revision_write_plan.expression_attribute_names",
                ),
            }
            expression_attribute_values = revision_write_plan.get("expression_attribute_values")
            if isinstance(expression_attribute_values, dict) and expression_attribute_values:
                put_kwargs["ExpressionAttributeValues"] = expression_attribute_values
            try:
                _checkpoint_put_item(
                    ddb_client,
                    table_name=table_name,
                    reason="persist checkpoint CURRENT record after merge",
                    item=put_kwargs.get("Item"),
                    attempt=attempt + 1,
                    **put_kwargs,
                )
                break
            except ClientError as exc:
                if _client_error_code(exc) != "ConditionalCheckFailedException" or attempt >= 1:
                    raise


def _extract_checkpoint_state_from_result(result: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
    if not isinstance(result, dict):
        return None

    checkpoint_state = result.get("checkpoint_state")
    if not isinstance(checkpoint_state, dict):
        return None

    normalized_state = _normalize_checkpoint_state(
        checkpoint_state,
        table_name=_safe_str_field(result.get("table_name"), field_name="table_name"),
        table_arn=_safe_str_field(result.get("table_arn"), field_name="table_arn"),
    )
    state_key = _safe_str_field(
        normalized_state.get("table_arn"),
        field_name="state.table_arn",
        required=False,
    ) or _safe_str_field(normalized_state.get("table_name"), field_name="state.table_name")
    return state_key, normalized_state


def _persist_checkpoint_state_result(
    checkpoint_store: Dict[str, Any],
    result: Dict[str, Any],
) -> Optional[Tuple[str, Dict[str, Any]]]:
    extracted_state = _extract_checkpoint_state_from_result(result)
    if extracted_state is None:
        return None

    state_key, normalized_state = extracted_state
    checkpoint_save(
        checkpoint_store,
        {
            "version": 2,
            "tables": {
                state_key: normalized_state,
            },
        },
    )
    result["checkpoint_state"] = normalized_state
    return state_key, normalized_state


def _persist_checkpoint_results(
    checkpoint_store: Dict[str, Any],
    results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    persistence_errors: List[Dict[str, Any]] = []

    for result in results:
        extracted_state = _extract_checkpoint_state_from_result(result)
        if extracted_state is None:
            continue

        state_key, normalized_state = extracted_state
        result["checkpoint_state"] = normalized_state

        try:
            _persist_checkpoint_state_result(checkpoint_store, result)
        except Exception as exc:
            persistence_errors.append(
                {
                    "state_key": state_key,
                    "table_name": normalized_state["table_name"],
                    "table_arn": normalized_state["table_arn"],
                    "error": str(exc),
                    "exception": exc,
                }
            )

    return persistence_errors


def _validate_output_table_schema(table_name: str, description: Dict[str, Any]) -> None:
    key_schema = description.get("KeySchema") if isinstance(description, dict) else None
    attr_defs = description.get("AttributeDefinitions") if isinstance(description, dict) else None
    if key_schema is None and attr_defs is None:
        return
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

    if "mesma conta" in message_l and "caller" in message_l:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A sessão AWS atual não pertence à mesma conta da tabela alvo.",
            "resolution": "Use ASSUME_ROLE para assumir a conta dona da tabela antes de executar o export.",
        }

    if "caller" in message_l and ("não pertence" in message_l or "not belong" in message_l):
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A sessão AWS atual não pertence à mesma conta da tabela alvo.",
            "resolution": "Use ASSUME_ROLE para assumir a conta dona da tabela antes de executar o export.",
        }

    if "caller=" in message_l and (
        "conta" in message_l
        or "account" in message_l
        or "conta dona da tabela" in message_l
        or "owner account" in message_l
    ):
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A sessão AWS atual não pertence à mesma conta da tabela alvo.",
            "resolution": "Use ASSUME_ROLE para assumir a conta dona da tabela antes de executar o export.",
        }

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
            "user_message": "O export demorou mais do que o limite de espera configurado nesta execução.",
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
    result = {
        "table_name": table_name,
        "table_arn": table_arn,
        "mode": mode,
        "status": "FAILED",
        "source": "runtime",
        "dry_run": dry_run,
        **fields,
    }
    return _apply_result_failure_classification(
        result,
        mode=mode,
        failure_category="runtime_failure",
    )


def _resolve_incremental_failure_type(mode: str, failure_category: str) -> str:
    normalized_mode = _safe_str_field(mode, field_name="mode", required=False).upper()
    normalized_category = _safe_str_field(failure_category, field_name="failure_category", required=False).lower()
    if normalized_mode != "INCREMENTAL":
        return ""
    if normalized_category not in {"runtime_failure", "checkpoint_failure", "pending_terminal_failure"}:
        return ""
    return f"incremental_{normalized_category}"


def _apply_result_failure_classification(
    result: Dict[str, Any],
    *,
    mode: str,
    failure_category: str,
) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result
    result["failure_category"] = _safe_str_field(
        failure_category,
        field_name="failure_category",
        required=False,
    ).lower()
    failure_type = _resolve_incremental_failure_type(mode, failure_category)
    if failure_type:
        result["failure_type"] = failure_type
        result["source"] = failure_type
    return result


def _apply_result_execution_context(
    result: Dict[str, Any],
    *,
    target: TableTarget,
    bucket: str,
    assume_role_arn: Optional[str],
    checkpoint_from: Optional[datetime] = None,
    checkpoint_to: Optional[datetime] = None,
) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result
    result["snapshot_bucket"] = bucket
    result["assume_role_arn"] = assume_role_arn
    result["table_account_id"] = target.account_id
    result["table_region"] = target.region
    if isinstance(checkpoint_from, datetime):
        result["checkpoint_from"] = _dt_to_iso(checkpoint_from)
    if isinstance(checkpoint_to, datetime):
        result["checkpoint_to"] = _dt_to_iso(checkpoint_to)
    return result


def _build_pending_terminal_failure_result(
    *,
    target: TableTarget,
    bucket: str,
    assume_role_arn: Optional[str],
    checkpoint_state: Dict[str, Any],
    terminal_failures: List[Dict[str, str]],
    dry_run: bool,
) -> Dict[str, Any]:
    primary_failure = terminal_failures[0] if terminal_failures else {}
    export_arn = _safe_str_field(primary_failure.get("export_arn"), field_name="pending_terminal_failure.export_arn", required=False)
    export_status = _safe_str_field(primary_failure.get("status"), field_name="pending_terminal_failure.status", required=False) or "FAILED"
    error = RuntimeError(
        f"Export incremental pendente terminou com status terminal. export_arn={export_arn} status={export_status}"
    )
    result = _build_table_error_result(
        table_name=target.table_name,
        table_arn=target.table_arn,
        mode="INCREMENTAL",
        error=error,
        dry_run=dry_run,
    )
    result["message"] = "Foi detectado um export incremental pendente com falha terminal; a execução foi interrompida para evitar avançar com checkpoint inconsistente."
    result["pending_terminal_failures"] = terminal_failures
    result["failed_export_arn"] = export_arn
    result["failed_export_status"] = export_status
    result["checkpoint_state"] = checkpoint_state
    return _apply_result_failure_classification(
        _apply_result_execution_context(
            result,
            target=target,
            bucket=bucket,
            assume_role_arn=assume_role_arn,
        ),
        mode="INCREMENTAL",
        failure_category="pending_terminal_failure",
    )


def _build_checkpoint_persistence_failure_result(
    result: Dict[str, Any],
    *,
    target: TableTarget,
    bucket: str,
    assume_role_arn: Optional[str],
    persistence_error: Dict[str, Any],
) -> Dict[str, Any]:
    failed_result = dict(result)
    previous_status = _safe_str_field(
        failed_result.get("status"),
        field_name="result.status",
        required=False,
    )
    failure_error = RuntimeError(
        f"Falha ao persistir checkpoint incremental. state_key={_safe_str_field(persistence_error.get('state_key'), field_name='persistence_error.state_key', required=False)} error={_safe_str_field(persistence_error.get('error'), field_name='persistence_error.error', required=False)}"
    )
    fields = _build_error_response_fields(failure_error)
    failed_result.update(fields)
    failed_result["status"] = "FAILED"
    failed_result["message"] = "O export incremental foi executado, mas a persistência do checkpoint falhou."
    failed_result["checkpoint_persistence_failed"] = True
    failed_result["checkpoint_persistence_error"] = _safe_str_field(
        persistence_error.get("error"),
        field_name="persistence_error.error",
        required=False,
    )
    failed_result["checkpoint_state_key"] = _safe_str_field(
        persistence_error.get("state_key"),
        field_name="persistence_error.state_key",
        required=False,
    )
    if previous_status:
        failed_result["previous_status"] = previous_status
    return _apply_result_failure_classification(
        _apply_result_execution_context(
            failed_result,
            target=target,
            bucket=bucket,
            assume_role_arn=assume_role_arn,
        ),
        mode=_safe_str_field(failed_result.get("mode"), field_name="result.mode", required=False),
        failure_category="checkpoint_failure",
    )


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
    auto_enable: bool,
) -> Dict[str, Optional[datetime]]:
    response = ddb_client.describe_continuous_backups(TableName=table_name)
    desc = _safe_dict_field(response.get("ContinuousBackupsDescription"), "ContinuousBackupsDescription")
    pitr_desc = _safe_dict_field(desc.get("PointInTimeRecoveryDescription"), "PointInTimeRecoveryDescription")
    pitr_status = _safe_str_field(pitr_desc.get("PointInTimeRecoveryStatus"), field_name="PointInTimeRecoveryStatus", required=False)

    if pitr_status == "ENABLED":
        return _extract_pitr_window(pitr_desc)

    if not auto_enable:
        _log_event(
            "table.pitr.enable.skipped",
            table_name=table_name,
            table_arn=table_arn,
            point_in_time_recovery_status=pitr_status,
            auto_enable=False,
            level=logging.WARNING,
        )
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
        auto_enable=bool(config.get("pitr_auto_enable")),
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
        auto_enable=bool(config.get("pitr_auto_enable")),
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
            if not checkpoint_to:
                checkpoint_to = _coerce_checkpoint_timestamp_to_utc(desc.get("ExportToTime"))
                if checkpoint_to is None:
                    checkpoint_to = _coerce_checkpoint_timestamp_to_utc(desc.get("ExportEndTime"))
                if checkpoint_to is not None:
                    checkpoint_to = _dt_to_iso(checkpoint_to)
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
    max_incremental_exports_per_cycle: Any = MAX_INCREMENTAL_EXPORTS_PER_CYCLE,
) -> Dict[str, Any]:
    incremental_cycle_limit = _resolve_max_incremental_exports_per_cycle(max_incremental_exports_per_cycle)
    refreshed_checkpoint_state = _refresh_last_incremental_export_metadata(
        ddb_client=ddb_client,
        checkpoint_state=checkpoint_state,
        table_name=table_name,
        table_arn=table_arn,
    )
    last_to_candidate = _resolve_checkpoint_last_to_timestamp(
        raw_last_to=refreshed_checkpoint_state.get("last_to"),
        table_name=table_name,
        table_arn=table_arn,
    )
    if not isinstance(last_to_candidate, datetime):
        last_export_arn = _safe_str_field(
            refreshed_checkpoint_state.get("last_export_arn"),
            field_name="checkpoint_state.last_export_arn",
            required=False,
        )
        if last_export_arn:
            try:
                last_export_desc = _describe_export_description(
                    ddb_client,
                    export_arn=last_export_arn,
                )
                candidate_last_to = _coerce_checkpoint_timestamp_to_utc(
                    last_export_desc.get("ExportToTime"),
                )
                if candidate_last_to is None:
                    candidate_last_to = _coerce_checkpoint_timestamp_to_utc(
                        last_export_desc.get("ExportEndTime"),
                    )
                if candidate_last_to is not None:
                    refreshed_checkpoint_state = {
                        **refreshed_checkpoint_state,
                        "last_to": _dt_to_iso(candidate_last_to),
                    }
                    last_to_candidate = candidate_last_to
                    _log_event(
                        "checkpoint.last_to.recovered_from_last_export",
                        table_name=table_name,
                        table_arn=table_arn,
                        export_arn=last_export_arn,
                        export_to=_dt_to_iso(candidate_last_to),
                        level=logging.INFO,
                    )
            except ClientError as exc:
                _log_event(
                    "checkpoint.last_to.recover_failed",
                    table_name=table_name,
                    table_arn=table_arn,
                    export_arn=last_export_arn,
                    code=_client_error_code(exc),
                    message=_client_error_message(exc),
                    level=logging.WARNING,
                )
    if not checkpoint_record_exists:
        return {
            "mode": "FULL",
            "reason": "checkpoint_record_missing",
            "checkpoint_state": refreshed_checkpoint_state,
            "last_to": last_to_candidate,
        }
    if checkpoint_state.get("last_mode") not in {"FULL", "INCREMENTAL", ""}:
        return {
            "mode": "FULL",
            "reason": "checkpoint_last_mode_invalid",
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
    if current_incremental_seq >= incremental_cycle_limit:
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


def _build_checkpoint_lock_active_result(
    *,
    target: TableTarget,
    bucket: str,
    assume_role_arn: Optional[str],
    checkpoint_state: Dict[str, Any],
    lock_expires_at: str,
) -> Dict[str, Any]:
    mode = _safe_str_field(checkpoint_state.get("last_mode"), field_name="checkpoint_state.last_mode", required=False) or "FULL"
    result = _build_pending_result(
        target=target,
        mode=mode,
        source="checkpoint_lock_active",
        message="Outra execução ainda está processando o checkpoint desta tabela.",
        bucket=bucket,
        assume_role_arn=assume_role_arn,
        checkpoint_state=checkpoint_state,
    )
    result["mode_selection_reason"] = "checkpoint_lock_active"
    if lock_expires_at:
        result["checkpoint_lock_expires_at"] = lock_expires_at
    return result


def _build_checkpoint_dry_run_diagnostics(
    *,
    target: TableTarget,
    load_diagnostics: Dict[str, Any],
    loaded_checkpoint_state: Dict[str, Any],
    checkpoint_record_exists: bool,
    checkpoint_state: Dict[str, Any],
    automatic_plan: Dict[str, Any],
    table_created_at: str,
    table_recreated_detected: bool,
) -> Dict[str, Any]:
    field_integrity = load_diagnostics.get("field_integrity") if isinstance(load_diagnostics.get("field_integrity"), dict) else {}
    planner_mode = _safe_str_field(automatic_plan.get("mode"), field_name="automatic_plan.mode", required=False).upper()
    planner_reason = _safe_str_field(automatic_plan.get("reason"), field_name="automatic_plan.reason", required=False)
    planned_last_to = automatic_plan.get("last_to")
    pending_exports = _normalize_pending_exports(checkpoint_state.get("pending_exports"))
    notes = list(load_diagnostics.get("notes") or [])
    errors: List[str] = []
    warnings: List[str] = []

    if not bool(load_diagnostics.get("record_found")):
        warnings.append("Checkpoint CURRENT não encontrado para a tabela solicitada.")
    if bool(load_diagnostics.get("rejected")):
        errors.append(
            {
                "legacy_partition_table_name_mismatch": "Checkpoint legado rejeitado porque o nome da tabela não corresponde ao alvo solicitado.",
                "legacy_partition_table_arn_mismatch": "Checkpoint legado rejeitado porque o TableArn do registro aponta para outra tabela.",
            }.get(
                _safe_str_field(load_diagnostics.get("rejected_reason"), field_name="checkpoint_diagnostics.rejected_reason", required=False),
                "Checkpoint rejeitado por inconsistência de identidade.",
            )
        )
    if table_recreated_detected:
        errors.append("Checkpoint invalidado porque a tabela atual foi recriada após a persistência do último estado.")

    last_mode_integrity = field_integrity.get("last_mode") if isinstance(field_integrity.get("last_mode"), dict) else {}
    last_to_integrity = field_integrity.get("last_to") if isinstance(field_integrity.get("last_to"), dict) else {}
    table_created_at_integrity = field_integrity.get("table_created_at") if isinstance(field_integrity.get("table_created_at"), dict) else {}
    incremental_seq_integrity = field_integrity.get("incremental_seq") if isinstance(field_integrity.get("incremental_seq"), dict) else {}
    item_count_integrity = field_integrity.get("last_export_item_count") if isinstance(field_integrity.get("last_export_item_count"), dict) else {}
    pending_integrity = field_integrity.get("pending_exports") if isinstance(field_integrity.get("pending_exports"), dict) else {}
    table_arn_integrity = field_integrity.get("table_arn") if isinstance(field_integrity.get("table_arn"), dict) else {}
    target_name_integrity = field_integrity.get("target_table_name") if isinstance(field_integrity.get("target_table_name"), dict) else {}

    if last_mode_integrity.get("reason") == "unsupported_value":
        errors.append("Campo LastMode possui valor não suportado.")
    elif last_mode_integrity.get("reason") == "missing":
        warnings.append("Campo LastMode está ausente; a integridade histórica do modo anterior fica degradada.")

    if last_to_integrity.get("reason") in {"missing", "invalid_timestamp"}:
        errors.append("Campo LastTo está ausente ou inválido para cálculo da próxima janela incremental.")
    elif last_to_integrity.get("reason") == "legacy_format_recovered":
        warnings.append("Campo LastTo estava em formato legado e foi recuperado para UTC.")

    if table_created_at_integrity.get("reason") == "invalid_timestamp":
        warnings.append("Campo TableCreatedAt existe, mas não pôde ser interpretado como timestamp UTC.")
    elif table_created_at_integrity.get("reason") == "missing_optional":
        warnings.append("Campo TableCreatedAt está ausente; a proteção contra recriação da tabela fica reduzida.")

    if incremental_seq_integrity.get("reason") == "invalid_integer":
        errors.append("Campo IncrementalSeq está inválido.")
    if item_count_integrity.get("reason") == "invalid_integer":
        warnings.append("Campo LastExportItemCount está inválido; o próximo incremental pode perder o contexto do volume anterior.")
    if pending_integrity and pending_integrity.get("reason") not in {"ok", "missing_optional"}:
        warnings.append("Campo PendingExports contém dados fora do contrato esperado e precisou ser normalizado.")
    if table_arn_integrity.get("reason") == "missing_optional":
        warnings.append("Campo TableArn não está persistido no checkpoint; a validação de identidade depende do lookup usado.")
    if target_name_integrity and not target_name_integrity.get("matches_requested"):
        errors.append("O nome lógico da tabela salvo no checkpoint não corresponde ao alvo solicitado.")

    planner_usable_reasons = {
        "initial_incremental_after_full",
        "previous_incremental_item_count_unknown",
        "previous_incremental_had_items",
        "previous_incremental_without_items",
        "incremental_cycle_limit_reached",
    }
    usable_for_planning = (
        checkpoint_record_exists
        and not bool(load_diagnostics.get("rejected"))
        and not table_recreated_detected
        and planner_reason in planner_usable_reasons
    )
    usable_for_incremental = usable_for_planning and planner_mode == "INCREMENTAL" and not bool(pending_exports)

    if bool(pending_exports):
        notes.append("Há export(es) pendente(s) no checkpoint; a execução não iniciaria um novo export agora.")
    if planner_reason == "incremental_cycle_limit_reached":
        notes.append("O checkpoint está consistente, mas o limite do ciclo incremental foi atingido; a próxima execução válida seria FULL.")

    if not bool(load_diagnostics.get("record_found")):
        integrity_status = "missing"
        summary = "Checkpoint ausente: a Lambda trataria a execução como primeira carga e iniciaria FULL."
    elif errors:
        integrity_status = "invalid"
        summary = "Checkpoint encontrado, mas há inconsistências que impedem o uso seguro do estado atual."
    elif warnings:
        integrity_status = "degraded"
        if planner_mode == "INCREMENTAL" and bool(pending_exports):
            summary = "Checkpoint recuperado com avisos, mas existe export pendente; nenhum novo incremental seria iniciado agora."
        elif usable_for_incremental:
            summary = "Checkpoint recuperado com avisos, mas ainda válido para incremental."
        elif planner_reason == "incremental_cycle_limit_reached":
            summary = "Checkpoint recuperado com avisos e consistente, porém o limite do ciclo incremental foi atingido; a Lambda iniciaria FULL."
        elif usable_for_planning:
            summary = "Checkpoint recuperado com avisos e utilizável para planejamento, porém a decisão automática atual não seguiria para incremental imediato."
        else:
            summary = "Checkpoint encontrado com avisos, mas a regra atual não permite reaproveitá-lo para incremental."
    else:
        integrity_status = "valid"
        if planner_mode == "INCREMENTAL" and bool(pending_exports):
            summary = "Checkpoint íntegro, mas existe export pendente; nenhum novo incremental seria iniciado agora."
        elif usable_for_incremental:
            summary = "Checkpoint íntegro e válido para incremental."
        elif planner_reason == "incremental_cycle_limit_reached":
            summary = "Checkpoint íntegro, mas o limite do ciclo incremental foi atingido; a Lambda iniciaria FULL."
        elif usable_for_planning:
            summary = "Checkpoint íntegro e consistente para planejamento, mas a decisão automática atual não seguiria para incremental imediato."
        else:
            summary = "Checkpoint íntegro, mas a regra atual não permite reaproveitá-lo neste momento."

    return {
        "summary": summary,
        "integrity_status": integrity_status,
        "usable_for_planning": usable_for_planning,
        "usable_for_incremental": usable_for_incremental,
        "lookup": {
            "record_found": bool(load_diagnostics.get("record_found")),
            "lookup_key": _safe_str_field(load_diagnostics.get("lookup_key"), field_name="checkpoint_diagnostics.lookup_key", required=False),
            "lookup_attempts": load_diagnostics.get("lookup_attempts", []),
            "used_legacy_partition": bool(load_diagnostics.get("used_legacy_partition")),
            "rejected": bool(load_diagnostics.get("rejected")),
            "rejected_reason": _safe_str_field(load_diagnostics.get("rejected_reason"), field_name="checkpoint_diagnostics.rejected_reason", required=False),
        },
        "field_integrity": field_integrity,
        "runtime_validation": {
            "requested_table_name": target.table_name,
            "requested_table_arn": target.table_arn,
            "checkpoint_record_exists_after_runtime_validation": checkpoint_record_exists,
            "table_created_at_runtime": table_created_at,
            "table_created_at_checkpoint": _safe_str_field(loaded_checkpoint_state.get("table_created_at"), field_name="loaded_checkpoint_state.table_created_at", required=False),
            "table_recreated_detected": table_recreated_detected,
            "pending_exports_count": len(pending_exports),
        },
        "automatic_plan": {
            "mode": planner_mode,
            "reason": planner_reason,
            "next_incremental_index": automatic_plan.get("next_incremental_index"),
            "checkpoint_last_to": _dt_to_iso(planned_last_to) if isinstance(planned_last_to, datetime) else "",
        },
        "warnings": warnings,
        "errors": errors,
        "notes": notes,
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
    bucket = _resolve_snapshot_bucket(config, target)
    bucket_owner = _resolve_optional_text(config.get("bucket_owner"))
    checkpoint_load_diagnostics: Dict[str, Any] = {}
    table_recreated_detected = False

    checkpoint_lock_owner_token = ""
    checkpoint_lock_acquired = False
    if not bool(config.get("dry_run")):
        checkpoint_lock_owner_token = _build_checkpoint_execution_lock_owner_token(
            config=config,
            target=target,
        )
        checkpoint_lock_result = _checkpoint_try_acquire_execution_lock(
            checkpoint_store,
            target_table_name=target.table_name,
            target_table_arn=target.table_arn,
            mode_hint="AUTO",
            owner_token=checkpoint_lock_owner_token,
        )
        checkpoint_lock_acquired = bool(checkpoint_lock_result.get("acquired"))
        if not checkpoint_lock_acquired:
            locked_checkpoint_state = _normalize_checkpoint_state(
                checkpoint_load_table_state(
                    checkpoint_store,
                    target_table_name=target.table_name,
                    target_table_arn=target.table_arn,
                ),
                table_name=target.table_name,
                table_arn=target.table_arn,
            )
            return _build_checkpoint_lock_active_result(
                target=target,
                bucket=bucket,
                assume_role_arn=assume_role_arn,
                checkpoint_state=locked_checkpoint_state,
                lock_expires_at=_safe_str_field(
                    checkpoint_lock_result.get("expires_at"),
                    field_name="checkpoint_lock.expires_at",
                    required=False,
                ),
            )

    try:
        raw_checkpoint_state, checkpoint_load_diagnostics = _checkpoint_load_table_state_with_diagnostics(
            checkpoint_store,
            target_table_name=target.table_name,
            target_table_arn=target.table_arn,
        )
        checkpoint_record_exists = bool(raw_checkpoint_state)
        _log_event(
            "snapshot.checkpoint.record.evaluated",
            table_name=target.table_name,
            table_arn=target.table_arn,
            checkpoint_record_exists=checkpoint_record_exists,
            has_pending_exports=bool(_normalize_pending_exports(raw_checkpoint_state.get("pending_exports"))),
            table_region=target.region,
            level=logging.INFO,
        )
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
        if checkpoint_record_exists and table_created_at and checkpoint_created_at:
            checkpoint_created_at_dt = _coerce_checkpoint_timestamp_to_utc(checkpoint_created_at)
            table_created_at_dt = _coerce_checkpoint_timestamp_to_utc(table_created_at)
            if checkpoint_created_at_dt and table_created_at_dt:
                changed_table_created_at = checkpoint_created_at_dt != table_created_at_dt
            else:
                changed_table_created_at = checkpoint_created_at != table_created_at
            if changed_table_created_at:
                _log_event(
                    "checkpoint.table_recreated.detected",
                    table_name=target.table_name,
                    table_arn=target.table_arn,
                    checkpoint_table_created_at=checkpoint_created_at,
                    runtime_table_created_at=table_created_at,
                    level=logging.WARNING,
                )
                table_recreated_detected = True
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
            max_incremental_exports_per_cycle=config.get("max_incremental_exports_per_cycle"),
        )
        checkpoint_state = automatic_plan["checkpoint_state"]
        pending_exports = _normalize_pending_exports(checkpoint_state.get("pending_exports"))
        checkpoint_diagnostics: Dict[str, Any] = {}
        if bool(config.get("dry_run")):
            checkpoint_diagnostics = _build_checkpoint_dry_run_diagnostics(
                target=target,
                load_diagnostics=checkpoint_load_diagnostics,
                loaded_checkpoint_state=raw_checkpoint_state,
                checkpoint_record_exists=checkpoint_record_exists,
                checkpoint_state=checkpoint_state,
                automatic_plan=automatic_plan,
                table_created_at=table_created_at,
                table_recreated_detected=table_recreated_detected,
            )
            _log_event(
                "snapshot.dry_run.checkpoint_diagnostics",
                table_name=target.table_name,
                table_arn=target.table_arn,
                integrity_status=_safe_str_field(checkpoint_diagnostics.get("integrity_status"), field_name="checkpoint_diagnostics.integrity_status", required=False),
                usable_for_planning=bool(checkpoint_diagnostics.get("usable_for_planning")),
                usable_for_incremental=bool(checkpoint_diagnostics.get("usable_for_incremental")),
                planned_mode=_safe_str_field(automatic_plan.get("mode"), field_name="automatic_plan.mode"),
                planned_reason=_safe_str_field(automatic_plan.get("reason"), field_name="automatic_plan.reason"),
                checkpoint_diagnostics=checkpoint_diagnostics,
                level=logging.INFO,
            )
        _log_event(
            "snapshot.checkpoint.plan.resolved",
            table_name=target.table_name,
            table_arn=target.table_arn,
            proposed_mode=automatic_plan.get("mode"),
            reason=_safe_str_field(automatic_plan.get("reason"), field_name="automatic_plan.reason"),
            checkpoint_record_exists=checkpoint_record_exists,
            checkpoint_last_mode=_safe_str_field(checkpoint_state.get("last_mode"), field_name="checkpoint_state.last_mode", required=False),
            checkpoint_last_to=_safe_str_field(checkpoint_state.get("last_to"), field_name="checkpoint_state.last_to", required=False),
            incremental_seq=_coerce_non_negative_int(checkpoint_state.get("incremental_seq"), field_name="checkpoint_state.incremental_seq", default=0),
            pending_exports_count=len(_normalize_pending_exports(checkpoint_state.get("pending_exports"))),
            level=logging.INFO,
        )

        def finalize_result(result: Dict[str, Any]) -> Dict[str, Any]:
            result["checkpoint_state"] = checkpoint_state
            if bool(config.get("dry_run")):
                return result

            persistence_errors = _persist_checkpoint_results(checkpoint_store, [result])
            for persistence_error in persistence_errors:
                _log_event(
                    "checkpoint.persist.immediate_failed",
                    table_name=persistence_error["table_name"],
                    table_arn=persistence_error["table_arn"],
                    state_key=persistence_error["state_key"],
                    error=persistence_error["error"],
                    level=logging.ERROR,
                )
            return result

        if pending_exports:
            result = _build_pending_result(
                target=target,
                mode="INCREMENTAL",
                source="pending_export_tracking",
                message="Já existe export em andamento para esta tabela.",
                bucket=bucket,
                assume_role_arn=assume_role_arn,
                checkpoint_state=checkpoint_state,
            )
            result["mode_selection_reason"] = "pending_export_tracking"
            if bool(config.get("dry_run")):
                planned_mode = _safe_str_field(automatic_plan.get("mode"), field_name="automatic_plan.mode").upper()
                _log_event(
                    "snapshot.dry_run.table_plan",
                    table_name=target.table_name,
                    table_arn=target.table_arn,
                    table_region=target.region,
                    planned_mode=planned_mode,
                    reason=_safe_str_field(automatic_plan.get("reason"), field_name="automatic_plan.reason"),
                    has_pending_exports=True,
                    checkpoint_record_exists=checkpoint_record_exists,
                    checkpoint_last_mode=_safe_str_field(checkpoint_state.get("last_mode"), field_name="checkpoint_state.last_mode", required=False),
                    checkpoint_last_to=_safe_str_field(checkpoint_state.get("last_to"), field_name="checkpoint_state.last_to", required=False),
                    incremental_seq=_coerce_non_negative_int(checkpoint_state.get("incremental_seq"), field_name="checkpoint_state.incremental_seq", default=0),
                    pending_exports_count=len(pending_exports),
                    dry_run=True,
                    level=logging.INFO,
                )
                result["checkpoint_diagnostics"] = checkpoint_diagnostics
                result["checkpoint_debug_summary"] = _safe_str_field(
                    checkpoint_diagnostics.get("summary"),
                    field_name="checkpoint_diagnostics.summary",
                    required=False,
                )
                return finalize_result(result)

        if bool(config.get("dry_run")):
            planned_mode = _safe_str_field(automatic_plan.get("mode"), field_name="automatic_plan.mode").upper()
            checkpoint_last_to = _safe_str_field(checkpoint_state.get("last_to"), field_name="checkpoint_state.last_to", required=False)
            _log_event(
                "snapshot.dry_run.table_plan",
                table_name=target.table_name,
                table_arn=target.table_arn,
                table_region=target.region,
                planned_mode=planned_mode,
                reason=_safe_str_field(automatic_plan.get("reason"), field_name="automatic_plan.reason"),
                has_pending_exports=bool(pending_exports),
                checkpoint_record_exists=checkpoint_record_exists,
                checkpoint_last_mode=_safe_str_field(checkpoint_state.get("last_mode"), field_name="checkpoint_state.last_mode", required=False),
                checkpoint_last_to=checkpoint_last_to,
                incremental_seq=_coerce_non_negative_int(checkpoint_state.get("incremental_seq"), field_name="checkpoint_state.incremental_seq", default=0),
                pending_exports_count=len(pending_exports),
                level=logging.INFO,
            )
            return finalize_result({
                "table_name": target.table_name,
                "table_arn": target.table_arn,
                "mode": planned_mode,
                "status": "PLANNED",
                "source": "dry_run",
                "snapshot_bucket": bucket,
                "checkpoint_record_exists": checkpoint_record_exists,
                "checkpoint_state": checkpoint_state,
                "pending_exports": pending_exports,
                "assume_role_arn": assume_role_arn,
                "table_account_id": target.account_id,
                "table_region": target.region,
                "mode_selection_reason": automatic_plan.get("reason"),
                "checkpoint_last_to": checkpoint_last_to,
                "checkpoint_diagnostics": checkpoint_diagnostics,
                "checkpoint_debug_summary": _safe_str_field(
                    checkpoint_diagnostics.get("summary"),
                    field_name="checkpoint_diagnostics.summary",
                    required=False,
                ),
                "message": "Execução em dry_run: nenhum export foi iniciado.",
            })

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

            result["mode_selection_reason"] = automatic_plan.get("reason")
            return finalize_result(result)

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
            return finalize_result(result)

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
            return finalize_result(result)

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

        incremental_result["mode_selection_reason"] = automatic_plan.get("reason")
        return finalize_result(incremental_result)
    finally:
        if checkpoint_lock_acquired:
            _checkpoint_release_execution_lock(
                checkpoint_store,
                target_table_name=target.table_name,
                target_table_arn=target.table_arn,
                owner_token=checkpoint_lock_owner_token,
            )


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


def snapshot_manager_build_checkpoint_state(entry: Dict[str, Any], previous_state: Dict[str, Any]) -> Dict[str, Any]:
    state_key = _safe_str_field(
        entry.get("table_arn"),
        field_name="entry.table_arn",
        required=False,
    ) or _safe_str_field(entry.get("table_name"), field_name="entry.table_name")
    return _merge_legacy_checkpoint_states(previous_state, entry, state_key=state_key)


def snapshot_manager_apply_result_to_checkpoint_state(
    manager: Dict[str, Any],
    checkpoint_state: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    _ = manager
    state_key = _safe_str_field(
        result.get("table_arn"),
        field_name="result.table_arn",
        required=False,
    ) or _safe_str_field(result.get("table_name"), field_name="result.table_name")
    if isinstance(result.get("checkpoint_state"), dict):
        return _merge_legacy_checkpoint_states(checkpoint_state, result["checkpoint_state"], state_key=state_key)

    next_state = _merge_legacy_checkpoint_states(checkpoint_state, result, state_key=state_key)
    mode = _safe_str_field(result.get("mode"), field_name="result.mode", required=False).upper()
    status = _safe_str_field(result.get("status"), field_name="result.status", required=False).upper()
    if status == "COMPLETED":
        next_state = {
            **next_state,
            "last_to": _safe_str_field(result.get("checkpoint_to"), field_name="result.checkpoint_to", required=False),
            "last_mode": mode,
            "source": _safe_str_field(result.get("source"), field_name="result.source", required=False),
            "pending_exports": [],
        }
    elif mode == "INCREMENTAL" and status in EXPORT_PENDING_STATUSES:
        export_arn = _safe_str_field(result.get("export_arn"), field_name="result.export_arn", required=False)
        checkpoint_to = _safe_str_field(result.get("checkpoint_to"), field_name="result.checkpoint_to", required=False)
        if export_arn and checkpoint_to:
            pending_exports = _normalize_pending_exports(next_state.get("pending_exports"))
            pending_exports.append(
                {
                    "export_arn": export_arn,
                    "checkpoint_to": checkpoint_to,
                    "mode": "INCREMENTAL",
                    "source": _safe_str_field(result.get("source"), field_name="result.source", required=False) or "native",
                }
            )
            next_state = {
                **next_state,
                "pending_exports": _dedupe_pending_exports(pending_exports),
            }
            history = next_state.get("history") if isinstance(next_state.get("history"), list) else []
            next_state = {
                **next_state,
                "history": _merge_checkpoint_histories(history, [_build_checkpoint_history_event(next_state)]),
            }

    return _compact_legacy_checkpoint_state(next_state, state_key=state_key)


def snapshot_manager_reconcile_pending_exports(
    previous_state: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    ddb_client: Any,
) -> Dict[str, Any]:
    next_state = _reconcile_pending_exports(
        ddb_client=ddb_client,
        state=previous_state,
        table_name=table_name,
        table_arn=table_arn,
    )
    if not _normalize_pending_exports(next_state.get("pending_exports")):
        next_state = dict(next_state)
        next_state.pop("pending_exports", None)
    return next_state


def snapshot_manager_build_run_response(
    manager: Dict[str, Any],
    results: List[Dict[str, Any]],
    checkpoint_error: Optional[str],
    checkpoint_error_feedback: Dict[str, str],
) -> Dict[str, Any]:
    config = _safe_dict_field(manager.get("config"), "manager.config")
    normalized_results: List[Dict[str, Any]] = []
    for item in _normalize_output_results(results, snapshot_bucket=_safe_str_field(config.get("bucket"), field_name="bucket", required=False)):
        row = dict(item)
        assume_role = _safe_str_field(row.pop("assume_role_arn", ""), field_name="assume_role_arn", required=False)
        if assume_role:
            row["assume_role"] = assume_role
        export_arn = _safe_str_field(row.get("export_arn"), field_name="export_arn", required=False)
        if export_arn and not _safe_str_field(row.get("export_job_id"), field_name="export_job_id", required=False):
            row["export_job_id"] = _resolve_export_job_id(export_arn)
        normalized_results.append(row)

    response: Dict[str, Any] = {
        "status": "partial_ok" if checkpoint_error else "ok",
        "run_id": config.get("run_id"),
        "mode": config.get("mode"),
        "dry_run": bool(config.get("dry_run")),
        "snapshot_bucket": config.get("bucket"),
        "results": normalized_results,
        "checkpoint_error": checkpoint_error,
        "updated_checkpoint": _safe_str_field(config.get("checkpoint_dynamodb_table_arn"), field_name="checkpoint_dynamodb_table_arn", required=False)
        or _safe_str_field(config.get("checkpoint_key"), field_name="checkpoint_key", required=False),
    }
    if checkpoint_error_feedback:
        response.update(
            {
                "checkpoint_error_detail": checkpoint_error_feedback.get("error_detail"),
                "checkpoint_user_message": checkpoint_error_feedback.get("user_message"),
                "checkpoint_resolution": checkpoint_error_feedback.get("resolution"),
            }
        )
    return response


def snapshot_manager_resolve_targets(manager: Dict[str, Any]) -> List[str]:
    config = _safe_dict_field(manager.get("config"), "manager.config")
    session = manager.get("session")
    if session is None:
        return _dedupe_values(_normalize_list(config.get("targets")), case_insensitive=True)
    return _resolve_targets(config, session=session)


def snapshot_manager_resolve_ignore(manager: Dict[str, Any]) -> List[str]:
    config = _safe_dict_field(manager.get("config"), "manager.config")
    session = manager.get("session")
    if session is None:
        return _dedupe_values(_normalize_list(config.get("ignore")), case_insensitive=True)
    return _resolve_ignore(config, session=session)


def snapshot_manager_prime_assumed_session_from_direct_targets(manager: Dict[str, Any]) -> None:
    _ = manager


def snapshot_manager_prime_assumed_session_from_targets(manager: Dict[str, Any], entries: List[Dict[str, Any]]) -> None:
    _ = (manager, entries)


def snapshot_manager_load_csv_source(manager: Dict[str, Any], source: str, *, source_name: str) -> str:
    s3_client = manager.get("s3")
    if s3_client is not None and source.startswith("s3://"):
        bucket, key = _parse_s3_uri(source)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response.get("Body")
        if body is None:
            raise RuntimeError(f"Objeto S3 vazio para {source}")
        return body.read().decode("utf-8")
    session = manager.get("session")
    if session is None:
        raise RuntimeError("manager.session ausente")
    return _load_text_from_csv_source(source_name, source, session=session, bucket_hint=_safe_str_field(manager.get("config", {}).get("bucket"), field_name="bucket", required=False))


def snapshot_manager_extract_session_identity(manager: Dict[str, Any], session: Any) -> str:
    cache = manager.setdefault("_session_identity_cache", {})
    lock = manager.setdefault("_session_identity_lock", threading.Lock())
    cache_key = id(session)
    with lock:
        cached = cache.get(cache_key)
        if cached:
            return cached
        sts_client = session.client("sts")
        caller_arn = _safe_str_field(sts_client.get_caller_identity().get("Arn"), field_name="GetCallerIdentity.Arn")
        cache[cache_key] = caller_arn
        return caller_arn


def snapshot_manager_resolve_execution_context(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if not table_arn.startswith("arn:"):
        return manager, {
            "session": manager.get("session"),
            "ddb": manager.get("ddb"),
            "s3": manager.get("s3"),
            "session_mode": "shared_session_without_arn",
            "assume_role_arn": _safe_str_field(manager.get("config", {}).get("assume_role_arn"), field_name="assume_role_arn", required=False),
            "table_account_id": "",
            "table_region": _safe_str_field(manager.get("default_region"), field_name="default_region", required=False) or _resolve_runtime_region(),
            "table_name": table_name,
            "table_arn": table_arn,
        }

    base_session = manager.get("session")
    if base_session is None:
        raise RuntimeError("manager.session ausente")
    runtime_region = _safe_str_field(manager.get("default_region"), field_name="default_region", required=False) or _resolve_runtime_region(
        getattr(base_session, "region_name", None)
    )
    target = _resolve_table_target(table_arn, session=base_session, runtime_region=runtime_region)
    execution_session, assume_role_arn = _resolve_assumed_session_for_target(
        base_session=base_session,
        config=_safe_dict_field(manager.get("config"), "manager.config"),
        table_account_id=target.account_id,
        cache=manager.setdefault("_assume_session_cache", {}),
    )
    return manager, {
        "session": execution_session,
        "ddb": _get_session_client(execution_session, "dynamodb", region_name=target.region),
        "s3": _get_session_client(execution_session, "s3"),
        "session_mode": "shared_session_by_table_region",
        "assume_role_arn": assume_role_arn,
        "table_account_id": target.account_id,
        "table_region": target.region,
        "table_name": target.table_name,
        "table_arn": target.table_arn,
    }


def snapshot_manager_validate_export_session_account(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    context: Dict[str, Any],
) -> None:
    _ = table_name
    if not table_arn.startswith("arn:"):
        return
    session = context.get("session") or manager.get("session")
    if session is None:
        return
    caller_arn = snapshot_manager_extract_session_identity(manager, session)
    caller_match = re.search(r"::(\d{12}):", caller_arn)
    target_account = _extract_table_arn_context(table_arn, field_name="table_arn").get("account_id")
    caller_account = caller_match.group(1) if caller_match else ""
    if caller_account and target_account and caller_account != target_account:
        raise RuntimeError(
            f"TableArn {table_arn} pertence à conta {target_account}, mas caller={caller_account}. "
            "A operação ExportTableToPointInTime exige sessão da conta dona da tabela."
        )


def snapshot_manager_partition_by_permission_precheck(
    manager: Dict[str, Any],
    entries: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    config = _safe_dict_field(manager.get("config"), "manager.config")
    if not bool(config.get("permission_precheck_enabled")):
        return manager, entries, []

    next_manager = dict(manager)
    cache = dict(next_manager.get("_execution_context_cache") or {})
    next_manager["_execution_context_cache"] = cache
    allowed_entries: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for entry in entries:
        table_name = _safe_str_field(entry.get("table_name"), field_name="entry.table_name")
        table_arn = _safe_str_field(entry.get("table_arn"), field_name="entry.table_arn")
        try:
            resolved = snapshot_manager_resolve_execution_context(next_manager, table_name, table_arn)
            _, context = resolved if isinstance(resolved, tuple) else (next_manager, resolved)
            session = context.get("session") or next_manager.get("session")
            if session is not None:
                context = {**context, "caller_arn": snapshot_manager_extract_session_identity(next_manager, session)}
            cache[table_arn or table_name] = context
            allowed_entries.append(entry)
        except Exception as exc:
            failures.append({"table_name": table_name, "table_arn": table_arn, "error": str(exc)})
    return next_manager, allowed_entries, failures


def snapshot_manager_prepare_execution_entries(
    manager: Dict[str, Any],
    entries: List[Dict[str, Any]],
    previous_tables: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    cache = manager.setdefault("_execution_context_cache", {})
    prepared_entries: List[Dict[str, Any]] = []
    for entry in entries:
        table_name = _safe_str_field(entry.get("table_name"), field_name="entry.table_name")
        table_arn = _safe_str_field(entry.get("table_arn"), field_name="entry.table_arn")
        context = cache.get(table_arn or table_name)
        if not isinstance(context, dict):
            resolved = snapshot_manager_resolve_execution_context(manager, table_name, table_arn)
            _, context = resolved if isinstance(resolved, tuple) else (manager, resolved)
            session = context.get("session") or manager.get("session")
            if session is not None and not context.get("caller_arn"):
                context = {**context, "caller_arn": snapshot_manager_extract_session_identity(manager, session)}
            cache[table_arn or table_name] = context
        prepared_entries.append(
            {
                **entry,
                "previous_state": previous_tables.get(table_arn or table_name, {}),
                "execution_context": context,
            }
        )
    return manager, prepared_entries


def snapshot_manager_set_active_session(
    manager: Dict[str, Any],
    session: Any,
    *,
    source: str,
    assumed_role_arn: Optional[str],
) -> Dict[str, Any]:
    _ = source
    next_manager = dict(manager)
    next_manager["session"] = session
    next_manager["_assume_session"] = session
    next_manager["default_region"] = _safe_str_field(getattr(session, "region_name", None), field_name="session.region_name", required=False)
    next_manager["_active_assume_role_arn"] = assumed_role_arn
    next_manager["_table_client_cache"] = {}
    if hasattr(session, "client"):
        resolved_region = _safe_str_field(next_manager.get("default_region"), field_name="default_region", required=False)
        next_manager["ddb"] = session.client("dynamodb", region_name=resolved_region or None)
        next_manager["s3"] = session.client("s3")
    return next_manager


def _build_target_from_execution_context(table_name: str, table_arn: str, execution_context: Dict[str, Any]) -> TableTarget:
    resolved_table_arn = table_arn if table_arn.startswith("arn:") else _safe_str_field(
        execution_context.get("table_arn"),
        field_name="execution_context.table_arn",
        required=False,
    ) or table_arn
    if resolved_table_arn.startswith("arn:"):
        context = _extract_table_arn_context(resolved_table_arn, field_name="table_arn")
        return TableTarget(
            raw_ref=resolved_table_arn,
            table_name=_safe_str_field(context.get("table_name"), field_name="table_name"),
            table_arn=resolved_table_arn,
            account_id=_safe_str_field(context.get("account_id"), field_name="account_id"),
            region=_safe_str_field(context.get("region"), field_name="region"),
        )
    return TableTarget(
        raw_ref=table_name,
        table_name=table_name,
        table_arn=resolved_table_arn,
        account_id=_safe_str_field(execution_context.get("table_account_id"), field_name="execution_context.table_account_id", required=False),
        region=_safe_str_field(execution_context.get("table_region"), field_name="execution_context.table_region"),
    )


def snapshot_manager_start_full_export(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    ddb_client: Any,
    execution_context: Dict[str, Any],
) -> Dict[str, Any]:
    target = _build_target_from_execution_context(table_name, table_arn, execution_context)
    base_config = _safe_dict_field(manager.get("config"), "manager.config")
    config = {
        **base_config,
        "pitr_auto_enable": base_config.get("pitr_auto_enable", True),
    }
    bucket = snapshot_manager_build_bucket_name(
        _safe_str_field(config.get("bucket"), field_name="bucket"),
        target.region,
    )
    return _start_full_export(
        config=config,
        target=target,
        ddb_client=ddb_client,
        bucket=bucket,
        bucket_owner=_safe_str_field(config.get("bucket_owner"), field_name="bucket_owner", required=False),
        assume_role_arn=_safe_str_field(execution_context.get("assume_role_arn"), field_name="execution_context.assume_role_arn", required=False),
    )


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
    _ = s3_client
    target = _build_target_from_execution_context(table_name, table_arn, execution_context)
    base_config = _safe_dict_field(manager.get("config"), "manager.config")
    config = {
        **base_config,
        "pitr_auto_enable": base_config.get("pitr_auto_enable", True),
    }
    bucket = snapshot_manager_build_bucket_name(
        _safe_str_field(config.get("bucket"), field_name="bucket"),
        target.region,
    )
    incremental_index = _coerce_non_negative_int(
        incremental_reference.get("incremental_index"),
        field_name="incremental_reference.incremental_index",
        default=1,
    ) or 1
    return _start_incremental_export(
        config=config,
        target=target,
        ddb_client=ddb_client,
        bucket=bucket,
        bucket_owner=_safe_str_field(config.get("bucket_owner"), field_name="bucket_owner", required=False),
        export_from=export_from,
        export_to=export_to,
        incremental_index=incremental_index,
        checkpoint_source=_safe_str_field(incremental_reference.get("checkpoint_source"), field_name="incremental_reference.checkpoint_source"),
        assume_role_arn=_safe_str_field(execution_context.get("assume_role_arn"), field_name="execution_context.assume_role_arn", required=False),
    )


def snapshot_manager_find_latest_full_export_checkpoint(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    *,
    s3_client: Any,
    execution_context: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    _ = table_arn
    config = _safe_dict_field(manager.get("config"), "manager.config")
    bucket = snapshot_manager_build_bucket_name(
        _safe_str_field(config.get("bucket"), field_name="bucket"),
        _safe_str_field(execution_context.get("table_region"), field_name="execution_context.table_region"),
    )
    prefix = f"DDB/{_safe_str_field(execution_context.get('table_account_id'), field_name='execution_context.table_account_id')}/{table_name}/FULL"
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents")
        if isinstance(contents, list) and contents:
            return contents[-1]
    return None


def _parse_new_layout_export_key(key: str) -> Optional[Dict[str, str]]:
    parts = [part for part in _safe_str_field(key, field_name="key").split("/") if part]
    if len(parts) < 5 or parts[0] != "DDB":
        return None
    if AWS_ACCOUNT_ID_PATTERN.fullmatch(parts[1]):
        run_part = parts[4]
        match = re.search(r"run_id=(\d{8})T", run_part)
        if not match:
            return None
        return {
            "export_date": match.group(1),
            "account_id": parts[1],
            "table_name": parts[2],
            "export_type": parts[3],
        }
    if len(parts) < 6 or not AWS_ACCOUNT_ID_PATTERN.fullmatch(parts[2]):
        return None
    return {
        "export_date": parts[1],
        "account_id": parts[2],
        "table_name": parts[3],
        "export_type": parts[4],
    }


def snapshot_manager_scan_to_s3_partitioned(
    manager: Dict[str, Any],
    table_name: str,
    table_arn: str,
    export_from: datetime,
    export_to: datetime,
    s3_prefix: str,
    *,
    ddb_client: Any,
    s3_client: Any,
    execution_context: Dict[str, Any],
) -> Dict[str, Any]:
    config = _safe_dict_field(manager.get("config"), "manager.config")
    updated_attr = _safe_str_field(config.get("fallback_updated_attr"), field_name="fallback_updated_attr")
    updated_attr_type = _safe_str_field(config.get("fallback_updated_attr_type"), field_name="fallback_updated_attr_type", required=False) or "string"
    if updated_attr_type != "string":
        raise ValueError("fallback_updated_attr_type inválido")

    paginator = ddb_client.get_paginator("scan")
    pages_scanned = 0
    items_written = 0
    for page in paginator.paginate(
        TableName=table_name,
        FilterExpression="#u BETWEEN :from AND :to",
        ExpressionAttributeNames={"#u": updated_attr},
        ExpressionAttributeValues={
            ":from": {"S": _dt_to_iso(export_from)},
            ":to": {"S": _dt_to_iso(export_to)},
        },
    ):
        pages_scanned += 1
        page_items = page.get("Items")
        if isinstance(page_items, list):
            items_written += len(page_items)

    bucket = snapshot_manager_build_bucket_name(
        _safe_str_field(config.get("bucket"), field_name="bucket"),
        _safe_str_field(execution_context.get("table_region"), field_name="execution_context.table_region"),
    )
    manifest_key = f"{s3_prefix}/manifest.json"
    manifest_payload = {
        "table_name": table_name,
        "table_arn": table_arn,
        "files": [],
        "total_items": items_written,
        "total_parts": 0,
        "from": _dt_to_iso(export_from),
        "to": _dt_to_iso(export_to),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(_to_json_safe(manifest_payload), ensure_ascii=False, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return {
        "table_name": table_name,
        "table_arn": table_arn,
        "status": "COMPLETED",
        "source": "scan_fallback",
        "snapshot_bucket": bucket,
        "s3_prefix": s3_prefix,
        "items_written": items_written,
        "files_written": 0,
        "pages_scanned": pages_scanned,
    }


def snapshot_manager_snapshot_table(
    manager: Dict[str, Any],
    entry: Dict[str, Any],
    previous_state: Dict[str, Any],
    execution_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    table_name = _safe_str_field(entry.get("table_name"), field_name="entry.table_name")
    table_arn = _safe_str_field(entry.get("table_arn"), field_name="entry.table_arn")
    resolved = execution_context
    if resolved is None:
        execution_resolution = snapshot_manager_resolve_execution_context(manager, table_name, table_arn)
        _, resolved = execution_resolution if isinstance(execution_resolution, tuple) else (manager, execution_resolution)
    snapshot_manager_validate_export_session_account(manager, table_name, table_arn, resolved)

    reconciled_state = snapshot_manager_reconcile_pending_exports(
        previous_state,
        table_name,
        table_arn,
        ddb_client=resolved.get("ddb"),
    )
    pending_exports = _normalize_pending_exports(reconciled_state.get("pending_exports"))
    if pending_exports:
        return {
            "table_name": table_name,
            "table_arn": table_arn,
            "mode": "INCREMENTAL",
            "status": "PENDING",
            "source": "pending_export_tracking",
            "pending_exports": pending_exports,
            "checkpoint_state": _compact_legacy_checkpoint_state(reconciled_state, state_key=table_arn),
        }

    config = _safe_dict_field(manager.get("config"), "manager.config")
    mode = _safe_str_field(config.get("mode"), field_name="mode", required=False).lower() or "full"
    if mode == "incremental" and _safe_str_field(reconciled_state.get("last_to"), field_name="last_to", required=False):
        last_to = _parse_iso_datetime(reconciled_state.get("last_to"))
        run_time = config.get("run_time")
        if not isinstance(last_to, datetime) or not isinstance(run_time, datetime):
            raise RuntimeError("Janela incremental inválida")
        export_from = last_to
        export_to = min(run_time, export_from + INCREMENTAL_EXPORT_MAX_WINDOW)
        if (export_to - export_from) < INCREMENTAL_EXPORT_MIN_WINDOW:
            return {
                "table_name": table_name,
                "table_arn": table_arn,
                "mode": "INCREMENTAL",
                "status": "SKIPPED",
                "source": "incremental_window_guard",
                "message": "Janela incremental menor que 15 minutos; export não iniciado.",
                "checkpoint_state": _compact_legacy_checkpoint_state(reconciled_state, state_key=table_arn),
            }
        return snapshot_manager_start_incremental_export(
            manager,
            table_name,
            table_arn,
            export_from,
            export_to,
            ddb_client=resolved.get("ddb"),
            s3_client=resolved.get("s3"),
            execution_context=resolved,
            incremental_reference={
                "checkpoint_from": _dt_to_iso(export_from),
                "checkpoint_source": "checkpoint",
                "incremental_index": 1,
            },
        )

    return snapshot_manager_start_full_export(
        manager,
        table_name,
        table_arn,
        ddb_client=resolved.get("ddb"),
        execution_context=resolved,
    )


def _resolve_export_job_id(export_arn: str) -> str:
    value = _safe_str_field(export_arn, field_name="export_arn", required=False)
    if not value:
        return ""
    return value.rsplit("/", 1)[-1]


def _resolve_tracking_export_job_ids(row: Dict[str, Any]) -> List[str]:
    job_ids: List[str] = []
    seen: set[str] = set()

    export_arn = _resolve_output_row_export_arn(row)
    if export_arn:
        job_id = _resolve_export_job_id(export_arn)
        if job_id and job_id not in seen:
            seen.add(job_id)
            job_ids.append(job_id)

    pending_exports = row.get("pending_exports")
    if not isinstance(pending_exports, list):
        return job_ids

    for pending_export in pending_exports:
        if not isinstance(pending_export, dict):
            continue
        pending_export_arn = _safe_str_field(
            pending_export.get("export_arn"),
            field_name="pending.export_arn",
            required=False,
        )
        pending_job_id = _resolve_export_job_id(pending_export_arn)
        if not pending_job_id or pending_job_id in seen:
            continue
        seen.add(pending_job_id)
        job_ids.append(pending_job_id)

    return job_ids


def _build_output_table_events(
    source: str,
    payload: Dict[str, Any],
    *,
    aws_request_id: Any,
    snapshot_bucket: Optional[str],
) -> List[Dict[str, Any]]:
    results = payload.get("results")
    if not isinstance(results, list):
        return []

    events: List[Dict[str, Any]] = []
    for row in results:
        if not isinstance(row, dict):
            continue
        table_name = _safe_str_field(row.get("table_name"), field_name="result.table_name", required=False)
        table_arn = _safe_str_field(row.get("table_arn"), field_name="result.table_arn", required=False)
        if not table_name or not table_arn:
            continue

        tracking_export_job_ids = _resolve_tracking_export_job_ids(row)
        pending_exports = _normalize_pending_exports(row.get("pending_exports"))
        checkpoint_from = _resolve_output_row_checkpoint_value(row, "checkpoint_from")
        checkpoint_to = _resolve_output_row_checkpoint_value(row, "checkpoint_to")
        table_status = _safe_str_field(row.get("status"), field_name="result.status", required=False).upper()
        assume_role = _safe_str_field(row.get("assume_role_arn"), field_name="result.assume_role_arn", required=False)
        if not assume_role:
            assume_role = _safe_str_field(row.get("assume_role"), field_name="result.assume_role", required=False)

        export_job_id = _resolve_export_job_id(_safe_str_field(row.get("export_arn"), field_name="result.export_arn", required=False))
        if not export_job_id and tracking_export_job_ids:
            export_job_id = tracking_export_job_ids[0]

        fields = {
            "source": source,
            "aws_request_id": aws_request_id,
            "run_id": payload.get("run_id"),
            "table_name": table_name,
            "table_arn": table_arn,
            "snapshot_bucket": _resolve_optional_text(snapshot_bucket, _safe_str_field(row.get("snapshot_bucket"), field_name="result.snapshot_bucket", required=False)),
            "table_status": table_status,
            "checkpoint_from": checkpoint_from,
            "checkpoint_to": checkpoint_to,
            "pending_export_count": len(pending_exports),
            "tracking_export_job_ids": tracking_export_job_ids,
        }
        if assume_role:
            fields["assume_role"] = assume_role
        if table_status:
            fields["status"] = table_status
        if export_job_id:
            fields["export_job_id"] = export_job_id
        if _safe_str_field(row.get("checkpoint_source"), field_name="result.checkpoint_source", required=False):
            fields["checkpoint_source"] = _safe_str_field(
                row.get("checkpoint_source"),
                field_name="result.checkpoint_source",
                required=False,
            )
        if _safe_str_field(row.get("mode"), field_name="result.mode", required=False):
            fields["mode"] = _safe_str_field(row.get("mode"), field_name="result.mode", required=False)
        if _safe_str_field(row.get("mode_selection_reason"), field_name="result.mode_selection_reason", required=False):
            fields["mode_selection_reason"] = _safe_str_field(
                row.get("mode_selection_reason"),
                field_name="result.mode_selection_reason",
                required=False,
            )
        if row.get("message"):
            fields["message"] = _safe_str_field(row.get("message"), field_name="result.message", required=False)
        if row.get("error"):
            fields["error"] = _safe_str_field(row.get("error"), field_name="result.error", required=False)

        events.append(
            {
                "action": "output.cloudwatch.table",
                "source": source,
                "aws_request_id": aws_request_id,
                "table_name": table_name,
                "table_arn": table_arn,
                "snapshot_bucket": fields["snapshot_bucket"],
                "table_status": fields["table_status"],
                "checkpoint_to": fields["checkpoint_to"],
                "pending_export_count": fields["pending_export_count"],
                "tracking_export_job_ids": fields["tracking_export_job_ids"],
                "checkpoint_from": fields["checkpoint_from"],
                "status": table_status,
                "table_status_reason": fields.get("status"),
                "source": source,
                "run_id": payload.get("run_id"),
                "assume_role": fields.get("assume_role"),
                "export_job_id": fields.get("export_job_id"),
                "checkpoint_source": fields.get("checkpoint_source"),
                "mode": fields.get("mode"),
                "mode_selection_reason": fields.get("mode_selection_reason"),
                "message": fields.get("message"),
                "error": fields.get("error"),
            }
        )

    return events


def _resolve_checkpoint_target(config: Dict[str, Any]) -> str:
    return _safe_str_field(config.get("checkpoint_dynamodb_table_arn"), field_name="checkpoint_dynamodb_table_arn", required=False)


def create_snapshot_manager(config: Dict[str, Any]) -> Dict[str, Any]:
    session = _build_aws_session(config)
    return {
        "config": config,
        "session": session,
        "_assume_session": session,
        "_output_session": session,
        "default_region": _resolve_runtime_region(getattr(session, "region_name", None)),
    }


def _snapshot_manager_run_legacy(manager: Dict[str, Any], event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    _ = event
    checkpoint_store = manager.get("checkpoint_store")
    if not isinstance(checkpoint_store, dict):
        raise RuntimeError("manager.checkpoint_store ausente")

    snapshot_manager_prime_assumed_session_from_direct_targets(manager)
    targets = snapshot_manager_resolve_targets(manager)
    ignore = {value.lower() for value in snapshot_manager_resolve_ignore(manager)}
    checkpoint_payload = checkpoint_load(checkpoint_store)
    previous_tables = checkpoint_payload.get("tables") if isinstance(checkpoint_payload.get("tables"), dict) else {}

    execution_entries = []
    for raw_target in targets:
        table_arn = raw_target if raw_target.startswith("arn:") else raw_target
        table_name = _extract_table_arn_context(raw_target).get("table_name") if raw_target.startswith("arn:") else raw_target
        entry = {
            "table_name": table_name,
            "table_arn": table_arn,
        }
        if table_name.lower() in ignore or table_arn.lower() in ignore:
            continue
        execution_entries.append(entry)

    snapshot_manager_prime_assumed_session_from_targets(manager, execution_entries)

    results: List[Dict[str, Any]] = []
    next_tables: Dict[str, Any] = {}
    for entry in execution_entries:
        state_key = _safe_str_field(entry.get("table_arn"), field_name="entry.table_arn", required=False) or _safe_str_field(
            entry.get("table_name"),
            field_name="entry.table_name",
        )
        previous_state = previous_tables.get(state_key, {})
        result = snapshot_manager_snapshot_table(manager, entry, previous_state)
        results.append(result)
        if isinstance(result.get("checkpoint_state"), dict):
            next_tables[state_key] = result["checkpoint_state"]
            continue
        if _safe_str_field(result.get("status"), field_name="result.status", required=False).upper() == "COMPLETED":
            next_tables[state_key] = snapshot_manager_apply_result_to_checkpoint_state(
                manager,
                snapshot_manager_build_checkpoint_state(entry, previous_state),
                result,
            )

    checkpoint_save(
        checkpoint_store,
        {
            "version": 1,
            "tables": next_tables,
        },
    )
    return snapshot_manager_build_run_response(manager, results, None, {})


def snapshot_manager_run(manager: Dict[str, Any], event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(manager.get("checkpoint_store"), dict):
        return _snapshot_manager_run_legacy(manager, event)
    config = _safe_dict_field(manager.get("config"), "manager.config")
    return _run_snapshot(config, event)


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
        persistence_errors = _persist_checkpoint_results(checkpoint_store, results)
        if persistence_errors:
            first_exception = persistence_errors[0]["exception"]
            checkpoint_error = "; ".join(
                f"{item['table_name']}: {item['error']}"
                for item in persistence_errors
            )
            checkpoint_error_feedback = _build_error_response_fields(first_exception)
            for item in persistence_errors:
                _log_event(
                    "snapshot.checkpoint.failed",
                    table_name=item["table_name"],
                    table_arn=item["table_arn"],
                    state_key=item["state_key"],
                    error=item["error"],
                    level=logging.ERROR,
                )

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
    config: Optional[Dict[str, Any]],
    context: Any,
) -> None:
    resolved_config = config if isinstance(config, dict) else {}
    if not bool(resolved_config.get("output_cloudwatch_enabled", True)):
        return

    aws_request_id = getattr(context, "aws_request_id", None)
    snapshot_bucket = _resolve_optional_text(
        payload.get("snapshot_bucket"),
        resolved_config.get("bucket"),
    )

    _log_event(
        "output.cloudwatch",
        source=source,
        aws_request_id=aws_request_id,
        run_id=payload.get("run_id"),
        snapshot_bucket=snapshot_bucket,
        output=payload,
    )

    for table_event in _build_output_table_events(
        source,
        payload,
        aws_request_id=aws_request_id,
        snapshot_bucket=snapshot_bucket,
    ):
        action = _safe_str_field(table_event.get("action"), field_name="table_event.action")
        fields = {
            key: value
            for key, value in table_event.items()
            if key != "action"
        }
        _log_event(action, **fields)


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


def _resolve_output_row_checkpoint_value(row: Dict[str, Any], field_name: str) -> str:
    direct_value = _safe_str_field(
        row.get(field_name),
        field_name=f"result.{field_name}",
        required=False,
    )
    if direct_value:
        return direct_value

    pending_exports = row.get("pending_exports")
    if not isinstance(pending_exports, list):
        return ""

    for item in pending_exports:
        if not isinstance(item, dict):
            continue
        pending_value = _safe_str_field(
            item.get(field_name),
            field_name=f"pending.{field_name}",
            required=False,
        )
        if pending_value:
            return pending_value
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

    resolved_session = session or _build_aws_session(config)

    table_name = _resolve_optional_text(config.get("output_dynamodb_table"))
    if not table_name:
        return

    region = _resolve_optional_text(config.get("output_dynamodb_region")) or _resolve_runtime_region(
        getattr(resolved_session, "region_name", None)
    )
    if not region:
        _log_event("output.dynamodb.write.skipped", reason="region_not_resolved", table_name=table_name, level=logging.WARNING)
        return

    ddb_client = _get_session_client(resolved_session, "dynamodb", region_name=region)
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
    pitr_auto_enable = _resolve_env_first_bool(
        _resolve_optional_text(payload.get("pitr_auto_enable"), payload.get("pitrAutoEnable")),
        "PITR_AUTO_ENABLE",
        False,
    )

    incremental_export_view_type = _resolve_incremental_export_view_type(
        os.getenv("INCREMENTAL_EXPORT_VIEW_TYPE"),
        payload.get("incremental_export_view_type"),
        payload.get("incrementalExportViewType"),
    )
    max_incremental_exports_per_cycle = _resolve_max_incremental_exports_per_cycle(
        os.getenv("MAX_INCREMENTAL_EXPORTS_PER_CYCLE"),
        payload.get("max_incremental_exports_per_cycle"),
        payload.get("maxIncrementalExportsPerCycle"),
    )

    output_cloudwatch_enabled = _resolve_env_first_bool(
        _resolve_optional_text(payload.get("output_cloudwatch_enabled"), payload.get("outputCloudwatchEnabled")),
        "OUTPUT_CLOUDWATCH_ENABLED",
        True,
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

    checkpoint_bucket = _resolve_optional_text(
        os.getenv("CHECKPOINT_BUCKET", ""),
        payload.get("checkpoint_bucket"),
        payload.get("checkpointBucket"),
        bucket,
    )
    checkpoint_key = _resolve_optional_text(
        os.getenv("CHECKPOINT_KEY", ""),
        payload.get("checkpoint_key"),
        payload.get("checkpointKey"),
        "snapshots/_checkpoint.json",
    )

    checkpoint_dynamodb_table_arn = _resolve_optional_text(
        os.getenv("CHECKPOINT_DYNAMODB_TABLE_ARN", ""),
        payload.get("checkpoint_dynamodb_table_arn"),
        payload.get("checkpointDynamodbTableArn"),
    )
    if output_dynamodb_enabled and not output_dynamodb_table:
        raise ValueError("OUTPUT_DYNAMODB_TABLE deve ser informado quando OUTPUT_DYNAMODB_ENABLED=true")
    if not checkpoint_dynamodb_table_arn and not (checkpoint_bucket and checkpoint_key):
        raise ValueError("CHECKPOINT_DYNAMODB_TABLE_ARN não definido")
    if checkpoint_dynamodb_table_arn:
        _extract_table_arn_context(checkpoint_dynamodb_table_arn, field_name="checkpoint_dynamodb_table_arn")

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
        "pitr_auto_enable": pitr_auto_enable,
        "incremental_export_view_type": incremental_export_view_type,
        "max_incremental_exports_per_cycle": max_incremental_exports_per_cycle,
        "checkpoint_dynamodb_table_arn": checkpoint_dynamodb_table_arn,
        "checkpoint_bucket": checkpoint_bucket,
        "checkpoint_key": checkpoint_key,
        "assume_role_arn": assume_role,
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
        pitr_auto_enable=pitr_auto_enable,
        incremental_export_view_type=incremental_export_view_type,
        max_incremental_exports_per_cycle=max_incremental_exports_per_cycle,
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
        manager = create_snapshot_manager(config)
        run_result = snapshot_manager_run(manager, event)

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

        if emit_cloudwatch_output and bool(config.get("output_cloudwatch_enabled", True)):
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
            session=manager.get("_output_session") or manager.get("_assume_session") or manager.get("session"),
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
        if emit_cloudwatch_output:
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
        if emit_cloudwatch_output:
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
        if emit_cloudwatch_output:
            _emit_output_to_cloudwatch("lambda_handler.runtime_error", response, config=config, context=context)
        return response
