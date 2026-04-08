"""AWS Lambda e CLI local para inventariar ARNs de tabelas DynamoDB e publicar CSV no S3.

Handler
=======

- `lambda_dynamodb_table_arns_csv.lambda_handler`

Visão geral
===========

Esta Lambda consulta o Cloud Control API para listar recursos do tipo
`AWS::DynamoDB::Table`, resolve o ARN de cada tabela e publica um CSV em um
bucket S3 escolhido em tempo de execução.

Fluxo:
1. resolve bucket, chave e regiões a partir do ambiente e/ou evento;
2. consulta `cloudcontrol.list_resources` por região;
3. extrai `TableArn`/`Arn` dos metadados quando disponível;
4. usa `dynamodb.describe_table` apenas quando o ARN não vier no metadata;
5. gera um CSV UTF-8 e faz `put_object` no bucket S3 de destino.

Quando uma `query` for informada, a Lambda muda para modo Athena:
1. executa a query no Athena;
2. aguarda conclusão;
3. lê o result set;
4. publica o CSV final no bucket escolhido.

Execução local
==============

O mesmo arquivo também pode ser executado localmente:

```bash
python3 lambda_dynamodb_table_arns_csv.py \
  --bucket meu-bucket \
  --prefix inventarios/dynamodb \
  --query "select table_arn from catalogo.schema.view"
```

Variáveis de ambiente
=====================

- `OUTPUT_BUCKET`, `S3_BUCKET` ou `DESTINATION_BUCKET`
- `OUTPUT_KEY` ou `S3_KEY`
- `OUTPUT_PREFIX` ou `S3_PREFIX`
- `REGIONS`, `AWS_REGIONS` ou `CLOUDCONTROL_REGIONS`
- `S3_REGION`
- `QUERY`, `ATHENA_QUERY`
- `ATHENA_DATABASE`
- `ATHENA_WORKGROUP`
- `ATHENA_CATALOG`
- `ATHENA_RESULT_PREFIX`
- `ATHENA_REGION`
- `LOG_LEVEL`

Payload suportado
=================

```json
{
  "bucket": "meu-bucket-de-relatorios",
  "prefix": "inventarios/dynamodb",
  "regions": ["sa-east-1", "us-east-1"],
  "query": "select table_arn from meu_schema.minha_view"
}
```

Campos aceitos no evento:
- `bucket`, `output_bucket`, `s3_bucket`, `destination_bucket`
- `key`, `output_key`, `s3_key`
- `prefix`, `output_prefix`, `s3_prefix`
- `regions`, `aws_regions`, `cloudcontrol_regions`
- `s3_region`
- `query`, `athena_query`
- `athena_database`
- `athena_workgroup`
- `athena_catalog`
- `athena_result_prefix`
- `athena_region`
- `mode`

Precedência
===========

As variáveis de ambiente têm precedência sobre o payload.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

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


DYNAMODB_TABLE_ARN_PATTERN = re.compile(
    r"^arn:(?P<partition>aws[a-zA-Z-]*)?:dynamodb:(?P<region>[^:]+):(?P<account_id>\d{12}):table/(?P<table_name>[^/]+)$"
)

LOGGER = logging.getLogger(__name__)


class DataIntegrityError(Exception):
    pass


def _configure_logging() -> None:
    log_level = _resolve_optional_text(
        os.environ.get("LOG_LEVEL"),
        fallback="INFO",
        field_name="LOG_LEVEL",
        required=False,
    ).upper()
    LOGGER.setLevel(getattr(logging, log_level, logging.INFO))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _build_aws_session() -> Any:
    return boto3.session.Session()


def _resolve_optional_text(
    *values: Any,
    fallback: Optional[str] = None,
    field_name: str,
    required: bool = False,
) -> str:
    for value in values:
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
    if fallback is not None:
        return fallback
    if required:
        raise ValueError(f"{field_name} é obrigatório")
    return ""


def _parse_regions(value: Any) -> list[str]:
    if isinstance(value, str):
        return sorted({token.strip() for token in value.split(",") if token.strip()})
    if isinstance(value, list):
        normalized = {
            item.strip()
            for item in value
            if isinstance(item, str) and item.strip()
        }
        return sorted(normalized)
    return []


def _parse_optional_int(
    *values: Any,
    fallback: Optional[int],
    field_name: str,
    minimum: Optional[int] = None,
) -> int:
    for value in values:
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, int):
            parsed_value = value
        elif isinstance(value, str) and value.strip():
            parsed_value = int(value.strip())
        else:
            continue

        if minimum is not None and parsed_value < minimum:
            raise ValueError(f"{field_name} deve ser >= {minimum}")
        return parsed_value

    if fallback is None:
        raise ValueError(f"{field_name} é obrigatório")
    if minimum is not None and fallback < minimum:
        raise ValueError(f"{field_name} deve ser >= {minimum}")
    return fallback


def _resolve_regions(payload: dict[str, Any]) -> list[str]:
    regions = _parse_regions(
        os.environ.get("REGIONS")
        or os.environ.get("AWS_REGIONS")
        or os.environ.get("CLOUDCONTROL_REGIONS")
    )
    if regions:
        return regions

    event_regions = _parse_regions(
        payload.get("regions")
        or payload.get("aws_regions")
        or payload.get("cloudcontrol_regions")
    )
    if event_regions:
        return event_regions

    fallback_region = _resolve_optional_text(
        os.environ.get("AWS_REGION"),
        os.environ.get("AWS_DEFAULT_REGION"),
        payload.get("region"),
        payload.get("aws_region"),
        field_name="region",
        required=True,
    )
    return [fallback_region]


def _build_output_key(payload: dict[str, Any], generated_at: datetime) -> str:
    configured_key = _resolve_optional_text(
        os.environ.get("OUTPUT_KEY"),
        os.environ.get("S3_KEY"),
        payload.get("key"),
        payload.get("output_key"),
        payload.get("s3_key"),
        field_name="key",
        required=False,
    )
    if configured_key:
        return configured_key

    prefix = _resolve_optional_text(
        os.environ.get("OUTPUT_PREFIX"),
        os.environ.get("S3_PREFIX"),
        payload.get("prefix"),
        payload.get("output_prefix"),
        payload.get("s3_prefix"),
        fallback="inventories/dynamodb-table-arns",
        field_name="prefix",
        required=False,
    ).strip("/")
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}/dynamodb-table-arns-{timestamp}.csv"


def _resolve_mode(payload: dict[str, Any]) -> str:
    explicit_mode = _resolve_optional_text(
        os.environ.get("INVENTORY_MODE"),
        payload.get("mode"),
        field_name="mode",
        required=False,
    ).lower()
    if explicit_mode:
        if explicit_mode not in {"cloudcontrol", "athena"}:
            raise ValueError("mode deve ser cloudcontrol ou athena")
        return explicit_mode

    query = _resolve_optional_text(
        os.environ.get("QUERY"),
        os.environ.get("ATHENA_QUERY"),
        payload.get("query"),
        payload.get("athena_query"),
        field_name="query",
        required=False,
    )
    return "athena" if query else "cloudcontrol"


def _resolve_athena_query(payload: dict[str, Any]) -> str:
    return _resolve_optional_text(
        os.environ.get("QUERY"),
        os.environ.get("ATHENA_QUERY"),
        payload.get("query"),
        payload.get("athena_query"),
        field_name="query",
        required=True,
    )


def _build_athena_result_output_location(
    *,
    bucket: str,
    payload: dict[str, Any],
    generated_at: datetime,
) -> str:
    prefix = _resolve_optional_text(
        os.environ.get("ATHENA_RESULT_PREFIX"),
        payload.get("athena_result_prefix"),
        fallback="athena-query-results/dynamodb-table-arns",
        field_name="athena_result_prefix",
        required=False,
    ).strip("/")
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    return f"s3://{bucket}/{prefix}/{timestamp}/"


def build_lambda_config(event: Optional[dict[str, Any]]) -> dict[str, Any]:
    payload = event if isinstance(event, dict) else {}
    generated_at = _now_utc()
    mode = _resolve_mode(payload)
    bucket = _resolve_optional_text(
        os.environ.get("OUTPUT_BUCKET"),
        os.environ.get("S3_BUCKET"),
        os.environ.get("DESTINATION_BUCKET"),
        payload.get("bucket"),
        payload.get("output_bucket"),
        payload.get("s3_bucket"),
        payload.get("destination_bucket"),
        field_name="bucket",
        required=True,
    )
    s3_region = _resolve_optional_text(
        os.environ.get("S3_REGION"),
        payload.get("s3_region"),
        payload.get("output_region"),
        field_name="s3_region",
        required=False,
    )
    regions = _resolve_regions(payload)
    key = _build_output_key(payload, generated_at)
    base_config = {
        "generated_at": generated_at,
        "mode": mode,
        "bucket": bucket,
        "key": key,
        "regions": regions,
        "s3_region": s3_region or regions[0],
    }
    if mode == "cloudcontrol":
        return base_config

    athena_region = _resolve_optional_text(
        os.environ.get("ATHENA_REGION"),
        payload.get("athena_region"),
        fallback=regions[0],
        field_name="athena_region",
        required=False,
    )
    return {
        **base_config,
        "query": _resolve_athena_query(payload),
        "athena_database": _resolve_optional_text(
            os.environ.get("ATHENA_DATABASE"),
            payload.get("athena_database"),
            field_name="athena_database",
            required=False,
        ),
        "athena_workgroup": _resolve_optional_text(
            os.environ.get("ATHENA_WORKGROUP"),
            payload.get("athena_workgroup"),
            fallback="primary",
            field_name="athena_workgroup",
            required=False,
        ),
        "athena_catalog": _resolve_optional_text(
            os.environ.get("ATHENA_CATALOG"),
            payload.get("athena_catalog"),
            field_name="athena_catalog",
            required=False,
        ),
        "athena_region": athena_region,
        "athena_poll_interval_seconds": _parse_optional_int(
            os.environ.get("ATHENA_POLL_INTERVAL_SECONDS"),
            payload.get("athena_poll_interval_seconds"),
            fallback=2,
            field_name="athena_poll_interval_seconds",
            minimum=0,
        ),
        "athena_max_poll_attempts": _parse_optional_int(
            os.environ.get("ATHENA_MAX_POLL_ATTEMPTS"),
            payload.get("athena_max_poll_attempts"),
            fallback=60,
            field_name="athena_max_poll_attempts",
            minimum=1,
        ),
        "athena_result_output_location": _build_athena_result_output_location(
            bucket=bucket,
            payload=payload,
            generated_at=generated_at,
        ),
    }


def _parse_json_object(raw_value: Any) -> dict[str, Any]:
    if not isinstance(raw_value, str):
        return {}
    raw_value = raw_value.strip()
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_table_name_from_arn(table_arn: str) -> str:
    match = DYNAMODB_TABLE_ARN_PATTERN.match(table_arn)
    if not match:
        return ""
    return match.group("table_name")


def _extract_region_from_arn(table_arn: str) -> str:
    match = DYNAMODB_TABLE_ARN_PATTERN.match(table_arn)
    if not match:
        return ""
    return match.group("region")


def _extract_account_id_from_arn(table_arn: str) -> str:
    match = DYNAMODB_TABLE_ARN_PATTERN.match(table_arn)
    if not match:
        return ""
    return match.group("account_id")


def _is_dynamodb_table_arn(value: str) -> bool:
    return bool(DYNAMODB_TABLE_ARN_PATTERN.match(value))


def _normalize_column_names(columns: list[str]) -> list[str]:
    return [column.strip().lower() for column in columns]


def _validate_non_empty_unique_columns(columns: list[str], *, source_label: str) -> None:
    if not columns:
        raise DataIntegrityError(f"{source_label} não retornou colunas")

    normalized_columns = _normalize_column_names(columns)
    if any(not column for column in normalized_columns):
        raise DataIntegrityError(f"{source_label} retornou coluna vazia")

    duplicated_columns = sorted(
        {
            column
            for column in normalized_columns
            if normalized_columns.count(column) > 1
        }
    )
    if duplicated_columns:
        raise DataIntegrityError(
            f"{source_label} retornou colunas duplicadas: {', '.join(duplicated_columns)}"
        )


def _column_index_by_name(columns: list[str]) -> dict[str, int]:
    return {
        column_name: index
        for index, column_name in enumerate(_normalize_column_names(columns))
    }


def _extract_row_value(row: list[str], index: Optional[int]) -> str:
    if index is None:
        return ""
    if index < 0 or index >= len(row):
        return ""
    return row[index].strip()


def _validate_table_arn_consistency(
    *,
    table_arn: str,
    table_name: str,
    region: str,
    account_id: str,
    source_label: str,
    row_number: int,
) -> None:
    if not table_arn:
        raise DataIntegrityError(
            f"{source_label} retornou table_arn vazio na linha {row_number}"
        )

    if not _is_dynamodb_table_arn(table_arn):
        raise DataIntegrityError(
            f"{source_label} retornou table_arn inválido na linha {row_number}: {table_arn}"
        )

    expected_table_name = _extract_table_name_from_arn(table_arn)
    expected_region = _extract_region_from_arn(table_arn)
    expected_account_id = _extract_account_id_from_arn(table_arn)

    if table_name and table_name != expected_table_name:
        raise DataIntegrityError(
            f"{source_label} retornou table_name inconsistente na linha {row_number}: "
            f"esperado {expected_table_name}, recebido {table_name}"
        )

    if region and region != expected_region:
        raise DataIntegrityError(
            f"{source_label} retornou region inconsistente na linha {row_number}: "
            f"esperado {expected_region}, recebido {region}"
        )

    if account_id and account_id != expected_account_id:
        raise DataIntegrityError(
            f"{source_label} retornou account_id inconsistente na linha {row_number}: "
            f"esperado {expected_account_id}, recebido {account_id}"
        )


def _validate_unique_table_arns(table_arns: list[str], *, source_label: str) -> None:
    duplicated_table_arns = sorted(
        {
            table_arn
            for table_arn in table_arns
            if table_arns.count(table_arn) > 1
        }
    )
    if duplicated_table_arns:
        raise DataIntegrityError(
            f"{source_label} retornou table_arn duplicado: {', '.join(duplicated_table_arns)}"
        )


def _pick_table_arn_candidate(resource_description: dict[str, Any]) -> str:
    identifier = _resolve_optional_text(
        resource_description.get("Identifier"),
        field_name="Identifier",
        required=False,
    )
    properties = _parse_json_object(resource_description.get("Properties"))
    candidates = (
        properties.get("TableArn"),
        properties.get("Arn"),
        properties.get("ARN"),
        identifier,
    )
    for candidate in candidates:
        if isinstance(candidate, str):
            normalized = candidate.strip()
            if normalized and _is_dynamodb_table_arn(normalized):
                return normalized
    return ""


def _resolve_table_identifier(resource_description: dict[str, Any]) -> str:
    identifier = _resolve_optional_text(
        resource_description.get("Identifier"),
        field_name="Identifier",
        required=False,
    )
    if _is_dynamodb_table_arn(identifier):
        return _extract_table_name_from_arn(identifier)
    return identifier


def _describe_table_arn(dynamodb_client: Any, identifier: str) -> str:
    if not identifier:
        return ""
    response = dynamodb_client.describe_table(TableName=identifier)
    table = response.get("Table") if isinstance(response, dict) else None
    if not isinstance(table, dict):
        return ""
    return _resolve_optional_text(
        table.get("TableArn"),
        field_name="TableArn",
        required=False,
    )


def _list_cloudcontrol_table_descriptions(cloudcontrol_client: Any) -> list[dict[str, Any]]:
    descriptions: list[dict[str, Any]] = []
    next_token: Optional[str] = None

    while True:
        request: dict[str, Any] = {
            "TypeName": "AWS::DynamoDB::Table",
            "MaxResults": 100,
        }
        if next_token:
            request["NextToken"] = next_token

        response = cloudcontrol_client.list_resources(**request)
        page_descriptions = response.get("ResourceDescriptions", []) if isinstance(response, dict) else []
        descriptions.extend(
            description for description in page_descriptions if isinstance(description, dict)
        )
        next_token = response.get("NextToken") if isinstance(response, dict) else None
        if not next_token:
            return descriptions


def _build_inventory_rows_for_region(
    *,
    session: Any,
    region: str,
) -> list[dict[str, str]]:
    cloudcontrol_client = session.client("cloudcontrol", region_name=region)
    dynamodb_client = session.client("dynamodb", region_name=region)
    rows: list[dict[str, str]] = []

    for resource_description in _list_cloudcontrol_table_descriptions(cloudcontrol_client):
        table_arn = _pick_table_arn_candidate(resource_description)
        identifier = _resolve_table_identifier(resource_description)
        if not table_arn:
            table_arn = _describe_table_arn(dynamodb_client, identifier)
        if not table_arn:
            continue

        rows.append(
            {
                "table_arn": table_arn,
                "table_name": _extract_table_name_from_arn(table_arn) or identifier,
                "region": _extract_region_from_arn(table_arn) or region,
                "account_id": _extract_account_id_from_arn(table_arn),
            }
        )

    deduplicated_rows = {row["table_arn"]: row for row in rows}
    return [deduplicated_rows[table_arn] for table_arn in sorted(deduplicated_rows)]


def _build_inventory_rows(config: dict[str, Any]) -> list[dict[str, str]]:
    session = _build_aws_session()
    rows_by_region = [
        _build_inventory_rows_for_region(session=session, region=region)
        for region in config["regions"]
    ]
    merged_rows = {
        row["table_arn"]: row
        for rows in rows_by_region
        for row in rows
    }
    return [merged_rows[table_arn] for table_arn in sorted(merged_rows)]


def _validate_cloudcontrol_rows(rows: list[dict[str, str]]) -> None:
    table_arns = [row["table_arn"] for row in rows]
    _validate_unique_table_arns(table_arns, source_label="cloudcontrol")

    for row_number, row in enumerate(rows, start=1):
        _validate_table_arn_consistency(
            table_arn=row.get("table_arn", "").strip(),
            table_name=row.get("table_name", "").strip(),
            region=row.get("region", "").strip(),
            account_id=row.get("account_id", "").strip(),
            source_label="cloudcontrol",
            row_number=row_number,
        )


def _render_csv(*, columns: list[str], rows: list[list[str]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _render_inventory_csv(rows: list[dict[str, str]]) -> str:
    columns = ["table_arn", "table_name", "region", "account_id"]
    ordered_rows = [[row[column] for column in columns] for row in rows]
    return _render_csv(columns=columns, rows=ordered_rows)


def _upload_csv(session: Any, *, bucket: str, key: str, body: str, s3_region: str) -> None:
    s3_client = session.client("s3", region_name=s3_region)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )


def _build_error_response_fields(error: Exception) -> dict[str, str]:
    message = str(error).strip() or error.__class__.__name__
    return {
        "error": message,
        "error_message": message,
    }


def _athena_query_context(config: dict[str, Any]) -> dict[str, str]:
    context: dict[str, str] = {}
    if config["athena_database"]:
        context["Database"] = config["athena_database"]
    if config["athena_catalog"]:
        context["Catalog"] = config["athena_catalog"]
    return context


def _start_athena_query(athena_client: Any, config: dict[str, Any]) -> str:
    request: dict[str, Any] = {
        "QueryString": config["query"],
        "WorkGroup": config["athena_workgroup"],
        "ResultConfiguration": {
            "OutputLocation": config["athena_result_output_location"],
        },
    }
    query_context = _athena_query_context(config)
    if query_context:
        request["QueryExecutionContext"] = query_context
    response = athena_client.start_query_execution(**request)
    return _resolve_optional_text(
        response.get("QueryExecutionId") if isinstance(response, dict) else None,
        field_name="QueryExecutionId",
        required=True,
    )


def _wait_for_athena_query(athena_client: Any, *, query_execution_id: str, config: dict[str, Any]) -> dict[str, Any]:
    attempts_remaining = config["athena_max_poll_attempts"]

    while attempts_remaining > 0:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution = response.get("QueryExecution", {}) if isinstance(response, dict) else {}
        status = query_execution.get("Status", {}) if isinstance(query_execution, dict) else {}
        state = _resolve_optional_text(
            status.get("State"),
            field_name="State",
            required=False,
        )

        if state == "SUCCEEDED":
            return query_execution
        if state in {"FAILED", "CANCELLED"}:
            reason = _resolve_optional_text(
                status.get("StateChangeReason"),
                fallback=f"query Athena terminou com estado {state}",
                field_name="StateChangeReason",
                required=False,
            )
            raise RuntimeError(reason)

        attempts_remaining -= 1
        if attempts_remaining == 0:
            raise TimeoutError(
                f"query Athena não concluiu após {config['athena_max_poll_attempts']} tentativas"
            )
        time.sleep(config["athena_poll_interval_seconds"])

    raise TimeoutError(
        f"query Athena não concluiu após {config['athena_max_poll_attempts']} tentativas"
    )


def _normalize_athena_row(row: dict[str, Any], expected_columns: int) -> list[str]:
    values = [
        datum.get("VarCharValue", "")
        for datum in row.get("Data", [])
        if isinstance(datum, dict)
    ]
    if len(values) > expected_columns:
        raise DataIntegrityError(
            f"resultado da query Athena retornou mais valores do que colunas esperadas: {len(values)} > {expected_columns}"
        )
    padded_values = values + [""] * max(0, expected_columns - len(values))
    return padded_values[:expected_columns]


def _load_athena_result_rows(athena_client: Any, *, query_execution_id: str) -> tuple[list[str], list[list[str]]]:
    columns: list[str] = []
    rows: list[list[str]] = []
    next_token: Optional[str] = None
    is_first_page = True

    while True:
        request: dict[str, Any] = {
            "QueryExecutionId": query_execution_id,
        }
        if next_token:
            request["NextToken"] = next_token

        response = athena_client.get_query_results(**request)
        result_set = response.get("ResultSet", {}) if isinstance(response, dict) else {}
        metadata = result_set.get("ResultSetMetadata", {}) if isinstance(result_set, dict) else {}
        column_info = metadata.get("ColumnInfo", []) if isinstance(metadata, dict) else []
        if not columns:
            columns = [
                _resolve_optional_text(column.get("Name"), field_name="column_name", required=False)
                for column in column_info
                if isinstance(column, dict)
            ]

        page_rows = result_set.get("Rows", []) if isinstance(result_set, dict) else []
        normalized_page_rows = [
            _normalize_athena_row(row, len(columns))
            for row in page_rows
            if isinstance(row, dict)
        ]

        if is_first_page and normalized_page_rows and normalized_page_rows[0] == columns:
            normalized_page_rows = normalized_page_rows[1:]

        rows.extend(normalized_page_rows)
        next_token = response.get("NextToken") if isinstance(response, dict) else None
        if not next_token:
            return columns, rows
        is_first_page = False


def _extract_table_arns_from_athena_rows(columns: list[str], rows: list[list[str]]) -> list[str]:
    lowered_columns = [column.lower() for column in columns]
    if "table_arn" not in lowered_columns:
        return []
    table_arn_index = lowered_columns.index("table_arn")
    return [
        row[table_arn_index].strip()
        for row in rows
        if len(row) > table_arn_index and row[table_arn_index].strip()
    ][:20]


def _validate_athena_rows(columns: list[str], rows: list[list[str]]) -> None:
    _validate_non_empty_unique_columns(columns, source_label="resultado da query Athena")
    column_indexes = _column_index_by_name(columns)

    if "table_arn" not in column_indexes:
        raise DataIntegrityError(
            "resultado da query Athena deve conter a coluna table_arn"
        )

    extracted_table_arns: list[str] = []
    for row_number, row in enumerate(rows, start=1):
        if len(row) != len(columns):
            raise DataIntegrityError(
                f"resultado da query Athena retornou linha {row_number} com {len(row)} colunas; esperado {len(columns)}"
            )

        table_arn = _extract_row_value(row, column_indexes.get("table_arn"))
        table_name = _extract_row_value(row, column_indexes.get("table_name"))
        region = _extract_row_value(row, column_indexes.get("region"))
        account_id = _extract_row_value(row, column_indexes.get("account_id"))

        _validate_table_arn_consistency(
            table_arn=table_arn,
            table_name=table_name,
            region=region,
            account_id=account_id,
            source_label="resultado da query Athena",
            row_number=row_number,
        )
        extracted_table_arns.append(table_arn)

    _validate_unique_table_arns(
        extracted_table_arns,
        source_label="resultado da query Athena",
    )


def _run_athena_inventory(config: dict[str, Any]) -> dict[str, Any]:
    session = _build_aws_session()
    athena_client = session.client("athena", region_name=config["athena_region"])
    query_execution_id = _start_athena_query(athena_client, config)
    query_execution = _wait_for_athena_query(
        athena_client,
        query_execution_id=query_execution_id,
        config=config,
    )
    columns, rows = _load_athena_result_rows(
        athena_client,
        query_execution_id=query_execution_id,
    )
    _validate_athena_rows(columns, rows)
    csv_content = _render_csv(columns=columns, rows=rows)
    _upload_csv(
        session,
        bucket=config["bucket"],
        key=config["key"],
        body=csv_content,
        s3_region=config["s3_region"],
    )
    s3_uri = f"s3://{config['bucket']}/{config['key']}"
    return {
        "status": "ok",
        "mode": "athena",
        "bucket": config["bucket"],
        "key": config["key"],
        "s3_uri": s3_uri,
        "generated_at": config["generated_at"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "athena_region": config["athena_region"],
        "athena_database": config["athena_database"],
        "athena_workgroup": config["athena_workgroup"],
        "query": config["query"],
        "query_execution_id": query_execution_id,
        "athena_output_location": config["athena_result_output_location"],
        "columns": columns,
        "row_count": len(rows),
        "table_arns_sample": _extract_table_arns_from_athena_rows(columns, rows),
        "query_state": _resolve_optional_text(
            query_execution.get("Status", {}).get("State") if isinstance(query_execution, dict) else None,
            fallback="SUCCEEDED",
            field_name="query_state",
            required=False,
        ),
    }


def _run_cloudcontrol_inventory(config: dict[str, Any]) -> dict[str, Any]:
    rows = _build_inventory_rows(config)
    _validate_cloudcontrol_rows(rows)
    csv_content = _render_inventory_csv(rows)
    session = _build_aws_session()
    _upload_csv(
        session,
        bucket=config["bucket"],
        key=config["key"],
        body=csv_content,
        s3_region=config["s3_region"],
    )
    s3_uri = f"s3://{config['bucket']}/{config['key']}"
    return {
        "status": "ok",
        "mode": "cloudcontrol",
        "bucket": config["bucket"],
        "key": config["key"],
        "s3_uri": s3_uri,
        "generated_at": config["generated_at"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "regions": config["regions"],
        "region_count": len(config["regions"]),
        "table_count": len(rows),
        "table_arns_sample": [row["table_arn"] for row in rows[:20]],
    }


def _run_inventory(config: dict[str, Any]) -> dict[str, Any]:
    if config["mode"] == "athena":
        return _run_athena_inventory(config)
    return _run_cloudcontrol_inventory(config)


class _LocalExecutionContext:
    aws_request_id = "local-cli"


def _parse_cli_json_payload(raw_payload: str, *, field_name: str) -> dict[str, Any]:
    try:
        parsed_payload = json.loads(raw_payload)
    except json.JSONDecodeError as error:
        raise ValueError(f"{field_name} não contém JSON válido") from error

    if not isinstance(parsed_payload, dict):
        raise ValueError(f"{field_name} deve representar um objeto JSON")
    return parsed_payload


def _parse_cli_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o inventário de tabelas DynamoDB via Cloud Control ou Athena.",
    )
    parser.add_argument("--event-json", help="Payload JSON inline.")
    parser.add_argument("--event-file", help="Arquivo JSON com o payload.")
    parser.add_argument("--bucket", help="Bucket S3 de saída.")
    parser.add_argument("--key", help="Chave S3 final do CSV.")
    parser.add_argument("--prefix", help="Prefixo S3 final do CSV.")
    parser.add_argument("--region", dest="regions", action="append", help="Região alvo. Pode repetir.")
    parser.add_argument("--s3-region", help="Região do bucket de saída.")
    parser.add_argument("--mode", choices=("cloudcontrol", "athena"), help="Modo explícito.")
    parser.add_argument("--query", help="Query completa do Athena.")
    parser.add_argument("--athena-database", help="Database opcional do Athena.")
    parser.add_argument("--athena-workgroup", help="Workgroup do Athena.")
    parser.add_argument("--athena-catalog", help="Catalog opcional do Athena.")
    parser.add_argument("--athena-region", help="Região do Athena.")
    parser.add_argument("--athena-result-prefix", help="Prefixo temporário de resultado do Athena.")
    parser.add_argument(
        "--athena-poll-interval-seconds",
        type=int,
        help="Intervalo entre polls do Athena.",
    )
    parser.add_argument(
        "--athena-max-poll-attempts",
        type=int,
        help="Máximo de polls do Athena.",
    )
    parser.add_argument("--aws-profile", help="Profile AWS local.")
    parser.add_argument("--aws-default-region", help="Região AWS padrão local.")
    return parser.parse_args(argv)


def _load_cli_event_payload(args: argparse.Namespace) -> dict[str, Any]:
    if args.event_json and args.event_file:
        raise ValueError("use apenas um entre --event-json e --event-file")

    if args.event_json:
        return _parse_cli_json_payload(args.event_json, field_name="--event-json")

    if args.event_file:
        with open(args.event_file, "r", encoding="utf-8") as event_file:
            return _parse_cli_json_payload(event_file.read(), field_name="--event-file")

    return {}


def _build_event_from_cli_args(args: argparse.Namespace) -> dict[str, Any]:
    base_event = _load_cli_event_payload(args)
    cli_fields = {
        "bucket": args.bucket,
        "key": args.key,
        "prefix": args.prefix,
        "s3_region": args.s3_region,
        "mode": args.mode,
        "query": args.query,
        "athena_database": args.athena_database,
        "athena_workgroup": args.athena_workgroup,
        "athena_catalog": args.athena_catalog,
        "athena_region": args.athena_region,
        "athena_result_prefix": args.athena_result_prefix,
        "athena_poll_interval_seconds": args.athena_poll_interval_seconds,
        "athena_max_poll_attempts": args.athena_max_poll_attempts,
    }
    merged_event = {
        **base_event,
        **{key: value for key, value in cli_fields.items() if value is not None},
    }

    if args.regions:
        merged_event["regions"] = args.regions

    return merged_event


def _apply_cli_environment_overrides(args: argparse.Namespace) -> None:
    if args.aws_profile:
        os.environ["AWS_PROFILE"] = args.aws_profile
    if args.aws_default_region:
        os.environ["AWS_REGION"] = args.aws_default_region
        os.environ["AWS_DEFAULT_REGION"] = args.aws_default_region


def run_local_cli(argv: Optional[list[str]] = None) -> int:
    try:
        cli_args = _parse_cli_args(argv)
        _apply_cli_environment_overrides(cli_args)
        event = _build_event_from_cli_args(cli_args)
        response = lambda_handler(event, _LocalExecutionContext())
    except Exception as error:
        response = {
            "ok": False,
            "status": "error",
            "error_type": "cli",
            **_build_error_response_fields(error),
        }

    json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0 if response.get("ok") else 1


def lambda_handler(event: Optional[dict[str, Any]], context: Any) -> dict[str, Any]:
    _configure_logging()
    event_keys = sorted(event.keys()) if isinstance(event, dict) else []
    LOGGER.info(
        "handler.start aws_request_id=%s event_keys=%s",
        getattr(context, "aws_request_id", None),
        event_keys,
    )

    try:
        config = build_lambda_config(event)
        result = _run_inventory(config)
        result_count = result.get("table_count", result.get("row_count"))
        result_regions = result.get("regions", [result.get("athena_region")] if result.get("athena_region") else [])
        LOGGER.info(
            "handler.success mode=%s bucket=%s key=%s result_count=%s regions=%s",
            result.get("mode"),
            result["bucket"],
            result["key"],
            result_count,
            ",".join(result_regions),
        )
        return {
            "ok": True,
            **result,
        }
    except ValueError as error:
        LOGGER.error("handler.config_error error=%s", error)
        return {
            "ok": False,
            "status": "error",
            "error_type": "config",
            **_build_error_response_fields(error),
        }
    except DataIntegrityError as error:
        LOGGER.error("handler.data_integrity_error error=%s", error)
        return {
            "ok": False,
            "status": "error",
            "error_type": "data_integrity",
            **_build_error_response_fields(error),
        }
    except (
        BotoCoreError,
        ClientError,
        ConnectTimeoutError,
        EndpointConnectionError,
        NoCredentialsError,
        NoRegionError,
        PartialCredentialsError,
        ProxyConnectionError,
        ReadTimeoutError,
        TimeoutError,
    ) as error:
        LOGGER.error("handler.aws_error error=%s", error)
        return {
            "ok": False,
            "status": "error",
            "error_type": "aws",
            **_build_error_response_fields(error),
        }
    except Exception as error:
        LOGGER.exception("handler.runtime_error")
        return {
            "ok": False,
            "status": "error",
            "error_type": "runtime",
            **_build_error_response_fields(error),
        }


if __name__ == "__main__":
    raise SystemExit(run_local_cli())
