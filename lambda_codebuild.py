"""AWS Lambda para disparar builds do CodeBuild em contas alvo via AssumeRole.

Uso
===

Handler
- `lambda_codebuild.lambda_handler`

Objetivo
- receber uma lista de ARNs de roles alvo
- fazer `AssumeRole` em cada conta alvo
- disparar `CodeBuild.start_build` no projeto informado
- opcionalmente aplicar `buildspec`, `sourceVersion` e `environmentVariables` na execução

Payload suportado
- `target_role_arns` ou `targetRoleArns`: lista de ARNs das roles alvo
- `codebuild_project_name` ou `codebuildProjectName`: nome do projeto no CodeBuild
- `codebuild_region` ou `codebuildRegion`: região do CodeBuild na conta alvo
- `codebuild_buildspec` ou `codebuildBuildspec` ou `buildspec`: buildspec override
- `codebuild_source_version` ou `codebuildSourceVersion` ou `sourceVersion`: branch, tag ou commit
- `codebuild_environment_variables` ou `codebuildEnvironmentVariables` ou `environmentVariables`:
  lista de variáveis no formato
  `[{ "name": "ENV_NAME", "value": "production", "type": "PLAINTEXT" }]`
- `assume_role_external_id` ou `assumeRoleExternalId`: external id opcional
- `assume_role_session_name_prefix` ou `assumeRoleSessionNamePrefix`: prefixo do nome da sessão STS
- `assume_role_duration_seconds` ou `assumeRoleDurationSeconds`: duração da sessão STS
- `max_workers` ou `maxWorkers`: paralelismo de disparo por role

Variáveis de ambiente
- `TARGET_ROLE_ARNS`
- `CODEBUILD_PROJECT_NAME`
- `CODEBUILD_REGION`
- `CODEBUILD_BUILDSPEC`
- `CODEBUILD_SOURCE_VERSION`
- `CODEBUILD_ENVIRONMENT_VARIABLES`
- `ASSUME_ROLE_EXTERNAL_ID`
- `ASSUME_ROLE_SESSION_NAME_PREFIX`
- `ASSUME_ROLE_DURATION_SECONDS`
- `MAX_WORKERS`
- `LOG_LEVEL`

Precedência
- variáveis de ambiente têm precedência sobre o payload

Exemplo de evento
```json
{
  "target_role_arns": [
    "arn:aws:iam::111111111111:role/codebuild-trigger",
    "arn:aws:iam::222222222222:role/codebuild-trigger"
  ],
  "codebuild_project_name": "deploy-project",
  "codebuild_region": "sa-east-1",
  "codebuild_buildspec": "buildspecs/deploy.yml",
  "codebuild_source_version": "refs/heads/main",
  "codebuild_environment_variables": [
    {
      "name": "ENV_NAME",
      "value": "production",
      "type": "PLAINTEXT"
    }
  ],
  "assume_role_external_id": "external-id-opcional",
  "assume_role_session_name_prefix": "codebuild-trigger",
  "assume_role_duration_seconds": 3600,
  "max_workers": 4
}
```

Resposta resumida
- `ok=true` e `status=ok` quando todos os builds forem iniciados
- `ok=false` e `status=partial_ok` quando ao menos uma role falhar
- `ok=false` e `status=error` quando houver erro de configuração ou erro global da execução

Observações operacionais
- cada execução gera um `run_id` para rastreabilidade
- o `start_build` usa `idempotencyToken`
- a Lambda apenas inicia o build; ela não acompanha conclusão do CodeBuild
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
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

logger = logging.getLogger()
if not logger.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

AWS_ACCOUNT_ID_PATTERN = re.compile(r"^\d{12}$")
ROLE_ARN_PATTERN = re.compile(r"^arn:aws[a-zA-Z-]*:iam::\d{12}:role/.+$")
CODEBUILD_ENV_VAR_TYPES = frozenset({"PLAINTEXT", "PARAMETER_STORE", "SECRETS_MANAGER"})


def _resolve_optional_text(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _resolve_optional_value(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return value
            continue
        if isinstance(value, (list, dict, tuple, set)):
            if value:
                return value
            continue
        return value
    return None


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


def _dt_to_iso_with_milliseconds(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _to_json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return _dt_to_iso_with_milliseconds(value)
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


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = str(value).split(",")
    normalized: List[str] = []
    for item in raw_items:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _dedupe_values(values: List[str]) -> List[str]:
    deduped: List[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _safe_str_field(value, field_name="value")
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _extract_account_id_from_role_arn(role_arn: str) -> str:
    value = _safe_str_field(role_arn, field_name="target_role_arn")
    if not ROLE_ARN_PATTERN.fullmatch(value):
        raise ValueError(f"target_role_arn inválido: {value}")
    return value.split(":")[4]


def _normalize_environment_variables(raw_value: Any) -> List[Dict[str, str]]:
    if raw_value is None:
        return []
    parsed_value = raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed_value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("codebuild_environment_variables deve ser JSON válido") from exc
    if not isinstance(parsed_value, list):
        raise ValueError("codebuild_environment_variables deve ser uma lista")

    normalized: List[Dict[str, str]] = []
    for index, item in enumerate(parsed_value):
        item_dict = _safe_dict_field(item, f"codebuild_environment_variables[{index}]")
        name = _safe_str_field(item_dict.get("name"), field_name=f"codebuild_environment_variables[{index}].name")
        value = _safe_str_field(item_dict.get("value"), field_name=f"codebuild_environment_variables[{index}].value")
        var_type = _safe_str_field(
            item_dict.get("type"),
            field_name=f"codebuild_environment_variables[{index}].type",
            required=False,
        ).upper() or "PLAINTEXT"
        if var_type not in CODEBUILD_ENV_VAR_TYPES:
            allowed_types = ", ".join(sorted(CODEBUILD_ENV_VAR_TYPES))
            raise ValueError(
                f"codebuild_environment_variables[{index}].type inválido: {var_type}. Valores permitidos: {allowed_types}"
            )
        normalized.append({"name": name, "value": value, "type": var_type})
    return normalized


def _build_error_response_fields(exc: BaseException) -> Dict[str, str]:
    message = str(exc)
    message_lower = message.lower()
    if "target_role_arns" in message_lower or "target_role_arn" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "Nenhuma role alvo válida foi informada para assumir a conta de destino.",
            "resolution": "Informe target_role_arns no payload ou TARGET_ROLE_ARNS no ambiente com ARNs IAM role válidos.",
        }
    if "codebuild_project_name" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "O nome do projeto do CodeBuild não foi informado.",
            "resolution": "Defina codebuild_project_name no payload ou CODEBUILD_PROJECT_NAME no ambiente.",
        }
    if "codebuild_region" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A região do CodeBuild não foi informada.",
            "resolution": "Defina codebuild_region no payload ou AWS_REGION/AWS_DEFAULT_REGION/CODEBUILD_REGION no ambiente.",
        }
    if "accessdenied" in message_lower or "acesso" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A execução não tem permissão para assumir a role ou iniciar o CodeBuild na conta alvo.",
            "resolution": "Revise a trust policy da role alvo e as permissões para sts:AssumeRole e codebuild:StartBuild.",
        }
    return {
        "error": message,
        "error_detail": message,
        "user_message": "A execução falhou por erro interno.",
        "resolution": "Revise os logs da Lambda e os parâmetros enviados no payload ou nas variáveis de ambiente.",
    }


def _client_error_code(exc: ClientError) -> str:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    error = response.get("Error")
    if not isinstance(error, dict):
        return exc.__class__.__name__
    return _safe_str_field(error.get("Code"), field_name="Error.Code", required=False) or exc.__class__.__name__


def _client_error_message(exc: ClientError) -> str:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    error = response.get("Error")
    if not isinstance(error, dict):
        return str(exc)
    return _safe_str_field(error.get("Message"), field_name="Error.Message", required=False) or str(exc)


def _build_aws_session() -> Any:
    return boto3.session.Session()


def _get_session_client(session: Any, service_name: str, *, region_name: Optional[str] = None) -> Any:
    return session.client(service_name, region_name=region_name)


def _build_assume_role_session_name(*, run_id: str, role_arn: str, prefix: str) -> str:
    digest = hashlib.sha1(role_arn.encode("utf-8")).hexdigest()[:12]
    sanitized_prefix = re.sub(r"[^a-zA-Z0-9+=,.@-]", "-", _safe_str_field(prefix, field_name="assume_role_session_name_prefix"))
    return f"{sanitized_prefix}-{run_id}-{digest}"[:64]


def _assume_role_session(
    *,
    base_session: Any,
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
        aws_access_key_id=_safe_str_field(credentials.get("AccessKeyId"), field_name="Credentials.AccessKeyId"),
        aws_secret_access_key=_safe_str_field(credentials.get("SecretAccessKey"), field_name="Credentials.SecretAccessKey"),
        aws_session_token=_safe_str_field(credentials.get("SessionToken"), field_name="Credentials.SessionToken", required=False),
        region_name=None,
    )


def build_codebuild_config(event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = event if isinstance(event, dict) else {}
    target_role_arns = _dedupe_values(
        _normalize_list(
            os.getenv("TARGET_ROLE_ARNS")
            or payload.get("target_role_arns")
            or payload.get("targetRoleArns")
            or payload.get("role_arns")
            or payload.get("roleArns")
            or ""
        )
    )
    if not target_role_arns:
        raise ValueError("target_role_arns é obrigatório")
    for role_arn in target_role_arns:
        _extract_account_id_from_role_arn(role_arn)

    codebuild_project_name = _safe_str_field(
        _resolve_optional_text(
            os.getenv("CODEBUILD_PROJECT_NAME", ""),
            payload.get("codebuild_project_name"),
            payload.get("codebuildProjectName"),
            payload.get("project_name"),
            payload.get("projectName"),
        ),
        field_name="codebuild_project_name",
    )
    codebuild_region = _safe_str_field(
        _resolve_optional_text(
            os.getenv("CODEBUILD_REGION", ""),
            payload.get("codebuild_region"),
            payload.get("codebuildRegion"),
            os.getenv("AWS_REGION", ""),
            os.getenv("AWS_DEFAULT_REGION", ""),
        ),
        field_name="codebuild_region",
    )
    codebuild_buildspec = _safe_str_field(
        _resolve_optional_text(
            os.getenv("CODEBUILD_BUILDSPEC", ""),
            payload.get("codebuild_buildspec"),
            payload.get("codebuildBuildspec"),
            payload.get("buildspec"),
        ),
        field_name="codebuild_buildspec",
        required=False,
    )
    codebuild_source_version = _safe_str_field(
        _resolve_optional_text(
            os.getenv("CODEBUILD_SOURCE_VERSION", ""),
            payload.get("codebuild_source_version"),
            payload.get("codebuildSourceVersion"),
            payload.get("source_version"),
            payload.get("sourceVersion"),
        ),
        field_name="codebuild_source_version",
        required=False,
    )
    environment_variables = _normalize_environment_variables(
        _resolve_optional_value(
            os.getenv("CODEBUILD_ENVIRONMENT_VARIABLES", ""),
            payload.get("codebuild_environment_variables"),
            payload.get("codebuildEnvironmentVariables"),
            payload.get("environment_variables"),
            payload.get("environmentVariables"),
        )
    )
    external_id = _safe_str_field(
        _resolve_optional_text(
            os.getenv("ASSUME_ROLE_EXTERNAL_ID", ""),
            payload.get("assume_role_external_id"),
            payload.get("assumeRoleExternalId"),
        ),
        field_name="assume_role_external_id",
        required=False,
    )
    session_name_prefix = _safe_str_field(
        _resolve_optional_text(
            os.getenv("ASSUME_ROLE_SESSION_NAME_PREFIX", ""),
            payload.get("assume_role_session_name_prefix"),
            payload.get("assumeRoleSessionNamePrefix"),
            "codebuild-trigger",
        ),
        field_name="assume_role_session_name_prefix",
    )
    try:
        assume_role_duration_seconds = int(
            _resolve_optional_text(
                os.getenv("ASSUME_ROLE_DURATION_SECONDS", ""),
                payload.get("assume_role_duration_seconds"),
                payload.get("assumeRoleDurationSeconds"),
                "3600",
            )
            or "3600"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("assume_role_duration_seconds deve ser um inteiro válido") from exc
    if assume_role_duration_seconds < 900 or assume_role_duration_seconds > 43200:
        raise ValueError("assume_role_duration_seconds deve estar entre 900 e 43200")
    try:
        max_workers = int(
            _resolve_optional_text(
                os.getenv("MAX_WORKERS", ""),
                payload.get("max_workers"),
                payload.get("maxWorkers"),
                "4",
            )
            or "4"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("max_workers deve ser um inteiro válido") from exc
    max_workers = max(1, max_workers)

    run_time = _now_utc()
    run_id = run_time.strftime("%Y%m%dT%H%M%SZ")
    config = {
        "run_id": run_id,
        "run_time": run_time,
        "target_role_arns": target_role_arns,
        "codebuild_project_name": codebuild_project_name,
        "codebuild_region": codebuild_region,
        "codebuild_buildspec": codebuild_buildspec,
        "codebuild_source_version": codebuild_source_version,
        "codebuild_environment_variables": environment_variables,
        "assume_role_external_id": external_id,
        "assume_role_session_name_prefix": session_name_prefix,
        "assume_role_duration_seconds": assume_role_duration_seconds,
        "max_workers": max_workers,
    }
    _log_event(
        "config.codebuild.resolved",
        run_id=run_id,
        target_count=len(target_role_arns),
        codebuild_project_name=codebuild_project_name,
        codebuild_region=codebuild_region,
        buildspec_override_enabled=bool(codebuild_buildspec),
        source_version=codebuild_source_version or None,
        environment_variable_count=len(environment_variables),
        max_workers=max_workers,
    )
    return config


def _build_start_build_request(config: Dict[str, Any], *, role_arn: str) -> Dict[str, Any]:
    request: Dict[str, Any] = {
        "projectName": _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name"),
        "idempotencyToken": hashlib.sha1(
            "|".join(
                [
                    _safe_str_field(config.get("run_id"), field_name="run_id"),
                    _safe_str_field(role_arn, field_name="target_role_arn"),
                    _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name"),
                    _safe_str_field(config.get("codebuild_source_version"), field_name="codebuild_source_version", required=False),
                ]
            ).encode("utf-8")
        ).hexdigest()[:32],
    }
    buildspec_override = _safe_str_field(
        config.get("codebuild_buildspec"),
        field_name="codebuild_buildspec",
        required=False,
    )
    if buildspec_override:
        request["buildspecOverride"] = buildspec_override
    source_version = _safe_str_field(
        config.get("codebuild_source_version"),
        field_name="codebuild_source_version",
        required=False,
    )
    if source_version:
        request["sourceVersion"] = source_version
    environment_variables = config.get("codebuild_environment_variables")
    if isinstance(environment_variables, list) and environment_variables:
        request["environmentVariablesOverride"] = [
            {
                "name": _safe_str_field(item.get("name"), field_name="codebuild_environment_variable.name"),
                "value": _safe_str_field(item.get("value"), field_name="codebuild_environment_variable.value"),
                "type": _safe_str_field(item.get("type"), field_name="codebuild_environment_variable.type"),
            }
            for item in environment_variables
            if isinstance(item, dict)
        ]
    return request


def _start_codebuild_for_role(base_session: Any, config: Dict[str, Any], role_arn: str) -> Dict[str, Any]:
    account_id = _extract_account_id_from_role_arn(role_arn)
    session_name = _build_assume_role_session_name(
        run_id=_safe_str_field(config.get("run_id"), field_name="run_id"),
        role_arn=role_arn,
        prefix=_safe_str_field(config.get("assume_role_session_name_prefix"), field_name="assume_role_session_name_prefix"),
    )
    _log_event(
        "codebuild.target.start",
        target_role_arn=role_arn,
        target_account_id=account_id,
        codebuild_project_name=config.get("codebuild_project_name"),
        codebuild_region=config.get("codebuild_region"),
        session_name=session_name,
    )
    assumed_session = _assume_role_session(
        base_session=base_session,
        role_arn=role_arn,
        external_id=_safe_str_field(config.get("assume_role_external_id"), field_name="assume_role_external_id", required=False),
        session_name=session_name,
        duration_seconds=int(config.get("assume_role_duration_seconds", 3600)),
    )
    codebuild_client = _get_session_client(
        assumed_session,
        "codebuild",
        region_name=_safe_str_field(config.get("codebuild_region"), field_name="codebuild_region"),
    )
    request = _build_start_build_request(config, role_arn=role_arn)
    response = codebuild_client.start_build(**request)
    build = _safe_dict_field(response.get("build"), "start_build.build")
    build_id = _safe_str_field(build.get("id"), field_name="build.id")
    build_arn = _safe_str_field(build.get("arn"), field_name="build.arn", required=False)
    build_number = build.get("buildNumber")
    build_status = _safe_str_field(build.get("buildStatus"), field_name="build.buildStatus", required=False) or "IN_PROGRESS"
    result = {
        "target_role_arn": role_arn,
        "target_account_id": account_id,
        "codebuild_project_name": _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name"),
        "codebuild_region": _safe_str_field(config.get("codebuild_region"), field_name="codebuild_region"),
        "build_id": build_id,
        "build_arn": build_arn,
        "build_number": build_number,
        "build_status": build_status,
        "status": "STARTED",
        "source_version": _safe_str_field(config.get("codebuild_source_version"), field_name="codebuild_source_version", required=False),
        "buildspec_override_applied": bool(_safe_str_field(config.get("codebuild_buildspec"), field_name="codebuild_buildspec", required=False)),
    }
    _log_event(
        "codebuild.target.started",
        target_role_arn=role_arn,
        target_account_id=account_id,
        build_id=build_id,
        build_arn=build_arn,
        build_status=build_status,
        codebuild_project_name=result["codebuild_project_name"],
        codebuild_region=result["codebuild_region"],
    )
    return result


def _build_target_error_result(config: Dict[str, Any], *, role_arn: str, error: BaseException) -> Dict[str, Any]:
    fields = _build_error_response_fields(error)
    account_id = ""
    try:
        account_id = _extract_account_id_from_role_arn(role_arn)
    except ValueError:
        account_id = ""
    result = {
        "target_role_arn": role_arn,
        "target_account_id": account_id,
        "codebuild_project_name": _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name", required=False),
        "codebuild_region": _safe_str_field(config.get("codebuild_region"), field_name="codebuild_region", required=False),
        "status": "FAILED",
        **fields,
    }
    return result


def _run_codebuild(config: Dict[str, Any]) -> Dict[str, Any]:
    base_session = _build_aws_session()
    role_arns = list(config.get("target_role_arns") or [])
    results: List[Dict[str, Any]] = []

    def execute_role(role_arn: str) -> Dict[str, Any]:
        try:
            return _start_codebuild_for_role(base_session, config, role_arn)
        except Exception as exc:
            _log_event(
                "codebuild.target.failed",
                target_role_arn=role_arn,
                codebuild_project_name=config.get("codebuild_project_name"),
                codebuild_region=config.get("codebuild_region"),
                error=str(exc),
                level=logging.WARNING,
            )
            return _build_target_error_result(config, role_arn=role_arn, error=exc)

    worker_count = min(max(1, int(config.get("max_workers", 1))), len(role_arns))
    if worker_count <= 1:
        results = [execute_role(role_arn) for role_arn in role_arns]
    else:
        indexed_results: List[Optional[Dict[str, Any]]] = [None] * len(role_arns)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(execute_role, role_arn): index
                for index, role_arn in enumerate(role_arns)
            }
            for future in as_completed(futures):
                indexed_results[futures[future]] = future.result()
        results = [result for result in indexed_results if isinstance(result, dict)]

    failed_count = sum(
        _safe_str_field(result.get("status"), field_name="result.status", required=False).upper() == "FAILED"
        for result in results
        if isinstance(result, dict)
    )
    response = {
        "status": "partial_ok" if failed_count else "ok",
        "run_id": _safe_str_field(config.get("run_id"), field_name="run_id"),
        "codebuild_project_name": _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name"),
        "codebuild_region": _safe_str_field(config.get("codebuild_region"), field_name="codebuild_region"),
        "target_count": len(role_arns),
        "results": results,
    }
    _log_event(
        "codebuild.run.completed",
        run_id=response["run_id"],
        status=response["status"],
        target_count=response["target_count"],
        failed_count=failed_count,
        codebuild_project_name=response["codebuild_project_name"],
        codebuild_region=response["codebuild_region"],
    )
    return response


def lambda_handler(event: Optional[Dict[str, Any]], context: Any) -> Dict[str, Any]:
    event_keys = sorted(event.keys()) if isinstance(event, dict) else []
    _log_event(
        "handler.start",
        has_event=event is not None,
        event_keys=event_keys,
        aws_request_id=getattr(context, "aws_request_id", None),
    )
    try:
        config = build_codebuild_config(event)
        run_result = _run_codebuild(config)
        response = {
            "ok": _safe_str_field(run_result.get("status"), field_name="status", required=False).lower() == "ok",
            **run_result,
        }
        _log_event(
            "handler.success" if response["ok"] else "handler.partial_failure",
            run_id=response.get("run_id"),
            status=response.get("status"),
            target_count=response.get("target_count"),
            codebuild_project_name=response.get("codebuild_project_name"),
            codebuild_region=response.get("codebuild_region"),
            level=logging.INFO if response["ok"] else logging.WARNING,
        )
        return response
    except ValueError as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.config_error", error=str(exc), level=logging.ERROR)
        return {
            "ok": False,
            "status": "error",
            "error_type": "config",
            **fields,
        }
    except (BotoCoreError, ClientError, ConnectTimeoutError, EndpointConnectionError, NoCredentialsError, NoRegionError,
            PartialCredentialsError, ProxyConnectionError, ReadTimeoutError, TimeoutError) as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.aws_error", error=str(exc), level=logging.ERROR)
        return {
            "ok": False,
            "status": "error",
            "error_type": "aws",
            **fields,
        }
    except Exception as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.runtime_error", error=str(exc), level=logging.ERROR)
        return {
            "ok": False,
            "status": "error",
            "error_type": "runtime",
            **fields,
        }