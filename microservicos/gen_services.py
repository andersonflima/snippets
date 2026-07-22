#!/usr/bin/env python3
"""Scaffold dos microserviços action-driven (FastAPI), um por ação.

Cada serviço é AUTOCONTIDO (sem packages compartilhadas): Dockerfile +
requirements.txt + app/ próprios. Expõe `POST /<svc>/execute` (o path que o
API Gateway integra via VPC Link -> NLB interno), além de /healthz e /readyz.
Entrada: account + resource + roleArn + region + params (envelope do contrato).
A ação roda na conta-alvo via STS:AssumeRole (boto3).

Rodar:  python3 microservicos/gen_services.py
"""
from __future__ import annotations

import os

HERE = os.path.dirname(os.path.abspath(__file__))


def camel(name: str) -> str:
    return "".join(p.capitalize() for p in name.replace("/", "-").split("-"))


# ---------------------------------------------------------------------------
# Templates comuns (mesmos para todo serviço — duplicados por design)
# ---------------------------------------------------------------------------
AWS_PY = '''\
"""Helper de credenciais: STS:AssumeRole -> boto3.Session na conta-alvo."""
from __future__ import annotations

import uuid

import boto3
from botocore.exceptions import ClientError


class ActionError(Exception):
    """Erro de ação mapeável para HTTP."""

    def __init__(self, code: str, message: str, http: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http = http


def assumed_session(account: str, role_arn: str, region: str) -> boto3.Session:
    try:
        sts = boto3.client("sts")
        resp = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=f"ms-{uuid.uuid4().hex[:12]}",
        )
    except ClientError as exc:  # assume-role negado / role inexistente
        raise ActionError("assume_role_denied", str(exc), 403) from exc
    cred = resp["Credentials"]
    return boto3.Session(
        aws_access_key_id=cred["AccessKeyId"],
        aws_secret_access_key=cred["SecretAccessKey"],
        aws_session_token=cred["SessionToken"],
        region_name=region,
    )
'''

GMUD_PY = '''\
"""Gate de GMUD: em ambiente produtivo, exige change autorizada no ServiceNow."""
from __future__ import annotations

import os

import httpx

from .aws import ActionError

PRODUCTIVE_ENVIRONMENTS = {"prod"}
SERVICENOW_SERVICE_URL = os.getenv("SERVICENOW_SERVICE_URL", "http://servicenow/servicenow/execute")


def ensure_change_authorized(action: str, req) -> None:
    """Bloqueia a execução se o ambiente é produtivo e não há GMUD autorizada.

    A change é sempre criada no ServiceNow (nunca por nós): em produção o código
    da GMUD (changeNumber) é obrigatório para ser buscada/validada/acompanhada.
    """
    if req.environment not in PRODUCTIVE_ENVIRONMENTS:
        return
    if not req.changeNumber:
        raise ActionError("validation_error", "changeNumber (GMUD) é obrigatório em produção", 400)
    payload = {
        "account": req.account,
        "resource": req.resource,
        "roleArn": req.roleArn,
        "region": req.region,
        "environment": req.environment,
        "changeNumber": req.changeNumber,
        "requestId": req.requestId,
        "params": {"operation": "validate", "action": action, "changeNumber": req.changeNumber},
    }
    try:
        resp = httpx.post(SERVICENOW_SERVICE_URL, json=payload, timeout=10.0)
    except httpx.HTTPError as exc:
        raise ActionError("upstream_error", f"falha ao validar GMUD: {exc}", 502) from exc
    if resp.status_code >= 400:
        raise ActionError("gmud_required", f"GMUD não autorizada ({resp.status_code})", 403)
    allowed = bool((resp.json().get("detail") or {}).get("allowed"))
    if not allowed:
        raise ActionError("gmud_required", "execução produtiva requer GMUD aprovada na janela", 403)
'''

RULES_PY = '''\
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

SERVICE = "__SVC__"

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


def _deny(message: str) -> None:
    from .aws import ActionError  # import tardio: evita acoplamento no load do módulo

    raise ActionError("rule_violation", message, 403)


def enforce_common(rules: dict, req) -> None:
    """Enforcement genérico (opt-in por chave). Ausência de chave = sem restrição."""
    allowed_regions = rules.get("allowedRegions")
    if allowed_regions and req.region not in allowed_regions:
        _deny(f"região não permitida: {req.region} (permitidas: {allowed_regions})")
    allowed_envs = rules.get("allowedEnvironments")
    if allowed_envs and req.environment not in allowed_envs:
        _deny(f"ambiente não permitido: {req.environment} (permitidos: {allowed_envs})")
    denied_envs = rules.get("deniedEnvironments")
    if denied_envs and req.environment in denied_envs:
        _deny(f"ambiente bloqueado por regra: {req.environment}")


def enforce_allowed(rules: dict, key: str, value, label: str) -> None:
    """Nega se `value` estiver definido e fora da allowlist `rules[key]`."""
    allowed = rules.get(key)
    if allowed and value is not None and value not in allowed:
        _deny(f"{label} não permitido: {value} (permitidos: {allowed})")


def enforce_denied(rules: dict, key: str, value, label: str) -> None:
    """Nega se `value` estiver na denylist `rules[key]`."""
    denied = rules.get(key)
    if denied and value is not None and value in denied:
        _deny(f"{label} bloqueado por regra: {value}")


def enforce_max(rules: dict, key: str, value, label: str) -> None:
    """Nega se `value` exceder o teto numérico `rules[key]`."""
    cap = rules.get(key)
    if cap is not None and value is not None and value > cap:
        _deny(f"{label} acima do limite permitido: {value} > {cap}")


def enforce_env_map(rules: dict, key: str, env: str, value, label: str) -> None:
    """Allowlist por ambiente: rules[key] = {env: [permitidos]} (opt-in por env)."""
    per_env = rules.get(key)
    if isinstance(per_env, dict) and env in per_env:
        allowed = per_env[env]
        if allowed and value is not None and value not in allowed:
            _deny(f"{label} não permitido em {env}: {value} (permitidos: {allowed})")
'''

INIT_PY = '"""Microserviço action-driven (autocontido)."""\n'

MAIN_PY = '''\
"""API do microserviço __SVC__ — exposto via API Gateway -> VPC Link -> NLB -> EKS."""
from __future__ import annotations

from botocore.exceptions import ClientError
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .aws import ActionError
from .handler import execute
__GUARD_IMPORT__from .models import ActionAccepted, ErrorResponse, __PYCLASS__Request

app = FastAPI(title="__SVC__ action microservice", version="1.0.0")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    return {"status": "ready"}


@app.exception_handler(RequestValidationError)
async def on_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Padroniza erros de validacao (Pydantic) no envelope ErrorResponse."""
    errors = exc.errors()
    message = "payload invalido"
    if errors:
        loc = ".".join(str(part) for part in errors[0].get("loc", []) if part != "body")
        message = f"payload invalido: {loc}: {errors[0].get('msg', '')}".strip(": ")
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            code="validation_error",
            message=message,
            requestId=request.headers.get("x-request-id"),
        ).model_dump(),
    )


@app.exception_handler(Exception)
async def on_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    """Captura qualquer erro inesperado, garantindo 500 no envelope ErrorResponse."""
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            code="internal_error",
            message="erro interno inesperado",
            requestId=request.headers.get("x-request-id"),
        ).model_dump(),
    )


def _client_error_to_http(exc: ClientError) -> tuple[int, str]:
    code = exc.response.get("Error", {}).get("Code", "")
    if "NotFound" in code or code.endswith("NotFoundFault"):
        return 404, "not_found"
    if "AccessDenied" in code or "Forbidden" in code or "Unauthorized" in code:
        return 403, "assume_role_denied"
    return 409, "conflict"


@app.post(
    "/__SVC__/execute",
    response_model=ActionAccepted,
    status_code=202,
    responses={
        400: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
    },
)
def run(req: __PYCLASS__Request):
    try:
        __GUARD_CALL__return execute(req)
    except ActionError as exc:
        return JSONResponse(
            status_code=exc.http,
            content=ErrorResponse(code=exc.code, message=exc.message, requestId=req.requestId).model_dump(),
        )
    except ClientError as exc:
        http, code = _client_error_to_http(exc)
        return JSONResponse(
            status_code=http,
            content=ErrorResponse(code=code, message=str(exc), requestId=req.requestId).model_dump(),
        )
'''

DOCKERFILE = '''\
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \\
    PYTHONDONTWRITEBYTECODE=1 \\
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \\
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz').status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
'''

DOCKERIGNORE = "__pycache__/\n*.pyc\n.venv/\n.git/\n*.md\n"

# --- deploy (padrão pdi-portal): infra/k8s por serviço -----------------------
# A IMAGEM é construída/publicada (ECR) por uma esteira; estes manifestos são
# aplicados no EKS por outra (GitOps), que atualiza a tag da imagem.
NAMESPACE_YAML = '''\
apiVersion: v1
kind: Namespace
metadata:
  name: microservicos
'''

K8S_API_YAML = '''\
# Deploy do microserviço __SVC__ (padrão pdi-portal, imagem ECR sa-east-1).
# Substitua <ACCOUNT_ID> pela conta da plataforma (a esteira de deploy sobrescreve
# a tag da imagem). Defina RULES_BACKEND (s3|dynamodb) e as chaves no ConfigMap.
apiVersion: v1
kind: ServiceAccount
metadata:
  name: __SVC__
  namespace: microservicos
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::<ACCOUNT_ID>:role/microservicos-__SVC__-irsa
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: __SVC__-config
  namespace: microservicos
data:
  AWS_REGION: sa-east-1
  RULES_REGION: sa-east-1
  RULES_CACHE_TTL: "60"
  RULES_BACKEND: dynamodb
  RULES_TABLE: microservicos-rules
  RULES_PK: service
  RULES_ATTR: rules
__EXTRA_ENV__---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: __SVC__
  namespace: microservicos
  labels:
    app: __SVC__
spec:
  replicas: 2
  selector:
    matchLabels:
      app: __SVC__
  template:
    metadata:
      labels:
        app: __SVC__
    spec:
      serviceAccountName: __SVC__
      containers:
        - name: __SVC__
          image: <ACCOUNT_ID>.dkr.ecr.sa-east-1.amazonaws.com/microservicos-__SVC__:latest
          ports:
            - containerPort: 8080
          envFrom:
            - configMapRef:
                name: __SVC__-config
          resources:
            requests:
              cpu: 50m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 256Mi
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8080
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
          securityContext:
            runAsNonRoot: true
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
---
apiVersion: v1
kind: Service
metadata:
  name: __SVC__
  namespace: microservicos
spec:
  selector:
    app: __SVC__
  ports:
    - port: 8080
      targetPort: 8080
'''

REQS_BASE = [
    "fastapi==0.115.6",
    "uvicorn[standard]==0.34.0",
    "boto3==1.35.90",
    "pydantic==2.10.4",
    "httpx==0.28.1",
]

def models_head(needs_any: bool) -> str:
    typing_imp = (
        "from typing import Any, Literal, Optional" if needs_any
        else "from typing import Literal, Optional"
    )
    return (
        '"""Modelos do contrato (envelope + params da ação)."""\n'
        "from __future__ import annotations\n\n"
        f"{typing_imp}\n\n"
        "from pydantic import BaseModel, Field\n\n\n"
    )

ENVELOPE_FIELDS = '''\
    account: str = Field(pattern=r"^\\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(description="Nome ou ARN do recurso alvo.")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\\d{12}:role/.+$", description="Role para assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\\d$", description="Região AWS.")
    environment: Literal["dev", "homol", "staging", "prod"] = Field(
        description="Ambiente target. 'prod' exige GMUD aprovada (ServiceNow)."
    )
    changeNumber: Optional[str] = Field(default=None, description="Número da GMUD (obrigatório p/ produção).")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida sem executar.")
'''

COMMON_MODELS = '''\


class ActionAccepted(BaseModel):
    operationId: str
    status: str = "accepted"
    resource: Optional[str] = None
    account: Optional[str] = None
    detail: Optional[dict] = None


class ErrorResponse(BaseModel):
    code: str
    message: str
    requestId: Optional[str] = None
'''


def py_type(t: str) -> str:
    return {
        "str": "str",
        "int": "int",
        "bool": "bool",
        "dict": "dict[str, Any]",
        "list[str]": "list[str]",
        "list[int]": "list[int]",
    }[t]


def render_params(pyclass: str, fields: list[tuple]) -> str:
    lines = [f"class {pyclass}Params(BaseModel):"]
    if not fields:
        lines.append("    pass")
    for name, t, required, default, desc in fields:
        ann = py_type(t)
        if required:
            lines.append(f'    {name}: {ann} = Field(description="{desc}")')
        else:
            if t in ("dict", "list[str]", "list[int]") and default is None:
                ann = f"Optional[{ann}]"
                lines.append(f'    {name}: {ann} = Field(default=None, description="{desc}")')
            elif default is None:
                ann = f"Optional[{ann}]"
                lines.append(f'    {name}: {ann} = Field(default=None, description="{desc}")')
            else:
                lit = repr(default)
                lines.append(f'    {name}: {ann} = Field(default={lit}, description="{desc}")')
    return "\n".join(lines) + "\n"


def render_request(pyclass: str) -> str:
    return (
        f"\n\nclass {pyclass}Request(BaseModel):\n"
        + ENVELOPE_FIELDS
        + f"    params: {pyclass}Params\n"
    )


# ---------------------------------------------------------------------------
# Definição das ações: fields (params) + handler.py completo
# ---------------------------------------------------------------------------
# Nota: os serviços-verbo (create, modify, destroy, start-stop, restore,
# replicate) agora são gerados por gen_action_services.py (a partir de
# catalog.json); storage foi absorvido por modify e dynamodb foi dissolvido.
# Este gerador cuida apenas dos serviços especiais (não-verbo) abaixo.
SERVICES: list[dict] = [
    {
        "name": "db-password",
        "summary": "Conecta no banco e troca a senha do usuário informado.",
        "fields": [
            ("dbIdentifier", "str", True, None, "Identificador da instância RDS."),
            ("username", "str", True, None, "Usuário cuja senha será trocada."),
            ("newPasswordSecretArn", "str", True, None, "ARN do segredo (Secrets Manager) com a nova senha."),
            ("engine", "str", False, None, "postgres|mysql|mariadb|aurora-postgresql|aurora-mysql."),
        ],
        "reqs": ["psycopg[binary]==3.2.3", "PyMySQL==1.1.1"],
        "handler": '''\
"""Ação db-password: conecta no banco (admin) e troca a senha de um usuário."""
from __future__ import annotations

import json
import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, DbPasswordRequest
from .rules import enforce_allowed, enforce_common, enforce_denied, load_rules


def _secret_password(raw: str) -> str:
    try:
        data = json.loads(raw)
        return data.get("password") or data.get("Password") or raw
    except (ValueError, TypeError):
        return raw


def _is_postgres(engine: str) -> bool:
    return "postgres" in engine


def _alter_postgres(host, port, admin_user, admin_pw, username, new_pw) -> None:
    import psycopg

    role = '"' + username.replace('"', '""') + '"'
    with psycopg.connect(
        host=host, port=port, user=admin_user, password=admin_pw,
        dbname="postgres", sslmode="require", connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"ALTER ROLE {role} WITH PASSWORD %s", (new_pw,))
        conn.commit()


def _alter_mysql(host, port, admin_user, admin_pw, username, new_pw) -> None:
    import pymysql

    conn = pymysql.connect(
        host=host, port=port, user=admin_user, password=admin_pw,
        ssl={"ssl": {}}, connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER USER %s@'%%' IDENTIFIED BY %s", (username, new_pw))
        conn.commit()
    finally:
        conn.close()


def execute(req: DbPasswordRequest) -> ActionAccepted:
    p = req.params
    rules = load_rules({})
    enforce_common(rules, req)
    enforce_allowed(rules, "allowedEngines", p.engine, "engine")
    enforce_denied(rules, "deniedUsernames", p.username, "usuário")
    session = assumed_session(req.account, req.roleArn, req.region)
    rds = session.client("rds")
    sm = session.client("secretsmanager")

    instances = rds.describe_db_instances(DBInstanceIdentifier=p.dbIdentifier)["DBInstances"]
    if not instances:
        raise ActionError("not_found", f"instância {p.dbIdentifier} não encontrada", 404)
    inst = instances[0]
    endpoint = inst.get("Endpoint") or {}
    host, port = endpoint.get("Address"), endpoint.get("Port")
    engine = (p.engine or inst.get("Engine") or "").lower()
    admin_user = inst["MasterUsername"]

    master_secret = (inst.get("MasterUserSecret") or {}).get("SecretArn")
    if not master_secret:
        raise ActionError("conflict", "instância sem MasterUserSecret gerenciado; configure credencial admin", 409)
    admin_pw = _secret_password(sm.get_secret_value(SecretId=master_secret)["SecretString"])
    new_pw = _secret_password(sm.get_secret_value(SecretId=p.newPasswordSecretArn)["SecretString"])

    if req.dryRun:
        detail = {"dryRun": True, "db": p.dbIdentifier, "user": p.username, "engine": engine}
    else:
        if _is_postgres(engine):
            _alter_postgres(host, port, admin_user, admin_pw, p.username, new_pw)
        else:
            _alter_mysql(host, port, admin_user, admin_pw, p.username, new_pw)
        detail = {"db": p.dbIdentifier, "user": p.username, "rotated": True}

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
''',
    },
    {
        "name": "kms",
        "summary": "Cria Custom KMS Key e vincula/re-encripta, substituindo a default.",
        "fields": [
            ("keyAlias", "str", True, None, "Alias da custom key."),
            ("description", "str", False, None, "Descrição da key."),
            ("targetResourceType", "str", True, None, "db-instance | db-snapshot."),
            ("targetResourceId", "str", True, None, "Recurso que receberá a key."),
            ("replaceInherited", "bool", False, True, "Substitui a key default/herdada."),
            ("keyPolicyJson", "str", False, None, "Política da key (JSON)."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação kms: cria Custom KMS Key, alias e re-encripta o recurso alvo."""
from __future__ import annotations

import uuid

from .aws import assumed_session
from .models import ActionAccepted, KmsRequest
from .rules import enforce_allowed, enforce_common, load_rules


def execute(req: KmsRequest) -> ActionAccepted:
    p = req.params
    rules = load_rules({})
    enforce_common(rules, req)
    enforce_allowed(rules, "allowedTargetResourceTypes", p.targetResourceType, "targetResourceType")
    session = assumed_session(req.account, req.roleArn, req.region)
    kms = session.client("kms")
    alias = p.keyAlias if p.keyAlias.startswith("alias/") else f"alias/{p.keyAlias}"

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "alias": alias, "target": p.targetResourceId},
        )

    key = kms.create_key(Description=p.description or f"custom key {alias}", KeyUsage="ENCRYPT_DECRYPT")["KeyMetadata"]
    key_id = key["KeyId"]
    kms.create_alias(AliasName=alias, TargetKeyId=key_id)
    if p.keyPolicyJson:
        kms.put_key_policy(KeyId=key_id, PolicyName="default", Policy=p.keyPolicyJson)

    detail = {"keyId": key_id, "alias": alias}
    if p.targetResourceType == "db-snapshot":
        rds = session.client("rds")
        new_id = f"{p.targetResourceId}-cmk"
        out = rds.copy_db_snapshot(
            SourceDBSnapshotIdentifier=p.targetResourceId,
            TargetDBSnapshotIdentifier=new_id,
            KmsKeyId=key_id,
        )
        detail["reEncryptedSnapshot"] = out["DBSnapshot"]["DBSnapshotIdentifier"]
    else:
        detail["note"] = "db-instance aplica a key em novo snapshot/restore (KMS não re-encripta in-place)."

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
''',
    },
    {
        "name": "vpc-link",
        "summary": "Cria acesso privado (PrivateLink) da conta do time ao banco.",
        "fields": [
            ("dbIdentifier", "str", True, None, "Banco a expor de forma privada."),
            ("consumerAccount", "str", True, None, "Conta consumidora (time)."),
            ("allowedPrincipals", "list[str]", False, None, "Principals autorizados."),
            ("endpointServiceId", "str", False, None, "VPC Endpoint Service id já existente."),
            ("ports", "list[int]", False, None, "Portas expostas."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação vpc-link: autoriza a conta consumidora no VPC Endpoint Service (PrivateLink)."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, VpcLinkRequest
from .rules import enforce_allowed, enforce_common, load_rules


def execute(req: VpcLinkRequest) -> ActionAccepted:
    p = req.params
    rules = load_rules({})
    enforce_common(rules, req)
    enforce_allowed(rules, "allowedConsumerAccounts", p.consumerAccount, "conta consumidora")
    session = assumed_session(req.account, req.roleArn, req.region)
    ec2 = session.client("ec2")

    principals = list(p.allowedPrincipals or [])
    principals.append(f"arn:aws:iam::{p.consumerAccount}:root")
    principals = sorted(set(principals))

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "principals": principals, "service": p.endpointServiceId},
        )

    if not p.endpointServiceId:
        raise ActionError("validation_error", "endpointServiceId é obrigatório p/ autorizar o consumidor", 400)
    ec2.modify_vpc_endpoint_service_permissions(
        ServiceId=p.endpointServiceId,
        AddAllowedPrincipals=principals,
    )
    detail = {"service": p.endpointServiceId, "grantedPrincipals": principals, "db": p.dbIdentifier}

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
''',
    },
    {
        "name": "servicenow",
        "summary": "Integração com ServiceNow para GMUD: valida/registra/consulta a change.",
        "gate": False,
        "fields": [
            ("operation", "str", True, None, "validate | register | status"),
            ("action", "str", False, None, "Ação/microserviço sendo gateada."),
            ("changeNumber", "str", False, None, "Número da GMUD/change."),
            ("operationId", "str", False, None, "Correlação da operação em andamento."),
            ("workNote", "str", False, None, "Nota de trabalho a registrar (operation=register)."),
            ("state", "str", False, None, "Estado/anotação de progresso (operation=register)."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação servicenow: GMUD via ServiceNow Table API (validate/register/status)."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import httpx

from .aws import ActionError
from .models import ActionAccepted, ServicenowRequest

# ServiceNow Change state: -1 = Implement (liberado p/ execução). Configurável.
ALLOWED_STATES = {s.strip() for s in os.getenv("SERVICENOW_ALLOWED_STATES", "-1,implement").split(",") if s.strip()}
CHANGE_TABLE = os.getenv("SERVICENOW_CHANGE_TABLE", "change_request")


def _client() -> httpx.Client:
    base = os.getenv("SERVICENOW_INSTANCE_URL")
    if not base:
        raise ActionError("validation_error", "SERVICENOW_INSTANCE_URL não configurado", 400)
    headers = {"Accept": "application/json"}
    auth = None
    token = os.getenv("SERVICENOW_TOKEN")
    user = os.getenv("SERVICENOW_USER")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif user:
        auth = (user, os.getenv("SERVICENOW_PASSWORD", ""))
    return httpx.Client(base_url=base.rstrip("/"), headers=headers, auth=auth, timeout=15.0)


def _get_change(client: httpx.Client, number: str) -> dict:
    resp = client.get(
        f"/api/now/table/{CHANGE_TABLE}",
        params={"sysparm_query": f"number={number}", "sysparm_limit": 1},
    )
    if resp.status_code in (401, 403):
        raise ActionError("assume_role_denied", "credencial ServiceNow inválida", 403)
    resp.raise_for_status()
    results = resp.json().get("result", [])
    if not results:
        raise ActionError("not_found", f"change {number} não encontrado", 404)
    return results[0]


def _within_window(change: dict) -> bool:
    start = change.get("start_date") or change.get("work_start")
    end = change.get("end_date") or change.get("work_end")
    if not start or not end:
        return False
    fmt = "%Y-%m-%d %H:%M:%S"
    try:
        begin = datetime.strptime(start, fmt).replace(tzinfo=timezone.utc)
        finish = datetime.strptime(end, fmt).replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    return begin <= datetime.now(timezone.utc) <= finish


def execute(req: ServicenowRequest) -> ActionAccepted:
    p = req.params
    number = p.changeNumber or req.changeNumber
    operation = p.operation

    if operation in ("validate", "status") and not number:
        raise ActionError("validation_error", "changeNumber é obrigatório", 400)

    try:
        with _client() as client:
            if operation == "validate":
                change = _get_change(client, number)
                state = str(change.get("state", ""))
                in_window = _within_window(change)
                allowed = state in ALLOWED_STATES and in_window
                detail = {
                    "operation": "validate", "change": number, "state": state,
                    "withinWindow": in_window, "allowed": allowed,
                }
            elif operation == "status":
                change = _get_change(client, number)
                detail = {"operation": "status", "change": number, "state": str(change.get("state", ""))}
            elif operation == "register":
                if req.dryRun:
                    detail = {"operation": "register", "change": number, "dryRun": True}
                else:
                    change = _get_change(client, number)
                    note = p.workNote or f"action={p.action} operationId={p.operationId or req.requestId}"
                    patch = client.patch(
                        f"/api/now/table/{CHANGE_TABLE}/{change['sys_id']}",
                        json={"work_notes": note},
                    )
                    patch.raise_for_status()
                    detail = {"operation": "register", "change": number, "registered": True}
            else:
                raise ActionError("validation_error", f"operation inválida: {operation}", 400)
    except httpx.HTTPError as exc:
        raise ActionError("upstream_error", f"ServiceNow: {exc}", 502) from exc

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
''',
    },
    {
        "name": "rds-data",
        "summary": "Wrapper seguro do RDS Data API: avalia o SQL contra regras (S3) antes de executar.",
        "k8s_extra_env": {"RULES_BUCKET": "microservicos-rds-data-rules", "RULES_KEY": "rds-data/rules.json"},
        "fields": [
            ("sql", "str", True, None, "SQL a executar (validado contra as regras)."),
            ("secretArn", "str", True, None, "ARN do segredo com credenciais do banco."),
            ("resourceArn", "str", False, None, "ARN do cluster Aurora (default: resource)."),
            ("database", "str", False, None, "Banco/Database alvo."),
            ("schema", "str", False, None, "Schema alvo."),
            ("parameters", "dict", False, None, "Parâmetros nomeados (name -> value)."),
            ("includeResultMetadata", "bool", False, False, "Inclui metadados das colunas."),
            ("rulesBucket", "str", False, None, "Bucket S3 das regras (default env RULES_BUCKET)."),
            ("rulesKey", "str", False, None, "Chave do .json de regras (default env RULES_KEY)."),
        ],
        "reqs": ["sqlparse==0.5.3"],
        "handler": '''\
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
TABLE_RE = re.compile(r"\\b(?:FROM|JOIN|INTO|UPDATE)\\s+([a-zA-Z_][\\w\\.]*)", re.IGNORECASE)

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
''',
    },
    {
        "name": "finops",
        "summary": "Varredura read-only de desperdício e recomendações de economia (RDS/EC2/EBS/EIP/ELB/snapshots).",
        "gate": False,
        "fields": [
            ("scope", "str", False, "all", "Escopo: all | rds | ec2 | ebs | eip | elb | snapshots."),
            ("lookbackDays", "int", False, 14, "Janela (dias) de métricas CloudWatch para detectar ociosidade."),
        ],
        "reqs": [],
        "handler": '''\
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
''',
    },
]


README_TMPL = '''\
# __SVC__ — microserviço action-driven

__SUMMARY__

Autocontido (sem packages compartilhadas). Roda em EKS atrás do NLB interno;
o acesso externo é pelo API Gateway (Cognito JWT) -> VPC Link -> NLB -> este pod.

## API

`POST /__SVC__/execute` — executa a ação. Health: `GET /healthz`, `GET /readyz`.

Corpo (envelope + params), conforme `contract/openapi.yaml`:

```json
{
  "account": "123456789012",
  "resource": "<nome-ou-arn-do-recurso>",
  "roleArn": "arn:aws:iam::123456789012:role/<role-assumivel>",
  "region": "us-east-1",
  "dryRun": false,
  "params": { }
}
```

A ação roda na conta-alvo via `STS:AssumeRole` no `roleArn`.

## Local

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080
```

## Container

```bash
docker build -t __SVC__ .
docker run -p 8080:8080 __SVC__
```
'''


def write(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(content)


def build_models(pyclass: str, fields: list[tuple]) -> str:
    needs_any = any(t == "dict" for _, t, *_ in fields)
    return models_head(needs_any) + render_params(pyclass, fields) + render_request(pyclass) + COMMON_MODELS


def main() -> None:
    count = 0
    for svc in SERVICES:
        name = svc["name"]
        pyclass = camel(name)
        root = os.path.join(HERE, name)
        app = os.path.join(root, "app")

        gated = svc.get("gate", True)
        guard_import = "from .gmud import ensure_change_authorized\n" if gated else ""
        guard_call = f'ensure_change_authorized("{name}", req)\n        ' if gated else ""

        write(os.path.join(app, "__init__.py"), INIT_PY)
        write(os.path.join(app, "aws.py"), AWS_PY)
        write(os.path.join(app, "rules.py"), RULES_PY.replace("__SVC__", name))
        if gated:
            write(os.path.join(app, "gmud.py"), GMUD_PY)
        write(os.path.join(app, "models.py"), build_models(pyclass, svc["fields"]))
        write(os.path.join(app, "handler.py"), svc["handler"])
        write(
            os.path.join(app, "main.py"),
            MAIN_PY.replace("__GUARD_IMPORT__", guard_import)
            .replace("__GUARD_CALL__", guard_call)
            .replace("__PYCLASS__", pyclass)
            .replace("__SVC__", name),
        )

        reqs = REQS_BASE + svc.get("reqs", [])
        write(os.path.join(root, "requirements.txt"), "\n".join(reqs) + "\n")
        write(os.path.join(root, "Dockerfile"), DOCKERFILE)
        write(os.path.join(root, ".dockerignore"), DOCKERIGNORE)
        write(
            os.path.join(root, "README.md"),
            README_TMPL.replace("__SVC__", name).replace("__SUMMARY__", svc["summary"]),
        )
        extra_env = "".join(f'  {k}: "{v}"\n' for k, v in svc.get("k8s_extra_env", {}).items())
        write(os.path.join(root, "infra", "k8s", "namespace.yaml"), NAMESPACE_YAML)
        write(
            os.path.join(root, "infra", "k8s", "api.yaml"),
            K8S_API_YAML.replace("__EXTRA_ENV__", extra_env).replace("__SVC__", name),
        )
        count += 1
        print("scaffolded", name)
    print(f"OK — {count} microserviços gerados")


if __name__ == "__main__":
    main()
