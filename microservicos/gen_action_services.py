"""Gerador dos action-services genéricos a partir de catalog.json.

Cada action-service é um dispatcher genérico (padrão do serviço dynamodb):
`params = {operation, args}` -> valida contra o catálogo -> aplica regra externa
(S3/DynamoDB) -> assume role -> `getattr(client, method)(**args)`.

Escopo inicial: RDS/Aurora, Elasticache, DynamoDB, particionados por verbo:
create, modify, destroy, start-stop, restore, replicate, describe (read-only),
data (data-plane DynamoDB). Serviços autocontidos (sem packages compartilhadas).

Uso:
    python gen_catalog.py            # gera catalog.json (fonte da verdade)
    python gen_action_services.py    # gera os serviços a partir do catálogo
"""
from __future__ import annotations

import json
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
CATALOG_PATH = os.path.join(ROOT, "catalog.json")

# Serviços gerados + metadados. gate=True => GMUD em prod. Data/describe não gateiam.
SERVICES = {
    "create": {"summary": "Provisiona recursos (create_*, purchase, register).", "gate": True},
    "modify": {"summary": "Altera recursos (modify_/update_/tag/attach/enable/scaling/resource-policy).", "gate": True},
    "destroy": {"summary": "Remove recursos (delete_/deregister/cancel).", "gate": True},
    "start-stop": {"summary": "Liga/desliga e failover (start_/stop_/reboot_/failover_).", "gate": True},
    "restore": {"summary": "Restore/copy/backtrack/export/import de recursos.", "gate": True},
    "replicate": {"summary": "Replicação/migração e share de snapshots cross-account.", "gate": True},
    "describe": {"summary": "Leitura read-only (describe_/list_/get_). Sem GMUD.", "gate": False},
    "data": {"summary": "Data-plane DynamoDB (item/query/scan/batch/transact/PartiQL).", "gate": False},
}

PY_CLASS = {
    "create": "Create", "modify": "Modify", "destroy": "Destroy",
    "start-stop": "StartStop", "restore": "Restore", "replicate": "Replicate",
    "describe": "Describe", "data": "Data",
}


# --------------------------------------------------------------------------- #
# Templates (autocontidos por serviço). __TOKENS__ substituídos por serviço.  #
# --------------------------------------------------------------------------- #

INIT_PY = '"""Action microservice package."""\n'

AWS_PY = '''"""Assume-role helper (STS) + erro de ação. Idêntico entre serviços."""
from __future__ import annotations

import boto3


class ActionError(Exception):
    """Erro de ação com código estável e status HTTP."""

    def __init__(self, code: str, message: str, http: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http = http


def assumed_session(account: str, role_arn: str, region: str) -> boto3.Session:
    """Assume a role no account alvo e devolve uma Session com creds temporárias."""
    sts = boto3.client("sts")
    resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=f"microservicos-{account}")
    c = resp["Credentials"]
    return boto3.Session(
        aws_access_key_id=c["AccessKeyId"],
        aws_secret_access_key=c["SecretAccessKey"],
        aws_session_token=c["SessionToken"],
        region_name=region,
    )
'''

OPERATIONS_HEAD = '''"""Catálogo de operações do serviço `__SVC__` (gerado de catalog.json).

NÃO editar à mão: rode `python gen_catalog.py && python gen_action_services.py`.
Cada operação mapeia name -> (client boto3, método, categoria, mutating,
resourceArg, resourceType). O handler despacha genericamente via getattr.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    key: str            # "<client>:<Name>" — chave global única
    name: str
    method: str
    client: str
    category: str
    mutating: bool
    resource_arg: str | None
    resource_type: str | None


_OPS: tuple[Operation, ...] = (
__ENTRIES__
)

CATALOG: dict[str, Operation] = {op.key: op for op in _OPS}
CLIENTS: tuple[str, ...] = tuple(sorted({op.client for op in _OPS}))


def resolve(key: str) -> Operation | None:
    """Resolve por chave '<client>:<Name>'. Aceita também o nome cru quando não
    houver colisão entre clients (conveniência)."""
    op = CATALOG.get(key)
    if op is not None:
        return op
    matches = [o for o in _OPS if o.name == key]
    return matches[0] if len(matches) == 1 else None


def resource_of(op: Operation, args: dict) -> str | None:
    if op.resource_arg and isinstance(args.get(op.resource_arg), str):
        return args[op.resource_arg]
    return None
'''

RULES_PY = '''"""Provedor de regras de bloqueio EXTERNAS (S3 ou DynamoDB).

As regras vivem fora da imagem e são atualizáveis sem redeploy. Backend por
RULES_BACKEND (s3|dynamodb), obrigatório. Leitura com cache TTL e fallback
resiliente: em miss/erro mantém o último valor válido (ou os defaults).

Env: RULES_BACKEND (s3|dynamodb), RULES_CACHE_TTL (60), RULES_REGION.
  s3: RULES_BUCKET, RULES_KEY_PREFIX (default "rules") -> <prefix>/<service>.json
  dynamodb: RULES_TABLE, RULES_PK (default "service"), RULES_ATTR (default "rules")
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

import boto3

SERVICE = "__SVC__"
DEFAULTS: dict[str, Any] = {"allowedRegions": [], "environments": {}, "exceptions": []}

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
    """Regras efetivas: DEFAULTS <- defaults <- documento externo. Nunca levanta
    por regra ausente (retorna cache/defaults)."""
    svc = service or SERVICE
    base = _merge(DEFAULTS, defaults or {})
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
'''

POLICY_PY = '''"""Enforcement genérico da regra externa (opt-in por chave; ausência = liberado).

Ordem: região -> exceção -> allow/deny de operação/categoria/resourceType por
ambiente -> GMUD. Tudo baseado no documento carregado por rules.load_rules.
"""
from __future__ import annotations

from dataclasses import dataclass

from .aws import ActionError
from .operations import Operation, resource_of


@dataclass(frozen=True)
class Decision:
    resource: str | None
    resource_type: str | None
    exception_id: str | None
    gmud_required: bool


def _deny(message: str) -> None:
    raise ActionError("rule_violation", message, 403)


def _op_in(names: list | None, op: Operation) -> bool:
    """Casa a operação por chave '<client>:<Op>' (precisa) OU nome curto (qualquer client)."""
    names = names or []
    return op.key in names or op.name in names


def _exception_active(exc: dict) -> bool:
    return True if not exc.get("expiresAt") else str(exc.get("expiresAt")) >= ""


def _matching_exception(rules: dict, req, op: Operation, resource: str | None) -> dict | None:
    for exc in rules.get("exceptions", []) or []:
        if exc.get("account") and exc["account"] != req.account:
            continue
        if exc.get("environment") not in (None, req.environment):
            continue
        if exc.get("resource") and exc["resource"] != resource:
            continue
        if not _exception_active(exc):
            continue
        ops = exc.get("allowOperations", []) or []
        cats = exc.get("allowCategories", []) or []
        if "*" in ops or _op_in(ops, op) or "*" in cats or op.category in cats:
            return exc
    return None


def _enforce_environment(rules: dict, req, op: Operation, resource_type: str | None) -> None:
    env = (rules.get("environments") or {}).get(req.environment) or {}
    if _op_in(env.get("deniedOperations"), op):
        _deny(f"operação bloqueada em {req.environment}: {op.key}")
    allowed_ops = env.get("allowedOperations")
    if allowed_ops and not _op_in(allowed_ops, op):
        _deny(f"operação não permitida em {req.environment}: {op.key}")
    if op.category in (env.get("deniedCategories") or []):
        _deny(f"categoria bloqueada em {req.environment}: {op.category}")
    allowed_cats = env.get("allowedCategories")
    if allowed_cats and op.category not in allowed_cats:
        _deny(f"categoria não permitida em {req.environment}: {op.category}")
    if resource_type:
        if resource_type in (env.get("deniedResourceTypes") or []):
            _deny(f"tipo de recurso bloqueado em {req.environment}: {resource_type}")
        allowed_rt = env.get("allowedResourceTypes")
        if allowed_rt and resource_type not in allowed_rt:
            _deny(f"tipo de recurso não permitido em {req.environment}: {resource_type}")


def _gmud_required(rules: dict, req, op: Operation) -> bool:
    env = (rules.get("environments") or {}).get(req.environment) or {}
    cats = env.get("gmudForCategories")
    if isinstance(cats, list):
        return op.category in cats
    if env.get("requireGmudForMutations", req.environment == "prod"):
        return op.mutating
    return False


def evaluate(rules: dict, req, op: Operation, args: dict) -> Decision:
    allowed_regions = rules.get("allowedRegions")
    if allowed_regions and req.region not in allowed_regions:
        _deny(f"região não permitida: {req.region} (permitidas: {allowed_regions})")

    resource = resource_of(op, args) or (req.resource if req.resource != "*" else None)
    resource_type = op.resource_type

    exc = _matching_exception(rules, req, op, resource)
    if exc is not None:
        return Decision(resource, resource_type, exc.get("reason") or "exception", False)

    _enforce_environment(rules, req, op, resource_type)
    return Decision(resource, resource_type, None, _gmud_required(rules, req, op))
'''

VALIDATE_PY = '''"""Validação dos args contra o input shape real da operação (botocore, offline).

Sem rede e sem credenciais: usa apenas os service models locais do botocore
(embarcado no boto3) para checar tipos e campos obrigatórios ANTES do dispatch.
Assim o dryRun também valida os params, não só o caminho real.
"""
from __future__ import annotations

from botocore.session import get_session
from botocore.validate import ParamValidator

from .aws import ActionError
from .operations import Operation

_session = get_session()
_shapes: dict[tuple[str, str], object] = {}


def _input_shape(client: str, op_name: str):
    key = (client, op_name)
    if key not in _shapes:
        model = _session.get_service_model(client).operation_model(op_name)
        _shapes[key] = model.input_shape
    return _shapes[key]


def validate_args(op: Operation, args: dict) -> None:
    shape = _input_shape(op.client, op.name)
    if shape is None:
        if args:
            raise ActionError("validation_error", f"{op.key} não aceita argumentos", 400)
        return
    report = ParamValidator().validate(args, shape)
    if report.has_errors():
        raise ActionError("validation_error", f"args inválidos p/ {op.key}: {report.generate_report()}", 400)
'''

GMUD_PY = '''"""Gate de GMUD (ServiceNow) para ambiente produtivo."""
from __future__ import annotations

import os

import httpx

from .aws import ActionError

SERVICENOW_URL = os.getenv("SERVICENOW_SERVICE_URL", "http://servicenow/servicenow/execute")


def ensure_change_authorized(action: str, req, required: bool) -> None:
    if not required:
        return
    if not req.changeNumber:
        raise ActionError("gmud_required", "changeNumber (GMUD) obrigatório para esta ação", 400)
    payload = {
        "account": req.account, "resource": req.resource, "roleArn": req.roleArn,
        "region": req.region, "environment": req.environment,
        "changeNumber": req.changeNumber, "params": {"operation": "validate", "action": action},
    }
    try:
        resp = httpx.post(SERVICENOW_URL, json=payload, timeout=10.0)
    except httpx.HTTPError as exc:
        raise ActionError("upstream_error", f"falha ao validar GMUD: {exc}", 502) from exc
    if resp.status_code >= 300:
        raise ActionError("gmud_required", f"GMUD não autorizada ({resp.status_code})", 403)
    detail = resp.json().get("detail") or {}
    if detail.get("allowed") is not True:
        raise ActionError("gmud_required", "mudança não está autorizada/na janela", 403)
'''

MODELS_PY = '''"""Contrato do serviço: envelope + params {operation, args}."""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class __CLS__Params(BaseModel):
    operation: str = Field(description="Nome da operação (ver enum do contrato/catálogo).")
    args: dict[str, Any] = Field(default_factory=dict, description="kwargs boto3 low-level, verbatim.")


class __CLS__Request(BaseModel):
    account: str = Field(pattern=r"^\\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(default="*", description="Nome/ARN do recurso alvo ('*' p/ ops de conta).")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\\d{12}:role/.+$", description="Role p/ assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\\d$", description="Região AWS.")
    environment: Literal["dev", "homol", "staging", "prod"] = Field(
        description="Ambiente alvo. 'prod' pode exigir GMUD conforme regra."
    )
    changeNumber: Optional[str] = Field(default=None, description="Número da GMUD (prod).")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida/prevê sem executar.")
    params: __CLS__Params


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

HANDLER_PY = '''"""Dispatch genérico: resolve op -> regra externa -> assume role -> boto3."""
from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from .aws import ActionError, assumed_session
from .models import ActionAccepted, __CLS__Request
from .operations import resolve
from .policy import evaluate
from .rules import load_rules
from .validate import validate_args
__GMUD_IMPORT__

def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return value


def execute(req: __CLS__Request) -> ActionAccepted:
    p = req.params
    op = resolve(p.operation)
    if op is None:
        raise ActionError("validation_error", f"operação não suportada por este serviço: {p.operation}", 400)

    args = dict(p.args or {})
    validate_args(op, args)
    rules = load_rules({})
    decision = evaluate(rules, req, op, args)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=decision.resource or req.resource, account=req.account,
            detail={
                "dryRun": True, "operation": op.name, "client": op.client, "method": op.method,
                "category": op.category, "mutating": op.mutating, "resourceType": decision.resource_type,
                "gmudRequired": decision.gmud_required, "exceptionApplied": decision.exception_id,
                "args": _jsonable(args),
            },
        )

__GMUD_CALL__
    session = assumed_session(req.account, req.roleArn, req.region)
    client = session.client(op.client)
    result = getattr(client, op.method)(**args)
    if isinstance(result, dict):
        result.pop("ResponseMetadata", None)

    return ActionAccepted(
        operationId=str(uuid.uuid4()), resource=decision.resource or req.resource, account=req.account,
        detail={"operation": op.name, "exceptionApplied": decision.exception_id, "result": _jsonable(result)},
    )
'''

MAIN_PY = '''"""FastAPI app do serviço `__SVC__` (POST /__SVC__/execute)."""
from __future__ import annotations

from botocore.exceptions import ClientError
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .aws import ActionError
from .handler import execute
from .models import ActionAccepted, ErrorResponse, __CLS__Request

app = FastAPI(title="__SVC__ action microservice", version="1.0.0")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    return {"status": "ready"}


def _err(http: int, code: str, message: str, request_id: str | None) -> JSONResponse:
    return JSONResponse(status_code=http, content=ErrorResponse(code=code, message=message, requestId=request_id).model_dump())


def _client_error_to_http(exc: ClientError) -> tuple[int, str]:
    code = exc.response.get("Error", {}).get("Code", "")
    if "NotFound" in code or code.endswith("NotFoundFault"):
        return 404, "not_found"
    if code in ("AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "Forbidden"):
        return 403, "assume_role_denied"
    return 409, "conflict"


@app.exception_handler(RequestValidationError)
async def _on_validation(request: Request, exc: RequestValidationError) -> JSONResponse:
    loc = "; ".join(".".join(str(p) for p in e["loc"]) + ": " + e["msg"] for e in exc.errors())
    return _err(422, "validation_error", loc, request.headers.get("x-request-id"))


@app.exception_handler(Exception)
async def _on_error(request: Request, exc: Exception) -> JSONResponse:
    return _err(500, "internal_error", str(exc), request.headers.get("x-request-id"))


@app.post("/__SVC__/execute", response_model=ActionAccepted, status_code=202,
          responses={c: {"model": ErrorResponse} for c in (400, 403, 404, 409, 422, 500, 502)})
def run(req: __CLS__Request) -> ActionAccepted | JSONResponse:
    try:
        return execute(req)
    except ActionError as exc:
        return _err(exc.http, exc.code, exc.message, req.requestId)
    except ClientError as exc:
        http, code = _client_error_to_http(exc)
        return _err(http, code, str(exc), req.requestId)
'''

REQUIREMENTS = "fastapi==0.115.6\nuvicorn[standard]==0.34.0\nboto3==1.35.90\npydantic==2.10.4\nhttpx==0.28.1\n"

DOCKERFILE = '''FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN adduser --disabled-password --uid 10001 appuser
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz').status==200 else 1)"
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
'''

DOCKERIGNORE = "__pycache__/\n*.pyc\n.venv/\ntests/\n"

NAMESPACE_YAML = '''apiVersion: v1
kind: Namespace
metadata:
  name: microservicos
'''

K8S_API_YAML = '''# Deploy do microserviço __SVC__ (imagem ECR sa-east-1).
# Substitua <ACCOUNT_ID> pela conta da plataforma (a esteira sobrescreve a tag).
# Regras externas via ConfigMap (RULES_BACKEND=dynamodb, tabela microservicos-rules).
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
  SERVICENOW_SERVICE_URL: http://servicenow/servicenow/execute
---
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

CONTRACT_HEAD = '''openapi: 3.0.3
info:
  title: __SVC__ — contrato (API Gateway path)
  version: 1.0.0
  description: >
    __SUMMARY__ O que pode / não pode é decidido por regras externas segregadas por
    ambiente, com exceções por conta + recurso. Exposto via API Gateway -> VPC Link
    -> NLB interno -> EKS. `params.operation` no formato "<client>:<Op>".
servers:
  - url: "https://${stageVariables.apiDomain}/${stageVariables.basePath}"
x-amazon-apigateway-request-validators:
  all:
    validateRequestBody: true
    validateRequestParameters: true
paths:
  /__SVC__:
    post:
      summary: Executa uma operação (governada por regras externas).
      operationId: __SVC_ID___execute
      tags:
        - __SVC__
      security:
        - CognitoJWT: []
      x-amazon-apigateway-request-validator: all
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/__CLS__Request"
      responses:
        202: { description: Ação aceita., content: { application/json: { schema: { $ref: "#/components/schemas/ActionAccepted" } } } }
        400: { description: Requisição inválida / operação não suportada., content: { application/json: { schema: { $ref: "#/components/schemas/Error" } } } }
        403: { description: AssumeRole negado / rule_violation / GMUD., content: { application/json: { schema: { $ref: "#/components/schemas/Error" } } } }
        404: { description: Recurso não encontrado., content: { application/json: { schema: { $ref: "#/components/schemas/Error" } } } }
        409: { description: Conflito / estado inválido., content: { application/json: { schema: { $ref: "#/components/schemas/Error" } } } }
        422: { description: Payload inválido., content: { application/json: { schema: { $ref: "#/components/schemas/Error" } } } }
        500: { description: Erro interno., content: { application/json: { schema: { $ref: "#/components/schemas/Error" } } } }
        502: { description: Falha no upstream., content: { application/json: { schema: { $ref: "#/components/schemas/Error" } } } }
      x-amazon-apigateway-integration:
        type: http_proxy
        httpMethod: POST
        connectionType: VPC_LINK
        connectionId: "${stageVariables.vpcLinkId}"
        uri: "http://${stageVariables.nlbDns}/__SVC__/execute"
        passthroughBehavior: when_no_match
        timeoutInMillis: 29000
        requestParameters:
          integration.request.header.X-Request-Id: context.requestId
        responses:
          default:
            statusCode: 200
components:
  securitySchemes:
    CognitoJWT:
      type: apiKey
      name: Authorization
      in: header
      x-amazon-apigateway-authtype: cognito_user_pools
      x-amazon-apigateway-authorizer:
        type: cognito_user_pools
        providerARNs:
          - "${COGNITO_USER_POOL_ARN}"
  schemas:
    __CLS__Request:
      type: object
      additionalProperties: false
      required: [account, roleArn, region, environment, params]
      properties:
        account: { type: string, description: Conta AWS alvo (12 dígitos)., pattern: "^\\\\d{12}$" }
        resource: { type: string, default: "*", description: "Recurso alvo (nome/ARN) ou '*' para operações de conta." }
        roleArn: { type: string, description: "Role para STS:AssumeRole.", pattern: "^arn:aws:iam::\\\\d{12}:role/.+$" }
        region: { type: string, description: Região AWS., pattern: "^[a-z]{2}-[a-z]+-\\\\d$" }
        environment: { type: string, enum: [dev, homol, staging, prod], description: "Ambiente target (regras segregam por ambiente)." }
        changeNumber: { type: string, description: "Número da GMUD (exigido p/ mutações em produção, salvo exceção)." }
        requestId: { type: string, description: Idempotency key opcional. }
        dryRun: { type: boolean, default: false, description: Valida contra as regras e resolve a operação sem chamar a AWS. }
        params:
          type: object
          additionalProperties: false
          required: [operation]
          properties:
            operation:
              type: string
              description: "Operação a executar, formato \\"<client>:<Op>\\"."
              enum:
__ENUM__
            args:
              type: object
              description: "Kwargs da operação boto3 (formato low-level), verbatim."
    ActionAccepted:
      type: object
      required: [operationId, status]
      properties:
        operationId: { type: string }
        status: { type: string, enum: [accepted, in-progress] }
        resource: { type: string }
        account: { type: string }
        detail: { type: object, description: Resultado (ou plano, em dryRun). }
    Error:
      type: object
      required: [code, message]
      properties:
        code: { type: string, enum: [validation_error, rule_violation, gmud_required, assume_role_denied, not_found, conflict, upstream_error, internal_error] }
        message: { type: string }
        requestId: { type: string }
'''


def _fmt_entry(e: dict) -> str:
    def q(v):
        return "None" if v is None else f'"{v}"'
    return (f'    Operation("{e["key"]}", "{e["name"]}", "{e["method"]}", "{e["client"]}", '
            f'"{e["category"]}", {e["mutating"]}, {q(e["resourceArg"])}, {q(e["resourceType"])}),')


def _write(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(content)


def gen_service(svc: str, ops: list[dict], meta: dict) -> None:
    cls = PY_CLASS[svc]
    base = os.path.join(ROOT, svc)
    app = os.path.join(base, "app")
    gated = meta["gate"]

    entries = "\n".join(_fmt_entry(e) for e in sorted(ops, key=lambda e: e["name"]))
    _write(os.path.join(app, "__init__.py"), INIT_PY)
    _write(os.path.join(app, "aws.py"), AWS_PY)
    _write(os.path.join(app, "operations.py"), OPERATIONS_HEAD.replace("__SVC__", svc).replace("__ENTRIES__", entries))
    _write(os.path.join(app, "rules.py"), RULES_PY.replace("__SVC__", svc))
    _write(os.path.join(app, "policy.py"), POLICY_PY)
    _write(os.path.join(app, "models.py"), MODELS_PY.replace("__CLS__", cls))
    _write(os.path.join(app, "validate.py"), VALIDATE_PY)

    gmud_import = "from .gmud import ensure_change_authorized\n" if gated else ""
    gmud_call = ("    ensure_change_authorized(op.name, req, decision.gmud_required)\n" if gated else "")
    _write(os.path.join(app, "handler.py"),
           HANDLER_PY.replace("__CLS__", cls).replace("__GMUD_IMPORT__", gmud_import).replace("__GMUD_CALL__", gmud_call))
    _write(os.path.join(app, "main.py"), MAIN_PY.replace("__SVC__", svc).replace("__CLS__", cls))
    if gated:
        _write(os.path.join(app, "gmud.py"), GMUD_PY)

    _write(os.path.join(base, "requirements.txt"), REQUIREMENTS)
    _write(os.path.join(base, "Dockerfile"), DOCKERFILE)
    _write(os.path.join(base, ".dockerignore"), DOCKERIGNORE)
    _write(os.path.join(base, "README.md"),
           f"# {svc}\n\n{meta['summary']}\n\nDispatcher genérico governado por regra externa "
           f"(S3/DynamoDB). Contrato: `POST /{svc}/execute` com `params.operation` (`<client>:<Op>`) "
           f"+ `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. {len(ops)} operações.\n")

    # Contrato OpenAPI (enum das ops) + infra k8s.
    enum = "\n".join(f'                - "{e["key"]}"' for e in sorted(ops, key=lambda e: e["key"]))
    contract = (CONTRACT_HEAD.replace("__SVC_ID__", svc.replace("-", "_"))
                .replace("__SVC__", svc).replace("__CLS__", cls)
                .replace("__SUMMARY__", meta["summary"]).replace("__ENUM__", enum))
    _write(os.path.join(base, "contract", "openapi.yaml"), contract)
    _write(os.path.join(base, "infra", "k8s", "namespace.yaml"), NAMESPACE_YAML)
    _write(os.path.join(base, "infra", "k8s", "api.yaml"), K8S_API_YAML.replace("__SVC__", svc))


def main() -> None:
    with open(CATALOG_PATH) as fh:
        catalog = json.load(fh)
    only = os.environ.get("ONLY")
    for svc, meta in SERVICES.items():
        if only and svc != only:
            continue
        ops = catalog.get(svc, [])
        gen_service(svc, ops, meta)
        print(f"gerado: {svc} ({len(ops)} ops, gate={meta['gate']})")


if __name__ == "__main__":
    main()
