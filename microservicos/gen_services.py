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

INIT_PY = '"""Microserviço action-driven (autocontido)."""\n'

MAIN_PY = '''\
"""API do microserviço __SVC__ — exposto via API Gateway -> VPC Link -> NLB -> EKS."""
from __future__ import annotations

from botocore.exceptions import ClientError
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .aws import ActionError
from .handler import execute
from .models import ActionAccepted, ErrorResponse, __PYCLASS__Request

app = FastAPI(title="__SVC__ action microservice", version="1.0.0")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    return {"status": "ready"}


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
        502: {"model": ErrorResponse},
    },
)
def run(req: __PYCLASS__Request):
    try:
        return execute(req)
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

REQS_BASE = [
    "fastapi==0.115.6",
    "uvicorn[standard]==0.34.0",
    "boto3==1.35.90",
    "pydantic==2.10.4",
]

def models_head(needs_any: bool) -> str:
    typing_imp = "from typing import Any, Optional" if needs_any else "from typing import Optional"
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
SERVICES: list[dict] = [
    {
        "name": "restore",
        "summary": "Restaura snapshot → instância ou cria snapshot de uma instância.",
        "fields": [
            ("operation", "str", True, None, "create-snapshot | restore-snapshot"),
            ("snapshotIdentifier", "str", False, None, "Snapshot origem (restore) ou destino (create)."),
            ("targetInstanceIdentifier", "str", False, None, "Instância resultante (restore)."),
            ("dbInstanceClass", "str", False, None, "Classe da instância restaurada."),
            ("dbSubnetGroupName", "str", False, None, "Subnet group de destino."),
            ("kmsKeyId", "str", False, None, "KMS key da instância/snapshot resultante."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação restore: cria snapshot ou restaura instância a partir de snapshot."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, RestoreRequest


def execute(req: RestoreRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)
    rds = session.client("rds")

    if req.dryRun:
        detail = {"dryRun": True, "operation": p.operation}
    elif p.operation == "create-snapshot":
        snap = p.snapshotIdentifier or f"{req.resource}-{uuid.uuid4().hex[:8]}"
        out = rds.create_db_snapshot(DBSnapshotIdentifier=snap, DBInstanceIdentifier=req.resource)
        detail = {"snapshot": out["DBSnapshot"]["DBSnapshotIdentifier"], "status": out["DBSnapshot"]["Status"]}
    elif p.operation == "restore-snapshot":
        if not (p.snapshotIdentifier and p.targetInstanceIdentifier):
            raise ActionError("validation_error", "restore exige snapshotIdentifier e targetInstanceIdentifier", 400)
        kwargs = {
            "DBInstanceIdentifier": p.targetInstanceIdentifier,
            "DBSnapshotIdentifier": p.snapshotIdentifier,
        }
        if p.dbInstanceClass:
            kwargs["DBInstanceClass"] = p.dbInstanceClass
        if p.dbSubnetGroupName:
            kwargs["DBSubnetGroupName"] = p.dbSubnetGroupName
        out = rds.restore_db_instance_from_db_snapshot(**kwargs)
        detail = {"instance": out["DBInstance"]["DBInstanceIdentifier"], "status": out["DBInstance"]["DBInstanceStatus"]}
    else:
        raise ActionError("validation_error", f"operation inválida: {p.operation}", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
''',
    },
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


def execute(req: KmsRequest) -> ActionAccepted:
    p = req.params
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
        "name": "replicate",
        "summary": "Copia recurso cross-account ou recria em outra region.",
        "fields": [
            ("sourceAccount", "str", False, None, "Conta de origem."),
            ("sourceRegion", "str", False, None, "Região de origem."),
            ("destinationAccount", "str", True, None, "Conta de destino."),
            ("destinationRegion", "str", True, None, "Região de destino."),
            ("resourceType", "str", True, None, "db-snapshot | db-instance | ami | kms-key."),
            ("resourceId", "str", True, None, "Recurso a replicar."),
            ("kmsKeyId", "str", False, None, "KMS key de destino p/ re-encriptar."),
            ("shareThenCopy", "bool", False, True, "Compartilha cross-account e copia."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação replicate: compartilha/copia recurso para outra conta/region."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, ReplicateRequest


def execute(req: ReplicateRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType, "to": p.destinationAccount},
        )

    if p.resourceType == "db-snapshot":
        rds = session.client("rds")
        rds.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=p.resourceId,
            AttributeName="restore",
            ValuesToAdd=[p.destinationAccount],
        )
        detail = {
            "sharedSnapshot": p.resourceId,
            "withAccount": p.destinationAccount,
            "note": "copy/re-encrypt sob a KMS de destino é executado pela conta de destino (passo replicate na conta-alvo).",
        }
    else:
        raise ActionError("validation_error", f"replicate de {p.resourceType} não suportado neste serviço", 400)

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


def execute(req: VpcLinkRequest) -> ActionAccepted:
    p = req.params
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
        "name": "modify",
        "summary": "Modify genérico — inclui instance class e engine version.",
        "fields": [
            ("resourceType", "str", True, None, "db-instance | db-cluster | ec2-instance."),
            ("modifications", "dict", True, None, "Campos a modificar (contrato por tipo)."),
            ("applyImmediately", "bool", False, False, "Aplica imediatamente."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação modify: aplica modificações (instance class, engine version, etc.)."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, ModifyRequest


def execute(req: ModifyRequest) -> ActionAccepted:
    p = req.params
    m = p.modifications or {}
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType, "modifications": m},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        kwargs = {"DBInstanceIdentifier": req.resource, "ApplyImmediately": p.applyImmediately}
        if m.get("dbInstanceClass"):
            kwargs["DBInstanceClass"] = m["dbInstanceClass"]
        if m.get("engineVersion"):
            kwargs["EngineVersion"] = m["engineVersion"]
            kwargs["AllowMajorVersionUpgrade"] = True
        if m.get("parameterGroupName"):
            kwargs["DBParameterGroupName"] = m["parameterGroupName"]
        if m.get("backupRetentionPeriod") is not None:
            kwargs["BackupRetentionPeriod"] = m["backupRetentionPeriod"]
        out = rds.modify_db_instance(**kwargs)
        detail = {"status": out["DBInstance"]["DBInstanceStatus"]}
    elif p.resourceType == "ec2-instance":
        ec2 = session.client("ec2")
        if m.get("instanceType"):
            ec2.modify_instance_attribute(InstanceId=req.resource, InstanceType={"Value": m["instanceType"]})
        detail = {"modified": list(m.keys())}
    else:
        raise ActionError("validation_error", f"modify de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
''',
    },
    {
        "name": "create",
        "summary": "Provisiona recursos (contrato por tipo).",
        "fields": [
            ("resourceType", "str", True, None, "db-instance | db-subnet-group | security-group."),
            ("spec", "dict", True, None, "Especificação do recurso (kwargs do boto3)."),
            ("waitUntilAvailable", "bool", False, True, "Aguarda ficar disponível."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação create: provisiona um recurso a partir da spec (kwargs do boto3)."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, CreateRequest


def execute(req: CreateRequest) -> ActionAccepted:
    p = req.params
    spec = dict(p.spec or {})
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType, "spec": spec},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        out = rds.create_db_instance(DBInstanceIdentifier=req.resource, **spec)
        detail = {"status": out["DBInstance"]["DBInstanceStatus"]}
    elif p.resourceType == "db-subnet-group":
        rds = session.client("rds")
        rds.create_db_subnet_group(DBSubnetGroupName=req.resource, **spec)
        detail = {"created": req.resource}
    else:
        raise ActionError("validation_error", f"create de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
''',
    },
    {
        "name": "destroy",
        "summary": "Remove recursos (cleanup pós-fluxo).",
        "fields": [
            ("resourceType", "str", True, None, "db-instance | db-snapshot | vpc-endpoint | security-group."),
            ("skipFinalSnapshot", "bool", False, True, "Pula snapshot final."),
            ("finalSnapshotIdentifier", "str", False, None, "Snapshot final, se skipFinalSnapshot=false."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação destroy: remove o recurso alvo (cleanup)."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, DestroyRequest


def execute(req: DestroyRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType, "target": req.resource},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        kwargs = {"DBInstanceIdentifier": req.resource, "SkipFinalSnapshot": p.skipFinalSnapshot}
        if not p.skipFinalSnapshot and p.finalSnapshotIdentifier:
            kwargs["FinalDBSnapshotIdentifier"] = p.finalSnapshotIdentifier
        rds.delete_db_instance(**kwargs)
    elif p.resourceType == "db-snapshot":
        session.client("rds").delete_db_snapshot(DBSnapshotIdentifier=req.resource)
    elif p.resourceType == "vpc-endpoint":
        session.client("ec2").delete_vpc_endpoints(VpcEndpointIds=[req.resource])
    elif p.resourceType == "security-group":
        session.client("ec2").delete_security_group(GroupId=req.resource)
    else:
        raise ActionError("validation_error", f"destroy de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail={"deleted": req.resource})
''',
    },
    {
        "name": "start-stop",
        "summary": "Liga/desliga recursos que suportam power.",
        "fields": [
            ("operation", "str", True, None, "start | stop."),
            ("resourceType", "str", True, None, "db-instance | db-cluster | ec2-instance."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação start-stop: liga/desliga o recurso alvo."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, StartStopRequest


def execute(req: StartStopRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)
    start = p.operation == "start"

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "operation": p.operation, "resourceType": p.resourceType},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        (rds.start_db_instance if start else rds.stop_db_instance)(DBInstanceIdentifier=req.resource)
    elif p.resourceType == "db-cluster":
        rds = session.client("rds")
        (rds.start_db_cluster if start else rds.stop_db_cluster)(DBClusterIdentifier=req.resource)
    elif p.resourceType == "ec2-instance":
        ec2 = session.client("ec2")
        (ec2.start_instances if start else ec2.stop_instances)(InstanceIds=[req.resource])
    else:
        raise ActionError("validation_error", f"start-stop de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail={"operation": p.operation})
''',
    },
    {
        "name": "storage",
        "summary": "Altera storage — tipo (gp3/io1/…) e aumento de tamanho.",
        "fields": [
            ("resourceType", "str", True, None, "db-instance | ec2-volume."),
            ("storageType", "str", False, None, "gp2 | gp3 | io1 | io2."),
            ("allocatedStorage", "int", False, None, "Novo tamanho (GiB) — só aumento."),
            ("iops", "int", False, None, "IOPS."),
            ("storageThroughput", "int", False, None, "Throughput (MiB/s)."),
            ("applyImmediately", "bool", False, False, "Aplica imediatamente."),
        ],
        "reqs": [],
        "handler": '''\
"""Ação storage: altera tipo de storage e aumenta tamanho."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, StorageRequest


def execute(req: StorageRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        kwargs = {"DBInstanceIdentifier": req.resource, "ApplyImmediately": p.applyImmediately}
        if p.allocatedStorage:
            kwargs["AllocatedStorage"] = p.allocatedStorage
        if p.storageType:
            kwargs["StorageType"] = p.storageType
        if p.iops:
            kwargs["Iops"] = p.iops
        if p.storageThroughput:
            kwargs["StorageThroughput"] = p.storageThroughput
        out = rds.modify_db_instance(**kwargs)
        detail = {"status": out["DBInstance"]["DBInstanceStatus"]}
    elif p.resourceType == "ec2-volume":
        ec2 = session.client("ec2")
        kwargs = {"VolumeId": req.resource}
        if p.allocatedStorage:
            kwargs["Size"] = p.allocatedStorage
        if p.storageType:
            kwargs["VolumeType"] = p.storageType
        if p.iops:
            kwargs["Iops"] = p.iops
        if p.storageThroughput:
            kwargs["Throughput"] = p.storageThroughput
        ec2.modify_volume(**kwargs)
        detail = {"modified": req.resource}
    else:
        raise ActionError("validation_error", f"storage de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
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

        write(os.path.join(app, "__init__.py"), INIT_PY)
        write(os.path.join(app, "aws.py"), AWS_PY)
        write(os.path.join(app, "models.py"), build_models(pyclass, svc["fields"]))
        write(os.path.join(app, "handler.py"), svc["handler"])
        write(os.path.join(app, "main.py"), MAIN_PY.replace("__PYCLASS__", pyclass).replace("__SVC__", name))

        reqs = REQS_BASE + svc.get("reqs", [])
        write(os.path.join(root, "requirements.txt"), "\n".join(reqs) + "\n")
        write(os.path.join(root, "Dockerfile"), DOCKERFILE)
        write(os.path.join(root, ".dockerignore"), DOCKERIGNORE)
        write(
            os.path.join(root, "README.md"),
            README_TMPL.replace("__SVC__", name).replace("__SUMMARY__", svc["summary"]),
        )
        count += 1
        print("scaffolded", name)
    print(f"OK — {count} microserviços gerados")


if __name__ == "__main__":
    main()
