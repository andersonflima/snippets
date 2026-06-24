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
