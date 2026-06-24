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
