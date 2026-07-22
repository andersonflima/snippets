"""Assume-role helper (STS) + erro de ação. Idêntico entre serviços."""
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
