"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class StorageParams(BaseModel):
    resourceType: str = Field(description="db-instance | ec2-volume.")
    storageType: Optional[str] = Field(default=None, description="gp2 | gp3 | io1 | io2.")
    allocatedStorage: Optional[int] = Field(default=None, description="Novo tamanho (GiB) — só aumento.")
    iops: Optional[int] = Field(default=None, description="IOPS.")
    storageThroughput: Optional[int] = Field(default=None, description="Throughput (MiB/s).")
    applyImmediately: bool = Field(default=False, description="Aplica imediatamente.")


class StorageRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(description="Nome ou ARN do recurso alvo.")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role para assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS.")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida sem executar.")
    params: StorageParams


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
