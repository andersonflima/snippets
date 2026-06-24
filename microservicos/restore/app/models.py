"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class RestoreParams(BaseModel):
    operation: str = Field(description="create-snapshot | restore-snapshot")
    snapshotIdentifier: Optional[str] = Field(default=None, description="Snapshot origem (restore) ou destino (create).")
    targetInstanceIdentifier: Optional[str] = Field(default=None, description="Instância resultante (restore).")
    dbInstanceClass: Optional[str] = Field(default=None, description="Classe da instância restaurada.")
    dbSubnetGroupName: Optional[str] = Field(default=None, description="Subnet group de destino.")
    kmsKeyId: Optional[str] = Field(default=None, description="KMS key da instância/snapshot resultante.")


class RestoreRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(description="Nome ou ARN do recurso alvo.")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role para assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS.")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida sem executar.")
    params: RestoreParams


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
