"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class ReplicateParams(BaseModel):
    sourceAccount: Optional[str] = Field(default=None, description="Conta de origem.")
    sourceRegion: Optional[str] = Field(default=None, description="Região de origem.")
    destinationAccount: str = Field(description="Conta de destino.")
    destinationRegion: str = Field(description="Região de destino.")
    resourceType: str = Field(description="db-snapshot | db-instance | ami | kms-key.")
    resourceId: str = Field(description="Recurso a replicar.")
    kmsKeyId: Optional[str] = Field(default=None, description="KMS key de destino p/ re-encriptar.")
    shareThenCopy: bool = Field(default=True, description="Compartilha cross-account e copia.")


class ReplicateRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(description="Nome ou ARN do recurso alvo.")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role para assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS.")
    environment: Literal["dev", "homol", "staging", "prod"] = Field(
        description="Ambiente target. 'prod' exige GMUD aprovada (ServiceNow)."
    )
    changeNumber: Optional[str] = Field(default=None, description="Número da GMUD (obrigatório p/ produção).")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida sem executar.")
    params: ReplicateParams


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
