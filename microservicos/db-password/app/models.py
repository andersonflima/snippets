"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class DbPasswordParams(BaseModel):
    dbIdentifier: str = Field(description="Identificador da instância RDS.")
    username: str = Field(description="Usuário cuja senha será trocada.")
    newPasswordSecretArn: str = Field(description="ARN do segredo (Secrets Manager) com a nova senha.")
    engine: Optional[str] = Field(default=None, description="postgres|mysql|mariadb|aurora-postgresql|aurora-mysql.")


class DbPasswordRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(description="Nome ou ARN do recurso alvo.")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role para assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS.")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida sem executar.")
    params: DbPasswordParams


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
