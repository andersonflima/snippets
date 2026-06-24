"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class CreateParams(BaseModel):
    resourceType: str = Field(description="db-instance | db-subnet-group | security-group.")
    spec: dict[str, Any] = Field(description="Especificação do recurso (kwargs do boto3).")
    waitUntilAvailable: bool = Field(default=True, description="Aguarda ficar disponível.")


class CreateRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(description="Nome ou ARN do recurso alvo.")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role para assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS.")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida sem executar.")
    params: CreateParams


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
