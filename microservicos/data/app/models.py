"""Contrato do serviço: envelope + params {operation, args}."""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class DataParams(BaseModel):
    operation: str = Field(description="Nome da operação (ver enum do contrato/catálogo).")
    args: dict[str, Any] = Field(default_factory=dict, description="kwargs boto3 low-level, verbatim.")


class DataRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(default="*", description="Nome/ARN do recurso alvo ('*' p/ ops de conta).")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role p/ assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS.")
    environment: Literal["dev", "homol", "staging", "prod"] = Field(
        description="Ambiente alvo. 'prod' pode exigir GMUD conforme regra."
    )
    changeNumber: Optional[str] = Field(default=None, description="Número da GMUD (prod).")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida/prevê sem executar.")
    params: DataParams


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
