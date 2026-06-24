"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class VpcLinkParams(BaseModel):
    dbIdentifier: str = Field(description="Banco a expor de forma privada.")
    consumerAccount: str = Field(description="Conta consumidora (time).")
    allowedPrincipals: Optional[list[str]] = Field(default=None, description="Principals autorizados.")
    endpointServiceId: Optional[str] = Field(default=None, description="VPC Endpoint Service id já existente.")
    ports: Optional[list[int]] = Field(default=None, description="Portas expostas.")


class VpcLinkRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(description="Nome ou ARN do recurso alvo.")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role para assume-role.")
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS.")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida sem executar.")
    params: VpcLinkParams


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
