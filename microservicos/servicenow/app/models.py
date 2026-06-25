"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class ServicenowParams(BaseModel):
    operation: str = Field(description="validate | register | status")
    action: Optional[str] = Field(default=None, description="Ação/microserviço sendo gateada.")
    changeNumber: Optional[str] = Field(default=None, description="Número da GMUD/change.")
    operationId: Optional[str] = Field(default=None, description="Correlação da operação em andamento.")
    workNote: Optional[str] = Field(default=None, description="Nota de trabalho a registrar (operation=register).")
    state: Optional[str] = Field(default=None, description="Estado/anotação de progresso (operation=register).")


class ServicenowRequest(BaseModel):
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
    params: ServicenowParams


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
