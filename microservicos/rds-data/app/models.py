"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class RdsDataParams(BaseModel):
    sql: str = Field(description="SQL a executar (validado contra as regras).")
    secretArn: str = Field(description="ARN do segredo com credenciais do banco.")
    resourceArn: Optional[str] = Field(default=None, description="ARN do cluster Aurora (default: resource).")
    database: Optional[str] = Field(default=None, description="Banco/Database alvo.")
    schema: Optional[str] = Field(default=None, description="Schema alvo.")
    parameters: Optional[dict[str, Any]] = Field(default=None, description="Parâmetros nomeados (name -> value).")
    includeResultMetadata: bool = Field(default=False, description="Inclui metadados das colunas.")
    rulesBucket: Optional[str] = Field(default=None, description="Bucket S3 das regras (default env RULES_BUCKET).")
    rulesKey: Optional[str] = Field(default=None, description="Chave do .json de regras (default env RULES_KEY).")


class RdsDataRequest(BaseModel):
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
    params: RdsDataParams


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
