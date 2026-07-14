"""Modelos do contrato (envelope + params da ação dbca)."""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class DbcaParams(BaseModel):
    queryId: str = Field(
        description="Id da query de metadados (configurada por admin). Vira o botão-ação no frontend.",
    )
    database: Optional[str] = Field(default=None, description="Banco/Database alvo (override do default da query).")
    parameters: Optional[dict[str, Any]] = Field(
        default=None, description="Parâmetros nomeados da query (name -> value)."
    )


class DbcaRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(description="Recurso alvo (nome/id/ARN do cluster Aurora ou tabela DynamoDB).")
    roleArn: Optional[str] = Field(
        default=None,
        pattern=r"^arn:aws:iam::\d{12}:role/.+$",
        description="Role para assume-role. Opcional: se ausente, derivada da conta pela convenção da plataforma.",
    )
    region: Optional[str] = Field(
        default=None, pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS. Opcional (default do serviço)."
    )
    environment: Literal["dev", "homol", "staging", "prod"] = Field(
        description="Ambiente target. As regras/queries podem variar por ambiente."
    )
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Descobre o recurso e resolve a query, sem executar.")
    params: DbcaParams


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
