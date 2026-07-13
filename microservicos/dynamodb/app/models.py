"""Modelos do contrato (envelope + params da ação DynamoDB)."""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class DynamoParams(BaseModel):
    operation: str = Field(
        description=(
            "Operação DynamoDB a executar (ex.: CreateTable, DeleteTable, UpdateTable, "
            "PutItem, GetItem, UpdateItem, DeleteItem, Query, Scan, BatchWriteItem, "
            "TransactWriteItems, CreateBackup, RestoreTableToPointInTime, "
            "UpdateContinuousBackups, ExecuteStatement, TagResource...). "
            "O que pode/não pode é decidido pelas regras externas por ambiente."
        )
    )
    args: dict[str, Any] = Field(
        default_factory=dict,
        description="Kwargs da operação boto3 (ex.: TableName, Item, KeySchema, KeyConditionExpression...).",
    )


class DynamoRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: str = Field(
        description="Recurso alvo (nome da tabela/backup/ARN, ou '*' para operações de conta como ListTables).",
    )
    roleArn: str = Field(
        pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role para STS:AssumeRole na conta alvo."
    )
    region: str = Field(pattern=r"^[a-z]{2}-[a-z]+-\d$", description="Região AWS.")
    environment: Literal["dev", "homol", "staging", "prod"] = Field(
        description="Ambiente target. As regras externas segregam o que é permitido por ambiente."
    )
    changeNumber: Optional[str] = Field(
        default=None, description="Número da GMUD (obrigatório para mutações em produção, salvo exceção liberada)."
    )
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida contra as regras e resolve a operação, sem chamar a AWS.")
    params: DynamoParams


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
