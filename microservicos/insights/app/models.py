"""Modelos do contrato (envelope + params da ação de insights).

Validadores propositalmente lenientes para que o frontend chame localmente:
`account`/`roleArn` seguem padrão (as Settings do frontend os fornecem), mas
`region` e `environment` têm default para permitir chamadas mínimas.
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

Action = Literal["resources", "metrics", "logs", "metadata", "finops"]
Product = Literal[
    "rds", "ec2", "ebs", "elb", "eip", "snapshot", "kms", "vpc-endpoint", "all"
]


class InsightsParams(BaseModel):
    action: Action = Field(description="Ação de analytics a executar.")
    product: Product = Field(default="all", description="Produto AWS alvo (ou 'all').")
    resourceId: Optional[str] = Field(default=None, description="ID do recurso (metrics/logs/metadata).")
    filters: Optional[dict] = Field(default=None, description="{search?, status?, env?, type?, tag?}.")
    metric: Optional[str] = Field(default=None, description="cpu|memory|connections|iops|storageUsed|latency|freeableMemory.")
    lookback: Optional[int] = Field(default=None, description="minutos (metrics/logs) ou dias (tendências finops).")
    level: Optional[str] = Field(default=None, description="logs: error|warn|info.")
    limit: Optional[int] = Field(default=None, description="Limite de itens retornados.")


class InsightsRequest(BaseModel):
    account: str = Field(pattern=r"^\d{12}$", description="Conta AWS alvo (12 dígitos).")
    resource: Optional[str] = Field(default=None, description="Nome ou ARN do recurso alvo (opcional).")
    roleArn: str = Field(pattern=r"^arn:aws:iam::\d{12}:role/.+$", description="Role para assume-role.")
    region: str = Field(default="sa-east-1", description="Região AWS.")
    environment: Literal["dev", "homol", "staging", "prod"] = Field(default="dev", description="Ambiente target.")
    requestId: Optional[str] = Field(default=None, description="Idempotency key opcional.")
    dryRun: bool = Field(default=False, description="Valida sem executar.")
    params: InsightsParams


class ActionResult(BaseModel):
    operationId: str
    status: str = "ok"
    product: str
    action: str
    detail: dict


class ErrorResponse(BaseModel):
    code: str
    message: str
    requestId: Optional[str] = None
