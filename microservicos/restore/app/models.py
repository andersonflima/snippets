"""Modelos do contrato (envelope + params da ação)."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class RestoreParams(BaseModel):
    operation: str = Field(
        description="create-snapshot | restore-snapshot | create-cluster-snapshot | restore-cluster-snapshot"
    )
    snapshotIdentifier: Optional[str] = Field(default=None, description="Snapshot origem (restore) ou destino (create).")
    targetInstanceIdentifier: Optional[str] = Field(default=None, description="Instância resultante (restore de instância).")
    targetClusterIdentifier: Optional[str] = Field(default=None, description="Cluster Aurora resultante (restore de cluster).")
    dbInstanceClass: Optional[str] = Field(default=None, description="Classe da instância restaurada / dos membros do cluster.")
    dbSubnetGroupName: Optional[str] = Field(default=None, description="Subnet group de destino.")
    engine: Optional[str] = Field(default=None, description="Engine do cluster Aurora (ex.: aurora-mysql, aurora-postgresql).")
    engineVersion: Optional[str] = Field(default=None, description="Versão da engine no restore de cluster.")
    clusterInstanceCount: Optional[int] = Field(default=None, ge=1, description="Membros a criar no cluster restaurado (default 1).")
    kmsKeyId: Optional[str] = Field(default=None, description="KMS key da instância/cluster/snapshot resultante.")


class RestoreRequest(BaseModel):
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
    params: RestoreParams


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
