"""Catálogo de operações do serviço `replicate` (gerado de catalog.json).

NÃO editar à mão: rode `python gen_catalog.py && python gen_action_services.py`.
Cada operação mapeia name -> (client boto3, método, categoria, mutating,
resourceArg, resourceType). O handler despacha genericamente via getattr.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    key: str            # "<client>:<Name>" — chave global única
    name: str
    method: str
    client: str
    category: str
    mutating: bool
    resource_arg: str | None
    resource_type: str | None


_OPS: tuple[Operation, ...] = (
    Operation("elasticache:CompleteMigration", "CompleteMigration", "complete_migration", "elasticache", "replicate", True, "ReplicationGroupId", "replication-group"),
    Operation("rds:ModifyDBClusterSnapshotAttribute", "ModifyDBClusterSnapshotAttribute", "modify_db_cluster_snapshot_attribute", "rds", "replicate", True, "DBClusterSnapshotIdentifier", "db-cluster-snapshot"),
    Operation("rds:ModifyDBSnapshotAttribute", "ModifyDBSnapshotAttribute", "modify_db_snapshot_attribute", "rds", "replicate", True, "DBSnapshotIdentifier", "db-snapshot"),
    Operation("elasticache:StartMigration", "StartMigration", "start_migration", "elasticache", "replicate", True, "ReplicationGroupId", "replication-group"),
    Operation("elasticache:TestMigration", "TestMigration", "test_migration", "elasticache", "replicate", True, "ReplicationGroupId", "replication-group"),
)

CATALOG: dict[str, Operation] = {op.key: op for op in _OPS}
CLIENTS: tuple[str, ...] = tuple(sorted({op.client for op in _OPS}))


def resolve(key: str) -> Operation | None:
    """Resolve por chave '<client>:<Name>'. Aceita também o nome cru quando não
    houver colisão entre clients (conveniência)."""
    op = CATALOG.get(key)
    if op is not None:
        return op
    matches = [o for o in _OPS if o.name == key]
    return matches[0] if len(matches) == 1 else None


def resource_of(op: Operation, args: dict) -> str | None:
    if op.resource_arg and isinstance(args.get(op.resource_arg), str):
        return args[op.resource_arg]
    return None
