"""Catálogo de operações do serviço `restore` (gerado de catalog.json).

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
    Operation("rds:BacktrackDBCluster", "BacktrackDBCluster", "backtrack_db_cluster", "rds", "backup", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:CancelExportTask", "CancelExportTask", "cancel_export_task", "rds", "backup", True, "ExportTaskIdentifier", "export-task"),
    Operation("rds:CopyDBClusterParameterGroup", "CopyDBClusterParameterGroup", "copy_db_cluster_parameter_group", "rds", "backup", True, "SourceDBClusterParameterGroupIdentifier", None),
    Operation("rds:CopyDBClusterSnapshot", "CopyDBClusterSnapshot", "copy_db_cluster_snapshot", "rds", "backup", True, "SourceDBClusterSnapshotIdentifier", None),
    Operation("rds:CopyDBParameterGroup", "CopyDBParameterGroup", "copy_db_parameter_group", "rds", "backup", True, "SourceDBParameterGroupIdentifier", None),
    Operation("rds:CopyDBSnapshot", "CopyDBSnapshot", "copy_db_snapshot", "rds", "backup", True, "OptionGroupName", "option-group"),
    Operation("rds:CopyOptionGroup", "CopyOptionGroup", "copy_option_group", "rds", "backup", True, "SourceOptionGroupIdentifier", None),
    Operation("elasticache:CopyServerlessCacheSnapshot", "CopyServerlessCacheSnapshot", "copy_serverless_cache_snapshot", "elasticache", "backup", True, "SourceServerlessCacheSnapshotName", None),
    Operation("elasticache:CopySnapshot", "CopySnapshot", "copy_snapshot", "elasticache", "backup", True, "SourceSnapshotName", None),
    Operation("elasticache:ExportServerlessCacheSnapshot", "ExportServerlessCacheSnapshot", "export_serverless_cache_snapshot", "elasticache", "backup", True, "ServerlessCacheSnapshotName", "serverless-cache-snapshot"),
    Operation("dynamodb:ExportTableToPointInTime", "ExportTableToPointInTime", "export_table_to_point_in_time", "dynamodb", "backup", True, "TableArn", "table"),
    Operation("dynamodb:ImportTable", "ImportTable", "import_table", "dynamodb", "backup", True, "S3BucketSource", None),
    Operation("rds:RestoreDBClusterFromS3", "RestoreDBClusterFromS3", "restore_db_cluster_from_s3", "rds", "backup", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:RestoreDBClusterFromSnapshot", "RestoreDBClusterFromSnapshot", "restore_db_cluster_from_snapshot", "rds", "backup", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:RestoreDBClusterToPointInTime", "RestoreDBClusterToPointInTime", "restore_db_cluster_to_point_in_time", "rds", "backup", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:RestoreDBInstanceFromDBSnapshot", "RestoreDBInstanceFromDBSnapshot", "restore_db_instance_from_db_snapshot", "rds", "backup", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:RestoreDBInstanceFromS3", "RestoreDBInstanceFromS3", "restore_db_instance_from_s3", "rds", "backup", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:RestoreDBInstanceToPointInTime", "RestoreDBInstanceToPointInTime", "restore_db_instance_to_point_in_time", "rds", "backup", True, "DBParameterGroupName", "db-parameter-group"),
    Operation("dynamodb:RestoreTableFromBackup", "RestoreTableFromBackup", "restore_table_from_backup", "dynamodb", "backup", True, "TargetTableName", "table"),
    Operation("dynamodb:RestoreTableToPointInTime", "RestoreTableToPointInTime", "restore_table_to_point_in_time", "dynamodb", "backup", True, "TargetTableName", "table"),
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
