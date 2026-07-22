"""Catálogo de operações do serviço `start-stop` (gerado de catalog.json).

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
    Operation("rds:FailoverDBCluster", "FailoverDBCluster", "failover_db_cluster", "rds", "power", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:FailoverGlobalCluster", "FailoverGlobalCluster", "failover_global_cluster", "rds", "power", True, "GlobalClusterIdentifier", "global-cluster"),
    Operation("elasticache:FailoverGlobalReplicationGroup", "FailoverGlobalReplicationGroup", "failover_global_replication_group", "elasticache", "power", True, "GlobalReplicationGroupId", "global-replication-group"),
    Operation("elasticache:RebootCacheCluster", "RebootCacheCluster", "reboot_cache_cluster", "elasticache", "power", True, "CacheClusterId", "cache-cluster"),
    Operation("rds:RebootDBCluster", "RebootDBCluster", "reboot_db_cluster", "rds", "power", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:RebootDBInstance", "RebootDBInstance", "reboot_db_instance", "rds", "power", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:RebootDBShardGroup", "RebootDBShardGroup", "reboot_db_shard_group", "rds", "power", True, "DBShardGroupIdentifier", None),
    Operation("rds:StartActivityStream", "StartActivityStream", "start_activity_stream", "rds", "power", True, "ResourceArn", "tagged-resource"),
    Operation("rds:StartDBCluster", "StartDBCluster", "start_db_cluster", "rds", "power", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:StartDBInstance", "StartDBInstance", "start_db_instance", "rds", "power", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:StartDBInstanceAutomatedBackupsReplication", "StartDBInstanceAutomatedBackupsReplication", "start_db_instance_automated_backups_replication", "rds", "power", True, "SourceDBInstanceArn", None),
    Operation("rds:StartExportTask", "StartExportTask", "start_export_task", "rds", "power", True, "ExportTaskIdentifier", "export-task"),
    Operation("rds:StopActivityStream", "StopActivityStream", "stop_activity_stream", "rds", "power", True, "ResourceArn", "tagged-resource"),
    Operation("rds:StopDBCluster", "StopDBCluster", "stop_db_cluster", "rds", "power", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:StopDBInstance", "StopDBInstance", "stop_db_instance", "rds", "power", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:StopDBInstanceAutomatedBackupsReplication", "StopDBInstanceAutomatedBackupsReplication", "stop_db_instance_automated_backups_replication", "rds", "power", True, "SourceDBInstanceArn", None),
    Operation("rds:SwitchoverBlueGreenDeployment", "SwitchoverBlueGreenDeployment", "switchover_blue_green_deployment", "rds", "power", True, "BlueGreenDeploymentIdentifier", "blue-green-deployment"),
    Operation("rds:SwitchoverGlobalCluster", "SwitchoverGlobalCluster", "switchover_global_cluster", "rds", "power", True, "GlobalClusterIdentifier", "global-cluster"),
    Operation("rds:SwitchoverReadReplica", "SwitchoverReadReplica", "switchover_read_replica", "rds", "power", True, "DBInstanceIdentifier", "db-instance"),
    Operation("elasticache:TestFailover", "TestFailover", "test_failover", "elasticache", "power", True, "ReplicationGroupId", "replication-group"),
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
