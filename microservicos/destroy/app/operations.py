"""Catálogo de operações do serviço `destroy` (gerado de catalog.json).

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
    Operation("dynamodb:DeleteBackup", "DeleteBackup", "delete_backup", "dynamodb", "delete", True, "BackupArn", "table-backup"),
    Operation("rds:DeleteBlueGreenDeployment", "DeleteBlueGreenDeployment", "delete_blue_green_deployment", "rds", "delete", True, "BlueGreenDeploymentIdentifier", "blue-green-deployment"),
    Operation("elasticache:DeleteCacheCluster", "DeleteCacheCluster", "delete_cache_cluster", "elasticache", "delete", True, "CacheClusterId", "cache-cluster"),
    Operation("elasticache:DeleteCacheParameterGroup", "DeleteCacheParameterGroup", "delete_cache_parameter_group", "elasticache", "delete", True, "CacheParameterGroupName", "cache-parameter-group"),
    Operation("elasticache:DeleteCacheSecurityGroup", "DeleteCacheSecurityGroup", "delete_cache_security_group", "elasticache", "delete", True, "CacheSecurityGroupName", "cache-security-group"),
    Operation("elasticache:DeleteCacheSubnetGroup", "DeleteCacheSubnetGroup", "delete_cache_subnet_group", "elasticache", "delete", True, "CacheSubnetGroupName", "cache-subnet-group"),
    Operation("rds:DeleteCustomDBEngineVersion", "DeleteCustomDBEngineVersion", "delete_custom_db_engine_version", "rds", "delete", True, "Engine", None),
    Operation("rds:DeleteDBCluster", "DeleteDBCluster", "delete_db_cluster", "rds", "delete", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:DeleteDBClusterAutomatedBackup", "DeleteDBClusterAutomatedBackup", "delete_db_cluster_automated_backup", "rds", "delete", True, "DbClusterResourceId", None),
    Operation("rds:DeleteDBClusterEndpoint", "DeleteDBClusterEndpoint", "delete_db_cluster_endpoint", "rds", "delete", True, "DBClusterEndpointIdentifier", None),
    Operation("rds:DeleteDBClusterParameterGroup", "DeleteDBClusterParameterGroup", "delete_db_cluster_parameter_group", "rds", "delete", True, "DBClusterParameterGroupName", "db-cluster-parameter-group"),
    Operation("rds:DeleteDBClusterSnapshot", "DeleteDBClusterSnapshot", "delete_db_cluster_snapshot", "rds", "delete", True, "DBClusterSnapshotIdentifier", "db-cluster-snapshot"),
    Operation("rds:DeleteDBInstance", "DeleteDBInstance", "delete_db_instance", "rds", "delete", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:DeleteDBInstanceAutomatedBackup", "DeleteDBInstanceAutomatedBackup", "delete_db_instance_automated_backup", "rds", "delete", True, None, None),
    Operation("rds:DeleteDBParameterGroup", "DeleteDBParameterGroup", "delete_db_parameter_group", "rds", "delete", True, "DBParameterGroupName", "db-parameter-group"),
    Operation("rds:DeleteDBProxy", "DeleteDBProxy", "delete_db_proxy", "rds", "delete", True, "DBProxyName", "db-proxy"),
    Operation("rds:DeleteDBProxyEndpoint", "DeleteDBProxyEndpoint", "delete_db_proxy_endpoint", "rds", "delete", True, "DBProxyEndpointName", "db-proxy-endpoint"),
    Operation("rds:DeleteDBSecurityGroup", "DeleteDBSecurityGroup", "delete_db_security_group", "rds", "delete", True, "DBSecurityGroupName", "db-security-group"),
    Operation("rds:DeleteDBShardGroup", "DeleteDBShardGroup", "delete_db_shard_group", "rds", "delete", True, "DBShardGroupIdentifier", None),
    Operation("rds:DeleteDBSnapshot", "DeleteDBSnapshot", "delete_db_snapshot", "rds", "delete", True, "DBSnapshotIdentifier", "db-snapshot"),
    Operation("rds:DeleteDBSubnetGroup", "DeleteDBSubnetGroup", "delete_db_subnet_group", "rds", "delete", True, "DBSubnetGroupName", "db-subnet-group"),
    Operation("rds:DeleteEventSubscription", "DeleteEventSubscription", "delete_event_subscription", "rds", "delete", True, "SubscriptionName", None),
    Operation("rds:DeleteGlobalCluster", "DeleteGlobalCluster", "delete_global_cluster", "rds", "delete", True, "GlobalClusterIdentifier", "global-cluster"),
    Operation("elasticache:DeleteGlobalReplicationGroup", "DeleteGlobalReplicationGroup", "delete_global_replication_group", "elasticache", "delete", True, "GlobalReplicationGroupId", "global-replication-group"),
    Operation("rds:DeleteIntegration", "DeleteIntegration", "delete_integration", "rds", "delete", True, "IntegrationIdentifier", None),
    Operation("rds:DeleteOptionGroup", "DeleteOptionGroup", "delete_option_group", "rds", "delete", True, "OptionGroupName", "option-group"),
    Operation("elasticache:DeleteReplicationGroup", "DeleteReplicationGroup", "delete_replication_group", "elasticache", "delete", True, "ReplicationGroupId", "replication-group"),
    Operation("dynamodb:DeleteResourcePolicy", "DeleteResourcePolicy", "delete_resource_policy", "dynamodb", "delete", True, "ResourceArn", "tagged-resource"),
    Operation("elasticache:DeleteServerlessCache", "DeleteServerlessCache", "delete_serverless_cache", "elasticache", "delete", True, "ServerlessCacheName", "serverless-cache"),
    Operation("elasticache:DeleteServerlessCacheSnapshot", "DeleteServerlessCacheSnapshot", "delete_serverless_cache_snapshot", "elasticache", "delete", True, "ServerlessCacheSnapshotName", "serverless-cache-snapshot"),
    Operation("elasticache:DeleteSnapshot", "DeleteSnapshot", "delete_snapshot", "elasticache", "delete", True, "SnapshotName", "cache-snapshot"),
    Operation("dynamodb:DeleteTable", "DeleteTable", "delete_table", "dynamodb", "delete", True, "TableName", "table"),
    Operation("rds:DeleteTenantDatabase", "DeleteTenantDatabase", "delete_tenant_database", "rds", "delete", True, "DBInstanceIdentifier", "db-instance"),
    Operation("elasticache:DeleteUser", "DeleteUser", "delete_user", "elasticache", "delete", True, "UserId", "cache-user"),
    Operation("elasticache:DeleteUserGroup", "DeleteUserGroup", "delete_user_group", "elasticache", "delete", True, "UserGroupId", "cache-user-group"),
    Operation("rds:DeregisterDBProxyTargets", "DeregisterDBProxyTargets", "deregister_db_proxy_targets", "rds", "delete", True, "DBProxyName", "db-proxy"),
    Operation("rds:RemoveFromGlobalCluster", "RemoveFromGlobalCluster", "remove_from_global_cluster", "rds", "delete", True, "GlobalClusterIdentifier", "global-cluster"),
    Operation("rds:RemoveRoleFromDBCluster", "RemoveRoleFromDBCluster", "remove_role_from_db_cluster", "rds", "delete", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:RemoveRoleFromDBInstance", "RemoveRoleFromDBInstance", "remove_role_from_db_instance", "rds", "delete", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:RemoveSourceIdentifierFromSubscription", "RemoveSourceIdentifierFromSubscription", "remove_source_identifier_from_subscription", "rds", "delete", True, "SubscriptionName", None),
    Operation("rds:RemoveTagsFromResource", "RemoveTagsFromResource", "remove_tags_from_resource", "rds", "delete", True, "ResourceName", "tagged-resource"),
    Operation("elasticache:RemoveTagsFromResource", "RemoveTagsFromResource", "remove_tags_from_resource", "elasticache", "delete", True, "ResourceName", "tagged-resource"),
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
