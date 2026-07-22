"""Catálogo de operações do serviço `create` (gerado de catalog.json).

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
    Operation("dynamodb:CreateBackup", "CreateBackup", "create_backup", "dynamodb", "provision", True, "TableName", "table"),
    Operation("rds:CreateBlueGreenDeployment", "CreateBlueGreenDeployment", "create_blue_green_deployment", "rds", "provision", True, "BlueGreenDeploymentName", None),
    Operation("elasticache:CreateCacheCluster", "CreateCacheCluster", "create_cache_cluster", "elasticache", "provision", True, "CacheClusterId", "cache-cluster"),
    Operation("elasticache:CreateCacheParameterGroup", "CreateCacheParameterGroup", "create_cache_parameter_group", "elasticache", "provision", True, "CacheParameterGroupName", "cache-parameter-group"),
    Operation("elasticache:CreateCacheSecurityGroup", "CreateCacheSecurityGroup", "create_cache_security_group", "elasticache", "provision", True, "CacheSecurityGroupName", "cache-security-group"),
    Operation("elasticache:CreateCacheSubnetGroup", "CreateCacheSubnetGroup", "create_cache_subnet_group", "elasticache", "provision", True, "CacheSubnetGroupName", "cache-subnet-group"),
    Operation("rds:CreateCustomDBEngineVersion", "CreateCustomDBEngineVersion", "create_custom_db_engine_version", "rds", "provision", True, "Engine", None),
    Operation("rds:CreateDBCluster", "CreateDBCluster", "create_db_cluster", "rds", "provision", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:CreateDBClusterEndpoint", "CreateDBClusterEndpoint", "create_db_cluster_endpoint", "rds", "provision", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:CreateDBClusterParameterGroup", "CreateDBClusterParameterGroup", "create_db_cluster_parameter_group", "rds", "provision", True, "DBClusterParameterGroupName", "db-cluster-parameter-group"),
    Operation("rds:CreateDBClusterSnapshot", "CreateDBClusterSnapshot", "create_db_cluster_snapshot", "rds", "provision", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:CreateDBInstance", "CreateDBInstance", "create_db_instance", "rds", "provision", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:CreateDBInstanceReadReplica", "CreateDBInstanceReadReplica", "create_db_instance_read_replica", "rds", "provision", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:CreateDBParameterGroup", "CreateDBParameterGroup", "create_db_parameter_group", "rds", "provision", True, "DBParameterGroupName", "db-parameter-group"),
    Operation("rds:CreateDBProxy", "CreateDBProxy", "create_db_proxy", "rds", "provision", True, "DBProxyName", "db-proxy"),
    Operation("rds:CreateDBProxyEndpoint", "CreateDBProxyEndpoint", "create_db_proxy_endpoint", "rds", "provision", True, "DBProxyName", "db-proxy"),
    Operation("rds:CreateDBSecurityGroup", "CreateDBSecurityGroup", "create_db_security_group", "rds", "provision", True, "DBSecurityGroupName", "db-security-group"),
    Operation("rds:CreateDBShardGroup", "CreateDBShardGroup", "create_db_shard_group", "rds", "provision", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:CreateDBSnapshot", "CreateDBSnapshot", "create_db_snapshot", "rds", "provision", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:CreateDBSubnetGroup", "CreateDBSubnetGroup", "create_db_subnet_group", "rds", "provision", True, "DBSubnetGroupName", "db-subnet-group"),
    Operation("rds:CreateEventSubscription", "CreateEventSubscription", "create_event_subscription", "rds", "provision", True, "SubscriptionName", None),
    Operation("rds:CreateGlobalCluster", "CreateGlobalCluster", "create_global_cluster", "rds", "provision", True, "GlobalClusterIdentifier", "global-cluster"),
    Operation("elasticache:CreateGlobalReplicationGroup", "CreateGlobalReplicationGroup", "create_global_replication_group", "elasticache", "provision", True, "GlobalReplicationGroupIdSuffix", None),
    Operation("dynamodb:CreateGlobalTable", "CreateGlobalTable", "create_global_table", "dynamodb", "provision", True, "GlobalTableName", "global-table"),
    Operation("rds:CreateIntegration", "CreateIntegration", "create_integration", "rds", "provision", True, "SourceArn", None),
    Operation("rds:CreateOptionGroup", "CreateOptionGroup", "create_option_group", "rds", "provision", True, "OptionGroupName", "option-group"),
    Operation("elasticache:CreateReplicationGroup", "CreateReplicationGroup", "create_replication_group", "elasticache", "provision", True, "ReplicationGroupId", "replication-group"),
    Operation("elasticache:CreateServerlessCache", "CreateServerlessCache", "create_serverless_cache", "elasticache", "provision", True, "ServerlessCacheName", "serverless-cache"),
    Operation("elasticache:CreateServerlessCacheSnapshot", "CreateServerlessCacheSnapshot", "create_serverless_cache_snapshot", "elasticache", "provision", True, "ServerlessCacheName", "serverless-cache"),
    Operation("elasticache:CreateSnapshot", "CreateSnapshot", "create_snapshot", "elasticache", "provision", True, "CacheClusterId", "cache-cluster"),
    Operation("dynamodb:CreateTable", "CreateTable", "create_table", "dynamodb", "provision", True, "TableName", "table"),
    Operation("rds:CreateTenantDatabase", "CreateTenantDatabase", "create_tenant_database", "rds", "provision", True, "DBInstanceIdentifier", "db-instance"),
    Operation("elasticache:CreateUser", "CreateUser", "create_user", "elasticache", "provision", True, "UserId", "cache-user"),
    Operation("elasticache:CreateUserGroup", "CreateUserGroup", "create_user_group", "elasticache", "provision", True, "UserGroupId", "cache-user-group"),
    Operation("elasticache:PurchaseReservedCacheNodesOffering", "PurchaseReservedCacheNodesOffering", "purchase_reserved_cache_nodes_offering", "elasticache", "provision", True, "ReservedCacheNodeId", "reserved-cache-node"),
    Operation("rds:PurchaseReservedDBInstancesOffering", "PurchaseReservedDBInstancesOffering", "purchase_reserved_db_instances_offering", "rds", "provision", True, "ReservedDBInstancesOfferingId", None),
    Operation("rds:RegisterDBProxyTargets", "RegisterDBProxyTargets", "register_db_proxy_targets", "rds", "provision", True, "DBProxyName", "db-proxy"),
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
