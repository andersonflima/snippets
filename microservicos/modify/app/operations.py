"""Catálogo de operações do serviço `modify` (gerado de catalog.json).

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
    Operation("rds:AddRoleToDBCluster", "AddRoleToDBCluster", "add_role_to_db_cluster", "rds", "config", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:AddRoleToDBInstance", "AddRoleToDBInstance", "add_role_to_db_instance", "rds", "config", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:AddSourceIdentifierToSubscription", "AddSourceIdentifierToSubscription", "add_source_identifier_to_subscription", "rds", "config", True, "SubscriptionName", None),
    Operation("rds:AddTagsToResource", "AddTagsToResource", "add_tags_to_resource", "rds", "config", True, "ResourceName", "tagged-resource"),
    Operation("elasticache:AddTagsToResource", "AddTagsToResource", "add_tags_to_resource", "elasticache", "config", True, "ResourceName", "tagged-resource"),
    Operation("rds:ApplyPendingMaintenanceAction", "ApplyPendingMaintenanceAction", "apply_pending_maintenance_action", "rds", "config", True, "ResourceIdentifier", None),
    Operation("elasticache:AuthorizeCacheSecurityGroupIngress", "AuthorizeCacheSecurityGroupIngress", "authorize_cache_security_group_ingress", "elasticache", "config", True, "CacheSecurityGroupName", "cache-security-group"),
    Operation("rds:AuthorizeDBSecurityGroupIngress", "AuthorizeDBSecurityGroupIngress", "authorize_db_security_group_ingress", "rds", "config", True, "DBSecurityGroupName", "db-security-group"),
    Operation("elasticache:BatchApplyUpdateAction", "BatchApplyUpdateAction", "batch_apply_update_action", "elasticache", "config", True, "ServiceUpdateName", None),
    Operation("elasticache:BatchStopUpdateAction", "BatchStopUpdateAction", "batch_stop_update_action", "elasticache", "config", True, "ServiceUpdateName", None),
    Operation("elasticache:DecreaseNodeGroupsInGlobalReplicationGroup", "DecreaseNodeGroupsInGlobalReplicationGroup", "decrease_node_groups_in_global_replication_group", "elasticache", "config", True, "GlobalReplicationGroupId", "global-replication-group"),
    Operation("elasticache:DecreaseReplicaCount", "DecreaseReplicaCount", "decrease_replica_count", "elasticache", "config", True, "ReplicationGroupId", "replication-group"),
    Operation("rds:DisableHttpEndpoint", "DisableHttpEndpoint", "disable_http_endpoint", "rds", "config", True, "ResourceArn", "tagged-resource"),
    Operation("dynamodb:DisableKinesisStreamingDestination", "DisableKinesisStreamingDestination", "disable_kinesis_streaming_destination", "dynamodb", "config", True, "TableName", "table"),
    Operation("elasticache:DisassociateGlobalReplicationGroup", "DisassociateGlobalReplicationGroup", "disassociate_global_replication_group", "elasticache", "config", True, "ReplicationGroupId", "replication-group"),
    Operation("rds:EnableHttpEndpoint", "EnableHttpEndpoint", "enable_http_endpoint", "rds", "config", True, "ResourceArn", "tagged-resource"),
    Operation("dynamodb:EnableKinesisStreamingDestination", "EnableKinesisStreamingDestination", "enable_kinesis_streaming_destination", "dynamodb", "config", True, "TableName", "table"),
    Operation("elasticache:IncreaseNodeGroupsInGlobalReplicationGroup", "IncreaseNodeGroupsInGlobalReplicationGroup", "increase_node_groups_in_global_replication_group", "elasticache", "config", True, "GlobalReplicationGroupId", "global-replication-group"),
    Operation("elasticache:IncreaseReplicaCount", "IncreaseReplicaCount", "increase_replica_count", "elasticache", "config", True, "ReplicationGroupId", "replication-group"),
    Operation("rds:ModifyActivityStream", "ModifyActivityStream", "modify_activity_stream", "rds", "config", True, None, None),
    Operation("elasticache:ModifyCacheCluster", "ModifyCacheCluster", "modify_cache_cluster", "elasticache", "config", True, "CacheClusterId", "cache-cluster"),
    Operation("elasticache:ModifyCacheParameterGroup", "ModifyCacheParameterGroup", "modify_cache_parameter_group", "elasticache", "config", True, "CacheParameterGroupName", "cache-parameter-group"),
    Operation("elasticache:ModifyCacheSubnetGroup", "ModifyCacheSubnetGroup", "modify_cache_subnet_group", "elasticache", "config", True, "CacheSubnetGroupName", "cache-subnet-group"),
    Operation("rds:ModifyCertificates", "ModifyCertificates", "modify_certificates", "rds", "config", True, None, None),
    Operation("rds:ModifyCurrentDBClusterCapacity", "ModifyCurrentDBClusterCapacity", "modify_current_db_cluster_capacity", "rds", "config", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:ModifyCustomDBEngineVersion", "ModifyCustomDBEngineVersion", "modify_custom_db_engine_version", "rds", "config", True, "Engine", None),
    Operation("rds:ModifyDBCluster", "ModifyDBCluster", "modify_db_cluster", "rds", "config", True, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:ModifyDBClusterEndpoint", "ModifyDBClusterEndpoint", "modify_db_cluster_endpoint", "rds", "config", True, "DBClusterEndpointIdentifier", None),
    Operation("rds:ModifyDBClusterParameterGroup", "ModifyDBClusterParameterGroup", "modify_db_cluster_parameter_group", "rds", "config", True, "DBClusterParameterGroupName", "db-cluster-parameter-group"),
    Operation("rds:ModifyDBInstance", "ModifyDBInstance", "modify_db_instance", "rds", "config", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:ModifyDBParameterGroup", "ModifyDBParameterGroup", "modify_db_parameter_group", "rds", "config", True, "DBParameterGroupName", "db-parameter-group"),
    Operation("rds:ModifyDBProxy", "ModifyDBProxy", "modify_db_proxy", "rds", "config", True, "DBProxyName", "db-proxy"),
    Operation("rds:ModifyDBProxyEndpoint", "ModifyDBProxyEndpoint", "modify_db_proxy_endpoint", "rds", "config", True, "DBProxyEndpointName", "db-proxy-endpoint"),
    Operation("rds:ModifyDBProxyTargetGroup", "ModifyDBProxyTargetGroup", "modify_db_proxy_target_group", "rds", "config", True, "DBProxyName", "db-proxy"),
    Operation("rds:ModifyDBRecommendation", "ModifyDBRecommendation", "modify_db_recommendation", "rds", "config", True, "RecommendationId", None),
    Operation("rds:ModifyDBShardGroup", "ModifyDBShardGroup", "modify_db_shard_group", "rds", "config", True, "DBShardGroupIdentifier", None),
    Operation("rds:ModifyDBSnapshot", "ModifyDBSnapshot", "modify_db_snapshot", "rds", "config", True, "DBSnapshotIdentifier", "db-snapshot"),
    Operation("rds:ModifyDBSubnetGroup", "ModifyDBSubnetGroup", "modify_db_subnet_group", "rds", "config", True, "DBSubnetGroupName", "db-subnet-group"),
    Operation("rds:ModifyEventSubscription", "ModifyEventSubscription", "modify_event_subscription", "rds", "config", True, "SubscriptionName", None),
    Operation("rds:ModifyGlobalCluster", "ModifyGlobalCluster", "modify_global_cluster", "rds", "config", True, "GlobalClusterIdentifier", "global-cluster"),
    Operation("elasticache:ModifyGlobalReplicationGroup", "ModifyGlobalReplicationGroup", "modify_global_replication_group", "elasticache", "config", True, "GlobalReplicationGroupId", "global-replication-group"),
    Operation("rds:ModifyIntegration", "ModifyIntegration", "modify_integration", "rds", "config", True, "IntegrationIdentifier", None),
    Operation("rds:ModifyOptionGroup", "ModifyOptionGroup", "modify_option_group", "rds", "config", True, "OptionGroupName", "option-group"),
    Operation("elasticache:ModifyReplicationGroup", "ModifyReplicationGroup", "modify_replication_group", "elasticache", "config", True, "ReplicationGroupId", "replication-group"),
    Operation("elasticache:ModifyReplicationGroupShardConfiguration", "ModifyReplicationGroupShardConfiguration", "modify_replication_group_shard_configuration", "elasticache", "config", True, "ReplicationGroupId", "replication-group"),
    Operation("elasticache:ModifyServerlessCache", "ModifyServerlessCache", "modify_serverless_cache", "elasticache", "config", True, "ServerlessCacheName", "serverless-cache"),
    Operation("rds:ModifyTenantDatabase", "ModifyTenantDatabase", "modify_tenant_database", "rds", "config", True, "DBInstanceIdentifier", "db-instance"),
    Operation("elasticache:ModifyUser", "ModifyUser", "modify_user", "elasticache", "config", True, "UserId", "cache-user"),
    Operation("elasticache:ModifyUserGroup", "ModifyUserGroup", "modify_user_group", "elasticache", "config", True, "UserGroupId", "cache-user-group"),
    Operation("rds:PromoteReadReplica", "PromoteReadReplica", "promote_read_replica", "rds", "config", True, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:PromoteReadReplicaDBCluster", "PromoteReadReplicaDBCluster", "promote_read_replica_db_cluster", "rds", "config", True, "DBClusterIdentifier", "db-cluster"),
    Operation("dynamodb:PutResourcePolicy", "PutResourcePolicy", "put_resource_policy", "dynamodb", "config", True, "ResourceArn", "tagged-resource"),
    Operation("elasticache:RebalanceSlotsInGlobalReplicationGroup", "RebalanceSlotsInGlobalReplicationGroup", "rebalance_slots_in_global_replication_group", "elasticache", "config", True, "GlobalReplicationGroupId", "global-replication-group"),
    Operation("elasticache:ResetCacheParameterGroup", "ResetCacheParameterGroup", "reset_cache_parameter_group", "elasticache", "config", True, "CacheParameterGroupName", "cache-parameter-group"),
    Operation("rds:ResetDBClusterParameterGroup", "ResetDBClusterParameterGroup", "reset_db_cluster_parameter_group", "rds", "config", True, "DBClusterParameterGroupName", "db-cluster-parameter-group"),
    Operation("rds:ResetDBParameterGroup", "ResetDBParameterGroup", "reset_db_parameter_group", "rds", "config", True, "DBParameterGroupName", "db-parameter-group"),
    Operation("elasticache:RevokeCacheSecurityGroupIngress", "RevokeCacheSecurityGroupIngress", "revoke_cache_security_group_ingress", "elasticache", "config", True, "CacheSecurityGroupName", "cache-security-group"),
    Operation("rds:RevokeDBSecurityGroupIngress", "RevokeDBSecurityGroupIngress", "revoke_db_security_group_ingress", "rds", "config", True, "DBSecurityGroupName", "db-security-group"),
    Operation("dynamodb:TagResource", "TagResource", "tag_resource", "dynamodb", "config", True, "ResourceArn", "tagged-resource"),
    Operation("dynamodb:UntagResource", "UntagResource", "untag_resource", "dynamodb", "config", True, "ResourceArn", "tagged-resource"),
    Operation("dynamodb:UpdateContinuousBackups", "UpdateContinuousBackups", "update_continuous_backups", "dynamodb", "config", True, "TableName", "table"),
    Operation("dynamodb:UpdateContributorInsights", "UpdateContributorInsights", "update_contributor_insights", "dynamodb", "config", True, "TableName", "table"),
    Operation("dynamodb:UpdateGlobalTable", "UpdateGlobalTable", "update_global_table", "dynamodb", "config", True, "GlobalTableName", "global-table"),
    Operation("dynamodb:UpdateGlobalTableSettings", "UpdateGlobalTableSettings", "update_global_table_settings", "dynamodb", "config", True, "GlobalTableName", "global-table"),
    Operation("dynamodb:UpdateKinesisStreamingDestination", "UpdateKinesisStreamingDestination", "update_kinesis_streaming_destination", "dynamodb", "config", True, "TableName", "table"),
    Operation("dynamodb:UpdateTable", "UpdateTable", "update_table", "dynamodb", "config", True, "TableName", "table"),
    Operation("dynamodb:UpdateTableReplicaAutoScaling", "UpdateTableReplicaAutoScaling", "update_table_replica_auto_scaling", "dynamodb", "config", True, "TableName", "table"),
    Operation("dynamodb:UpdateTimeToLive", "UpdateTimeToLive", "update_time_to_live", "dynamodb", "config", True, "TableName", "table"),
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
