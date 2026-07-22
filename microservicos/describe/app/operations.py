"""Catálogo de operações do serviço `describe` (gerado de catalog.json).

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
    Operation("rds:DescribeAccountAttributes", "DescribeAccountAttributes", "describe_account_attributes", "rds", "read", False, None, None),
    Operation("dynamodb:DescribeBackup", "DescribeBackup", "describe_backup", "dynamodb", "read", False, "BackupArn", "table-backup"),
    Operation("rds:DescribeBlueGreenDeployments", "DescribeBlueGreenDeployments", "describe_blue_green_deployments", "rds", "read", False, "BlueGreenDeploymentIdentifier", "blue-green-deployment"),
    Operation("elasticache:DescribeCacheClusters", "DescribeCacheClusters", "describe_cache_clusters", "elasticache", "read", False, "CacheClusterId", "cache-cluster"),
    Operation("elasticache:DescribeCacheEngineVersions", "DescribeCacheEngineVersions", "describe_cache_engine_versions", "elasticache", "read", False, None, None),
    Operation("elasticache:DescribeCacheParameterGroups", "DescribeCacheParameterGroups", "describe_cache_parameter_groups", "elasticache", "read", False, "CacheParameterGroupName", "cache-parameter-group"),
    Operation("elasticache:DescribeCacheParameters", "DescribeCacheParameters", "describe_cache_parameters", "elasticache", "read", False, "CacheParameterGroupName", "cache-parameter-group"),
    Operation("elasticache:DescribeCacheSecurityGroups", "DescribeCacheSecurityGroups", "describe_cache_security_groups", "elasticache", "read", False, "CacheSecurityGroupName", "cache-security-group"),
    Operation("elasticache:DescribeCacheSubnetGroups", "DescribeCacheSubnetGroups", "describe_cache_subnet_groups", "elasticache", "read", False, "CacheSubnetGroupName", "cache-subnet-group"),
    Operation("rds:DescribeCertificates", "DescribeCertificates", "describe_certificates", "rds", "read", False, None, None),
    Operation("dynamodb:DescribeContinuousBackups", "DescribeContinuousBackups", "describe_continuous_backups", "dynamodb", "read", False, "TableName", "table"),
    Operation("dynamodb:DescribeContributorInsights", "DescribeContributorInsights", "describe_contributor_insights", "dynamodb", "read", False, "TableName", "table"),
    Operation("rds:DescribeDBClusterAutomatedBackups", "DescribeDBClusterAutomatedBackups", "describe_db_cluster_automated_backups", "rds", "read", False, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:DescribeDBClusterBacktracks", "DescribeDBClusterBacktracks", "describe_db_cluster_backtracks", "rds", "read", False, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:DescribeDBClusterEndpoints", "DescribeDBClusterEndpoints", "describe_db_cluster_endpoints", "rds", "read", False, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:DescribeDBClusterParameterGroups", "DescribeDBClusterParameterGroups", "describe_db_cluster_parameter_groups", "rds", "read", False, "DBClusterParameterGroupName", "db-cluster-parameter-group"),
    Operation("rds:DescribeDBClusterParameters", "DescribeDBClusterParameters", "describe_db_cluster_parameters", "rds", "read", False, "DBClusterParameterGroupName", "db-cluster-parameter-group"),
    Operation("rds:DescribeDBClusterSnapshotAttributes", "DescribeDBClusterSnapshotAttributes", "describe_db_cluster_snapshot_attributes", "rds", "read", False, "DBClusterSnapshotIdentifier", "db-cluster-snapshot"),
    Operation("rds:DescribeDBClusterSnapshots", "DescribeDBClusterSnapshots", "describe_db_cluster_snapshots", "rds", "read", False, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:DescribeDBClusters", "DescribeDBClusters", "describe_db_clusters", "rds", "read", False, "DBClusterIdentifier", "db-cluster"),
    Operation("rds:DescribeDBEngineVersions", "DescribeDBEngineVersions", "describe_db_engine_versions", "rds", "read", False, None, None),
    Operation("rds:DescribeDBInstanceAutomatedBackups", "DescribeDBInstanceAutomatedBackups", "describe_db_instance_automated_backups", "rds", "read", False, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:DescribeDBInstances", "DescribeDBInstances", "describe_db_instances", "rds", "read", False, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:DescribeDBLogFiles", "DescribeDBLogFiles", "describe_db_log_files", "rds", "read", False, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:DescribeDBMajorEngineVersions", "DescribeDBMajorEngineVersions", "describe_db_major_engine_versions", "rds", "read", False, None, None),
    Operation("rds:DescribeDBParameterGroups", "DescribeDBParameterGroups", "describe_db_parameter_groups", "rds", "read", False, "DBParameterGroupName", "db-parameter-group"),
    Operation("rds:DescribeDBParameters", "DescribeDBParameters", "describe_db_parameters", "rds", "read", False, "DBParameterGroupName", "db-parameter-group"),
    Operation("rds:DescribeDBProxies", "DescribeDBProxies", "describe_db_proxies", "rds", "read", False, "DBProxyName", "db-proxy"),
    Operation("rds:DescribeDBProxyEndpoints", "DescribeDBProxyEndpoints", "describe_db_proxy_endpoints", "rds", "read", False, "DBProxyName", "db-proxy"),
    Operation("rds:DescribeDBProxyTargetGroups", "DescribeDBProxyTargetGroups", "describe_db_proxy_target_groups", "rds", "read", False, "DBProxyName", "db-proxy"),
    Operation("rds:DescribeDBProxyTargets", "DescribeDBProxyTargets", "describe_db_proxy_targets", "rds", "read", False, "DBProxyName", "db-proxy"),
    Operation("rds:DescribeDBRecommendations", "DescribeDBRecommendations", "describe_db_recommendations", "rds", "read", False, None, None),
    Operation("rds:DescribeDBSecurityGroups", "DescribeDBSecurityGroups", "describe_db_security_groups", "rds", "read", False, "DBSecurityGroupName", "db-security-group"),
    Operation("rds:DescribeDBShardGroups", "DescribeDBShardGroups", "describe_db_shard_groups", "rds", "read", False, None, None),
    Operation("rds:DescribeDBSnapshotAttributes", "DescribeDBSnapshotAttributes", "describe_db_snapshot_attributes", "rds", "read", False, "DBSnapshotIdentifier", "db-snapshot"),
    Operation("rds:DescribeDBSnapshotTenantDatabases", "DescribeDBSnapshotTenantDatabases", "describe_db_snapshot_tenant_databases", "rds", "read", False, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:DescribeDBSnapshots", "DescribeDBSnapshots", "describe_db_snapshots", "rds", "read", False, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:DescribeDBSubnetGroups", "DescribeDBSubnetGroups", "describe_db_subnet_groups", "rds", "read", False, "DBSubnetGroupName", "db-subnet-group"),
    Operation("dynamodb:DescribeEndpoints", "DescribeEndpoints", "describe_endpoints", "dynamodb", "read", False, None, None),
    Operation("rds:DescribeEngineDefaultClusterParameters", "DescribeEngineDefaultClusterParameters", "describe_engine_default_cluster_parameters", "rds", "read", False, "DBParameterGroupFamily", None),
    Operation("rds:DescribeEngineDefaultParameters", "DescribeEngineDefaultParameters", "describe_engine_default_parameters", "rds", "read", False, "DBParameterGroupFamily", None),
    Operation("elasticache:DescribeEngineDefaultParameters", "DescribeEngineDefaultParameters", "describe_engine_default_parameters", "elasticache", "read", False, "CacheParameterGroupFamily", None),
    Operation("rds:DescribeEventCategories", "DescribeEventCategories", "describe_event_categories", "rds", "read", False, None, None),
    Operation("rds:DescribeEventSubscriptions", "DescribeEventSubscriptions", "describe_event_subscriptions", "rds", "read", False, None, None),
    Operation("rds:DescribeEvents", "DescribeEvents", "describe_events", "rds", "read", False, None, None),
    Operation("elasticache:DescribeEvents", "DescribeEvents", "describe_events", "elasticache", "read", False, None, None),
    Operation("dynamodb:DescribeExport", "DescribeExport", "describe_export", "dynamodb", "read", False, "ExportArn", "table-export"),
    Operation("rds:DescribeExportTasks", "DescribeExportTasks", "describe_export_tasks", "rds", "read", False, "ExportTaskIdentifier", "export-task"),
    Operation("rds:DescribeGlobalClusters", "DescribeGlobalClusters", "describe_global_clusters", "rds", "read", False, "GlobalClusterIdentifier", "global-cluster"),
    Operation("elasticache:DescribeGlobalReplicationGroups", "DescribeGlobalReplicationGroups", "describe_global_replication_groups", "elasticache", "read", False, "GlobalReplicationGroupId", "global-replication-group"),
    Operation("dynamodb:DescribeGlobalTable", "DescribeGlobalTable", "describe_global_table", "dynamodb", "read", False, "GlobalTableName", "global-table"),
    Operation("dynamodb:DescribeGlobalTableSettings", "DescribeGlobalTableSettings", "describe_global_table_settings", "dynamodb", "read", False, "GlobalTableName", "global-table"),
    Operation("dynamodb:DescribeImport", "DescribeImport", "describe_import", "dynamodb", "read", False, "ImportArn", "table-import"),
    Operation("rds:DescribeIntegrations", "DescribeIntegrations", "describe_integrations", "rds", "read", False, None, None),
    Operation("dynamodb:DescribeKinesisStreamingDestination", "DescribeKinesisStreamingDestination", "describe_kinesis_streaming_destination", "dynamodb", "read", False, "TableName", "table"),
    Operation("dynamodb:DescribeLimits", "DescribeLimits", "describe_limits", "dynamodb", "read", False, None, None),
    Operation("rds:DescribeOptionGroupOptions", "DescribeOptionGroupOptions", "describe_option_group_options", "rds", "read", False, "EngineName", None),
    Operation("rds:DescribeOptionGroups", "DescribeOptionGroups", "describe_option_groups", "rds", "read", False, "OptionGroupName", "option-group"),
    Operation("rds:DescribeOrderableDBInstanceOptions", "DescribeOrderableDBInstanceOptions", "describe_orderable_db_instance_options", "rds", "read", False, "Engine", None),
    Operation("rds:DescribePendingMaintenanceActions", "DescribePendingMaintenanceActions", "describe_pending_maintenance_actions", "rds", "read", False, None, None),
    Operation("elasticache:DescribeReplicationGroups", "DescribeReplicationGroups", "describe_replication_groups", "elasticache", "read", False, "ReplicationGroupId", "replication-group"),
    Operation("elasticache:DescribeReservedCacheNodes", "DescribeReservedCacheNodes", "describe_reserved_cache_nodes", "elasticache", "read", False, "ReservedCacheNodeId", "reserved-cache-node"),
    Operation("elasticache:DescribeReservedCacheNodesOfferings", "DescribeReservedCacheNodesOfferings", "describe_reserved_cache_nodes_offerings", "elasticache", "read", False, None, None),
    Operation("rds:DescribeReservedDBInstances", "DescribeReservedDBInstances", "describe_reserved_db_instances", "rds", "read", False, None, None),
    Operation("rds:DescribeReservedDBInstancesOfferings", "DescribeReservedDBInstancesOfferings", "describe_reserved_db_instances_offerings", "rds", "read", False, None, None),
    Operation("elasticache:DescribeServerlessCacheSnapshots", "DescribeServerlessCacheSnapshots", "describe_serverless_cache_snapshots", "elasticache", "read", False, "ServerlessCacheName", "serverless-cache"),
    Operation("elasticache:DescribeServerlessCaches", "DescribeServerlessCaches", "describe_serverless_caches", "elasticache", "read", False, "ServerlessCacheName", "serverless-cache"),
    Operation("rds:DescribeServerlessV2PlatformVersions", "DescribeServerlessV2PlatformVersions", "describe_serverless_v2_platform_versions", "rds", "read", False, None, None),
    Operation("elasticache:DescribeServiceUpdates", "DescribeServiceUpdates", "describe_service_updates", "elasticache", "read", False, None, None),
    Operation("elasticache:DescribeSnapshots", "DescribeSnapshots", "describe_snapshots", "elasticache", "read", False, "CacheClusterId", "cache-cluster"),
    Operation("rds:DescribeSourceRegions", "DescribeSourceRegions", "describe_source_regions", "rds", "read", False, None, None),
    Operation("dynamodb:DescribeTable", "DescribeTable", "describe_table", "dynamodb", "read", False, "TableName", "table"),
    Operation("dynamodb:DescribeTableReplicaAutoScaling", "DescribeTableReplicaAutoScaling", "describe_table_replica_auto_scaling", "dynamodb", "read", False, "TableName", "table"),
    Operation("rds:DescribeTenantDatabases", "DescribeTenantDatabases", "describe_tenant_databases", "rds", "read", False, "DBInstanceIdentifier", "db-instance"),
    Operation("dynamodb:DescribeTimeToLive", "DescribeTimeToLive", "describe_time_to_live", "dynamodb", "read", False, "TableName", "table"),
    Operation("elasticache:DescribeUpdateActions", "DescribeUpdateActions", "describe_update_actions", "elasticache", "read", False, None, None),
    Operation("elasticache:DescribeUserGroups", "DescribeUserGroups", "describe_user_groups", "elasticache", "read", False, "UserGroupId", "cache-user-group"),
    Operation("elasticache:DescribeUsers", "DescribeUsers", "describe_users", "elasticache", "read", False, "UserId", "cache-user"),
    Operation("rds:DescribeValidDBInstanceModifications", "DescribeValidDBInstanceModifications", "describe_valid_db_instance_modifications", "rds", "read", False, "DBInstanceIdentifier", "db-instance"),
    Operation("rds:DownloadDBLogFilePortion", "DownloadDBLogFilePortion", "download_db_log_file_portion", "rds", "read", False, "DBInstanceIdentifier", "db-instance"),
    Operation("dynamodb:GetResourcePolicy", "GetResourcePolicy", "get_resource_policy", "dynamodb", "read", False, "ResourceArn", "tagged-resource"),
    Operation("elasticache:ListAllowedNodeTypeModifications", "ListAllowedNodeTypeModifications", "list_allowed_node_type_modifications", "elasticache", "read", False, "CacheClusterId", "cache-cluster"),
    Operation("dynamodb:ListBackups", "ListBackups", "list_backups", "dynamodb", "read", False, "TableName", "table"),
    Operation("dynamodb:ListContributorInsights", "ListContributorInsights", "list_contributor_insights", "dynamodb", "read", False, "TableName", "table"),
    Operation("dynamodb:ListExports", "ListExports", "list_exports", "dynamodb", "read", False, "TableArn", "table"),
    Operation("dynamodb:ListGlobalTables", "ListGlobalTables", "list_global_tables", "dynamodb", "read", False, None, None),
    Operation("dynamodb:ListImports", "ListImports", "list_imports", "dynamodb", "read", False, "TableArn", "table"),
    Operation("dynamodb:ListTables", "ListTables", "list_tables", "dynamodb", "read", False, None, None),
    Operation("rds:ListTagsForResource", "ListTagsForResource", "list_tags_for_resource", "rds", "read", False, "ResourceName", "tagged-resource"),
    Operation("elasticache:ListTagsForResource", "ListTagsForResource", "list_tags_for_resource", "elasticache", "read", False, "ResourceName", "tagged-resource"),
    Operation("dynamodb:ListTagsOfResource", "ListTagsOfResource", "list_tags_of_resource", "dynamodb", "read", False, "ResourceArn", "tagged-resource"),
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
