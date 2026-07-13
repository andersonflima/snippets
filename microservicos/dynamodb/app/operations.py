"""Catálogo de operações DynamoDB suportadas.

Cada operação declara: o método boto3 (client `dynamodb`), a categoria (para as
regras poderem liberar/bloquear por grupo), se é mutante (afeta o gate de GMUD em
produção) e a chave de argumento que carrega o recurso alvo (para casar exceções
e checar naming/modelagem). O conjunto cobre control plane + data plane — ou seja,
"tudo que é possível" no DynamoDB via boto3.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    name: str
    method: str
    category: str  # read | write | ddl | config | backup
    mutating: bool
    resource_arg: str | None  # arg que carrega o recurso alvo (TableName/ResourceArn/...)


def _op(name: str, method: str, category: str, mutating: bool, resource_arg: str | None) -> Operation:
    return Operation(name, method, category, mutating, resource_arg)


# fmt: off
_OPS: tuple[Operation, ...] = (
    # ---- data plane: leitura (read) ----
    _op("GetItem",             "get_item",              "read",   False, "TableName"),
    _op("Query",               "query",                 "read",   False, "TableName"),
    _op("Scan",                "scan",                  "read",   False, "TableName"),
    _op("BatchGetItem",        "batch_get_item",        "read",   False, None),
    _op("TransactGetItems",    "transact_get_items",    "read",   False, None),
    # ---- data plane: escrita (write) ----
    _op("PutItem",             "put_item",              "write",  True,  "TableName"),
    _op("UpdateItem",          "update_item",           "write",  True,  "TableName"),
    _op("DeleteItem",          "delete_item",           "write",  True,  "TableName"),
    _op("BatchWriteItem",      "batch_write_item",      "write",  True,  None),
    _op("TransactWriteItems",  "transact_write_items",  "write",  True,  None),
    # PartiQL — INSERT/UPDATE/DELETE mutam; tratadas como escrita por segurança.
    _op("ExecuteStatement",    "execute_statement",     "write",  True,  None),
    _op("BatchExecuteStatement", "batch_execute_statement", "write", True, None),
    _op("ExecuteTransaction",  "execute_transaction",   "write",  True,  None),
    # ---- control plane: DDL / estrutura (ddl) ----
    _op("CreateTable",         "create_table",          "ddl",    True,  "TableName"),
    _op("DeleteTable",         "delete_table",          "ddl",    True,  "TableName"),
    _op("UpdateTable",         "update_table",          "ddl",    True,  "TableName"),
    _op("DescribeTable",       "describe_table",        "read",   False, "TableName"),
    _op("ListTables",          "list_tables",           "read",   False, None),
    _op("UpdateTimeToLive",    "update_time_to_live",   "ddl",    True,  "TableName"),
    _op("DescribeTimeToLive",  "describe_time_to_live", "read",   False, "TableName"),
    _op("CreateGlobalTable",   "create_global_table",   "ddl",    True,  "GlobalTableName"),
    _op("UpdateGlobalTable",   "update_global_table",   "ddl",    True,  "GlobalTableName"),
    _op("UpdateGlobalTableSettings", "update_global_table_settings", "ddl", True, "GlobalTableName"),
    _op("DescribeGlobalTable", "describe_global_table", "read",   False, "GlobalTableName"),
    _op("DescribeGlobalTableSettings", "describe_global_table_settings", "read", False, "GlobalTableName"),
    _op("ListGlobalTables",    "list_global_tables",    "read",   False, None),
    _op("DescribeKinesisStreamingDestination", "describe_kinesis_streaming_destination", "read", False, "TableName"),
    _op("EnableKinesisStreamingDestination", "enable_kinesis_streaming_destination", "config", True, "TableName"),
    _op("DisableKinesisStreamingDestination", "disable_kinesis_streaming_destination", "config", True, "TableName"),
    # ---- configuração operacional (config) ----
    _op("UpdateContinuousBackups", "update_continuous_backups", "config", True, "TableName"),
    _op("DescribeContinuousBackups", "describe_continuous_backups", "read", False, "TableName"),
    _op("TagResource",         "tag_resource",          "config", True,  "ResourceArn"),
    _op("UntagResource",       "untag_resource",        "config", True,  "ResourceArn"),
    _op("ListTagsOfResource",  "list_tags_of_resource", "read",   False, "ResourceArn"),
    _op("UpdateContributorInsights", "update_contributor_insights", "config", True, "TableName"),
    _op("DescribeContributorInsights", "describe_contributor_insights", "read", False, "TableName"),
    _op("DescribeLimits",      "describe_limits",       "read",   False, None),
    _op("DescribeEndpoints",   "describe_endpoints",    "read",   False, None),
    # ---- backup / restore / export / import (backup) ----
    _op("CreateBackup",        "create_backup",         "backup", True,  "TableName"),
    _op("DeleteBackup",        "delete_backup",         "backup", True,  "BackupArn"),
    _op("DescribeBackup",      "describe_backup",       "read",   False, "BackupArn"),
    _op("ListBackups",         "list_backups",          "read",   False, None),
    _op("RestoreTableFromBackup", "restore_table_from_backup", "backup", True, "TargetTableName"),
    _op("RestoreTableToPointInTime", "restore_table_to_point_in_time", "backup", True, "TargetTableName"),
    _op("ExportTableToPointInTime", "export_table_to_point_in_time", "backup", True, "TableArn"),
    _op("DescribeExport",      "describe_export",       "read",   False, "ExportArn"),
    _op("ListExports",         "list_exports",          "read",   False, None),
    _op("ImportTable",         "import_table",          "backup", True,  None),
    _op("DescribeImport",      "describe_import",        "read",   False, "ImportArn"),
    _op("ListImports",         "list_imports",          "read",   False, None),
)
# fmt: on

CATALOG: dict[str, Operation] = {op.name: op for op in _OPS}

CATEGORIES: tuple[str, ...] = ("read", "write", "ddl", "config", "backup")


def resolve(name: str) -> Operation | None:
    return CATALOG.get(name)


def resource_of(op: Operation, args: dict) -> str | None:
    """Extrai o recurso alvo dos args (TableName/ResourceArn/...) quando existir."""
    if not op.resource_arg:
        return None
    value = args.get(op.resource_arg)
    return value if isinstance(value, str) else None
