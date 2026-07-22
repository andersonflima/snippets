"""Gerador de catálogo de operações a partir do botocore (fonte da verdade).

Introspecta os clients boto3 alvo (rds, elasticache, dynamodb) e classifica CADA
operação: qual action-service a possui (partição por verbo), categoria, se é
mutante, o argumento que nomeia o recurso alvo (resource_arg) e o tipo de recurso.

Saída: `catalog.json` (consumido por gen_services.py / gen_contracts.py) + um
resumo de validação no stdout. Nada de rede: usa apenas os service models locais
do botocore. Escopo inicial: RDS/Aurora, Elasticache, DynamoDB.
"""
from __future__ import annotations

import collections
import json
import os
import re
from typing import Optional

import botocore.session
from botocore import xform_name

TARGET_CLIENTS = ("rds", "elasticache", "dynamodb")

# --- Partição por verbo -> action-service --------------------------------------
# Prefixo (primeira palavra PascalCase da operação) -> serviço dono.
VERB_TO_SERVICE = {
    "Create": "create", "Purchase": "create", "Register": "create",
    "Delete": "destroy", "Deregister": "destroy", "Remove": "destroy",
    "Modify": "modify", "Update": "modify", "Add": "modify", "Attach": "modify",
    "Detach": "modify", "Associate": "modify", "Disassociate": "modify",
    "Promote": "modify", "Reset": "modify", "Apply": "modify", "Enable": "modify",
    "Disable": "modify", "Authorize": "modify", "Revoke": "modify",
    "Tag": "modify", "Untag": "modify", "Put": "modify",
    "Increase": "modify", "Decrease": "modify", "Rebalance": "modify", "Batch": "modify",
    "Start": "start-stop", "Stop": "start-stop", "Reboot": "start-stop",
    "Failover": "start-stop", "Switchover": "start-stop", "Test": "start-stop",
    "Restore": "restore", "Copy": "restore", "Backtrack": "restore",
    "Export": "restore", "Import": "restore", "Cancel": "restore",
    "Describe": "describe", "List": "describe", "Get": "describe", "Download": "describe",
}

# Overrides explícitos por operação (quando o verbo genérico erra a intenção).
OP_OVERRIDES = {
    # DynamoDB data-plane -> serviço 'data'
    "PutItem": "data", "GetItem": "data", "UpdateItem": "data", "DeleteItem": "data",
    "Query": "data", "Scan": "data", "BatchGetItem": "data", "BatchWriteItem": "data",
    "TransactGetItems": "data", "TransactWriteItems": "data",
    "ExecuteStatement": "data", "BatchExecuteStatement": "data", "ExecuteTransaction": "data",
    # Migração/replicação
    "StartMigration": "replicate", "CompleteMigration": "replicate", "TestMigration": "replicate",
    # Snapshot share cross-account (replicação)
    "ModifyDBSnapshotAttribute": "replicate", "ModifyDBClusterSnapshotAttribute": "replicate",
    # Resource policy é config/modify (fica em modify) — mantido pelo verbo Put.
    # PutResourcePolicy do dynamodb NÃO é data-plane:
    "PutResourcePolicy": "modify", "GetResourcePolicy": "describe", "DeleteResourcePolicy": "destroy",
    # Backups: criar/deletar backup ficam em create/destroy pelo verbo; restore fica em restore.
}

# Categorias (coarse) por serviço — usadas por regras para allow/deny em bloco.
SERVICE_CATEGORY = {
    "create": "provision", "destroy": "delete", "modify": "config",
    "start-stop": "power", "restore": "backup", "replicate": "replicate",
    "describe": "read", "data": "data",
}
READ_SERVICES = {"describe"}

# Chaves conhecidas que nomeiam o recurso alvo, por client (ordem = prioridade).
RESOURCE_KEYS = {
    "rds": [
        "DBInstanceIdentifier", "DBClusterIdentifier", "DBSnapshotIdentifier",
        "DBClusterSnapshotIdentifier", "GlobalClusterIdentifier", "DBProxyName",
        "DBProxyEndpointName", "DBParameterGroupName", "DBClusterParameterGroupName",
        "DBSubnetGroupName", "OptionGroupName", "DBSecurityGroupName",
        "BlueGreenDeploymentIdentifier", "CustomDBEngineVersionIdentifier",
        "ExportTaskIdentifier", "SourceDBInstanceIdentifier",
        "SourceDBClusterIdentifier", "TargetDBInstanceIdentifier", "ResourceName",
    ],
    "elasticache": [
        "CacheClusterId", "ReplicationGroupId", "GlobalReplicationGroupId",
        "ServerlessCacheName", "ServerlessCacheSnapshotName", "SnapshotName",
        "CacheParameterGroupName", "CacheSubnetGroupName", "CacheSecurityGroupName",
        "UserId", "UserGroupId", "ReservedCacheNodeId", "ResourceName",
    ],
    "dynamodb": [
        "TableName", "GlobalTableName", "TargetTableName", "BackupArn",
        "TableArn", "ExportArn", "ImportArn", "ResourceArn",
    ],
}

# resource_arg key -> tipo de recurso lógico (para regras allow/deny por tipo).
RESOURCE_TYPE = {
    "DBInstanceIdentifier": "db-instance", "SourceDBInstanceIdentifier": "db-instance",
    "TargetDBInstanceIdentifier": "db-instance",
    "DBClusterIdentifier": "db-cluster", "SourceDBClusterIdentifier": "db-cluster",
    "DBSnapshotIdentifier": "db-snapshot", "DBClusterSnapshotIdentifier": "db-cluster-snapshot",
    "GlobalClusterIdentifier": "global-cluster", "DBProxyName": "db-proxy",
    "DBProxyEndpointName": "db-proxy-endpoint", "DBParameterGroupName": "db-parameter-group",
    "DBClusterParameterGroupName": "db-cluster-parameter-group", "DBSubnetGroupName": "db-subnet-group",
    "OptionGroupName": "option-group", "DBSecurityGroupName": "db-security-group",
    "BlueGreenDeploymentIdentifier": "blue-green-deployment",
    "CustomDBEngineVersionIdentifier": "custom-engine-version", "ExportTaskIdentifier": "export-task",
    "CacheClusterId": "cache-cluster", "ReplicationGroupId": "replication-group",
    "GlobalReplicationGroupId": "global-replication-group", "ServerlessCacheName": "serverless-cache",
    "ServerlessCacheSnapshotName": "serverless-cache-snapshot", "SnapshotName": "cache-snapshot",
    "CacheParameterGroupName": "cache-parameter-group", "CacheSubnetGroupName": "cache-subnet-group",
    "CacheSecurityGroupName": "cache-security-group", "UserId": "cache-user",
    "UserGroupId": "cache-user-group", "ReservedCacheNodeId": "reserved-cache-node",
    "TableName": "table", "GlobalTableName": "global-table", "TargetTableName": "table",
    "BackupArn": "table-backup", "TableArn": "table", "ExportArn": "table-export",
    "ImportArn": "table-import", "ResourceArn": "tagged-resource", "ResourceName": "tagged-resource",
}


def _first_word(op: str) -> str:
    m = re.match(r"[A-Z][a-z0-9]*", op)
    return m.group(0) if m else op


def _service_for(op: str) -> str:
    if op in OP_OVERRIDES:
        return OP_OVERRIDES[op]
    return VERB_TO_SERVICE.get(_first_word(op), "modify")


def _resource_arg(client: str, input_members: list[str], required: list[str]) -> Optional[str]:
    known = RESOURCE_KEYS[client]
    for key in known:  # prioridade pela ordem em RESOURCE_KEYS
        if key in input_members:
            return key
    return required[0] if required else None


def build_catalog() -> dict:
    session = botocore.session.get_session()
    catalog: dict[str, list[dict]] = collections.defaultdict(list)
    for client in TARGET_CLIENTS:
        model = session.get_service_model(client)
        for op_name in sorted(model.operation_names):
            op_model = model.operation_model(op_name)
            input_shape = op_model.input_shape
            members = list(input_shape.members.keys()) if input_shape else []
            required = list(getattr(input_shape, "required_members", []) or []) if input_shape else []
            svc = _service_for(op_name)
            res_key = _resource_arg(client, members, required)
            entry = {
                "key": f"{client}:{op_name}",
                "name": op_name,
                "method": xform_name(op_name),
                "client": client,
                "service": svc,
                "category": SERVICE_CATEGORY.get(svc, "config"),
                "mutating": svc not in READ_SERVICES,
                "resourceArg": res_key,
                "resourceType": RESOURCE_TYPE.get(res_key) if res_key else None,
            }
            catalog[svc].append(entry)
    return catalog


def main() -> None:
    catalog = build_catalog()
    out = os.path.join(os.path.dirname(__file__), "catalog.json")
    flat = {svc: sorted(ops, key=lambda e: e["name"]) for svc, ops in sorted(catalog.items())}
    with open(out, "w") as fh:
        json.dump(flat, fh, indent=2)

    total = sum(len(v) for v in flat.values())
    print(f"catalog.json escrito: {total} operações em {len(flat)} serviços -> {out}\n")
    for svc, ops in flat.items():
        by_client = collections.Counter(e["client"] for e in ops)
        no_res = [e["name"] for e in ops if not e["resourceArg"]]
        print(f"  {svc:11s} {len(ops):3d}  ({dict(by_client)})"
              + (f"  sem resourceArg: {no_res}" if no_res else ""))
    print("\nAmostra de inferência resourceArg/resourceType (5 por serviço):")
    for svc, ops in flat.items():
        print(f"  [{svc}]")
        for e in ops[:5]:
            print(f"    {e['name']:42s} {e['client']:11s} arg={e['resourceArg']} type={e['resourceType']}")


if __name__ == "__main__":
    main()
