"""Operações Aurora (cluster-based): snapshot de cluster e restore de cluster.

Aurora é orientado a cluster: o snapshot é do cluster inteiro e o restore cria
um cluster novo, sem instâncias de compute. Por isso o restore aqui também cria
os membros (writer/readers) para o cluster ficar utilizável.
"""
from __future__ import annotations

import uuid

from .aws import ActionError


def create_cluster_snapshot(rds, req, p) -> dict:
    """Cria snapshot do cluster Aurora `req.resource`."""
    snap = p.snapshotIdentifier or f"{req.resource}-{uuid.uuid4().hex[:8]}"
    out = rds.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier=snap,
        DBClusterIdentifier=req.resource,
    )
    snapshot = out["DBClusterSnapshot"]
    return {
        "clusterSnapshot": snapshot["DBClusterSnapshotIdentifier"],
        "status": snapshot["Status"],
    }


def restore_cluster_snapshot(rds, req, p) -> dict:
    """Restaura um snapshot Aurora em um cluster novo e cria seus membros."""
    if not (p.snapshotIdentifier and p.targetClusterIdentifier and p.engine):
        raise ActionError(
            "validation_error",
            "restore de cluster exige snapshotIdentifier, targetClusterIdentifier e engine",
            400,
        )
    if not p.dbInstanceClass:
        raise ActionError(
            "validation_error",
            "restore de cluster exige dbInstanceClass para os membros",
            400,
        )
    kwargs = {
        "DBClusterIdentifier": p.targetClusterIdentifier,
        "SnapshotIdentifier": p.snapshotIdentifier,
        "Engine": p.engine,
    }
    if p.engineVersion:
        kwargs["EngineVersion"] = p.engineVersion
    if p.dbSubnetGroupName:
        kwargs["DBSubnetGroupName"] = p.dbSubnetGroupName
    if p.kmsKeyId:
        kwargs["KmsKeyId"] = p.kmsKeyId
    cluster = rds.restore_db_cluster_from_snapshot(**kwargs)["DBCluster"]

    members = _create_cluster_members(rds, p)
    return {
        "cluster": cluster["DBClusterIdentifier"],
        "status": cluster.get("Status"),
        "members": members,
    }


def _create_cluster_members(rds, p) -> list[str]:
    """Cria os membros de compute do cluster restaurado (1..N)."""
    count = p.clusterInstanceCount or 1
    created: list[str] = []
    for index in range(1, count + 1):
        member = f"{p.targetClusterIdentifier}-{index}"
        rds.create_db_instance(
            DBInstanceIdentifier=member,
            DBInstanceClass=p.dbInstanceClass,
            Engine=p.engine,
            DBClusterIdentifier=p.targetClusterIdentifier,
        )
        created.append(member)
    return created
