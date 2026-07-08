"""Ação restore: snapshot/restore de instância RDS ou de cluster Aurora."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .cluster import create_cluster_snapshot, restore_cluster_snapshot
from .models import ActionAccepted, RestoreRequest
from .rules import enforce_allowed, enforce_common, load_rules


def _create_snapshot(rds, req, p) -> dict:
    snap = p.snapshotIdentifier or f"{req.resource}-{uuid.uuid4().hex[:8]}"
    out = rds.create_db_snapshot(DBSnapshotIdentifier=snap, DBInstanceIdentifier=req.resource)
    snapshot = out["DBSnapshot"]
    return {"snapshot": snapshot["DBSnapshotIdentifier"], "status": snapshot["Status"]}


def _restore_snapshot(rds, req, p) -> dict:
    if not (p.snapshotIdentifier and p.targetInstanceIdentifier):
        raise ActionError("validation_error", "restore exige snapshotIdentifier e targetInstanceIdentifier", 400)
    kwargs = {
        "DBInstanceIdentifier": p.targetInstanceIdentifier,
        "DBSnapshotIdentifier": p.snapshotIdentifier,
    }
    if p.dbInstanceClass:
        kwargs["DBInstanceClass"] = p.dbInstanceClass
    if p.dbSubnetGroupName:
        kwargs["DBSubnetGroupName"] = p.dbSubnetGroupName
    out = rds.restore_db_instance_from_db_snapshot(**kwargs)
    instance = out["DBInstance"]
    return {"instance": instance["DBInstanceIdentifier"], "status": instance["DBInstanceStatus"]}


_OPERATIONS = {
    "create-snapshot": _create_snapshot,
    "restore-snapshot": _restore_snapshot,
    "create-cluster-snapshot": create_cluster_snapshot,
    "restore-cluster-snapshot": restore_cluster_snapshot,
}


def execute(req: RestoreRequest) -> ActionAccepted:
    p = req.params
    rules = load_rules({})
    enforce_common(rules, req)
    enforce_allowed(rules, "allowedOperations", p.operation, "operação")
    enforce_allowed(rules, "allowedInstanceClasses", p.dbInstanceClass, "instance class")
    enforce_allowed(rules, "allowedEngines", p.engine, "engine")

    action = _OPERATIONS.get(p.operation)
    if action is None:
        raise ActionError("validation_error", f"operation inválida: {p.operation}", 400)

    session = assumed_session(req.account, req.roleArn, req.region)
    rds = session.client("rds")

    detail = {"dryRun": True, "operation": p.operation} if req.dryRun else action(rds, req, p)
    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
