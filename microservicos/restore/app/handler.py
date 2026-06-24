"""Ação restore: cria snapshot ou restaura instância a partir de snapshot."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, RestoreRequest


def execute(req: RestoreRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)
    rds = session.client("rds")

    if req.dryRun:
        detail = {"dryRun": True, "operation": p.operation}
    elif p.operation == "create-snapshot":
        snap = p.snapshotIdentifier or f"{req.resource}-{uuid.uuid4().hex[:8]}"
        out = rds.create_db_snapshot(DBSnapshotIdentifier=snap, DBInstanceIdentifier=req.resource)
        detail = {"snapshot": out["DBSnapshot"]["DBSnapshotIdentifier"], "status": out["DBSnapshot"]["Status"]}
    elif p.operation == "restore-snapshot":
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
        detail = {"instance": out["DBInstance"]["DBInstanceIdentifier"], "status": out["DBInstance"]["DBInstanceStatus"]}
    else:
        raise ActionError("validation_error", f"operation inválida: {p.operation}", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
