"""Ação destroy: remove o recurso alvo (cleanup)."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, DestroyRequest


def execute(req: DestroyRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType, "target": req.resource},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        kwargs = {"DBInstanceIdentifier": req.resource, "SkipFinalSnapshot": p.skipFinalSnapshot}
        if not p.skipFinalSnapshot and p.finalSnapshotIdentifier:
            kwargs["FinalDBSnapshotIdentifier"] = p.finalSnapshotIdentifier
        rds.delete_db_instance(**kwargs)
    elif p.resourceType == "db-snapshot":
        session.client("rds").delete_db_snapshot(DBSnapshotIdentifier=req.resource)
    elif p.resourceType == "vpc-endpoint":
        session.client("ec2").delete_vpc_endpoints(VpcEndpointIds=[req.resource])
    elif p.resourceType == "security-group":
        session.client("ec2").delete_security_group(GroupId=req.resource)
    else:
        raise ActionError("validation_error", f"destroy de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail={"deleted": req.resource})
