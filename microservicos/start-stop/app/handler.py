"""Ação start-stop: liga/desliga o recurso alvo."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, StartStopRequest


def execute(req: StartStopRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)
    start = p.operation == "start"

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "operation": p.operation, "resourceType": p.resourceType},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        (rds.start_db_instance if start else rds.stop_db_instance)(DBInstanceIdentifier=req.resource)
    elif p.resourceType == "db-cluster":
        rds = session.client("rds")
        (rds.start_db_cluster if start else rds.stop_db_cluster)(DBClusterIdentifier=req.resource)
    elif p.resourceType == "ec2-instance":
        ec2 = session.client("ec2")
        (ec2.start_instances if start else ec2.stop_instances)(InstanceIds=[req.resource])
    else:
        raise ActionError("validation_error", f"start-stop de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail={"operation": p.operation})
