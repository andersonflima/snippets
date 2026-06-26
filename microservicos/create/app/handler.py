"""Ação create: provisiona um recurso a partir da spec (kwargs do boto3)."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, CreateRequest


def execute(req: CreateRequest) -> ActionAccepted:
    p = req.params
    spec = dict(p.spec or {})
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType, "spec": spec},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        out = rds.create_db_instance(DBInstanceIdentifier=req.resource, **spec)
        detail = {"status": out["DBInstance"]["DBInstanceStatus"]}
    elif p.resourceType == "db-subnet-group":
        rds = session.client("rds")
        rds.create_db_subnet_group(DBSubnetGroupName=req.resource, **spec)
        detail = {"created": req.resource}
    else:
        raise ActionError("validation_error", f"create de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
