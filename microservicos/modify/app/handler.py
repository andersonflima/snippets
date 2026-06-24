"""Ação modify: aplica modificações (instance class, engine version, etc.)."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, ModifyRequest


def execute(req: ModifyRequest) -> ActionAccepted:
    p = req.params
    m = p.modifications or {}
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType, "modifications": m},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        kwargs = {"DBInstanceIdentifier": req.resource, "ApplyImmediately": p.applyImmediately}
        if m.get("dbInstanceClass"):
            kwargs["DBInstanceClass"] = m["dbInstanceClass"]
        if m.get("engineVersion"):
            kwargs["EngineVersion"] = m["engineVersion"]
            kwargs["AllowMajorVersionUpgrade"] = True
        if m.get("parameterGroupName"):
            kwargs["DBParameterGroupName"] = m["parameterGroupName"]
        if m.get("backupRetentionPeriod") is not None:
            kwargs["BackupRetentionPeriod"] = m["backupRetentionPeriod"]
        out = rds.modify_db_instance(**kwargs)
        detail = {"status": out["DBInstance"]["DBInstanceStatus"]}
    elif p.resourceType == "ec2-instance":
        ec2 = session.client("ec2")
        if m.get("instanceType"):
            ec2.modify_instance_attribute(InstanceId=req.resource, InstanceType={"Value": m["instanceType"]})
        detail = {"modified": list(m.keys())}
    else:
        raise ActionError("validation_error", f"modify de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
