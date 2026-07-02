"""Ação storage: altera tipo de storage e aumenta tamanho."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, StorageRequest
from .rules import enforce_allowed, enforce_common, enforce_max, load_rules


def execute(req: StorageRequest) -> ActionAccepted:
    p = req.params
    rules = load_rules({})
    enforce_common(rules, req)
    enforce_allowed(rules, "allowedResourceTypes", p.resourceType, "resourceType")
    enforce_allowed(rules, "allowedStorageTypes", p.storageType, "storage type")
    enforce_max(rules, "maxAllocatedStorage", p.allocatedStorage, "allocatedStorage")
    enforce_max(rules, "maxIops", p.iops, "iops")
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType},
        )

    if p.resourceType == "db-instance":
        rds = session.client("rds")
        kwargs = {"DBInstanceIdentifier": req.resource, "ApplyImmediately": p.applyImmediately}
        if p.allocatedStorage:
            kwargs["AllocatedStorage"] = p.allocatedStorage
        if p.storageType:
            kwargs["StorageType"] = p.storageType
        if p.iops:
            kwargs["Iops"] = p.iops
        if p.storageThroughput:
            kwargs["StorageThroughput"] = p.storageThroughput
        out = rds.modify_db_instance(**kwargs)
        detail = {"status": out["DBInstance"]["DBInstanceStatus"]}
    elif p.resourceType == "ec2-volume":
        ec2 = session.client("ec2")
        kwargs = {"VolumeId": req.resource}
        if p.allocatedStorage:
            kwargs["Size"] = p.allocatedStorage
        if p.storageType:
            kwargs["VolumeType"] = p.storageType
        if p.iops:
            kwargs["Iops"] = p.iops
        if p.storageThroughput:
            kwargs["Throughput"] = p.storageThroughput
        ec2.modify_volume(**kwargs)
        detail = {"modified": req.resource}
    else:
        raise ActionError("validation_error", f"storage de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
