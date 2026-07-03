"""Ação kms: cria Custom KMS Key, alias e re-encripta o recurso alvo."""
from __future__ import annotations

import uuid

from .aws import assumed_session
from .models import ActionAccepted, KmsRequest
from .rules import enforce_allowed, enforce_common, load_rules


def execute(req: KmsRequest) -> ActionAccepted:
    p = req.params
    rules = load_rules({})
    enforce_common(rules, req)
    enforce_allowed(rules, "allowedTargetResourceTypes", p.targetResourceType, "targetResourceType")
    session = assumed_session(req.account, req.roleArn, req.region)
    kms = session.client("kms")
    alias = p.keyAlias if p.keyAlias.startswith("alias/") else f"alias/{p.keyAlias}"

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "alias": alias, "target": p.targetResourceId},
        )

    key = kms.create_key(Description=p.description or f"custom key {alias}", KeyUsage="ENCRYPT_DECRYPT")["KeyMetadata"]
    key_id = key["KeyId"]
    kms.create_alias(AliasName=alias, TargetKeyId=key_id)
    if p.keyPolicyJson:
        kms.put_key_policy(KeyId=key_id, PolicyName="default", Policy=p.keyPolicyJson)

    detail = {"keyId": key_id, "alias": alias}
    if p.targetResourceType == "db-snapshot":
        rds = session.client("rds")
        new_id = f"{p.targetResourceId}-cmk"
        out = rds.copy_db_snapshot(
            SourceDBSnapshotIdentifier=p.targetResourceId,
            TargetDBSnapshotIdentifier=new_id,
            KmsKeyId=key_id,
        )
        detail["reEncryptedSnapshot"] = out["DBSnapshot"]["DBSnapshotIdentifier"]
    else:
        detail["note"] = "db-instance aplica a key em novo snapshot/restore (KMS não re-encripta in-place)."

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
