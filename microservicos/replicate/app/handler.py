"""Ação replicate: compartilha/copia recurso para outra conta/region."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, ReplicateRequest


def execute(req: ReplicateRequest) -> ActionAccepted:
    p = req.params
    session = assumed_session(req.account, req.roleArn, req.region)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "resourceType": p.resourceType, "to": p.destinationAccount},
        )

    if p.resourceType == "db-snapshot":
        rds = session.client("rds")
        rds.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=p.resourceId,
            AttributeName="restore",
            ValuesToAdd=[p.destinationAccount],
        )
        detail = {
            "sharedSnapshot": p.resourceId,
            "withAccount": p.destinationAccount,
            "note": "copy/re-encrypt sob a KMS de destino é executado pela conta de destino (passo replicate na conta-alvo).",
        }
    else:
        raise ActionError("validation_error", f"replicate de {p.resourceType} não suportado neste serviço", 400)

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
