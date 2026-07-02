"""Ação vpc-link: autoriza a conta consumidora no VPC Endpoint Service (PrivateLink)."""
from __future__ import annotations

import uuid

from .aws import ActionError, assumed_session
from .models import ActionAccepted, VpcLinkRequest
from .rules import enforce_allowed, enforce_common, load_rules


def execute(req: VpcLinkRequest) -> ActionAccepted:
    p = req.params
    rules = load_rules({})
    enforce_common(rules, req)
    enforce_allowed(rules, "allowedConsumerAccounts", p.consumerAccount, "conta consumidora")
    session = assumed_session(req.account, req.roleArn, req.region)
    ec2 = session.client("ec2")

    principals = list(p.allowedPrincipals or [])
    principals.append(f"arn:aws:iam::{p.consumerAccount}:root")
    principals = sorted(set(principals))

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=req.resource, account=req.account,
            detail={"dryRun": True, "principals": principals, "service": p.endpointServiceId},
        )

    if not p.endpointServiceId:
        raise ActionError("validation_error", "endpointServiceId é obrigatório p/ autorizar o consumidor", 400)
    ec2.modify_vpc_endpoint_service_permissions(
        ServiceId=p.endpointServiceId,
        AddAllowedPrincipals=principals,
    )
    detail = {"service": p.endpointServiceId, "grantedPrincipals": principals, "db": p.dbIdentifier}

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
