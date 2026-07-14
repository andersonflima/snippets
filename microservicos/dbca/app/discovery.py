"""Auto-descoberta do recurso: classifica o tipo, lê VPC/endpoint e o secret.

A partir de conta + recurso (nome/id/ARN), descobre — do nosso lado — se é um
cluster Aurora (e qual engine) ou uma tabela DynamoDB, e coleta VPC/endpoint
automaticamente. O usuário não precisa saber nada disso.
"""
from __future__ import annotations

from dataclasses import dataclass

from botocore.exceptions import ClientError

from .aws import ActionError

SECRET_TAG_KEY = "dbca:secretArn"


@dataclass
class Discovery:
    resource_type: str  # "aurora" | "dynamodb"
    engine: str  # "aurora-postgresql" | "aurora-mysql" | "dynamodb"
    identifier: str
    arn: str | None
    vpc_id: str | None
    endpoint: str | None
    secret_arn: str | None


def _cluster_id_from(resource: str) -> str:
    if resource.startswith("arn:aws:rds:") and ":cluster:" in resource:
        return resource.split(":cluster:")[-1]
    return resource


def _table_name_from(resource: str) -> str:
    if resource.startswith("arn:aws:dynamodb:") and ":table/" in resource:
        return resource.split(":table/")[-1]
    return resource


def _vpc_from_subnet_group(rds, subnet_group_name: str | None) -> str | None:
    if not subnet_group_name:
        return None
    try:
        groups = rds.describe_db_subnet_groups(DBSubnetGroupName=subnet_group_name).get("DBSubnetGroups", [])
        return groups[0].get("VpcId") if groups else None
    except ClientError:
        return None


def _secret_from_tags(rds, arn: str | None) -> str | None:
    if not arn:
        return None
    try:
        tags = rds.list_tags_for_resource(ResourceName=arn).get("TagList", [])
        for tag in tags:
            if tag.get("Key") == SECRET_TAG_KEY and tag.get("Value"):
                return tag["Value"]
    except ClientError:
        return None
    return None


def classify(session, resource: str, region: str) -> Discovery:
    # 1) Aurora cluster?
    is_dynamo_arn = resource.startswith("arn:aws:dynamodb:")
    if not is_dynamo_arn:
        rds = session.client("rds")
        try:
            resp = rds.describe_db_clusters(DBClusterIdentifier=_cluster_id_from(resource))
            clusters = resp.get("DBClusters", [])
            if clusters:
                c = clusters[0]
                engine = c.get("Engine", "")
                if not engine.startswith("aurora"):
                    raise ActionError(
                        "validation_error",
                        f"cluster '{resource}' não é Aurora (engine={engine}); v1 suporta Aurora + DynamoDB",
                        400,
                    )
                arn = c.get("DBClusterArn")
                return Discovery(
                    resource_type="aurora",
                    engine=engine,
                    identifier=c.get("DBClusterIdentifier", resource),
                    arn=arn,
                    vpc_id=_vpc_from_subnet_group(rds, c.get("DBSubnetGroup")),
                    endpoint=c.get("Endpoint"),
                    secret_arn=_secret_from_tags(rds, arn),
                )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if "DBClusterNotFoundFault" not in code and "NotFound" not in code:
                raise

    # 2) DynamoDB table?
    ddb = session.client("dynamodb")
    try:
        table = ddb.describe_table(TableName=_table_name_from(resource))["Table"]
        return Discovery(
            resource_type="dynamodb",
            engine="dynamodb",
            identifier=table["TableName"],
            arn=table.get("TableArn"),
            vpc_id=None,
            endpoint=None,
            secret_arn=None,
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if "ResourceNotFoundException" not in code and "NotFound" not in code:
            raise

    raise ActionError(
        "not_found",
        f"recurso não encontrado como cluster Aurora nem tabela DynamoDB: {resource}",
        404,
    )
