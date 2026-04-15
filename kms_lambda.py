import boto3
import json
from typing import Optional, Dict, Any

# ---------- PURE HELPERS ----------


def get_first(items):
    return items[0] if items else None


def normalize(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    return value if value else None


# ---------- ELASTICACHE ----------


def fetch_cache_cluster(client, cluster_id: str) -> Optional[Dict[str, Any]]:
    response = client.describe_cache_clusters(
        CacheClusterId=cluster_id, ShowCacheNodeInfo=True
    )
    return get_first(response.get("CacheClusters", []))


def fetch_replication_group(
    client, replication_group_id: str
) -> Optional[Dict[str, Any]]:
    response = client.describe_replication_groups(
        ReplicationGroupId=replication_group_id
    )
    return get_first(response.get("ReplicationGroups", []))


def extract_kms_from_cluster(cluster) -> Optional[str]:
    return normalize(cluster.get("KmsKeyId"))


def extract_replication_group_id(cluster) -> Optional[str]:
    return normalize(cluster.get("ReplicationGroupId"))


def extract_kms_from_replication_group(group) -> Optional[str]:
    if not group:
        return None
    return normalize(group.get("KmsKeyId"))


# ---------- CORE ----------


def resolve_kms_key(elasticache_client, cluster_id: str) -> Optional[str]:
    cluster = fetch_cache_cluster(elasticache_client, cluster_id)

    if not cluster:
        return None

    kms = extract_kms_from_cluster(cluster)
    if kms:
        return kms

    replication_group_id = extract_replication_group_id(cluster)

    if replication_group_id:
        group = fetch_replication_group(elasticache_client, replication_group_id)
        kms = extract_kms_from_replication_group(group)
        if kms:
            return kms

    return None


# ---------- KMS POLICY ----------


def get_kms_policy(kms_client, key_id: str, policy_name: str) -> Dict:
    response = kms_client.get_key_policy(KeyId=key_id, PolicyName=policy_name)
    return json.loads(response["Policy"])


def put_kms_policy(kms_client, key_id: str, policy_name: str, policy: Dict):
    kms_client.put_key_policy(
        KeyId=key_id, PolicyName=policy_name, Policy=json.dumps(policy)
    )


def upsert_statement(policy: Dict, statement: Dict) -> Dict:
    statements = policy.get("Statement", [])

    # evita duplicação por Sid
    existing = next(
        (s for s in statements if s.get("Sid") == statement.get("Sid")), None
    )

    if existing:
        statements = [
            statement if s.get("Sid") == statement.get("Sid") else s for s in statements
        ]
    else:
        statements.append(statement)

    policy["Statement"] = statements
    return policy


def build_statement(account_id: str) -> Dict:
    return {
        "Sid": f"AllowAccessAccount{account_id}",
        "Effect": "Allow",
        "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
        "Action": [
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:GenerateDataKey",
            "kms:DescribeKey",
        ],
        "Resource": "*",
    }


def update_kms_policy(kms_client, key_id: str, policy_name: str, account_id: str):
    policy = get_kms_policy(kms_client, key_id, policy_name)

    statement = build_statement(account_id)

    updated_policy = upsert_statement(policy, statement)

    put_kms_policy(kms_client, key_id, policy_name, updated_policy)


# ---------- HANDLER ----------


def handler(event, context):
    cluster_id = event.get("cluster_id")
    region = event.get("region")
    policy_name = event.get("policy_name")
    account_id = event.get("account_id")

    if not all([cluster_id, region, policy_name, account_id]):
        return {
            "statusCode": 400,
            "body": "cluster_id, region, policy_name, account_id are required",
        }

    elasticache = boto3.client("elasticache", region_name=region)
    kms_client = boto3.client("kms", region_name=region)

    kms_key = resolve_kms_key(elasticache, cluster_id)

    if not kms_key:
        return {
            "statusCode": 404,
            "body": f"KMS key not found for cluster {cluster_id}",
        }

    try:
        update_kms_policy(kms_client, kms_key, policy_name, account_id)
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to update KMS policy: {str(e)}"}

    return {
        "statusCode": 200,
        "body": {
            "cluster_id": cluster_id,
            "kms_key_id": kms_key,
            "policy_updated": True,
        },
    }
