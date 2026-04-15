import boto3
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

def fetch_cache_cluster(
    client, cluster_id: str
) -> Optional[Dict[str, Any]]:
    response = client.describe_cache_clusters(
        CacheClusterId=cluster_id,
        ShowCacheNodeInfo=True
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

def resolve_kms_key(
    elasticache_client,
    cluster_id: str
) -> Optional[str]:

    cluster = fetch_cache_cluster(elasticache_client, cluster_id)

    if not cluster:
        return None

    # 1. direto do cluster
    kms = extract_kms_from_cluster(cluster)
    if kms:
        return kms

    # 2. via replication group
    replication_group_id = extract_replication_group_id(cluster)

    if replication_group_id:
        group = fetch_replication_group(
            elasticache_client,
            replication_group_id
        )
        kms = extract_kms_from_replication_group(group)
        if kms:
            return kms

    return None


# ---------- HANDLER ----------

def handler(event, context):
    cluster_id = event.get("cluster_id")
    region = event.get("region")

    if not cluster_id or not region:
        return {
            "statusCode": 400,
            "body": "cluster_id and region are required"
        }

    elasticache = boto3.client(
        "elasticache",
        region_name=region
    )

    kms_key = resolve_kms_key(
        elasticache,
        cluster_id
    )

    if not kms_key:
        return {
            "statusCode": 404,
            "body": f"KMS key not found for cluster {cluster_id}"
        }

    return {
        "statusCode": 200,
        "body": {
            "cluster_id": cluster_id,
            "kms_key_id": kms_key
        }
    }
