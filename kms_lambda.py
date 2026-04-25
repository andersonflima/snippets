import boto3
import json
from typing import Optional, Dict, Any, Iterable

# ---------- PURE HELPERS ----------


def get_first(items):
    return items[0] if items else None


def normalize(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    return value if value else None


def format_error(error: Exception) -> str:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        payload = response.get("Error", {})
        code = normalize(str(payload.get("Code", "")))
        message = normalize(str(payload.get("Message", "")))
        if code and message:
            return f"{code}: {message}"
        if message:
            return message
    message = normalize(str(error))
    return message or error.__class__.__name__


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


def normalize_alias_name(alias_name: Optional[str]) -> Optional[str]:
    raw_alias = normalize(alias_name)
    if not raw_alias:
        return None
    return raw_alias if raw_alias.startswith("alias/") else f"alias/{raw_alias}"


def resolve_kms_from_cluster(elasticache_client, cluster_id: str) -> Optional[str]:
    try:
        cluster = fetch_cache_cluster(elasticache_client, cluster_id)
    except Exception:
        return None

    if not cluster:
        return None

    kms = extract_kms_from_cluster(cluster)
    if kms:
        return kms

    replication_group_id = extract_replication_group_id(cluster)
    if not replication_group_id:
        return None

    try:
        group = fetch_replication_group(elasticache_client, replication_group_id)
    except Exception:
        return None

    return extract_kms_from_replication_group(group)


def resolve_kms_by_exact_alias(kms_client, alias_name: Optional[str]) -> Optional[str]:
    normalized_alias = normalize_alias_name(alias_name)
    if not normalized_alias:
        return None
    try:
        response = kms_client.describe_key(KeyId=normalized_alias)
    except Exception:
        return None
    key_metadata = response.get("KeyMetadata", {})
    return normalize(key_metadata.get("KeyId")) or normalize(key_metadata.get("Arn"))


def iterate_aliases(kms_client) -> Iterable[Dict[str, Any]]:
    marker: Optional[str] = None
    while True:
        request = {"Marker": marker} if marker else {}
        response = kms_client.list_aliases(**request)
        aliases = response.get("Aliases", [])
        for alias in aliases:
            yield alias
        if not response.get("Truncated"):
            return
        marker = normalize(response.get("NextMarker"))
        if not marker:
            return


def resolve_kms_by_alias_scan(kms_client, hint: Optional[str]) -> Optional[str]:
    normalized_hint = normalize(hint)
    if not normalized_hint:
        return None
    lowered_hint = normalized_hint.lower()
    try:
        aliases = iterate_aliases(kms_client)
        for alias in aliases:
            alias_name = normalize(alias.get("AliasName"))
            target_key_id = normalize(alias.get("TargetKeyId"))
            if not alias_name or not target_key_id:
                continue
            if lowered_hint in alias_name.lower():
                return target_key_id
    except Exception:
        return None
    return None


# ---------- CORE ----------


def resolve_kms_key(
    elasticache_client,
    kms_client,
    cluster_id: Optional[str],
    kms_alias: Optional[str],
) -> Optional[str]:
    resolver_chain = [
        lambda: resolve_kms_from_cluster(elasticache_client, cluster_id) if cluster_id else None,
        lambda: resolve_kms_by_exact_alias(kms_client, kms_alias),
        lambda: resolve_kms_by_exact_alias(kms_client, cluster_id),
        lambda: resolve_kms_by_alias_scan(kms_client, kms_alias),
        lambda: resolve_kms_by_alias_scan(kms_client, cluster_id),
    ]

    for resolver in resolver_chain:
        kms_key_id = normalize(resolver())
        if kms_key_id:
            return kms_key_id
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
            "kms:ReEncrypt*",
            "kms:GenerateDataKey*",
            "kms:DescribeKey",
        ],
        "Resource": "*",
        "Condition": {
            "ArnLike": {
                "aws:PrincipalArn": [
                    f"arn:aws:iam::{account_id}:role/itau-github-repo-*",
                    f"arn:aws:iam::{account_id}:role/itau-codebuild-data-execution-role",
                ]
            }
        },
    }


def update_kms_policy(kms_client, key_id: str, policy_name: str, account_id: str):
    policy = get_kms_policy(kms_client, key_id, policy_name)

    statement = build_statement(account_id)

    updated_policy = upsert_statement(policy, statement)

    put_kms_policy(kms_client, key_id, policy_name, updated_policy)


# ---------- HANDLER ----------


def handler(event, context):
    cluster_id = normalize(event.get("cluster_id"))
    kms_alias = normalize(
        event.get("kms_alias")
        or event.get("key_alias")
        or event.get("alias_name")
    )
    region = normalize(event.get("region"))
    policy_name = normalize(event.get("policy_name"))
    account_id = normalize(event.get("account_id"))

    if not all([region, policy_name, account_id]) or not (cluster_id or kms_alias):
        return {
            "statusCode": 400,
            "body": (
                "region, policy_name, account_id are required and at least one of "
                "cluster_id or kms_alias must be informed"
            ),
        }

    elasticache = boto3.client("elasticache", region_name=region)
    kms_client = boto3.client("kms", region_name=region)

    kms_key = resolve_kms_key(
        elasticache_client=elasticache,
        kms_client=kms_client,
        cluster_id=cluster_id,
        kms_alias=kms_alias,
    )

    if not kms_key:
        return {
            "statusCode": 404,
            "body": (
                f"KMS key not found using cluster_id={cluster_id} "
                f"and kms_alias={kms_alias}"
            ),
        }

    try:
        update_kms_policy(kms_client, kms_key, policy_name, account_id)
    except Exception as error:
        return {
            "statusCode": 500,
            "body": f"Failed to update KMS policy: {format_error(error)}",
        }

    return {
        "statusCode": 200,
        "body": {
            "cluster_id": cluster_id,
            "kms_key_id": kms_key,
            "policy_updated": True,
        },
    }


def lambda_handler(event, context):
    return handler(event, context)
