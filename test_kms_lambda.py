import json
import sys
import types
import unittest
from unittest.mock import patch


def install_boto3_stub() -> None:
    boto3_module = types.ModuleType("boto3")

    def _not_configured(*args, **kwargs):
        raise AssertionError("boto3.client should be patched in tests")

    boto3_module.client = _not_configured
    sys.modules.setdefault("boto3", boto3_module)


install_boto3_stub()

import kms_lambda


class FakeElasticacheClient:
    def __init__(
        self,
        cache_cluster_response=None,
        replication_group_response=None,
        raise_cluster=False,
        raise_replication_group=False,
    ):
        self.cache_cluster_response = cache_cluster_response or {"CacheClusters": []}
        self.replication_group_response = replication_group_response or {"ReplicationGroups": []}
        self.raise_cluster = raise_cluster
        self.raise_replication_group = raise_replication_group

    def describe_cache_clusters(self, CacheClusterId, ShowCacheNodeInfo):
        _ = (CacheClusterId, ShowCacheNodeInfo)
        if self.raise_cluster:
            raise RuntimeError("cluster not found")
        return self.cache_cluster_response

    def describe_replication_groups(self, ReplicationGroupId):
        _ = ReplicationGroupId
        if self.raise_replication_group:
            raise RuntimeError("replication group not found")
        return self.replication_group_response


class FakeKmsClient:
    def __init__(
        self,
        describe_key_map=None,
        alias_pages=None,
        policy=None,
        put_error=None,
    ):
        self.describe_key_map = describe_key_map or {}
        self.alias_pages = alias_pages or [{"Aliases": [], "Truncated": False}]
        self.policy = policy or {"Version": "2012-10-17", "Statement": []}
        self.put_error = put_error
        self.put_calls = []
        self.get_calls = []

    def describe_key(self, KeyId):
        response = self.describe_key_map.get(KeyId)
        if response is None:
            raise RuntimeError("key not found")
        return response

    def list_aliases(self, Marker=None):
        if Marker is None:
            return self.alias_pages[0]
        for page in self.alias_pages:
            if page.get("Marker") == Marker:
                return page
        return {"Aliases": [], "Truncated": False}

    def get_key_policy(self, KeyId, PolicyName):
        _ = (KeyId, PolicyName)
        self.get_calls.append({"KeyId": KeyId, "PolicyName": PolicyName})
        return {"Policy": json.dumps(self.policy)}

    def put_key_policy(self, KeyId, PolicyName, Policy):
        if self.put_error:
            raise self.put_error
        self.put_calls.append(
            {
                "KeyId": KeyId,
                "PolicyName": PolicyName,
                "Policy": json.loads(Policy),
            }
        )


class ErrorWithResponse(Exception):
    def __init__(self, response):
        super().__init__("aws error")
        self.response = response


class KmsLambdaTests(unittest.TestCase):
    def test_resolve_kms_key_prefers_cluster_kms(self):
        elasticache_client = FakeElasticacheClient(
            cache_cluster_response={
                "CacheClusters": [{"CacheClusterId": "elasticache-redis-tracking", "KmsKeyId": "kms-cluster"}]
            }
        )
        kms_client = FakeKmsClient()
        key_id = kms_lambda.resolve_kms_key(
            elasticache_client=elasticache_client,
            kms_client=kms_client,
            cluster_id="elasticache-redis-tracking",
            kms_alias=None,
        )
        self.assertEqual(key_id, "kms-cluster")

    def test_resolve_kms_key_uses_replication_group(self):
        elasticache_client = FakeElasticacheClient(
            cache_cluster_response={
                "CacheClusters": [{"CacheClusterId": "elasticache-redis-tracking", "ReplicationGroupId": "redis-rg"}]
            },
            replication_group_response={
                "ReplicationGroups": [{"ReplicationGroupId": "redis-rg", "KmsKeyId": "kms-rg"}]
            },
        )
        kms_client = FakeKmsClient()
        key_id = kms_lambda.resolve_kms_key(
            elasticache_client=elasticache_client,
            kms_client=kms_client,
            cluster_id="elasticache-redis-tracking",
            kms_alias=None,
        )
        self.assertEqual(key_id, "kms-rg")

    def test_resolve_kms_key_uses_explicit_alias(self):
        elasticache_client = FakeElasticacheClient(raise_cluster=True)
        kms_client = FakeKmsClient(
            describe_key_map={
                "alias/tracking-kms": {"KeyMetadata": {"KeyId": "kms-by-alias"}}
            }
        )
        key_id = kms_lambda.resolve_kms_key(
            elasticache_client=elasticache_client,
            kms_client=kms_client,
            cluster_id="elasticache-redis-tracking",
            kms_alias="tracking-kms",
        )
        self.assertEqual(key_id, "kms-by-alias")

    def test_resolve_kms_key_scans_alias_by_cluster_hint(self):
        elasticache_client = FakeElasticacheClient(raise_cluster=True)
        kms_client = FakeKmsClient(
            alias_pages=[
                {
                    "Aliases": [
                        {"AliasName": "alias/random", "TargetKeyId": "kms-random"},
                        {"AliasName": "alias/elasticache-redis-tracking-kms", "TargetKeyId": "kms-by-scan"},
                    ],
                    "Truncated": False,
                }
            ]
        )
        key_id = kms_lambda.resolve_kms_key(
            elasticache_client=elasticache_client,
            kms_client=kms_client,
            cluster_id="elasticache-redis-tracking",
            kms_alias=None,
        )
        self.assertEqual(key_id, "kms-by-scan")

    def test_handler_updates_policy_with_print_payload(self):
        elasticache_client = FakeElasticacheClient(
            cache_cluster_response={
                "CacheClusters": [{"CacheClusterId": "elasticache-redis-tracking", "KmsKeyId": "kms-cluster"}]
            }
        )
        kms_client = FakeKmsClient()

        def fake_boto3_client(service_name, region_name=None):
            _ = region_name
            if service_name == "elasticache":
                return elasticache_client
            if service_name == "kms":
                return kms_client
            raise AssertionError(f"unexpected service: {service_name}")

        event = {
            "cluster_id": "elasticache-redis-tracking",
            "region": "sa-east-1",
            "policy_name": "default",
            "account_id": "526177858629",
        }

        with patch("kms_lambda.boto3.client", side_effect=fake_boto3_client):
            response = kms_lambda.handler(event, None)

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["body"]["kms_key_id"], "kms-cluster")
        self.assertEqual(len(kms_client.put_calls), 1)
        self.assertEqual(kms_client.put_calls[0]["PolicyName"], "default")
        policy_document = kms_client.put_calls[0]["Policy"]
        self.assertEqual(policy_document["Version"], "2012-10-17")
        self.assertTrue(policy_document["Id"].startswith("Rds-Kms-"))
        self.assertEqual(len(policy_document["Statement"]), 2)
        self.assertEqual(kms_client.get_calls, [])

        statements = policy_document["Statement"]
        admin_by_role_statement = next(
            statement
            for statement in statements
            if statement.get("Sid") == "Allows admin of the key"
        )
        principal_arns = admin_by_role_statement["Condition"]["ArnLike"]["aws:PrincipalArn"]
        self.assertIn(
            "arn:aws:iam::526177858629:role/itau-github-repo-*",
            principal_arns,
        )
        self.assertIn(
            "arn:aws:iam::526177858629:role/itau-codebuild-data-execution-role",
            principal_arns,
        )
        logs_statement = next(
            statement
            for statement in statements
            if statement.get("Sid") == "Allow use of key in another account"
        )
        self.assertEqual(
            logs_statement["Principal"]["Service"],
            "logs.sa-east-1.amazonaws.com",
        )
        self.assertIn("kms:GetKeyPolicy", logs_statement["Action"])

    def test_handler_returns_detailed_put_policy_error(self):
        elasticache_client = FakeElasticacheClient(
            cache_cluster_response={
                "CacheClusters": [{"CacheClusterId": "elasticache-redis-tracking", "KmsKeyId": "kms-cluster"}]
            }
        )
        kms_client = FakeKmsClient(
            put_error=ErrorWithResponse(
                {
                    "Error": {
                        "Code": "MalformedPolicyDocumentException",
                        "Message": "The policy contains an invalid principal",
                    }
                }
            )
        )

        def fake_boto3_client(service_name, region_name=None):
            _ = region_name
            if service_name == "elasticache":
                return elasticache_client
            if service_name == "kms":
                return kms_client
            raise AssertionError(f"unexpected service: {service_name}")

        event = {
            "cluster_id": "elasticache-redis-tracking",
            "region": "sa-east-1",
            "policy_name": "default",
            "account_id": "526177858629",
        }

        with patch("kms_lambda.boto3.client", side_effect=fake_boto3_client):
            response = kms_lambda.handler(event, None)

        self.assertEqual(response["statusCode"], 500)
        self.assertIn("MalformedPolicyDocumentException", response["body"])
        self.assertIn("invalid principal", response["body"].lower())


if __name__ == "__main__":
    unittest.main()
