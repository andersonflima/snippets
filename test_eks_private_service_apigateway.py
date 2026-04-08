import os
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import eks_private_service_apigateway as eks_script


class EksPrivateServiceApiGatewayTests(unittest.TestCase):
    def test_build_config_from_args_uses_direct_nlb_mode(self) -> None:
        args = Namespace(
            region="sa-east-1",
            cluster_name=None,
            service_name=None,
            nlb_arn=None,
            nlb_name="autoservice-app-dev",
            nlb_subnet_id=["subnet-a", "subnet-b"],
            target_group_arn=None,
            target_group_name=None,
            target_id=["i-abc123"],
            target_type="instance",
            listener_protocol="TCP",
            aws_endpoint_url=None,
            namespace="default",
            service_port=80,
            target_port=3000,
            selector=[],
            annotation=[],
            nlb_scheme="internal",
            vpc_link_name=None,
            api_name=None,
            stage_name="prod",
            api_endpoint_type="REGIONAL",
            timeout_seconds=900,
            poll_interval_seconds=10,
            skip_kubeconfig_update=False,
            skip_cluster_check=False,
            log_level="INFO",
            dry_run=False,
        )

        config = eks_script.build_config_from_args(args)

        self.assertEqual(config["mode"], eks_script.DIRECT_NLB_MODE)
        self.assertEqual(config["nlb_name"], "autoservice-app-dev")
        self.assertEqual(config["nlb_scheme"], "internal")
        self.assertEqual(config["target_group_name"], "autoservice-app-dev-tg")
        self.assertEqual(config["service_name"], "autoservice-app-dev")

    def test_normalize_kubeconfig_exec_api_version_file_replaces_legacy_values(self) -> None:
        legacy_content = """
apiVersion: v1
kind: Config
users:
- name: cluster-a
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1alpha1
      command: aws
- name: cluster-b
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1aplha1
      command: aws
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            kubeconfig_path = os.path.join(temp_dir, "config")
            with open(kubeconfig_path, "w", encoding="utf-8") as kubeconfig_file:
                kubeconfig_file.write(legacy_content)

            changed = eks_script.normalize_kubeconfig_exec_api_version_file(kubeconfig_path)

            self.assertTrue(changed)
            with open(kubeconfig_path, "r", encoding="utf-8") as kubeconfig_file:
                updated_content = kubeconfig_file.read()
            self.assertNotIn(eks_script.LEGACY_EXEC_API_VERSION, updated_content)
            self.assertNotIn(eks_script.LEGACY_EXEC_API_VERSION_TYPO, updated_content)
            self.assertIn(eks_script.SUPPORTED_EXEC_API_VERSION, updated_content)
            self.assertTrue(os.path.exists(f"{kubeconfig_path}.bak"))

    def test_normalize_kubeconfig_exec_api_versions_uses_kubeconfig_env(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first_kubeconfig_path = os.path.join(temp_dir, "config-a")
            second_kubeconfig_path = os.path.join(temp_dir, "config-b")
            for kubeconfig_path in (first_kubeconfig_path, second_kubeconfig_path):
                with open(kubeconfig_path, "w", encoding="utf-8") as kubeconfig_file:
                    kubeconfig_file.write(
                        f"apiVersion: {eks_script.LEGACY_EXEC_API_VERSION}\n"
                    )

            with patch.dict(
                os.environ,
                {"KUBECONFIG": os.pathsep.join([first_kubeconfig_path, second_kubeconfig_path])},
                clear=False,
            ):
                changed_paths = eks_script.normalize_kubeconfig_exec_api_versions()

            self.assertEqual(
                changed_paths,
                [first_kubeconfig_path, second_kubeconfig_path],
            )

    def test_is_legacy_exec_credential_error_detects_stdout_decode_failure(self) -> None:
        message = (
            'Falha ao executar kubectl apply -f -: error validating "STDIN": '
            'getting credentials: decoding stdout: no kind "ExecCredential" '
            'is registered for version "client.authentication.k8s.io/v1alpha1"'
        )

        detected = eks_script.is_legacy_exec_credential_error(message)

        self.assertTrue(detected)

    def test_build_ephemeral_kubeconfig_uses_static_token(self) -> None:
        cluster = {
            "name": "cluster-dev",
            "endpoint": "https://example.eks.amazonaws.com",
            "certificateAuthority": {"data": "Y2VydA=="},
        }

        kubeconfig = eks_script.build_ephemeral_kubeconfig(cluster, "token-123")

        self.assertEqual(kubeconfig["kind"], "Config")
        self.assertEqual(
            kubeconfig["clusters"][0]["cluster"]["certificate-authority-data"],
            "Y2VydA==",
        )
        self.assertEqual(kubeconfig["users"][0]["user"]["token"], "token-123")
        self.assertEqual(kubeconfig["current-context"], "cluster-dev-context")

    def test_parse_eks_token_expiration_supports_z_suffix(self) -> None:
        expiration = eks_script.parse_eks_token_expiration("2026-04-08T20:10:00Z")

        self.assertEqual(
            expiration,
            datetime(2026, 4, 8, 20, 10, 0, tzinfo=timezone.utc),
        )

    def test_extract_service_hostname_accepts_hostname_or_ip(self) -> None:
        hostname_service = {
            "status": {"loadBalancer": {"ingress": [{"hostname": "internal-nlb.amazonaws.com"}]}}
        }
        ip_service = {
            "status": {"loadBalancer": {"ingress": [{"ip": "10.0.0.15"}]}}
        }

        hostname = eks_script.extract_service_hostname(hostname_service)
        ip_value = eks_script.extract_service_hostname(ip_service)

        self.assertEqual(hostname, "internal-nlb.amazonaws.com")
        self.assertEqual(ip_value, "10.0.0.15")

    def test_summarize_pending_service_nlb_includes_latest_events(self) -> None:
        service = {
            "spec": {
                "type": "LoadBalancer",
                "clusterIP": "172.20.10.20",
            }
        }
        events = [
            {
                "type": "Normal",
                "reason": "EnsuringLoadBalancer",
                "message": "Ensuring load balancer",
                "lastTimestamp": "2026-04-08T17:26:30Z",
            },
            {
                "type": "Warning",
                "reason": "SyncLoadBalancerFailed",
                "message": "subnet tag missing",
                "lastTimestamp": "2026-04-08T17:26:40Z",
            },
        ]

        summary = eks_script.summarize_pending_service_nlb(service, events)

        self.assertIn("type=LoadBalancer", summary)
        self.assertIn("clusterIP=172.20.10.20", summary)
        self.assertIn("Warning/SyncLoadBalancerFailed: subnet tag missing", summary)

    def test_run_kubectl_command_falls_back_to_static_token_on_legacy_exec_error(self) -> None:
        config = {
            "region": "sa-east-1",
            "cluster_name": "cluster-dev",
            "aws_endpoint_url": None,
        }
        eks_client = object()
        legacy_error = RuntimeError(
            'Falha ao executar kubectl apply -f -: '
            'decoding stdout: no kind "ExecCredential" is registered for version '
            '"client.authentication.k8s.io/v1alpha1"'
        )

        with patch.object(
            eks_script,
            "run_command",
            side_effect=[
                legacy_error,
                (
                    '{"status":{"token":"k8s-token",'
                    '"expirationTimestamp":"2026-04-08T20:10:00Z"}}'
                ),
                "applied",
            ],
        ) as run_command_mock, patch.object(
            eks_script,
            "load_cluster_details",
            return_value={
                "name": "cluster-dev",
                "endpoint": "https://example.eks.amazonaws.com",
                "certificateAuthority": {"data": "Y2VydA=="},
            },
        ):
            output = eks_script.run_kubectl_command(
                config,
                eks_client,
                ["apply", "-f", "-"],
                input_text='{"kind":"Service"}',
            )

        self.assertEqual(output, "applied")
        self.assertEqual(run_command_mock.call_count, 3)
        first_command = run_command_mock.call_args_list[0].args[0]
        second_command = run_command_mock.call_args_list[1].args[0]
        third_command = run_command_mock.call_args_list[2].args[0]
        self.assertEqual(first_command, ["kubectl", "apply", "-f", "-"])
        self.assertEqual(
            second_command,
            [
                "aws",
                "eks",
                "get-token",
                "--region",
                "sa-east-1",
                "--cluster-name",
                "cluster-dev",
            ],
        )
        self.assertEqual(third_command[0:2], ["kubectl", "--kubeconfig"])
        self.assertEqual(third_command[-3:], ["apply", "-f", "-"])

    def test_run_kubectl_command_reuses_static_token_mode_without_retrying_default_exec(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            kubeconfig_path = os.path.join(temp_dir, "cluster-dev-kubeconfig.json")
            with open(kubeconfig_path, "w", encoding="utf-8") as kubeconfig_file:
                kubeconfig_file.write("{}")

            config = {
                "region": "sa-east-1",
                "cluster_name": "cluster-dev",
                "aws_endpoint_url": None,
                "_kubectl_auth_state": {
                    "mode": "static-token",
                    "warning_logged": True,
                    "cluster": {
                        "name": "cluster-dev",
                        "endpoint": "https://example.eks.amazonaws.com",
                        "certificateAuthority": {"data": "Y2VydA=="},
                    },
                    "token": "cached-token",
                    "token_expiration": datetime.now(timezone.utc) + timedelta(minutes=10),
                    "kubeconfig_path": kubeconfig_path,
                },
            }

            with patch.object(
                eks_script,
                "run_command",
                return_value="service-json",
            ) as run_command_mock:
                output = eks_script.run_kubectl_command(
                    config,
                    object(),
                    ["get", "svc", "autoservice-app-dev", "-n", "default", "-o", "json"],
                )

        self.assertEqual(output, "service-json")
        self.assertEqual(run_command_mock.call_count, 1)
        command = run_command_mock.call_args.args[0]
        self.assertEqual(command[0:2], ["kubectl", "--kubeconfig"])
        self.assertNotIn("aws", command)

    def test_wait_for_nlb_hostname_raises_timeout_with_service_diagnostics(self) -> None:
        config = {
            "namespace": "default",
            "service_name": "autoservice-app-dev",
            "timeout_seconds": 1,
            "poll_interval_seconds": 0,
        }
        service = {
            "spec": {
                "type": "LoadBalancer",
                "clusterIP": "172.20.10.20",
            },
            "status": {"loadBalancer": {"ingress": []}},
        }
        events = [
            {
                "type": "Warning",
                "reason": "SyncLoadBalancerFailed",
                "message": "subnet tag missing",
                "lastTimestamp": "2026-04-08T17:26:40Z",
            }
        ]

        with patch.object(
            eks_script,
            "load_service",
            return_value=service,
        ), patch.object(
            eks_script,
            "load_service_events",
            return_value=events,
        ), patch.object(
            eks_script.time,
            "sleep",
            return_value=None,
        ):
            with self.assertRaises(TimeoutError) as raised_error:
                eks_script.wait_for_nlb_hostname(config, object())

        self.assertIn("Último diagnóstico", str(raised_error.exception))
        self.assertIn("SyncLoadBalancerFailed", str(raised_error.exception))

    def test_execute_direct_mode_configures_apigateway_without_kubectl(self) -> None:
        config = {
            "mode": eks_script.DIRECT_NLB_MODE,
            "region": "sa-east-1",
            "cluster_name": None,
            "service_name": "autoservice-app-dev",
            "namespace": "default",
            "aws_endpoint_url": None,
            "service_port": 80,
            "target_port": 3000,
            "selector": {"app": "autoservice-app-dev"},
            "annotations": {},
            "nlb_arn": None,
            "nlb_name": "autoservice-app-dev",
            "nlb_subnet_ids": ["subnet-a", "subnet-b"],
            "target_group_arn": None,
            "target_group_name": "autoservice-app-dev-tg",
            "target_ids": ["i-abc123"],
            "target_type": "instance",
            "listener_protocol": "TCP",
            "vpc_link_name": "autoservice-app-dev-vpc-link",
            "api_name": "autoservice-app-dev-api",
            "stage_name": "prod",
            "api_endpoint_type": "REGIONAL",
            "timeout_seconds": 900,
            "poll_interval_seconds": 10,
            "skip_kubeconfig_update": False,
            "skip_cluster_check": False,
            "_kubectl_auth_state": None,
        }

        with patch.object(
            eks_script,
            "ensure_commands_exist",
        ) as ensure_commands_exist_mock, patch.object(
            eks_script,
            "build_clients",
            return_value={
                "eks": object(),
                "apigateway": object(),
                "elbv2": object(),
            },
        ), patch.object(
            eks_script,
            "ensure_direct_nlb_backend",
            return_value={
                "load_balancer": {
                    "DNSName": "internal-autoservice-app-dev.amazonaws.com",
                    "LoadBalancerArn": "arn:aws:elasticloadbalancing:sa-east-1:123:loadbalancer/net/autoservice/abc",
                },
                "target_group": None,
                "listener": {"ListenerArn": "listener-arn"},
            },
        ) as ensure_direct_nlb_backend_mock, patch.object(
            eks_script,
            "ensure_vpc_link",
            return_value={"id": "vpclink-123"},
        ) as ensure_vpc_link_mock, patch.object(
            eks_script,
            "ensure_api_gateway",
            return_value={
                "api_id": "api-123",
                "deployment_id": "deploy-123",
                "api_url": "https://api.example.com/prod",
            },
        ):
            result = eks_script.execute(config)

        ensure_commands_exist_mock.assert_called_once_with(())
        ensure_direct_nlb_backend_mock.assert_called_once()
        ensure_vpc_link_mock.assert_called_once()
        self.assertEqual(result["mode"], eks_script.DIRECT_NLB_MODE)
        self.assertEqual(result["nlb_arn"], "arn:aws:elasticloadbalancing:sa-east-1:123:loadbalancer/net/autoservice/abc")
        self.assertEqual(result["vpc_link_id"], "vpclink-123")

    def test_ensure_vpc_link_recreates_when_target_arn_differs(self) -> None:
        apigateway_client = Mock()
        apigateway_client.create_vpc_link.return_value = {"id": "vpclink-new"}
        config = {
            "vpc_link_name": "autoservice-app-dev-vpc-link",
            "timeout_seconds": 900,
            "poll_interval_seconds": 10,
        }

        with patch.object(
            eks_script,
            "get_named_vpc_links",
            return_value=[
                {
                    "id": "vpclink-old",
                    "name": "autoservice-app-dev-vpc-link",
                    "targetArns": ["arn:aws:elasticloadbalancing:sa-east-1:123:loadbalancer/net/old/abc"],
                }
            ],
        ), patch.object(
            eks_script,
            "wait_for_vpc_link_deletion",
            return_value=None,
        ) as wait_for_vpc_link_deletion_mock, patch.object(
            eks_script,
            "wait_for_vpc_link",
            return_value={"id": "vpclink-new", "targetArns": ["arn:new"]},
        ) as wait_for_vpc_link_mock:
            result = eks_script.ensure_vpc_link(
                apigateway_client,
                "arn:new",
                config,
            )

        apigateway_client.delete_vpc_link.assert_called_once_with(vpcLinkId="vpclink-old")
        wait_for_vpc_link_deletion_mock.assert_called_once()
        apigateway_client.create_vpc_link.assert_called_once_with(
            name="autoservice-app-dev-vpc-link",
            targetArns=["arn:new"],
        )
        wait_for_vpc_link_mock.assert_called_once_with(apigateway_client, "vpclink-new", config)
        self.assertEqual(result["id"], "vpclink-new")


if __name__ == "__main__":
    unittest.main()
