import os
import tempfile
import unittest
from unittest.mock import patch

import eks_private_service_apigateway as eks_script


class EksPrivateServiceApiGatewayTests(unittest.TestCase):
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
            side_effect=[legacy_error, '{"status":{"token":"k8s-token"}}', "applied"],
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


if __name__ == "__main__":
    unittest.main()
