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


if __name__ == "__main__":
    unittest.main()
