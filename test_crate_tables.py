import unittest
from contextlib import redirect_stderr
import io

import crate_tables


class CrateTablesTests(unittest.TestCase):
    def test_build_default_prefix_uses_tabela(self) -> None:
        prefix = crate_tables.build_default_prefix()

        self.assertEqual(prefix, "tabela")

    def test_build_table_names_uses_sequential_suffix(self) -> None:
        table_names = crate_tables.build_table_names("tabela", 3)

        self.assertEqual(
            table_names,
            [
                "tabela1",
                "tabela2",
                "tabela3",
            ],
        )

    def test_iter_item_target_sizes_preserves_total_and_limit(self) -> None:
        sizes = list(crate_tables.iter_item_target_sizes(total_bytes=1000, max_item_bytes=300))

        self.assertEqual(sum(sizes), 1000)
        self.assertTrue(all(size <= 300 for size in sizes))
        self.assertEqual(len(sizes), 4)

    def test_build_item_for_target_size_matches_requested_size(self) -> None:
        item = crate_tables.build_item_for_target_size("orders", 7, 1024)

        self.assertEqual(crate_tables.estimate_item_size_bytes(item), 1024)
        self.assertEqual(item["pk"], "orders#00000007")
        self.assertEqual(len(item["payload"]), 1000)

    def test_minimum_item_size_bytes_matches_empty_payload_item(self) -> None:
        minimum_size = crate_tables.minimum_item_size_bytes("orders")

        self.assertEqual(minimum_size, len("pk") + len("orders#00000001") + len("payload"))

    def test_parse_args_rejects_item_size_above_safe_limit(self) -> None:
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                crate_tables.parse_args(["--tables", "1", "--gib", "1", "--item-kib", "351"])


if __name__ == "__main__":
    unittest.main()
