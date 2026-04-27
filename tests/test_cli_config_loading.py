import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import cli


class LoadCfgTests(unittest.TestCase):
    def test_load_cfg_uses_repo_relative_config_outside_cwd_and_preserves_env_override(self):
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                with mock.patch.dict(os.environ, {"POLYMARKET_WALLET_ADDRESS": "0xabc123"}, clear=False):
                    cfg = cli.load_cfg()
            finally:
                os.chdir(original_cwd)

        self.assertEqual(cfg["runtime"]["dir"], "data/runtime")
        self.assertEqual(cfg["polymarket"]["wallet_address"], "0xabc123")
        self.assertFalse((Path(tmpdir) / "logs").exists())
        self.assertFalse((Path(tmpdir) / "data" / "runtime").exists())
        self.assertTrue((cli.PROJECT_ROOT / "logs").exists())
        self.assertTrue((cli.PROJECT_ROOT / "data" / "runtime").exists())


if __name__ == "__main__":
    unittest.main()
