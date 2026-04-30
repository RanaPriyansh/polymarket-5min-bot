import unittest
from pathlib import Path


SYSTEMD_DIR = Path("deploy/systemd")


class SystemdInstallAssetsTests(unittest.TestCase):
    def test_install_and_uninstall_reference_all_shipped_polymarket_units(self):
        units = sorted(path.name for path in SYSTEMD_DIR.glob("polymarket-paper-*.*") if path.suffix in {".service", ".timer"})
        install = (SYSTEMD_DIR / "install.sh").read_text(encoding="utf-8")
        uninstall = (SYSTEMD_DIR / "uninstall.sh").read_text(encoding="utf-8")
        for unit in units:
            self.assertIn(unit, install, f"install.sh missing {unit}")
            self.assertIn(unit, uninstall, f"uninstall.sh missing {unit}")

    def test_bakeoff_service_is_bounded_locked_and_candidate_only(self):
        text = (SYSTEMD_DIR / "polymarket-paper-bakeoff.service").read_text(encoding="utf-8")
        self.assertIn("/usr/bin/flock -n /run/polymarket-paper-ops.lock", text)
        self.assertIn("configs/candidate-edge-bakeoff.yaml", text)
        self.assertIn("--allow-candidate-trial", text)
        self.assertIn("TimeoutStartSec=12min", text)
        self.assertNotIn("RuntimeMaxSec=", text)

    def test_research_service_enforces_edges_with_single_reload_guard(self):
        text = (SYSTEMD_DIR / "polymarket-paper-research.service").read_text(encoding="utf-8")
        self.assertIn("/usr/bin/flock -n /run/polymarket-paper-ops.lock", text)
        self.assertIn("--enforce-edges", text)
        self.assertIn("reload_paper_service_if_edge_applied.py", text)
        self.assertNotIn("RuntimeMaxSec=", text)
        self.assertNotIn("bash -lc", text)

    def test_healthcheck_unit_is_alert_only_no_restart(self):
        text = (SYSTEMD_DIR / "polymarket-paper-bot-healthcheck.service").read_text(encoding="utf-8")
        self.assertIn("ALERT-ONLY", text)
        self.assertIn("--no-restart", text)
        self.assertNotIn("auto-heal", text.lower())


if __name__ == "__main__":
    unittest.main()
