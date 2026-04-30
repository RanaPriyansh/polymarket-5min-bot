#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path

EDGE_PATH = Path("/root/obsidian-hermes-vault/projects/polymarket-5min-bot/data/research/edge_enforcement_latest.json")
SERVICE = "polymarket-paper-bot.service"


def main() -> int:
    if not EDGE_PATH.exists():
        print(f"No edge enforcement artifact at {EDGE_PATH}; no reload")
        return 0
    payload = json.loads(EDGE_PATH.read_text(encoding="utf-8"))
    applied = [
        item
        for item in payload.get("applied_decisions", [])
        if item.get("applied") and item.get("action") in {"promote_to_paper_active", "demote"}
    ]
    if not applied:
        print("No applied edge config changes; no reload")
        return 0
    is_active = subprocess.run(["systemctl", "is-active", "--quiet", SERVICE], check=False).returncode == 0
    if not is_active:
        print(f"{SERVICE} is not active; refusing to override protective stop")
        return 0
    print(f"Applied edge decisions detected ({len(applied)}); restarting active paper service to load config")
    return subprocess.run(["systemctl", "restart", SERVICE], check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
