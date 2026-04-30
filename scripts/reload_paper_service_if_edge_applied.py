#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EDGE_PATH = PROJECT_ROOT / "data/research/edge_enforcement_latest.json"
DEFAULT_STATE_PATH = PROJECT_ROOT / "data/research/edge_reload_state.json"
SERVICE = "polymarket-paper-bot.service"
MAX_ARTIFACT_AGE_SECONDS = 300


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _has_applied_runtime_decision(payload: dict[str, Any]) -> bool:
    return any(
        item.get("applied") and item.get("action") in {"promote_to_paper_active", "demote"}
        for item in payload.get("applied_decisions", [])
    )


def _service_active(service: str) -> bool:
    return subprocess.run(["systemctl", "is-active", "--quiet", service], check=False).returncode == 0


def maybe_reload(edge_path: Path, state_path: Path, *, service: str = SERVICE, max_age_seconds: int = MAX_ARTIFACT_AGE_SECONDS) -> int:
    if not edge_path.exists():
        print(f"No edge enforcement artifact at {edge_path}; no reload")
        return 0
    try:
        payload = _read_json(edge_path)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Invalid edge enforcement artifact at {edge_path}; refusing reload: {exc}")
        return 0

    if not _has_applied_runtime_decision(payload):
        print("No applied edge config changes; no reload")
        return 0

    created_at = float(payload.get("created_at") or 0.0)
    age = time.time() - created_at if created_at else float("inf")
    if age > max_age_seconds:
        print(f"Edge artifact is stale ({age:.1f}s > {max_age_seconds}s); no reload")
        return 0

    digest = _hash_payload(payload)
    state = {}
    if state_path.exists():
        try:
            state = _read_json(state_path)
        except (OSError, json.JSONDecodeError):
            state = {}
    if state.get("last_reloaded_hash") == digest:
        print("Edge artifact already consumed; no reload")
        return 0

    if not _service_active(service):
        print(f"{service} is not active; refusing to override protective stop")
        return 0

    print(f"Applied edge decisions detected; restarting active {service} to load config")
    rc = subprocess.run(["systemctl", "restart", service], check=False).returncode
    if rc == 0:
        _write_json(
            state_path,
            {
                "last_reloaded_hash": digest,
                "last_reloaded_created_at": created_at,
                "last_reloaded_ts": time.time(),
                "service": service,
            },
        )
    return rc


def main() -> int:
    parser = argparse.ArgumentParser(description="Reload paper service only once after a fresh applied edge decision")
    parser.add_argument("--edge-path", default=str(DEFAULT_EDGE_PATH))
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--service", default=SERVICE)
    parser.add_argument("--max-age-seconds", type=int, default=MAX_ARTIFACT_AGE_SECONDS)
    args = parser.parse_args()
    return maybe_reload(
        Path(args.edge_path),
        Path(args.state_path),
        service=args.service,
        max_age_seconds=args.max_age_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
