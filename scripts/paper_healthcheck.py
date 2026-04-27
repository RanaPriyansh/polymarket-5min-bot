#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from paper_health_policy import evaluate_healthcheck_restart_policy
from status_utils import render_status_text, runtime_health_payload


DEFAULT_RESTART_COMMAND = ["systemctl", "restart", "polymarket-paper-bot.service"]


def _paper_process_entries() -> list[tuple[int, str]]:
    proc = subprocess.run(
        ["ps", "-eo", "pid=,args="],
        check=False,
        capture_output=True,
        text=True,
    )
    entries: list[tuple[int, str]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line or "cli.py run" not in line or "--mode paper" not in line:
            continue
        parts = line.split(maxsplit=1)
        try:
            pid = int(parts[0])
        except (IndexError, ValueError):
            continue
        cmd = parts[1] if len(parts) > 1 else ""
        entries.append((pid, cmd))
    return entries


def paper_process_count() -> int:
    return len(_paper_process_entries())


def service_main_pid() -> int:
    proc = subprocess.run(
        ["systemctl", "show", "polymarket-paper-bot.service", "-p", "MainPID", "--no-pager"],
        check=False,
        capture_output=True,
        text=True,
    )
    for line in proc.stdout.splitlines():
        if not line.startswith("MainPID="):
            continue
        try:
            return int(line.split("=", 1)[1].strip() or 0)
        except ValueError:
            return 0
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Safe paper bot healthcheck with protective-stop guard.")
    parser.add_argument("--runtime-dir", default="data/runtime")
    parser.add_argument("--max-heartbeat-age", type=int, default=180)
    args = parser.parse_args()

    payload = runtime_health_payload(args.runtime_dir, max_heartbeat_age=args.max_heartbeat_age)
    process_entries = _paper_process_entries()
    process_count = len(process_entries)
    main_pid = service_main_pid()
    managed_paper_process_count = sum(1 for pid, _ in process_entries if main_pid > 0 and pid == main_pid)
    decision = evaluate_healthcheck_restart_policy(
        payload,
        paper_process_count=process_count,
        service_main_pid=main_pid,
        managed_paper_process_count=managed_paper_process_count,
    )

    print(render_status_text(args.runtime_dir))
    print(f"Healthy: {payload['healthy']} (threshold={args.max_heartbeat_age}s)")
    print(
        "Auto-heal policy: reason={reason} should_restart={restart} paper_processes={count} managed_paper_processes={managed} service_main_pid={main_pid} protective_stop={protective}".format(
            reason=decision["reason"],
            restart=decision["should_restart"],
            count=decision["paper_process_count"],
            managed=decision["managed_paper_process_count"],
            main_pid=decision["service_main_pid"],
            protective=decision["protective_stop"],
        )
    )
    if decision["protective_stop_reasons"]:
        print("Protective stop markers: " + "; ".join(decision["protective_stop_reasons"]))

    if not decision["should_restart"]:
        return 0

    print("Auto-heal action: restarting polymarket-paper-bot.service")
    restart = subprocess.run(DEFAULT_RESTART_COMMAND, check=False, capture_output=True, text=True)
    if restart.stdout.strip():
        print(restart.stdout.strip())
    if restart.stderr.strip():
        print(restart.stderr.strip(), file=sys.stderr)
    if restart.returncode != 0:
        print(f"Auto-heal restart failed with exit code {restart.returncode}", file=sys.stderr)
        return restart.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
