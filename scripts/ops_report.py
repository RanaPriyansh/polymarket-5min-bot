#!/usr/bin/env python3
"""
Canonical operator entry point.

Produces all 4 operator surfaces:
  1. ops_status.py                -- current run state
  2. ops_evidence.py              -- strategy family metrics  
  3. ops_settlement_diagnostics.py -- settlement lifecycle proof
  4. ops_daily_summary.py         -- daily activity compression

Usage:
    python scripts/ops_report.py              -- all to stdout
    python scripts/ops_report.py --write      -- writes all to data/runtime/ops_*.txt
    python scripts/ops_report.py --status     -- only status
    python scripts/ops_report.py --evidence   -- only evidence
    python scripts/ops_report.py --settlement -- only settlement diagnostics
    python scripts/ops_report.py --daily      -- only daily summary
    python scripts/ops_report.py --date 2026-04-05  -- daily for specific date
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"


def load_script(name: str):
    path = SCRIPTS / name
    spec = importlib.util.spec_from_file_location(name.replace(".py", ""), path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    write_mode = "--write" in sys.argv
    run_status = "--status" in sys.argv or not any(x in sys.argv for x in ("--evidence", "--settlement", "--daily"))
    run_evidence = "--evidence" in sys.argv or not any(x in sys.argv for x in ("--status", "--settlement", "--daily"))
    run_settlement = "--settlement" in sys.argv or not any(x in sys.argv for x in ("--status", "--evidence", "--daily"))
    run_daily = "--daily" in sys.argv or not any(x in sys.argv for x in ("--status", "--evidence", "--settlement"))

    # Build argv for each script
    base_args = [sys.argv[0]]
    if write_mode:
        base_args.append("--write")

    date_args = []
    for i, arg in enumerate(sys.argv):
        if arg == "--date" and i + 1 < len(sys.argv):
            date_args.extend(["--date", sys.argv[i + 1]])
            break

    if run_status:
        mod = load_script("ops_status.py")
        old_argv = sys.argv
        sys.argv = base_args
        mod.main()
        sys.argv = old_argv
        print()

    if run_evidence:
        mod = load_script("ops_evidence.py")
        old_argv = sys.argv
        sys.argv = base_args
        mod.main()
        sys.argv = old_argv
        print()

    if run_settlement:
        mod = load_script("ops_settlement_diagnostics.py")
        old_argv = sys.argv
        sys.argv = base_args
        mod.main()
        sys.argv = old_argv
        print()

    if run_daily:
        mod = load_script("ops_daily_summary.py")
        old_argv = sys.argv
        sys.argv = base_args + date_args
        mod.main()
        sys.argv = old_argv
        print()


if __name__ == "__main__":
    main()
