"""
Contradiction-first gate for autoresearch.

Computes a GREEN/YELLOW/RED gate state from runtime inputs before any
autoresearch cycle is allowed to emit experiments or recommendations.

Gate states:
  RED    — blocks all output; emits contradiction report only
  YELLOW — allows one hypothesis with confidence < 0.5
  GREEN  — allows one evidence-backed experiment recommendation

compute_gate_state(inputs: dict) -> tuple[str, list[str]]
  Returns (state, reasons) where reasons is a non-empty list if state != GREEN.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


def compute_gate_state(inputs: Dict[str, Any]) -> Tuple[str, List[str]]:
    """
    Compute the contradiction-first gate state.

    inputs dict keys (all optional, safe defaults assumed if missing):
      win_rate: float                         (0.0 - 1.0)
      resolved_count: int                     (number of settled trades with known pnl)
      settlement_pnl_computable: bool         (True if slot_settled has realized_pnl)
      run_lineage_fragmentation: int          (distinct run_ids in last 2 hours)
      circuit_breaker_fired_unreviewed: bool  (any unreviewed circuit breaker snapshot)
      contradiction_log_open: int             (count of unresolved contradictions)
      # Optional — for YELLOW only
      # resolved_count < 50 or win_rate < 0.40 triggers YELLOW if not already RED

    Returns: (state: str, reasons: list[str])
      state is "RED", "YELLOW", or "GREEN"
      reasons is [] for GREEN, non-empty list of human-readable strings otherwise
    """
    win_rate = float(inputs.get("win_rate", 0.0))
    resolved_count = int(inputs.get("resolved_count", 0))
    settlement_pnl_computable = bool(inputs.get("settlement_pnl_computable", True))
    run_lineage_fragmentation = int(inputs.get("run_lineage_fragmentation", 0))
    circuit_breaker_fired_unreviewed = bool(inputs.get("circuit_breaker_fired_unreviewed", False))
    contradiction_log_open = int(inputs.get("contradiction_log_open", 0))

    red_reasons: List[str] = []

    if win_rate < 0.20 and resolved_count >= 20:
        red_reasons.append(
            f"win_rate={win_rate:.3f} < 0.20 with resolved_count={resolved_count} >= 20"
        )
    if not settlement_pnl_computable:
        red_reasons.append(
            "settlement_pnl_computable=False: slot_settled schema missing realized_pnl"
        )
    if run_lineage_fragmentation >= 4:
        red_reasons.append(
            f"run_lineage_fragmentation={run_lineage_fragmentation} >= 4 (experiment fragmented)"
        )
    if circuit_breaker_fired_unreviewed:
        red_reasons.append(
            "circuit_breaker_fired_unreviewed=True: prior circuit breaker stop not yet reviewed"
        )
    if contradiction_log_open >= 1:
        red_reasons.append(
            f"contradiction_log_open={contradiction_log_open}: unresolved contradictions exist"
        )

    if red_reasons:
        return ("RED", red_reasons)

    yellow_reasons: List[str] = []

    if resolved_count < 50:
        yellow_reasons.append(
            f"resolved_count={resolved_count} < 50: insufficient settled trades"
        )
    if win_rate < 0.40:
        yellow_reasons.append(
            f"win_rate={win_rate:.3f} < 0.40: not yet above profitability threshold"
        )
    if run_lineage_fragmentation >= 2:
        yellow_reasons.append(
            f"run_lineage_fragmentation={run_lineage_fragmentation} >= 2 (some fragmentation)"
        )

    if yellow_reasons:
        return ("YELLOW", yellow_reasons)

    return ("GREEN", [])


def check_settlement_pnl_computable(runtime_dir: str) -> bool:
    """
    Check whether the ledger has any slot_settled events with non-null realized_pnl.
    Returns True if at least one event has realized_pnl populated.
    Returns False if no such events exist or ledger is absent.
    """
    import json
    import sqlite3
    from pathlib import Path

    ledger_path = Path(runtime_dir) / "ledger.db"
    if not ledger_path.exists():
        return False
    try:
        conn = sqlite3.connect(str(ledger_path))
        cur = conn.cursor()
        cur.execute(
            "SELECT payload_json FROM ledger_events WHERE event_type='slot_settled' LIMIT 20"
        )
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            try:
                payload = json.loads(row[0])
                if payload.get("realized_pnl") is not None:
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False


def count_run_lineage_fragmentation(runtime_dir: str, window_seconds: float = 7200.0) -> int:
    """
    Count distinct run_ids with events in the last window_seconds.
    Returns 0 if ledger is absent.
    """
    import sqlite3
    import time
    from pathlib import Path

    ledger_path = Path(runtime_dir) / "ledger.db"
    if not ledger_path.exists():
        return 0
    try:
        conn = sqlite3.connect(str(ledger_path))
        cur = conn.cursor()
        cutoff = time.time() - window_seconds
        cur.execute(
            "SELECT COUNT(DISTINCT run_id) FROM ledger_events WHERE event_ts >= ?",
            (cutoff,),
        )
        result = cur.fetchone()
        conn.close()
        return int(result[0]) if result else 0
    except Exception:
        return 0


def has_unreviewed_circuit_breaker(runtime_dir: str) -> bool:
    """
    Check if any forensic snapshot with trigger=circuit_breaker exists
    and has not been marked reviewed.
    A snapshot is considered unreviewed if its manifest.json lacks 'reviewed_ts'.
    """
    import json
    from pathlib import Path

    snapshots_dir = Path(runtime_dir) / "forensic-snapshots"
    if not snapshots_dir.exists():
        return False
    for manifest_path in snapshots_dir.glob("*/manifest.json"):
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
            if manifest.get("trigger") == "circuit_breaker" and "reviewed_ts" not in manifest:
                return True
        except Exception:
            continue
    return False


def build_gate_inputs(runtime_dir: str, status: dict | None = None) -> dict:
    """
    Build the gate inputs dict from runtime artifacts.
    status: optional pre-loaded status.json dict (avoids re-reading).
    """
    import json
    from pathlib import Path

    if status is None:
        status_path = Path(runtime_dir) / "status.json"
        if status_path.exists():
            try:
                with open(status_path) as f:
                    status = json.load(f)
            except Exception:
                status = {}
        else:
            status = {}

    win_rate = float(status.get("win_rate", 0.0))
    resolved_count = int(status.get("resolved_trade_count", 0))

    return {
        "win_rate": win_rate,
        "resolved_count": resolved_count,
        "settlement_pnl_computable": check_settlement_pnl_computable(runtime_dir),
        "run_lineage_fragmentation": count_run_lineage_fragmentation(runtime_dir),
        "circuit_breaker_fired_unreviewed": has_unreviewed_circuit_breaker(runtime_dir),
        "contradiction_log_open": 0,  # not yet tracked in ledger — defaults to 0
    }
