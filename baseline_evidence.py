from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict

from ledger import SQLiteLedger
from runtime_telemetry import RuntimeTelemetry


def build_baseline_evidence(
    runtime_dir: str | Path,
    *,
    strategy_family: str = "toxicity_mm",
    event_limit: int = 5000,
    sample_limit: int = 5000,
) -> Dict[str, Any]:
    telemetry = RuntimeTelemetry(runtime_dir)
    status = telemetry.read_status()
    current_run_id = telemetry.current_run_id()
    metrics = telemetry.read_strategy_metrics()
    family_metrics = dict(metrics.get(strategy_family, {}) or {})
    events = telemetry.read_events(limit=event_limit, run_id=current_run_id)
    samples = telemetry.read_market_samples(limit=sample_limit, run_id=current_run_id)

    ledger_path = Path(runtime_dir) / "ledger.db"
    ledger_events = []
    current_run_ledger_events = []
    distinct_run_ids: list[str] = []
    if ledger_path.exists():
        ledger = SQLiteLedger(ledger_path)
        ledger_events = ledger.list_events()
        if current_run_id:
            current_run_ledger_events = ledger.list_events(run_id=current_run_id)
        distinct_run_ids = sorted({event.run_id for event in ledger_events})

    fill_events = [event for event in events if event.get("event_type") == "order.filled"]
    quote_skip_events = [event for event in events if event.get("event_type") == "quote.skipped"]
    market_skip_events = [event for event in events if event.get("event_type") == "market.skipped_toxic_book"]

    quote_skip_reasons = Counter()
    for event in quote_skip_events:
        for reason in event.get("payload", {}).get("reasons", []):
            quote_skip_reasons[str(reason)] += 1

    market_skip_reasons = Counter()
    for event in market_skip_events:
        for reason in event.get("payload", {}).get("reasons", []):
            market_skip_reasons[str(reason)] += 1

    sample_reasons = Counter()
    for sample in samples:
        for reason in sample.get("book_reasons", []):
            sample_reasons[str(reason)] += 1

    slot_settled_current = [event for event in current_run_ledger_events if event.event_type == "slot_settled"]
    slot_settled_all = [event for event in ledger_events if event.event_type == "slot_settled"]
    risk_snapshots_current = [event for event in current_run_ledger_events if event.event_type == "risk_snapshot_recorded"]

    risk = status.get("risk", {}) or {}
    governance = {
        "baseline_strategy": status.get("baseline_strategy"),
        "research_candidates": status.get("research_candidates", []) or [],
    }

    return {
        "runtime_dir": str(Path(runtime_dir)),
        "current_run_id": current_run_id,
        "strategy_family": strategy_family,
        "governance": governance,
        "current_run": {
            "phase": status.get("phase"),
            "mode": status.get("mode"),
            "loop_count": int(status.get("loop_count", 0) or 0),
            "fetched_markets": int(status.get("fetched_markets", 0) or 0),
            "processed_markets": int(status.get("processed_markets", 0) or 0),
            "fill_event_count": len(fill_events),
            "slot_settled_count": len(slot_settled_current),
            "risk_snapshot_count": len(risk_snapshots_current),
            "open_position_count": int(status.get("open_position_count", 0) or 0),
            "pending_resolution_count": len(status.get("pending_resolution_slots", []) or []),
            "resolved_trade_count": int(status.get("resolved_trade_count", 0) or 0),
            "win_rate": float(status.get("win_rate", 0.0) or 0.0),
            "latest_settlement": status.get("latest_settlement"),
        },
        "strategy_metrics": family_metrics,
        "risk": {
            "capital": float(risk.get("capital", 0.0) or 0.0),
            "realized_pnl_total": float(risk.get("realized_pnl_total", 0.0) or 0.0),
            "unrealized_pnl_total": float(risk.get("unrealized_pnl_total", 0.0) or 0.0),
            "mark_to_market_capital": float(risk.get("mark_to_market_capital", 0.0) or 0.0),
            "max_drawdown": float(risk.get("max_drawdown", 0.0) or 0.0),
            "marked_position_count": int(risk.get("marked_position_count", 0) or 0),
            "unmarked_position_count": int(risk.get("unmarked_position_count", 0) or 0),
            "open_order_count": int(risk.get("open_order_count", 0) or 0),
            "total_gross_exposure": float(risk.get("total_gross_exposure", 0.0) or 0.0),
            "exposure_by_asset": risk.get("exposure_by_asset", {}) or {},
            "exposure_by_interval": risk.get("exposure_by_interval", {}) or {},
        },
        "skip_analysis": {
            "sample_skip_reasons": dict(sample_reasons.most_common(10)),
            "quote_skip_reasons": dict(quote_skip_reasons.most_common(10)),
            "market_skip_reasons": dict(market_skip_reasons.most_common(10)),
        },
        "restart_continuity": {
            "distinct_run_ids_in_ledger": distinct_run_ids,
            "observed_restart_count": max(0, len(distinct_run_ids) - 1),
            "ledger_event_count_current_run": len(current_run_ledger_events),
            "ledger_event_count_all_runs": len(ledger_events),
            "all_runs_slot_settled_count": len(slot_settled_all),
        },
    }


def render_baseline_evidence_text(payload: Dict[str, Any]) -> str:
    current = payload["current_run"]
    risk = payload["risk"]
    governance = payload["governance"]
    strategy_metrics = payload["strategy_metrics"]
    restart = payload["restart_continuity"]
    skips = payload["skip_analysis"]

    lines = [
        f"Runtime dir: {payload['runtime_dir']}",
        f"Run id: {payload.get('current_run_id') or 'unknown'}",
        f"Strategy family: {payload['strategy_family']}",
        f"Baseline strategy: {governance.get('baseline_strategy') or 'unset'}",
        f"Research candidates: {', '.join(governance.get('research_candidates', [])) or 'none'}",
        f"Phase: {current['phase']} | Mode: {current['mode']} | Loop count: {current['loop_count']}",
        f"Markets: fetched={current['fetched_markets']} processed={current['processed_markets']}",
        f"Evidence: fills={current['fill_event_count']} settled_slots={current['slot_settled_count']} resolved={current['resolved_trade_count']} risk_snapshots={current['risk_snapshot_count']}",
        f"PnL: realized={risk['realized_pnl_total']:.4f} unrealized={risk['unrealized_pnl_total']:.4f} capital={risk['capital']:.4f} mtm_capital={risk['mark_to_market_capital']:.4f}",
        f"Risk: drawdown={risk['max_drawdown']:.2%} open_orders={risk['open_order_count']} gross_exposure={risk['total_gross_exposure']:.4f} marked={risk['marked_position_count']} unmarked={risk['unmarked_position_count']}",
        f"Strategy metrics: quotes={strategy_metrics.get('quotes_submitted', 0)} resting={strategy_metrics.get('orders_resting', 0)} filled={strategy_metrics.get('orders_filled', 0)} cancels={strategy_metrics.get('cancellations', 0)} seen={strategy_metrics.get('markets_seen', 0)} toxic_skips={strategy_metrics.get('toxic_book_skips', 0)} pnl={float(strategy_metrics.get('realized_pnl', 0.0)):.4f}",
        f"Restarts observed in ledger: {restart['observed_restart_count']} across {len(restart['distinct_run_ids_in_ledger'])} runs",
        f"Ledger continuity: current_run_events={restart['ledger_event_count_current_run']} all_run_events={restart['ledger_event_count_all_runs']} all_settled_slots={restart['all_runs_slot_settled_count']}",
        f"Top sample skip reasons: {skips['sample_skip_reasons'] or '{}'}",
        f"Top quote skip reasons: {skips['quote_skip_reasons'] or '{}'}",
        f"Top market skip reasons: {skips['market_skip_reasons'] or '{}'}",
    ]

    if current.get("latest_settlement"):
        lines.append(f"Latest settlement: {current['latest_settlement']}")
    return "\n".join(lines)
