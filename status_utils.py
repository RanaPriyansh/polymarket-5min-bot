from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from runtime_telemetry import RuntimeTelemetry


def runtime_status_payload(runtime_dir: str | Path) -> dict[str, Any]:
    telemetry = RuntimeTelemetry(runtime_dir)
    status = telemetry.read_status()
    strategy_metrics = telemetry.read_strategy_metrics()
    heartbeat_ts = float(status.get("heartbeat_ts", 0.0) or 0.0)
    now_ts = time.time()
    heartbeat_age = max(0.0, now_ts - heartbeat_ts) if heartbeat_ts else None
    return {
        "runtime_dir": str(Path(runtime_dir)),
        "status": status,
        "strategy_metrics": strategy_metrics,
        "heartbeat_age_seconds": heartbeat_age,
        "recent_events": telemetry.read_events(limit=10),
    }


def runtime_health_payload(runtime_dir: str | Path, max_heartbeat_age: int = 180) -> dict[str, Any]:
    payload = runtime_status_payload(runtime_dir)
    heartbeat_age = payload.get("heartbeat_age_seconds")
    healthy = heartbeat_age is not None and heartbeat_age <= max_heartbeat_age
    return {
        **payload,
        "healthy": healthy,
        "max_heartbeat_age_seconds": max_heartbeat_age,
    }


def render_status_text(runtime_dir: str | Path) -> str:
    payload = runtime_status_payload(runtime_dir)
    status = payload["status"]
    risk = status.get("risk", {}) or {}
    strategy_metrics = payload["strategy_metrics"] or {}
    heartbeat_age = payload.get("heartbeat_age_seconds")
    heartbeat_text = "n/a" if heartbeat_age is None else f"{heartbeat_age:.1f}s"

    baseline = status.get("baseline_strategy")
    research_candidates = status.get("research_candidates", []) or []

    lines = [
        f"Runtime dir: {payload['runtime_dir']}",
        f"Run id: {status.get('run_id', 'unknown')}",
        f"Phase: {status.get('phase', 'unknown')} | Mode: {status.get('mode', 'unknown')} | Loop: {status.get('loop_count', 0)}",
        f"Heartbeat age: {heartbeat_text}",
        f"Markets: fetched={status.get('fetched_markets', 0)} processed={status.get('processed_markets', 0)} toxic_skips={status.get('toxic_skips', 0)}",
        f"Capital: ${float(risk.get('capital', status.get('bankroll', 0.0))):.2f} | Realized: {float(risk.get('realized_pnl_total', 0.0)):.4f} | Unrealized: {float(risk.get('unrealized_pnl_total', 0.0)):.4f} | Drawdown: {float(risk.get('max_drawdown', 0.0)):.2%}",
        f"Open positions: {status.get('open_position_count', 0)} | Open orders: {int(risk.get('open_order_count', status.get('open_order_count', 0)))} | Pending settlements: {len(status.get('pending_resolution_slots', []))}",
        f"Marks: marked={int(risk.get('marked_position_count', 0))} unmarked={int(risk.get('unmarked_position_count', 0))}",
        f"Exposure: gross={float(risk.get('total_gross_exposure', 0.0)):.4f} position={float(risk.get('gross_position_exposure', 0.0)):.4f} open_orders={float(risk.get('gross_open_order_exposure', 0.0)):.4f}",
        f"Resolved: {status.get('resolved_trade_count', 0)} | Win rate: {float(status.get('win_rate', 0.0)):.2%}",
    ]

    if baseline:
        lines.append(f"Baseline strategy: {baseline}")
    if research_candidates:
        lines.append(f"Research candidates: {', '.join(str(item) for item in research_candidates)}")

    if strategy_metrics:
        lines.append("Strategy metrics:")
        for family, metrics in sorted(strategy_metrics.items()):
            lines.append(
                "  - {family}: quotes={quotes} resting={resting} filled={filled} cancels={cancels} pnl={pnl:.4f} seen={seen} toxic_skips={toxic}".format(
                    family=family,
                    quotes=metrics.get("quotes_submitted", 0),
                    resting=metrics.get("orders_resting", 0),
                    filled=metrics.get("orders_filled", 0),
                    cancels=metrics.get("cancellations", 0),
                    pnl=float(metrics.get("realized_pnl", 0.0)),
                    seen=metrics.get("markets_seen", 0),
                    toxic=metrics.get("toxic_book_skips", 0),
                )
            )

    latest_settlement = status.get("latest_settlement")
    if latest_settlement:
        lines.append(f"Latest settlement: {latest_settlement}")

    return "\n".join(lines)
