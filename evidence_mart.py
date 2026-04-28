from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ledger import LedgerEvent, SQLiteLedger
from runtime_telemetry import RuntimeTelemetry


CANONICAL_EVIDENCE_FIELDS = [
    "experiment_id",
    "run_id",
    "strategy_family",
    "strategy_variant",
    "slot_id",
    "market_slug",
    "condition_id",
    "token_id",
    "outcome",
    "asset",
    "timeframe",
    "side",
    "order_type",
    "maker_or_taker",
    "order_id",
    "price",
    "size",
    "notional",
    "best_bid",
    "best_ask",
    "mid",
    "spread",
    "book_timestamp",
    "book_age_ms",
    "external_spot_source",
    "external_spot_price",
    "external_spot_timestamp",
    "external_spot_age_ms",
    "strike_or_open_price",
    "time_to_expiry_s",
    "model_fair",
    "model_edge",
    "entry_reason",
    "vetoes_triggered",
    "risk_budget_used",
    "inventory_before",
    "inventory_after",
    "paper_fill_reason",
    "paper_fill_confidence",
    "simulator_assumption",
    "fees_estimated",
    "reward_estimated",
    "settlement_result",
    "settled_pnl",
    "markout_5s",
    "markout_15s",
    "markout_60s",
    "markout_300s",
]


@dataclass(frozen=True)
class EvidenceGate:
    state: str
    reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {"state": self.state, "reasons": list(self.reasons)}


def blank_evidence_row() -> dict[str, Any]:
    return {field: None for field in CANONICAL_EVIDENCE_FIELDS}


def _slot_parts(slot_id: str | None) -> tuple[str | None, str | None]:
    if not slot_id:
        return None, None
    parts = str(slot_id).split(":")
    if len(parts) >= 2:
        return parts[0], f"{parts[1]}m"
    return None, None


def _jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _ledger_events(runtime_dir: Path, run_id: str | None = None) -> list[LedgerEvent]:
    db_path = runtime_dir / "ledger.db"
    if not db_path.exists():
        return []
    try:
        return SQLiteLedger(db_path).list_events(run_id=run_id)
    except sqlite3.DatabaseError:
        return []


def _event_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    return payload if isinstance(payload, dict) else {}


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def _base_from_payload(payload: dict[str, Any], *, run_id: str | None = None) -> dict[str, Any]:
    row = blank_evidence_row()
    slot_id = payload.get("slot_id")
    asset, timeframe = _slot_parts(slot_id)
    row.update({
        "experiment_id": payload.get("experiment_id"),
        "run_id": run_id or payload.get("run_id"),
        "strategy_family": payload.get("strategy_family"),
        "strategy_variant": payload.get("strategy_variant"),
        "slot_id": slot_id,
        "market_slug": payload.get("market_slug"),
        "condition_id": payload.get("condition_id"),
        "token_id": payload.get("token_id"),
        "outcome": payload.get("outcome"),
        "asset": payload.get("asset", asset),
        "timeframe": payload.get("timeframe", timeframe),
        "side": payload.get("side"),
        "order_type": payload.get("order_kind") or payload.get("order_type"),
        "maker_or_taker": payload.get("maker_or_taker"),
        "order_id": payload.get("order_id"),
        "price": payload.get("price") or payload.get("fill_price"),
        "size": payload.get("size") or payload.get("fill_size") or payload.get("quantity"),
        "best_bid": payload.get("best_bid"),
        "best_ask": payload.get("best_ask"),
        "book_timestamp": payload.get("book_timestamp") or payload.get("observed_ts"),
        "book_age_ms": payload.get("book_age_ms"),
        "external_spot_source": payload.get("external_spot_source"),
        "external_spot_price": payload.get("external_spot_price"),
        "external_spot_timestamp": payload.get("external_spot_timestamp"),
        "external_spot_age_ms": payload.get("external_spot_age_ms") or payload.get("spot_age_ms"),
        "strike_or_open_price": payload.get("strike_or_open_price"),
        "time_to_expiry_s": payload.get("time_to_expiry_s"),
        "model_fair": payload.get("model_fair"),
        "model_edge": payload.get("model_edge"),
        "entry_reason": payload.get("entry_reason") or payload.get("reason"),
        "vetoes_triggered": payload.get("vetoes_triggered"),
        "risk_budget_used": payload.get("risk_budget_used"),
        "inventory_before": payload.get("inventory_before"),
        "inventory_after": payload.get("inventory_after"),
        "paper_fill_reason": payload.get("paper_fill_reason") or payload.get("fill_reason"),
        "paper_fill_confidence": payload.get("paper_fill_confidence"),
        "simulator_assumption": payload.get("simulator_assumption"),
        "fees_estimated": payload.get("fees_estimated"),
        "reward_estimated": payload.get("reward_estimated"),
        "settlement_result": payload.get("settlement_result") or payload.get("winning_outcome"),
        "settled_pnl": _first_present(payload, "settled_pnl", "realized_pnl_delta", "realized_pnl"),
        "markout_5s": payload.get("markout_5s"),
        "markout_15s": payload.get("markout_15s"),
        "markout_60s": payload.get("markout_60s"),
        "markout_300s": payload.get("markout_300s"),
    })
    if row["best_bid"] is not None and row["best_ask"] is not None:
        best_bid = float(row["best_bid"])
        best_ask = float(row["best_ask"])
        row["mid"] = (best_bid + best_ask) / 2.0
        row["spread"] = max(0.0, best_ask - best_bid)
    if row["price"] is not None and row["size"] is not None:
        row["notional"] = float(row["price"]) * float(row["size"])
    return row


def build_evidence_rows(runtime_dir: str | Path, *, run_id: str | None = None) -> list[dict[str, Any]]:
    runtime_path = Path(runtime_dir)
    telemetry = RuntimeTelemetry(runtime_path)
    resolved_run_id = run_id or telemetry.current_run_id()
    rows: list[dict[str, Any]] = []
    orders: dict[str, dict[str, Any]] = {}

    for event in _ledger_events(runtime_path, run_id=resolved_run_id):
        payload = dict(event.payload)
        payload.setdefault("run_id", event.run_id)
        if event.event_type == "order_created":
            order_id = event.aggregate_id
            payload["order_id"] = order_id
            orders[order_id] = _base_from_payload(payload, run_id=event.run_id)
        elif event.event_type == "fill_applied":
            order_id = event.aggregate_id
            base = dict(orders.get(order_id, blank_evidence_row()))
            fill_row = _base_from_payload(payload, run_id=event.run_id)
            base.update({key: value for key, value in fill_row.items() if value is not None})
            base["order_id"] = order_id
            rows.append(base)
        elif event.event_type == "slot_settled":
            payload["slot_id"] = payload.get("slot_id") or event.aggregate_id
            rows.append(_base_from_payload(payload, run_id=event.run_id))

    for event_row in _jsonl_rows(runtime_path / "events.jsonl"):
        if resolved_run_id and event_row.get("run_id") != resolved_run_id:
            continue
        event_type = event_row.get("event_type")
        if event_type not in {"strategy.candidate", "scanner.opportunity", "order.filled", "market.settled"}:
            continue
        payload = _event_payload(event_row)
        payload.setdefault("run_id", event_row.get("run_id"))
        rows.append(_base_from_payload(payload, run_id=event_row.get("run_id")))

    return rows


def validate_evidence_rows(rows: Iterable[dict[str, Any]]) -> EvidenceGate:
    materialized = list(rows)
    reasons: list[str] = []
    for idx, row in enumerate(materialized):
        if row.get("paper_fill_reason") or row.get("settlement_result") or row.get("settled_pnl") is not None:
            if not row.get("strategy_family"):
                reasons.append(f"row_{idx}_missing_strategy_family")
            if not row.get("slot_id"):
                reasons.append(f"row_{idx}_missing_slot_id")
    if not materialized:
        reasons.append("no_evidence_rows")
    return EvidenceGate(state="GREEN" if not reasons else "RED", reasons=reasons)


def family_performance(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_family: dict[str, dict[str, Any]] = {}
    for row in rows:
        family = str(row.get("strategy_family") or "unknown")
        entry = by_family.setdefault(family, {"strategy_family": family, "rows": 0, "settled_count": 0, "settled_pnl": 0.0})
        entry["rows"] += 1
        if row.get("settled_pnl") is not None:
            entry["settled_count"] += 1
            entry["settled_pnl"] += float(row.get("settled_pnl") or 0.0)
    for entry in by_family.values():
        count = max(1, int(entry["settled_count"]))
        entry["avg_settled_pnl"] = entry["settled_pnl"] / count
    return sorted(by_family.values(), key=lambda item: item["settled_pnl"])


def tte_performance(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets = {
        "<=60s": lambda t: t <= 60,
        "60-120s": lambda t: 60 < t <= 120,
        "120-300s": lambda t: 120 < t <= 300,
        ">300s": lambda t: t > 300,
        "unknown": lambda t: False,
    }
    results = {name: {"bucket": name, "rows": 0, "settled_count": 0, "settled_pnl": 0.0} for name in buckets}
    for row in rows:
        tte = row.get("time_to_expiry_s")
        name = "unknown"
        if tte is not None:
            value = float(tte)
            for bucket_name, predicate in buckets.items():
                if bucket_name != "unknown" and predicate(value):
                    name = bucket_name
                    break
        entry = results[name]
        entry["rows"] += 1
        if row.get("settled_pnl") is not None:
            entry["settled_count"] += 1
            entry["settled_pnl"] += float(row.get("settled_pnl") or 0.0)
    return list(results.values())


def markout_vs_settlement(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    divergences = []
    for row in rows:
        markout = row.get("markout_60s")
        settled = row.get("settled_pnl")
        if markout is None or settled is None:
            continue
        if float(markout) > 0 and float(settled) < 0:
            divergences.append({
                "order_id": row.get("order_id"),
                "slot_id": row.get("slot_id"),
                "strategy_family": row.get("strategy_family"),
                "markout_60s": markout,
                "settled_pnl": settled,
            })
    return {"positive_60s_negative_settlement_count": len(divergences), "examples": divergences[:50]}


def render_evidence_markdown(payload: dict[str, Any]) -> str:
    gate = payload["gate"]
    family_rows = payload.get("family_performance", [])
    tte_rows = payload.get("tte_performance", [])
    lines = [
        "# Profitability Evidence Mart",
        "",
        f"Generated: {payload['generated_ts']:.0f}",
        f"Run scope: {payload.get('run_id') or 'all'}",
        f"Rows: {payload['row_count']}",
        f"Gate: {gate['state']}",
    ]
    if gate["reasons"]:
        lines.append("Gate reasons: " + ", ".join(gate["reasons"]))
    lines.extend(["", "## Family Performance"])
    for row in family_rows:
        lines.append(
            f"- {row['strategy_family']}: settled={row['settled_count']} "
            f"pnl={row['settled_pnl']:.6f} avg={row['avg_settled_pnl']:.6f}"
        )
    lines.extend(["", "## TTE Performance"])
    for row in tte_rows:
        lines.append(f"- {row['bucket']}: rows={row['rows']} settled={row['settled_count']} pnl={row['settled_pnl']:.6f}")
    markout = payload.get("markout_vs_settlement", {})
    lines.extend([
        "",
        "## Markout Vs Settlement",
        f"Positive 60s markout but negative settlement: {markout.get('positive_60s_negative_settlement_count', 0)}",
    ])
    return "\n".join(lines) + "\n"


def build_evidence_mart(
    runtime_dir: str | Path,
    *,
    artifact_dir: str | Path = "data/research/evidence_mart",
    run_id: str | None = None,
) -> dict[str, Any]:
    rows = build_evidence_rows(runtime_dir, run_id=run_id)
    gate = validate_evidence_rows(rows)
    payload = {
        "generated_ts": time.time(),
        "run_id": run_id or RuntimeTelemetry(runtime_dir).current_run_id(),
        "row_count": len(rows),
        "canonical_fields": CANONICAL_EVIDENCE_FIELDS,
        "gate": gate.to_dict(),
        "rows": rows,
        "family_performance": family_performance(rows),
        "tte_performance": tte_performance(rows),
        "markout_vs_settlement": markout_vs_settlement(rows),
    }
    out_dir = Path(artifact_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "latest.json").write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    (out_dir / "latest.md").write_text(render_evidence_markdown(payload), encoding="utf-8")
    return payload
