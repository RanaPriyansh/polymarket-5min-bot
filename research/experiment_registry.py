from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

PROMOTE_IF = {"settled_trades_>=": 30, "win_rate_>=": 0.45, "pnl_per_trade_>=": 0.05}
KILL_IF = {"settled_trades_>=": 20, "pnl_per_trade_<": -0.20}


@dataclass
class FamilyScore:
    family: str
    settled_trades: int
    realized_pnl: float
    pnl_per_trade: float
    win_rate: float
    fragmentation: int
    promotion_state: str
    last_evidence_ts: float
    promote_if: dict[str, Any] = field(default_factory=lambda: dict(PROMOTE_IF))
    kill_if: dict[str, Any] = field(default_factory=lambda: dict(KILL_IF))

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["realized_pnl"] = round(float(self.realized_pnl), 6)
        payload["pnl_per_trade"] = round(float(self.pnl_per_trade), 6)
        payload["win_rate"] = round(float(self.win_rate), 6)
        return payload


def decide_promotion_state(
    *,
    settled_trades: int,
    win_rate: float,
    pnl_per_trade: float,
    configured_state: str,
) -> str:
    if settled_trades >= KILL_IF["settled_trades_>="] and pnl_per_trade < KILL_IF["pnl_per_trade_<"]:
        return "demoted"
    if (
        settled_trades >= PROMOTE_IF["settled_trades_>="]
        and win_rate >= PROMOTE_IF["win_rate_>="]
        and pnl_per_trade >= PROMOTE_IF["pnl_per_trade_>="]
    ):
        return "active"
    return configured_state or "candidate"


def _event_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def _event_family(event: dict[str, Any]) -> str | None:
    payload = _event_payload(event)
    family = payload.get("strategy_family") or payload.get("family")
    if family:
        return str(family)
    return None


def _event_realized_pnl(event: dict[str, Any]) -> float | None:
    payload = _event_payload(event)
    for key in ("realized_pnl", "realized_pnl_delta", "pnl"):
        if payload.get(key) is not None:
            try:
                return float(payload[key])
            except (TypeError, ValueError):
                return None
    return None


def _event_is_win(event: dict[str, Any], pnl: float) -> bool:
    payload = _event_payload(event)
    if payload.get("is_win") is not None:
        return bool(payload.get("is_win"))
    return pnl > 0


def build_family_scoreboard(
    events: Iterable[dict[str, Any]],
    *,
    active_families: Iterable[str] = (),
    candidate_families: Iterable[str] = (),
    run_fragmentation: int = 1,
) -> list[FamilyScore]:
    configured: dict[str, str] = {}
    for family in active_families:
        configured[str(family)] = "active"
    for family in candidate_families:
        configured.setdefault(str(family), "candidate")

    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"settled": 0, "pnl": 0.0, "wins": 0, "last_ts": 0.0})
    for event in events:
        if event.get("event_type") != "slot_settled":
            continue
        family = _event_family(event)
        if not family:
            continue
        pnl = _event_realized_pnl(event)
        if pnl is None:
            continue
        row = stats[family]
        row["settled"] += 1
        row["pnl"] += pnl
        row["wins"] += 1 if _event_is_win(event, pnl) else 0
        try:
            row["last_ts"] = max(float(row["last_ts"]), float(event.get("ts") or event.get("event_ts") or 0.0))
        except (TypeError, ValueError):
            pass
        configured.setdefault(family, "candidate")

    rows: list[FamilyScore] = []
    for family in sorted(configured):
        row = stats[family]
        settled = int(row["settled"])
        realized_pnl = float(row["pnl"])
        pnl_per_trade = realized_pnl / settled if settled else 0.0
        win_rate = float(row["wins"]) / settled if settled else 0.0
        configured_state = configured.get(family, "candidate")
        state = decide_promotion_state(
            settled_trades=settled,
            win_rate=win_rate,
            pnl_per_trade=pnl_per_trade,
            configured_state=configured_state,
        )
        rows.append(
            FamilyScore(
                family=family,
                settled_trades=settled,
                realized_pnl=realized_pnl,
                pnl_per_trade=pnl_per_trade,
                win_rate=win_rate,
                fragmentation=int(run_fragmentation or 1),
                promotion_state=state,
                last_evidence_ts=float(row["last_ts"] or 0.0),
            )
        )
    return rows


def write_family_scoreboard(artifact_dir: str | Path, rows: Iterable[FamilyScore]) -> Path:
    path = Path(artifact_dir) / "family_scoreboard.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps([row.to_dict() for row in rows], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def verdict_from_scoreboard(rows: Iterable[FamilyScore]) -> str:
    actions = []
    for row in rows:
        if row.promotion_state == "active":
            actions.append(f"KEEP {row.family}" if row.settled_trades else f"WATCH {row.family}")
        elif row.promotion_state == "demoted":
            actions.append(f"KILL {row.family}")
        elif row.settled_trades >= PROMOTE_IF["settled_trades_>="]:
            actions.append(f"REVIEW {row.family}")
        else:
            actions.append(f"WATCH {row.family}")
    return " | ".join(actions) if actions else "WATCH no-family-evidence"
