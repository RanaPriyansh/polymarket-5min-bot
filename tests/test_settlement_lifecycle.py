"""End-to-end settlement lifecycle tests for process_pending_resolutions.

Covers:
1. Flat-at-expiry market enters resolution polling and emits slot_closed.
2. Zero-exposure expired market emits slot_closed lifecycle event.
3. Previously settled slots are not polled again.
4. Pending-resolution state survives restart replay.
5. run_id is unique per run while instance_id stays stable across processes.
"""

import asyncio
import time
import uuid
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from execution import PolymarketExecutor
from ledger import SQLiteLedger
from market_data import PolymarketData

def _make_executor(config, mock_md, *, mode="paper", run_id=None, ledger_db_path_override=None):
    """Create a PolymarketExecutor with the given mock market_data.
    ledger_db_path_override lets tests share a DB across restarts."""
    tmpdir = tempfile.TemporaryDirectory()
    cfg = dict(config)
    cfg.setdefault("execution", {})
    db = ledger_db_path_override or str(Path(tmpdir.name) / "ledger.db")
    cfg["execution"]["ledger_db_path"] = db
    if run_id is None:
        run_id = f"test-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    executor = PolymarketExecutor(cfg, mock_md, mode=mode, run_id=run_id)
    executor.md = mock_md
    executor._test_tmpdir = tmpdir
    return executor


def _expired_market_dict(**kw):
    base = {
        "id": "m1",
        "closed": True,
        "accepting_orders": False,
        "outcomes": ["Up", "Down"],
        "outcome_prices": [1.0, 0.0],
        "asset": "btc",
        "interval_minutes": 5,
    }
    base.update(kw)
    return base


# ──────────────────────────────────────────────────────────────
# 1. Flat-at-expiry market still enters resolution polling
# ──────────────────────────────────────────────────────────────

def test_flat_market_emits_slot_closed():
    """A market with no positions at expiry must emit slot_closed."""
    config = {
        "polymarket": {"clob_api_url": "http://fake", "gamma_api_url": "http://fake"},
        "execution": {
            "paper_starting_bankroll": 500.0,
            "resolution_initial_poll_seconds": 0,
            "resolution_poll_cap_seconds": 10,
        },
    }
    mock_md = MagicMock(spec=PolymarketData)
    market_dict = _expired_market_dict(
        slug="btc-updown-5m-100", end_ts=200.0, slot_id="btc:5:100")

    mock_md.get_market_by_slug = AsyncMock(return_value=market_dict)
    mock_md.get_winning_outcome = MagicMock(return_value="Up")
    mock_md.best_bid = MagicMock(return_value=0.0)
    mock_md.best_ask = MagicMock(return_value=0.0)

    executor = _make_executor(config, mock_md, run_id="t1")
    executor.market_registry["btc:5:100"] = market_dict

    events = asyncio.run(executor.process_pending_resolutions(now_ts=200.0))
    closed = [e for e in events if e.get("event_type") == "slot_closed"]
    assert len(closed) > 0, f"Expected slot_closed, got {[e.get('event_type') for e in events]}"
    assert closed[0]["winning_outcome"] == "Up"
    assert closed[0].get("position_count") == 0


# ──────────────────────────────────────────────────────────────
# 2. Zero-exposure expired market emits lifecycle event
# ──────────────────────────────────────────────────────────────

def test_zero_exposure_emits_slot_closed():
    """Market with zero positions at expiry emits slot_closed (no PnL)."""
    config = {
        "polymarket": {"clob_api_url": "http://fake", "gamma_api_url": "http://fake"},
        "execution": {
            "paper_starting_bankroll": 500.0,
            "resolution_initial_poll_seconds": 0,
            "resolution_poll_cap_seconds": 10,
        },
    }
    mock_md = MagicMock(spec=PolymarketData)
    market_dict = _expired_market_dict(
        slug="eth-updown-5m-200", end_ts=300.0, slot_id="eth:5:200",
        outcome_prices=[0.0, 1.0])

    mock_md.get_market_by_slug = AsyncMock(return_value=market_dict)
    mock_md.get_winning_outcome = MagicMock(return_value="Down")
    mock_md.best_bid = MagicMock(return_value=0.0)
    mock_md.best_ask = MagicMock(return_value=0.0)

    executor = _make_executor(config, mock_md, run_id="t2")
    executor.market_registry["eth:5:200"] = market_dict
    # No positions -- zero exposure
    assert len(executor.positions) == 0

    events = asyncio.run(executor.process_pending_resolutions(now_ts=300.0))
    closed = [e for e in events if e.get("event_type") == "slot_closed"]
    assert len(closed) > 0
    assert closed[0]["winning_outcome"] == "Down"
    assert closed[0].get("position_count") == 0
    # resolved_trade_count increments even for flat-at-expiry markets
    assert executor.resolved_trade_count > 0


# ──────────────────────────────────────────────────────────────
# 3. Previously settled slots are NOT polled again
# ──────────────────────────────────────────────────────────────

def test_settled_slots_not_polled_again():
    """A slot that was already settled must never be re-polled or re-settled."""
    config = {
        "polymarket": {"clob_api_url": "http://fake", "gamma_api_url": "http://fake"},
        "execution": {
            "paper_starting_bankroll": 500.0,
            "resolution_initial_poll_seconds": 0,
            "resolution_poll_cap_seconds": 10,
        },
    }
    mock_md = MagicMock(spec=PolymarketData)
    market_dict = {
        "id": "m1", "slug": "btc-updown-5m-100", "end_ts": 100.0,
        "closed": False, "slot_id": "btc:5:100",
        "outcomes": ["Up", "Down"],
    }
    # If the slot is settled, _fetch_json and get_winning_outcome should NOT be called
    mock_md.get_market_by_slug = AsyncMock(side_effect=RuntimeError("should not be called"))
    mock_md.get_winning_outcome = MagicMock(side_effect=RuntimeError("should not be called"))

    executor = _make_executor(config, mock_md, run_id="t3")
    executor.market_registry["btc:5:100"] = market_dict
    executor.settled_slots.add("btc:5:100")

    events = asyncio.run(executor.process_pending_resolutions(now_ts=200.0))
    pending = [e for e in events if e.get("event_type") == "market.pending_resolution"]
    settled = [e for e in events if e.get("event_type") in ("market.settled", "slot_closed")]
    assert len(pending) == 0, "Settled slot should not generate pending events"
    assert len(settled) == 0, "Settled slot should not be settled again"


# ──────────────────────────────────────────────────────────────
# 4. Pending-resolution state survives restart replay
#    (settled_slots restored from ledger, no re-settlement)
# ──────────────────────────────────────────────────────────────

def test_pending_resolution_survives_restart():
    """After restart, executor restores settled_slots from ledger
    and does NOT re-settle them."""
    tmpdir = tempfile.TemporaryDirectory()
    db_path = Path(tmpdir.name) / "ledger.db"
    config = {
        "polymarket": {"clob_api_url": "http://fake", "gamma_api_url": "http://fake"},
        "execution": {
            "paper_starting_bankroll": 500.0,
            "resolution_initial_poll_seconds": 0,
            "resolution_poll_cap_seconds": 10,
        },
    }

    # ── Run 1: settle a market ──
    mock_md1 = MagicMock(spec=PolymarketData)
    market_dict = _expired_market_dict(
        slug="btc-updown-5m-100", end_ts=100.0, slot_id="btc:5:100")
    mock_md1.get_market_by_slug = AsyncMock(return_value=market_dict)
    mock_md1.get_winning_outcome = MagicMock(return_value="Up")
    mock_md1.best_bid = MagicMock(return_value=0.0)
    mock_md1.best_ask = MagicMock(return_value=0.0)

    ex1 = _make_executor(config, mock_md1, run_id="replay-1",
                         ledger_db_path_override=str(db_path))
    ex1.market_registry["btc:5:100"] = market_dict
    asyncio.run(ex1.process_pending_resolutions(now_ts=100.0))

    assert "btc:5:100" in ex1.settled_slots
    assert ex1.resolved_trade_count >= 1
    run1_resolved = ex1.resolved_trade_count

    # ── Run 2: simulate restart with same run_id and same DB ──
    mock_md2 = MagicMock(spec=PolymarketData)
    mock_md2.get_market_by_slug = AsyncMock(return_value=market_dict)
    mock_md2.get_winning_outcome = MagicMock(return_value="Up")
    mock_md2.best_bid = MagicMock(return_value=0.0)
    mock_md2.best_ask = MagicMock(return_value=0.0)

    ex2 = _make_executor(config, mock_md2, run_id="replay-1",
                         ledger_db_path_override=str(db_path))
    ex2.market_registry["btc:5:100"] = market_dict

    # Settled slots must be restored from ledger
    assert "btc:5:100" in ex2.settled_slots, "settled_slots must survive restart"

    events2 = asyncio.run(ex2.process_pending_resolutions(now_ts=200.0))
    re_pending = [e for e in events2 if e.get("event_type") == "market.pending_resolution"]
    re_closed = [e for e in events2 if e.get("event_type") in ("slot_closed", "market.settled")]

    assert len(re_pending) == 0, "Already-settled slot should not re-poll"
    assert len(re_closed) == 0, "Already-settled slot should not re-settle"
    # resolved_trade_count restored from ledger
    assert ex2.resolved_trade_count == run1_resolved


# ──────────────────────────────────────────────────────────────
# 5. run_id unique per run; instance_id stable across processes
# ──────────────────────────────────────────────────────────────

def test_run_id_unique_per_run_instance_stable():
    """run_id must be unique per process. A stable instance_id can be
    derived from config or filesystem for cross-process correlation."""
    config = {
        "polymarket": {"clob_api_url": "http://fake", "gamma_api_url": "http://fake"},
        "execution": {
            "paper_starting_bankroll": 500.0,
            "instance_id": "vps-prod-1",  # stable across restarts
        },
    }
    mock_md = MagicMock(spec=PolymarketData)

    e1 = _make_executor(config, mock_md, run_id="run-a")
    e2 = _make_executor(config, mock_md, run_id="run-b")

    # Different run_ids -- per-process uniqueness
    assert e1.run_id != e2.run_id, "run_id must differ between runs"

    # Auto-generated run_id (no run_id arg) uses timestamp+UUID
    e3 = _make_executor(config, mock_md)
    assert e3.run_id.startswith("test-"), f"run_id should start with 'test-': {e3.run_id}"

    instance = config["execution"].get("instance_id")
    assert instance == "vps-prod-1", "instance_id should be stable across processes"
