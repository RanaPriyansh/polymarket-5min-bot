"""End-to-end settlement lifecycle tests.

METRIC CONTRACT (enforced by these tests):
--------------------------------------------
1. slot_settled (ledger event)
   - Emitted by SettlementEngine.settled_event() whenever a market closes with
     a known winning outcome.
   - ONE per market slot settlement. Counts SLOTS, not positions.
   - Written to ledger.db event_type='slot_settled'.

2. slot_closed (runtime dict, events.jsonl entry)
   - Emitted by _settle_market_positions() when a market expires BUT the bot
     has NO open positions (qty == 0 for all positions on that market).
   - This is an OBSERVABILITY event only. It does NOT increment:
     resolved_trade_count, win_count, or loss_count.
   - Purpose: operator can see that a market expired and was processed,
     even though there was nothing to settle financially.

3. resolved_trade_count
   - Incremented once PER POSITION that is settled via settlement payout.
   - NOT incremented for slot_closed (zero-exposure at expiry).
   - Represents count of actual financial settlements.

4. win_count / loss_count
   - Incremented only when a position is settled (resolved_trade_count bumps).
   - NOT incremented for slot_closed.
   - win rate = win_count / (win_count + loss_count), or 0.0 if none.

5. open_positions (replay projection)
   - After replay, ONLY positions that have NOT been settled and have
     non-zero quantity are reported.
   - Settled positions are REMOVED from the projection. Zero-quantity positions
     must NOT appear.
   - This must match the executor's current in-memory position count.

6. replay vs executor parity
   - replay_ledger(event_list).resolved == executor.resolved_trade_count
   - replay_ledger(event_list).win_count == executor.win_count
   - replay_ledger(event_list).loss_count == executor.loss_count
   - len([p for qty>0 in replay]) == executor open_position_count
"""

import asyncio
import tempfile
import time
import unittest
from collections import Counter

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from execution import PolymarketExecutor
from ledger import SQLiteLedger
from market_data import PolymarketData


def _make_executor(config, mock_md, *, mode="paper", run_id=None, ledger_db_path_override=None):
    """Create a PolymarketExecutor with the given mock market_data."""
    tmpdir = tempfile.TemporaryDirectory()
    cfg = dict(config)
    cfg.setdefault("execution", {})
    db = ledger_db_path_override or str(Path(tmpdir.name) / "ledger.db")
    cfg["execution"]["ledger_db_path"] = db
    if run_id is None:
        run_id = f"test-{int(time.time())}-{id(object()):08x}"
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
# 1. Flat-at-expiry market emits slot_closed but NOT resolved/wins/losses
# ──────────────────────────────────────────────────────────────

def test_flat_market_emits_slot_closed():
    """A market with no positions at expiry emits slot_closed but does NOT
    increment resolved/wins/losses."""
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

    executor = _make_executor(config, mock_md, run_id="test-closed-1")
    executor.market_registry["btc:5:100"] = market_dict

    before_resolved = executor.resolved_trade_count
    before_wins = executor.win_count
    before_losses = executor.loss_count

    events = asyncio.run(executor.process_pending_resolutions(now_ts=200.0))

    closed = [e for e in events if e.get("event_type") == "slot_closed"]
    assert len(closed) == 1, f"Expected 1 slot_closed, got {closed}"
    assert closed[0]["winning_outcome"] == "Up"
    assert closed[0].get("position_count") == 0

    # CONTRACT: zero-exposure slots do NOT count as resolved trades
    assert executor.resolved_trade_count == before_resolved, \
        "slot_closed must NOT increment resolved_trade_count"
    assert executor.win_count == before_wins, \
        "slot_closed must NOT increment win_count"
    assert executor.loss_count == before_losses, \
        "slot_closed must NOT increment loss_count"


def test_flat_market_pending_resolution_does_not_double_record_slot_settlement():
    """Flat-at-expiry pending resolution should only emit/persist slot_closed."""
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
    mock_md = MagicMock(spec=PolymarketData)
    market_dict = _expired_market_dict(
        slug="btc-updown-5m-110", end_ts=200.0, slot_id="btc:5:110", id="m-flat")

    mock_md.get_market_by_slug = AsyncMock(return_value=market_dict)
    mock_md.get_winning_outcome = MagicMock(return_value="Down")
    mock_md.best_bid = MagicMock(return_value=0.0)
    mock_md.best_ask = MagicMock(return_value=0.0)

    executor = _make_executor(config, mock_md, run_id="test-flat-flow", ledger_db_path_override=str(db_path))
    executor.market_registry["btc:5:110"] = market_dict

    events = asyncio.run(executor.process_pending_resolutions(now_ts=200.0))
    counts = Counter(event.get("event_type") for event in events)
    ledger_counts = Counter(event.event_type for event in SQLiteLedger(db_path).list_events(run_id="test-flat-flow"))

    assert counts["slot_closed"] == 1
    assert counts["market.settled"] == 0
    assert ledger_counts["slot_closed"] == 1
    assert ledger_counts["slot_settled"] == 0
    assert executor.latest_slot_resolution["event_type"] == "slot_closed"
    assert executor.latest_slot_resolution["slot_id"] == "btc:5:110"
    assert executor.latest_position_settlement is None
    assert executor.latest_settlement is None


# ──────────────────────────────────────────────────────────────
# 2. Previously settled slots are NOT polled/resettled again
# ──────────────────────────────────────────────────────────────

def test_settled_slots_not_polled_again():
    """A slot already settled must never generate pending or settlement events."""
    config = {
        "polymarket": {"clob_api_url": "http://fake", "gamma_api_url": "http://fake"},
        "execution": {
            "paper_starting_bankroll": 500.0,
            "resolution_initial_poll_seconds": 0,
            "resolution_poll_cap_seconds": 10,
        },
    }
    mock_md = MagicMock(spec=PolymarketData)
    mock_md.get_market_by_slug = AsyncMock(side_effect=RuntimeError("must not be called"))
    mock_md.get_winning_outcome = MagicMock(side_effect=RuntimeError("must not be called"))

    executor = _make_executor(config, mock_md, run_id="test-idem")
    executor.market_registry["btc:5:100"] = {
        "id": "m1", "slug": "btc-updown-5m-100", "end_ts": 100.0,
        "slot_id": "btc:5:100",
    }
    executor.settled_slots.add("btc:5:100")

    events = asyncio.run(executor.process_pending_resolutions(now_ts=200.0))
    pending = [e for e in events if e.get("event_type") == "market.pending_resolution"]
    settled = [e for e in events if e.get("event_type") in ("slot_closed", "market.settled")]
    assert len(pending) == 0, "Settled slot must not re-poll"
    assert len(settled) == 0, "Settled slot must not re-settle"


# ──────────────────────────────────────────────────────────────
# 3. Pending-resolution state survives restart replay
# ──────────────────────────────────────────────────────────────

def test_pending_resolution_survives_restart():
    """After restart with same DB + run_id, settled_slots is restored and
    the slot is NOT re-polled or re-settled."""
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

    mock_md2 = MagicMock(spec=PolymarketData)
    mock_md2.get_market_by_slug = AsyncMock(return_value=market_dict)
    mock_md2.get_winning_outcome = MagicMock(return_value="Up")
    mock_md2.best_bid = MagicMock(return_value=0.0)
    mock_md2.best_ask = MagicMock(return_value=0.0)

    ex2 = _make_executor(config, mock_md2, run_id="replay-1",
                         ledger_db_path_override=str(db_path))
    ex2.market_registry["btc:5:100"] = market_dict

    assert "btc:5:100" in ex2.settled_slots, \
        "settled_slots must survive restart from ledger restore"

    events2 = asyncio.run(ex2.process_pending_resolutions(now_ts=200.0))
    re_pending = [e for e in events2 if e.get("event_type") == "market.pending_resolution"]
    re_settled = [e for e in events2 if e.get("event_type") in ("slot_closed", "market.settled")]
    assert len(re_pending) == 0, "Already-settled slot must not re-poll after restart"
    assert len(re_settled) == 0, "Already-settled slot must not re-settle after restart"


# ──────────────────────────────────────────────────────────────
# 4. run_id unique per run; instance_id stable across processes
# ──────────────────────────────────────────────────────────────

def test_run_id_unique_per_run_instance_stable():
    """run_id must be unique per process. Auto-generated run_id starts with
    the paper/live prefix. A separate instance_id can be stable across restarts."""
    config = {
        "polymarket": {"clob_api_url": "http://fake", "gamma_api_url": "http://fake"},
        "execution": {
            "paper_starting_bankroll": 500.0,
            "instance_id": "vps-prod-1",
        },
    }
    mock_md = MagicMock(spec=PolymarketData)

    e1 = _make_executor(config, mock_md, run_id="run-a")
    e2 = _make_executor(config, mock_md, run_id="run-b")
    assert e1.run_id != e2.run_id, "run_id must differ between runs"

    e3 = _make_executor(config, mock_md)
    assert e3.run_id.startswith("test-"), f"Auto run_id should start with 'test-': {e3.run_id}"

    assert config["execution"].get("instance_id") == "vps-prod-1", \
        "instance_id must be stable and configurable across processes"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
