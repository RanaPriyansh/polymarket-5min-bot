from __future__ import annotations

import json
import shutil
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from research.loop import read_jsonl_tail


_STRUCTURAL_EVENT_TYPES = {"market.runtime_baseline_untradeable"}
_GOVERNANCE_EVENT_TYPES = {"market.entry_blocked"}
_QUOTED_OR_ENTERED_EVENT_TYPES = {"quote.submitted", "order.opened", "order.filled", "signal.executed"}
_QUOTE_SKIP_EVENT_TYPES = {"quote.skipped"}
_DISCOVERY_ONLY_EVENT_TYPES = {"market.discovered", "market.fetch_error"}
_BOUNDED_RECENT_RUN_SCOPE = "bounded_recent_run"
_DISCOVERED_MARKET_EVENT_TYPES = (
    _STRUCTURAL_EVENT_TYPES
    | _GOVERNANCE_EVENT_TYPES
    | _QUOTED_OR_ENTERED_EVENT_TYPES
    | _QUOTE_SKIP_EVENT_TYPES
    | _DISCOVERY_ONLY_EVENT_TYPES
)


class RuntimeTelemetry:
    def __init__(self, runtime_dir: str | Path):
        self.runtime_dir = Path(runtime_dir)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.runtime_dir / "status.json"
        self.events_path = self.runtime_dir / "events.jsonl"
        self.strategy_metrics_path = self.runtime_dir / "strategy_metrics.json"
        self.market_samples_path = self.runtime_dir / "market_samples.jsonl"
        self.latest_status_text_path = self.runtime_dir / "latest-status.txt"

    @staticmethod
    def make_run_id(prefix: str = "paper") -> str:
        return f"{prefix}-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    def append_event(self, event_type: str, payload: Dict, *, run_id: str | None = None) -> None:
        resolved_run_id = run_id or payload.get("run_id") or self.current_run_id()
        event = {
            "ts": time.time(),
            "event_type": event_type,
            "run_id": resolved_run_id,
            "payload": payload,
        }
        with self.events_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, sort_keys=True, default=str) + "\n")
        self._invalidate_market_eligibility_snapshot()

    def append_market_sample(self, payload: Dict, *, run_id: str | None = None) -> None:
        resolved_run_id = run_id or payload.get("run_id") or self.current_run_id()
        sample = {
            "ts": time.time(),
            "run_id": resolved_run_id,
            **payload,
        }
        with self.market_samples_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(sample, sort_keys=True, default=str) + "\n")
        self._invalidate_market_eligibility_snapshot()

    def _invalidate_market_eligibility_snapshot(self) -> None:
        current = self.read_json(self.status_path)
        if not isinstance(current, dict) or "market_eligibility" not in current:
            return
        current.pop("market_eligibility", None)
        current["market_eligibility_stale"] = True
        self.status_path.write_text(json.dumps(current, indent=2, sort_keys=True, default=str), encoding="utf-8")
        self.latest_status_text_path.write_text(self._render_latest_status_text(current), encoding="utf-8")

    def update_status(self, **fields) -> Dict:
        current = self.read_json(self.status_path) or {}
        current.update(fields)
        if not isinstance(fields.get("market_eligibility"), dict):
            resolved_run_id = str(current.get("run_id")) if current.get("run_id") else None
            discovered_markets = current.get("fetched_markets")
            current["market_eligibility"] = self.summarize_market_eligibility(
                run_id=resolved_run_id,
                discovered_markets=int(discovered_markets) if discovered_markets is not None else None,
            )
        current.pop("market_eligibility_stale", None)
        current["heartbeat_ts"] = time.time()
        self.status_path.write_text(json.dumps(current, indent=2, sort_keys=True, default=str), encoding="utf-8")
        self.latest_status_text_path.write_text(self._render_latest_status_text(current), encoding="utf-8")
        return current

    def write_strategy_metrics(self, metrics: Dict) -> None:
        self.strategy_metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True, default=str), encoding="utf-8")

    def write_runtime_snapshot(self, **snapshot) -> Dict:
        return self.update_status(**snapshot)

    def read_json(self, path: Path) -> Optional[Dict]:
        if not path.exists():
            return None
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return None
        if not raw.strip():
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def read_status(self) -> Dict:
        return self.read_json(self.status_path) or {}

    def current_run_id(self) -> str | None:
        status = self.read_status()
        run_id = status.get("run_id")
        return str(run_id) if run_id else None

    def read_strategy_metrics(self) -> Dict:
        return self.read_json(self.strategy_metrics_path) or {}

    @staticmethod
    def _row_run_id(row: Dict[str, Any]) -> str | None:
        run_id = row.get("run_id")
        if run_id:
            return str(run_id)
        payload = row.get("payload") or {}
        if isinstance(payload, dict) and payload.get("run_id"):
            return str(payload.get("run_id"))
        return None

    def read_jsonl(self, path: Path, limit: Optional[int] = None, run_id: str | None = None) -> Iterable[Dict]:
        row_filter = None if run_id is None else lambda row: self._row_run_id(row) == run_id
        return list(read_jsonl_tail(path, limit=limit, row_filter=row_filter))

    def read_events(self, limit: Optional[int] = None, run_id: str | None = None):
        return list(self.read_jsonl(self.events_path, limit=limit, run_id=run_id))

    def read_market_samples(self, limit: Optional[int] = None, run_id: str | None = None):
        return list(self.read_jsonl(self.market_samples_path, limit=limit, run_id=run_id))

    @staticmethod
    def _market_key(payload: Dict[str, Any]) -> str | None:
        market_id = payload.get("market_id")
        if market_id:
            return str(market_id)
        market_slug = payload.get("market_slug")
        if market_slug:
            return f"slug:{market_slug}"
        slot_id = payload.get("slot_id")
        if slot_id:
            return f"slot:{slot_id}"
        return None

    @staticmethod
    def _market_alias_tokens(payload: Dict[str, Any]) -> list[str]:
        tokens: list[str] = []
        market_id = payload.get("market_id")
        market_slug = payload.get("market_slug")
        slot_id = payload.get("slot_id")
        if market_id:
            tokens.append(f"id:{market_id}")
        if market_slug:
            tokens.append(f"slug:{market_slug}")
        if slot_id:
            tokens.append(f"slot:{slot_id}")
        return tokens

    @classmethod
    def _market_alias_map(cls, payloads: Iterable[Dict[str, Any]]) -> Dict[str, str]:
        parent: Dict[str, str] = {}

        def find(token: str) -> str:
            root = parent.setdefault(token, token)
            while parent[root] != root:
                parent[root] = parent[parent[root]]
                root = parent[root]
            while token != root:
                next_token = parent[token]
                parent[token] = root
                token = next_token
            return root

        def union(left: str, right: str) -> None:
            left_root = find(left)
            right_root = find(right)
            if left_root != right_root:
                parent[right_root] = left_root

        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            tokens = cls._market_alias_tokens(payload)
            for token in tokens:
                parent.setdefault(token, token)
            if len(tokens) < 2:
                continue
            first = tokens[0]
            for token in tokens[1:]:
                union(first, token)

        components: Dict[str, list[str]] = {}
        for token in list(parent):
            components.setdefault(find(token), []).append(token)

        alias_map: Dict[str, str] = {}

        def preference(token: str) -> tuple[int, str]:
            if token.startswith("id:"):
                return (0, token)
            if token.startswith("slug:"):
                return (1, token)
            return (2, token)

        for tokens in components.values():
            canonical = min(tokens, key=preference)
            for token in tokens:
                alias_map[token] = canonical
        return alias_map

    @classmethod
    def _canonical_market_key(cls, payload: Dict[str, Any], alias_map: Dict[str, str] | None = None) -> str | None:
        tokens = cls._market_alias_tokens(payload)
        if not tokens:
            return None
        if alias_map:
            for token in tokens:
                canonical = alias_map.get(token)
                if canonical:
                    return canonical
        return tokens[0]

    @staticmethod
    def _reason_counter(events: Iterable[Dict[str, Any]]) -> Counter[str]:
        counter: Counter[str] = Counter()
        for event in events:
            payload = event.get("payload") or {}
            if not isinstance(payload, dict):
                continue
            reasons = payload.get("reasons") or []
            if isinstance(reasons, str):
                reasons = [reasons]
            for reason in reasons:
                normalized = str(reason).strip()
                if normalized:
                    counter[normalized] += 1
        return counter

    @classmethod
    def _market_keys_from_rows(cls, rows: Iterable[Dict[str, Any]]) -> set[str]:
        return {
            key
            for row in rows
            for key in [cls._canonical_market_key((row.get("payload") or {}))]
            if key is not None
        }

    @classmethod
    def _market_keys_from_samples(cls, rows: Iterable[Dict[str, Any]]) -> set[str]:
        return {
            key
            for row in rows
            for key in [cls._canonical_market_key(row if isinstance(row, dict) else {})]
            if key is not None
        }

    @classmethod
    def _market_keys_from_rows_with_aliases(
        cls,
        rows: Iterable[Dict[str, Any]],
        alias_map: Dict[str, str],
    ) -> set[str]:
        return {
            key
            for row in rows
            for key in [cls._canonical_market_key((row.get("payload") or {}), alias_map)]
            if key is not None
        }

    @classmethod
    def _market_keys_from_samples_with_aliases(
        cls,
        rows: Iterable[Dict[str, Any]],
        alias_map: Dict[str, str],
    ) -> set[str]:
        return {
            key
            for row in rows
            for key in [cls._canonical_market_key(row if isinstance(row, dict) else {}, alias_map)]
            if key is not None
        }

    @classmethod
    def summarize_market_eligibility_events(
        cls,
        events: Iterable[Dict[str, Any]],
        *,
        market_samples: Iterable[Dict[str, Any]] | None = None,
        discovered_market_keys: Iterable[str] | None = None,
        discovered_markets: int | None = None,
        top_n: int = 3,
    ) -> Dict[str, Any]:
        event_rows = list(events)
        sample_rows = list(market_samples or [])
        structural_events = [event for event in event_rows if event.get("event_type") in _STRUCTURAL_EVENT_TYPES]
        governance_events = [event for event in event_rows if event.get("event_type") in _GOVERNANCE_EVENT_TYPES]
        quoted_or_entered_events = [event for event in event_rows if event.get("event_type") in _QUOTED_OR_ENTERED_EVENT_TYPES]
        quote_skip_events = [event for event in event_rows if event.get("event_type") in _QUOTE_SKIP_EVENT_TYPES]

        alias_map = cls._market_alias_map(
            [(row.get("payload") or {}) for row in event_rows if isinstance(row, dict)]
            + [row for row in sample_rows if isinstance(row, dict)]
        )

        structural_market_keys = cls._market_keys_from_rows_with_aliases(structural_events, alias_map)
        governance_market_keys = cls._market_keys_from_rows_with_aliases(governance_events, alias_map)
        quoted_or_entered_market_keys = cls._market_keys_from_rows_with_aliases(quoted_or_entered_events, alias_map)
        inferred_discovered_market_keys = cls._market_keys_from_rows_with_aliases(
            (event for event in event_rows if event.get("event_type") in _DISCOVERED_MARKET_EVENT_TYPES),
            alias_map,
        )
        supplied_discovered_market_keys = cls._market_keys_from_samples_with_aliases(sample_rows, alias_map)
        supplied_discovered_market_keys.update(
            str(key).strip() for key in (discovered_market_keys or []) if str(key).strip()
        )

        structural_reason_counts = cls._reason_counter(structural_events)
        governance_reason_counts = cls._reason_counter(governance_events)
        quote_skip_reason_counts = cls._reason_counter(quote_skip_events)

        inferred_discovered = len(inferred_discovered_market_keys)
        if supplied_discovered_market_keys:
            resolved_discovered = len(supplied_discovered_market_keys | inferred_discovered_market_keys)
        elif discovered_markets is not None:
            resolved_discovered = max(int(discovered_markets), inferred_discovered)
        else:
            resolved_discovered = inferred_discovered

        return {
            "discovered_markets": max(resolved_discovered, 0),
            "structurally_untradeable_markets": len(structural_market_keys),
            "governance_blocked_markets": len(governance_market_keys),
            "quoted_or_entered_markets": len(quoted_or_entered_market_keys),
            "structural_event_count": len(structural_events),
            "governance_event_count": len(governance_events),
            "quoted_or_entered_event_count": len(quoted_or_entered_events),
            "quote_skip_event_count": len(quote_skip_events),
            "top_structural_reasons": [[reason, count] for reason, count in structural_reason_counts.most_common(top_n)],
            "top_governance_reasons": [[reason, count] for reason, count in governance_reason_counts.most_common(top_n)],
            "top_quote_skip_reasons": [[reason, count] for reason, count in quote_skip_reason_counts.most_common(top_n)],
            "event_window_count": len(event_rows),
            "summary_scope": _BOUNDED_RECENT_RUN_SCOPE,
            "reason_counts_scope": "recent_run_events",
            "inferred_discovered_markets": inferred_discovered,
        }

    def summarize_market_eligibility(
        self,
        *,
        event_limit: int = 2000,
        sample_limit: int = 2000,
        run_id: str | None = None,
        discovered_markets: int | None = None,
        top_n: int = 3,
    ) -> Dict[str, Any]:
        resolved_run_id = run_id or self.current_run_id()
        events = self.read_events(limit=event_limit, run_id=resolved_run_id)
        market_samples = self.read_market_samples(limit=sample_limit, run_id=resolved_run_id)
        return self.summarize_market_eligibility_events(
            events,
            market_samples=market_samples,
            discovered_market_keys=self._market_keys_from_samples(market_samples),
            discovered_markets=discovered_markets,
            top_n=top_n,
        )

    @staticmethod
    def _snapshot_label(snapshot_ts: float) -> str:
        return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(snapshot_ts))

    def _artifact_metadata(self, source: Path, destination: Path) -> Dict[str, Any]:
        stat = destination.stat()
        return {
            "source": str(source),
            "dest": str(destination),
            "mtime": stat.st_mtime,
            "size_bytes": stat.st_size,
        }

    def _copy_artifact_if_present(self, source: Path, destination: Path, artifacts: list[Dict[str, Any]]) -> None:
        if not source.exists() or not source.is_file():
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        artifacts.append(self._artifact_metadata(source, destination))

    def preserve_run_evidence(
        self,
        *,
        trigger: str,
        run_id: str | None = None,
        snapshot_ts: float | None = None,
    ) -> Dict[str, Any]:
        snapshot_epoch = snapshot_ts if snapshot_ts is not None else time.time()
        snapshot_label = self._snapshot_label(snapshot_epoch)
        data_dir = self.runtime_dir.parent
        snapshot_root = data_dir / "forensic-snapshots"
        snapshot_dir = snapshot_root / snapshot_label
        suffix = 1
        while snapshot_dir.exists():
            snapshot_dir = snapshot_root / f"{snapshot_label}-{suffix}"
            suffix += 1
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        status = self.read_status()
        resolved_run_id = run_id or status.get("run_id") or self.current_run_id()
        risk = status.get("risk", {}) or {}
        artifacts: list[Dict[str, Any]] = []

        self._copy_artifact_if_present(self.status_path, snapshot_dir / "status.json", artifacts)
        self._copy_artifact_if_present(self.events_path, snapshot_dir / "events.jsonl", artifacts)
        self._copy_artifact_if_present(self.market_samples_path, snapshot_dir / "market_samples.jsonl", artifacts)
        self._copy_artifact_if_present(self.strategy_metrics_path, snapshot_dir / "strategy_metrics.json", artifacts)
        self._copy_artifact_if_present(self.runtime_dir / "ledger.db", snapshot_dir / "ledger.db", artifacts)
        self._copy_artifact_if_present(self.latest_status_text_path, snapshot_dir / "latest-status.txt", artifacts)

        research_dir = data_dir / "research"
        self._copy_artifact_if_present(research_dir / "latest.json", snapshot_dir / "research-latest.json", artifacts)
        self._copy_artifact_if_present(research_dir / "latest.md", snapshot_dir / "research-latest.md", artifacts)

        for ops_file in sorted(path for path in self.runtime_dir.glob("ops*") if path.is_file()):
            self._copy_artifact_if_present(ops_file, snapshot_dir / ops_file.name, artifacts)
        for runtime_artifact in (
            self.runtime_dir / "fill_markout_audit_latest.md",
            self.runtime_dir / "settlement_latency_audit_latest.md",
            self.runtime_dir / "reconcile_metrics_latest.txt",
        ):
            self._copy_artifact_if_present(runtime_artifact, snapshot_dir / runtime_artifact.name, artifacts)

        manifest = {
            "snapshot_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(snapshot_epoch)),
            "snapshot_dir": str(snapshot_dir),
            "trigger": trigger,
            "run_id": resolved_run_id,
            "bankroll": risk.get("capital", status.get("bankroll")),
            "realized_pnl": risk.get("realized_pnl_total", risk.get("daily_pnl")),
            "win_rate": status.get("win_rate"),
            "resolved_trade_count": status.get("resolved_trade_count"),
            "artifacts": artifacts,
        }
        manifest_path = snapshot_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str), encoding="utf-8")
        return manifest

    @staticmethod
    def _render_latest_status_text(status: Dict) -> str:
        risk = status.get("risk", {}) or {}
        eligibility = status.get("market_eligibility") or {}
        summary_scope = str(eligibility.get("summary_scope") or "run")
        reason_scope = str(eligibility.get("reason_counts_scope") or "")
        eligibility_scope = (
            "bounded recent-run"
            if summary_scope in {"run", _BOUNDED_RECENT_RUN_SCOPE} and reason_scope == "recent_run_events"
            else summary_scope
        )
        pause_policy = status.get("pause_policy") or "n/a"
        pause_scope = status.get("pause_scope") or "n/a"
        pause_reason = status.get("pause_reason") or "n/a"
        if status.get("market_eligibility_stale"):
            market_eligibility_line = "market_eligibility[stale]=pending_refresh"
        else:
            market_eligibility_line = (
                f"market_eligibility[{eligibility_scope}]=discovered:{int(eligibility.get('discovered_markets', 0) or 0)} structural:{int(eligibility.get('structurally_untradeable_markets', 0) or 0)} governance:{int(eligibility.get('governance_blocked_markets', 0) or 0)} quoted_entered:{int(eligibility.get('quoted_or_entered_markets', 0) or 0)}"
            )
        return (
            f"run_id={status.get('run_id', 'unknown')}\n"
            f"phase={status.get('phase', 'unknown')} mode={status.get('mode', 'unknown')} loop_count={status.get('loop_count', 0)}\n"
            f"{market_eligibility_line}\n"
            f"runtime_gate={status.get('gate_state', 'unknown')} new_order_pause={bool(status.get('new_order_pause', False))}\n"
            f"pause_policy={pause_policy} pause_scope={pause_scope} pause_reason={pause_reason}\n"
            f"bankroll={float(risk.get('capital', status.get('bankroll', 0.0))):.2f}\n"
            f"open_position_count={status.get('open_position_count', 0)} resolved_trade_count={status.get('resolved_trade_count', 0)}\n"
            f"heartbeat_ts={status.get('heartbeat_ts', 0)}\n"
        )
