from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Dict, List

from research.loop import ResearchAdapter, ResearchCycleResult, ResearchInsight, make_cycle_id
from runtime_telemetry import RuntimeTelemetry


class PolymarketRuntimeResearchAdapter(ResearchAdapter):
    def __init__(self, runtime_dir: str | Path, sample_limit: int = 200):
        self.telemetry = RuntimeTelemetry(runtime_dir)
        self.sample_limit = sample_limit

    def run(self) -> ResearchCycleResult:
        status = self.telemetry.read_status()
        metrics = self.telemetry.read_strategy_metrics()
        events = self.telemetry.read_events(limit=500)
        samples = self.telemetry.read_market_samples(limit=self.sample_limit)

        insights: List[ResearchInsight] = []

        if metrics:
            for family, family_metrics in metrics.items():
                toxic_skip_rate = family_metrics.get("toxic_book_skips", 0) / max(family_metrics.get("markets_seen", 1), 1)
                fill_rate = family_metrics.get("orders_filled", 0) / max(family_metrics.get("quotes_submitted", 1), 1)
                insights.append(
                    ResearchInsight(
                        title=f"{family} runtime quality",
                        observation=(
                            f"{family} realized pnl={family_metrics.get('realized_pnl', 0.0):.4f}, "
                            f"fill_rate={fill_rate:.1%}, toxic_skip_rate={toxic_skip_rate:.1%}"
                        ),
                        recommendation=(
                            "Tighten filters or widen quoting spread"
                            if toxic_skip_rate < 0.2 and fill_rate < 0.05
                            else "Promote current filter set and inspect top skipped books"
                        ),
                        confidence=0.72,
                        evidence=family_metrics,
                    )
                )

        if samples:
            skip_counter = Counter()
            for sample in samples:
                for reason in sample.get("book_reasons", []):
                    skip_counter[reason] += 1
            if skip_counter:
                reason, count = skip_counter.most_common(1)[0]
                insights.append(
                    ResearchInsight(
                        title="Dominant runtime toxicity reason",
                        observation=f"Most frequent structural skip was {reason} ({count} samples)",
                        recommendation="Use this reason as the first branch in market pre-filtering and parameter sweeps.",
                        confidence=0.81,
                        evidence={"top_reason": reason, "count": count},
                    )
                )

        fill_events = [e for e in events if e.get("event_type") == "order.filled"]
        if fill_events:
            top_fill = fill_events[-1]["payload"]
            insights.append(
                ResearchInsight(
                    title="Latest fill artifact",
                    observation=(
                        f"Latest fill came from {top_fill.get('strategy_family')} on {top_fill.get('market_id')} "
                        f"for pnl_delta={top_fill.get('realized_pnl_delta', 0.0):.4f}"
                    ),
                    recommendation="Replay this market regime in backtests and compare against nearby skipped books.",
                    confidence=0.68,
                    evidence=top_fill,
                )
            )

        if not insights:
            insights.append(
                ResearchInsight(
                    title="No live runtime artifacts yet",
                    observation="Autoresearch found no status/metric/sample artifacts in runtime dir.",
                    recommendation="Run paper mode with runtime telemetry enabled before trusting any research output.",
                    confidence=0.95,
                    evidence={"runtime_dir": str(self.telemetry.runtime_dir)},
                )
            )

        summary = (
            f"Analyzed runtime artifacts from {self.telemetry.runtime_dir}. "
            f"Loaded {len(metrics)} families, {len(events)} events, {len(samples)} market samples."
        )
        return ResearchCycleResult(
            cycle_id=make_cycle_id("runtime"),
            created_at=status.get("heartbeat_ts", 0.0),
            source="live-runtime-artifacts",
            summary=summary,
            insights=insights,
            raw_context={
                "status": status,
                "metrics": metrics,
                "events_analyzed": len(events),
                "samples_analyzed": len(samples),
            },
        )
