from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Dict, List

from research.loop import (
    ResearchAdapter,
    ResearchContext,
    ResearchCycleResult,
    ResearchExperimentResult,
    ResearchHypothesis,
    ResearchInsight,
    make_cycle_id,
)
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

        hypotheses: List[ResearchHypothesis] = []
        experiments: List[ResearchExperimentResult] = []
        insights: List[ResearchInsight] = []
        next_actions: List[str] = []

        family_summaries = self._family_summaries(metrics)
        skip_counter = self._skip_counter(samples)
        fill_events = [e for e in events if e.get("event_type") == "order.filled"]
        signal_events = [e for e in events if e.get("event_type") == "signal.executed"]
        quote_skips = [e for e in events if e.get("event_type") == "quote.skipped"]

        for family, summary in family_summaries.items():
            hypotheses.append(
                ResearchHypothesis(
                    hypothesis_id=f"family-{family}",
                    title=f"{family} should justify capital with real fills",
                    thesis=(
                        f"{family} needs live evidence that fill rate and pnl per fill are strong enough "
                        f"to keep capital allocated."
                    ),
                    confidence=summary["confidence"],
                    evidence=summary,
                )
            )
            experiments.append(
                ResearchExperimentResult(
                    experiment_id=f"runtime-{family}",
                    hypothesis_id=f"family-{family}",
                    summary=(
                        f"Observed {summary['quotes_submitted']} quotes, {summary['orders_filled']} fills, "
                        f"fill_rate={summary['fill_rate']:.1%}, pnl_per_fill={summary['pnl_per_fill']:.4f}."
                    ),
                    metrics=summary,
                    evidence=metrics.get(family, {}),
                )
            )
            insights.append(
                ResearchInsight(
                    title=f"{family} runtime quality",
                    observation=(
                        f"{family} realized pnl={summary['realized_pnl']:.4f}, fill_rate={summary['fill_rate']:.1%}, "
                        f"skip_rate={summary['skip_rate']:.1%}, pnl_per_fill={summary['pnl_per_fill']:.4f}"
                    ),
                    recommendation=summary["recommendation"],
                    confidence=summary["confidence"],
                    evidence=summary,
                )
            )
            next_actions.append(summary["action"])

        if skip_counter:
            top_reason, top_count = skip_counter.most_common(1)[0]
            insights.append(
                ResearchInsight(
                    title="Dominant runtime skip reason",
                    observation=f"Top structural skip is {top_reason} across {top_count} sampled books.",
                    recommendation=self._skip_recommendation(top_reason),
                    confidence=0.84,
                    evidence={"top_reason": top_reason, "count": top_count, "all_reasons": dict(skip_counter)},
                )
            )
            next_actions.append(self._skip_action(top_reason))

        if status:
            fetched = int(status.get("fetched_markets", 0) or 0)
            processed = int(status.get("processed_markets", 0) or 0)
            open_positions = int(status.get("open_position_count", 0) or 0)
            open_orders = int(status.get("risk", {}).get("open_order_count", 0) or 0)
            if fetched > 0:
                process_rate = processed / fetched
                insights.append(
                    ResearchInsight(
                        title="Market supply conversion",
                        observation=(
                            f"Only {processed}/{fetched} fetched markets survived the tradeability gauntlet "
                            f"({process_rate:.1%} conversion). Open positions={open_positions}, open orders={open_orders}."
                        ),
                        recommendation=(
                            "Keep the discovery universe fixed, but simplify the pre-filter tree so one bad book property does not eliminate the whole slot."
                            if process_rate < 0.6
                            else "Market conversion is healthy; focus on strategy edge, not discovery breadth."
                        ),
                        confidence=0.77,
                        evidence={
                            "fetched_markets": fetched,
                            "processed_markets": processed,
                            "process_rate": process_rate,
                            "open_positions": open_positions,
                            "open_orders": open_orders,
                        },
                    )
                )

        if not fill_events and not signal_events and metrics:
            top_blocker = skip_counter.most_common(1)[0][0] if skip_counter else "no_fill_events"
            insights.append(
                ResearchInsight(
                    title="Why edge is not compounding yet",
                    observation=(
                        "Runtime telemetry exists, but the bot is not converting observation into enough decisive fills. "
                        f"Top blocker: {top_blocker}."
                    ),
                    recommendation=(
                        "Promote opening_range and time_decay into the active set, reduce reliance on EMA-only entries, and shorten the polling cadence so the 5m windows produce enough ticks."
                    ),
                    confidence=0.86,
                    evidence={
                        "fills": len(fill_events),
                        "signals": len(signal_events),
                        "top_blocker": top_blocker,
                    },
                )
            )
            next_actions.append("Activate breakout + time-decay strategies and run at 5s cadence for a real comparison window.")

        if fill_events:
            latest_fill = fill_events[-1].get("payload", {})
            insights.append(
                ResearchInsight(
                    title="Latest fill artifact",
                    observation=(
                        f"Latest fill came from {latest_fill.get('strategy_family')} on {latest_fill.get('market_id')} "
                        f"for pnl_delta={latest_fill.get('realized_pnl_delta', 0.0):.4f}."
                    ),
                    recommendation="Replay the exact slot regime, compare to skipped books, and decide whether the trigger should scale or be demoted.",
                    confidence=0.71,
                    evidence=latest_fill,
                )
            )

        if quote_skips:
            quote_skip_reasons = Counter()
            for event in quote_skips:
                for reason in event.get("payload", {}).get("reasons", []):
                    quote_skip_reasons[reason] += 1
            if quote_skip_reasons:
                reason, count = quote_skip_reasons.most_common(1)[0]
                insights.append(
                    ResearchInsight(
                        title="Quote engine waste",
                        observation=f"Market-making skipped quotes mostly because of {reason} ({count} times).",
                        recommendation="Either turn this into an explicit hard gate in config or redesign the quote engine so it adapts instead of blindly failing.",
                        confidence=0.73,
                        evidence={"top_quote_skip_reason": reason, "count": count},
                    )
                )

        if not insights:
            insights.append(
                ResearchInsight(
                    title="No live runtime artifacts yet",
                    observation="Autoresearch found no usable status, metric, event, or sample artifacts.",
                    recommendation="Run paper mode with runtime telemetry enabled before trusting any research output.",
                    confidence=0.95,
                    evidence={"runtime_dir": str(self.telemetry.runtime_dir)},
                )
            )
            next_actions.append("Start the paper bot and collect at least 20 loops before evaluating strategy quality.")

        insights.sort(key=lambda insight: insight.confidence, reverse=True)
        next_actions = self._unique(next_actions)
        top_recommendation = insights[0].recommendation if insights else None
        summary = (
            f"Analyzed runtime artifacts from {self.telemetry.runtime_dir}. "
            f"Loaded {len(metrics)} families, {len(events)} events, {len(samples)} market samples, {len(fill_events)} fills."
        )
        return ResearchCycleResult(
            cycle_id=make_cycle_id("runtime"),
            created_at=float(status.get("heartbeat_ts", 0.0) or 0.0),
            source="live-runtime-artifacts",
            summary=summary,
            insights=insights,
            raw_context={
                "status": status,
                "metrics": metrics,
                "events_analyzed": len(events),
                "samples_analyzed": len(samples),
                "fill_events": len(fill_events),
            },
            context=ResearchContext(
                source="live-runtime-artifacts",
                runtime_dir=str(self.telemetry.runtime_dir),
                sample_limit=self.sample_limit,
                metadata={
                    "families": list(metrics.keys()),
                    "fetched_markets": status.get("fetched_markets", 0),
                    "processed_markets": status.get("processed_markets", 0),
                },
            ),
            hypotheses=hypotheses,
            experiments=experiments,
            next_actions=next_actions,
            top_recommendation=top_recommendation,
        )

    def _family_summaries(self, metrics: Dict) -> Dict[str, Dict]:
        summaries: Dict[str, Dict] = {}
        for family, family_metrics in metrics.items():
            quotes = float(family_metrics.get("quotes_submitted", 0) or 0)
            fills = float(family_metrics.get("orders_filled", 0) or 0)
            seen = float(family_metrics.get("markets_seen", 0) or 0)
            skips = float(family_metrics.get("toxic_book_skips", 0) or 0)
            realized_pnl = float(family_metrics.get("realized_pnl", 0.0) or 0.0)
            fill_rate = fills / max(quotes, 1.0)
            skip_rate = skips / max(seen, 1.0)
            pnl_per_fill = realized_pnl / max(fills, 1.0)

            if fills <= 0 and quotes <= 0:
                recommendation = "Kill or replace this family unless it starts emitting decisions within the next sample window."
                action = f"Demote {family} unless it produces real decisions in the next 20 loops."
                confidence = 0.9
            elif fills <= 0:
                recommendation = "The family is seeing markets but not converting. Loosen its trigger or stop allocating attention to it."
                action = f"Reduce the trigger strictness or capital weight for {family}."
                confidence = 0.83
            elif pnl_per_fill < 0:
                recommendation = "Negative pnl per fill means the family is paying to learn. Tighten entry quality or reduce size immediately."
                action = f"Shrink {family} size and inspect losing fills by asset/interval."
                confidence = 0.8
            elif fill_rate < 0.02:
                recommendation = "The family is too timid. Increase decision frequency or it cannot compound inside 5m windows."
                action = f"Raise decision density for {family} by lowering warmup/trigger thresholds."
                confidence = 0.78
            else:
                recommendation = "The family is producing live evidence. Keep it active and measure whether it scales cleanly."
                action = f"Keep {family} active and compare its pnl per fill against the new entrants."
                confidence = 0.7

            summaries[family] = {
                "quotes_submitted": int(quotes),
                "orders_filled": int(fills),
                "markets_seen": int(seen),
                "toxic_book_skips": int(skips),
                "realized_pnl": round(realized_pnl, 6),
                "fill_rate": fill_rate,
                "skip_rate": skip_rate,
                "pnl_per_fill": round(pnl_per_fill, 6),
                "recommendation": recommendation,
                "action": action,
                "confidence": confidence,
            }
        return summaries

    @staticmethod
    def _skip_counter(samples: List[Dict]) -> Counter:
        skip_counter: Counter = Counter()
        for sample in samples:
            for reason in sample.get("book_reasons", []):
                skip_counter[reason] += 1
        return skip_counter

    @staticmethod
    def _skip_recommendation(reason: str) -> str:
        if reason.startswith("wide_spread"):
            return "Split spread rules by strategy. Market making can tolerate wider spreads than directional entries."
        if reason.startswith("thin_depth") or reason.startswith("thin_notional"):
            return "Use lower depth/notional thresholds for 5m markets or route these slots to breakout-style strategies instead of MM."
        if reason == "high_vpin":
            return "Raise the VPIN threshold dynamically when fills are too scarce, not statically forever."
        return "Turn this skip reason into a measurable config experiment instead of leaving it as folklore."

    @staticmethod
    def _skip_action(reason: str) -> str:
        if reason.startswith("wide_spread"):
            return "Introduce per-strategy spread tolerances and compare MM vs directional entry acceptance."
        if reason.startswith("thin_depth") or reason.startswith("thin_notional"):
            return "Route thin books to breakout/time-decay strategies and reserve MM for healthier books."
        if reason == "high_vpin":
            return "Make VPIN threshold adaptive to recent fill-rate scarcity."
        return f"Run a parameter sweep around skip reason: {reason}."

    @staticmethod
    def _unique(items: List[str]) -> List[str]:
        seen = set()
        ordered: List[str] = []
        for item in items:
            if item and item not in seen:
                ordered.append(item)
                seen.add(item)
        return ordered
