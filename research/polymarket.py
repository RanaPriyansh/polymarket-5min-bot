from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List
import copy
import json

import pandas as pd
import yaml

from backtest_engine import Backtester
from .loop import (
    JsonArtifactStore,
    ResearchContext,
    ResearchExperimentResult,
    ResearchHypothesis,
    ResearchInsight,
    ResearchLoop,
    ResearchLoopConfig,
    SimpleMarkdownReportWriter,
)


class PolymarketHypothesisGenerator:
    DEFAULT_HYPOTHESES = [
        {
            "id": "mr-dev-06",
            "statement": "Mean reversion strengthens when deviation threshold is tightened to 6%.",
            "rationale": "Short-duration order books often overreact, so earlier entries may improve total opportunity capture.",
            "metadata": {"kind": "mean_reversion", "deviation_threshold": 0.06},
        },
        {
            "id": "mr-dev-08",
            "statement": "Mean reversion with the current 8% threshold remains the canonical baseline.",
            "rationale": "This is the current production default and should remain the benchmark.",
            "metadata": {"kind": "mean_reversion", "deviation_threshold": 0.08},
        },
        {
            "id": "liq-filter",
            "statement": "A higher liquidity floor will improve execution quality without destroying opportunity count.",
            "rationale": "Thin books create fake alpha. Better to trade fewer, cleaner markets.",
            "metadata": {"kind": "liquidity_filter", "min_volume": 15000},
        },
    ]

    def generate(self, context: ResearchContext, limit: int) -> List[ResearchHypothesis]:
        candidates = context.inputs.get("hypotheses") or self.DEFAULT_HYPOTHESES
        hypotheses = []
        for raw in candidates[:limit]:
            hypotheses.append(
                ResearchHypothesis(
                    id=raw["id"],
                    statement=raw["statement"],
                    rationale=raw.get("rationale", ""),
                    metadata=raw.get("metadata", {}),
                )
            )
        return hypotheses


class PolymarketExperimentRunner:
    def __init__(self, config: Dict[str, Any], data_path: str | Path):
        self.config = config
        self.data_path = Path(data_path)

    def run(self, hypothesis: ResearchHypothesis, context: ResearchContext) -> ResearchExperimentResult:
        kind = hypothesis.metadata.get("kind", "mean_reversion")
        if kind == "mean_reversion":
            return self._run_mean_reversion(hypothesis, context)
        if kind == "liquidity_filter":
            return self._run_liquidity_filter(hypothesis)
        return ResearchExperimentResult(
            hypothesis_id=hypothesis.id,
            status="skipped",
            score=0.0,
            summary=f"Unsupported experiment kind: {kind}",
            evidence={"kind": kind},
        )

    def _load_data(self) -> pd.DataFrame:
        backtester = Backtester(copy.deepcopy(self.config), initial_capital=1000.0)
        return backtester.load_historical_orderbooks(str(self.data_path))

    def _run_mean_reversion(self, hypothesis: ResearchHypothesis, context: ResearchContext) -> ResearchExperimentResult:
        cfg = copy.deepcopy(self.config)
        deviation = float(hypothesis.metadata.get("deviation_threshold", 0.08))
        cfg["strategies"]["mean_reversion_5min"]["deviation_threshold"] = deviation
        backtester = Backtester(cfg, initial_capital=1000.0)
        df = backtester.load_historical_orderbooks(str(self.data_path))
        market_limit = int(context.constraints.get("max_markets", 5))
        market_ids = list(df["market_id"].dropna().unique())[:market_limit]

        results = []
        for market_id in market_ids:
            result = backtester.simulate_mean_reversion(df, market_id=market_id, outcome="YES")
            if result.total_trades > 0:
                results.append(result)

        if not results:
            return ResearchExperimentResult(
                hypothesis_id=hypothesis.id,
                status="completed",
                score=0.0,
                summary="No trades generated on sample data.",
                evidence={"market_limit": market_limit, "deviation_threshold": deviation},
            )

        total_trades = sum(result.total_trades for result in results)
        total_pnl = sum(result.total_pnl for result in results)
        weighted_win_rate = sum(result.win_rate * result.total_trades for result in results) / total_trades
        avg_sharpe = sum(result.sharpe for result in results) / len(results)
        score = round(total_pnl, 4)
        return ResearchExperimentResult(
            hypothesis_id=hypothesis.id,
            status="completed",
            score=score,
            summary=(
                f"Deviation {deviation:.0%}: {total_trades} trades, "
                f"win rate {weighted_win_rate:.1%}, pnl ${total_pnl:.2f}, avg sharpe {avg_sharpe:.2f}"
            ),
            evidence={
                "market_limit": market_limit,
                "deviation_threshold": deviation,
                "total_trades": total_trades,
                "total_pnl": round(total_pnl, 4),
                "weighted_win_rate": round(weighted_win_rate, 6),
                "average_sharpe": round(avg_sharpe, 6),
            },
        )

    def _run_liquidity_filter(self, hypothesis: ResearchHypothesis) -> ResearchExperimentResult:
        min_volume = float(hypothesis.metadata.get("min_volume", 15000))
        df = self._load_data()
        total_rows = len(df)
        eligible_rows = int((df["volume"] >= min_volume).sum())
        ratio = eligible_rows / total_rows if total_rows else 0.0
        avg_spread = float((df["best_ask"] - df["best_bid"]).mean()) if total_rows else 0.0
        score = round(ratio * 100.0 - avg_spread * 100.0, 4)
        return ResearchExperimentResult(
            hypothesis_id=hypothesis.id,
            status="completed",
            score=score,
            summary=(
                f"Liquidity floor ${min_volume:,.0f}: {eligible_rows}/{total_rows} snapshots eligible "
                f"({ratio:.1%}), avg spread {avg_spread:.4f}"
            ),
            evidence={
                "min_volume": min_volume,
                "eligible_rows": eligible_rows,
                "total_rows": total_rows,
                "eligible_ratio": round(ratio, 6),
                "average_spread": round(avg_spread, 6),
            },
        )


class PolymarketResultAnalyzer:
    def analyze(
        self,
        context: ResearchContext,
        hypotheses: List[ResearchHypothesis],
        experiments: List[ResearchExperimentResult],
    ) -> List[ResearchInsight]:
        insights: List[ResearchInsight] = []
        by_id = {hypothesis.id: hypothesis for hypothesis in hypotheses}
        ranked = sorted(experiments, key=lambda experiment: experiment.score if experiment.score is not None else float("-inf"), reverse=True)
        for idx, experiment in enumerate(ranked):
            hypothesis = by_id.get(experiment.hypothesis_id)
            if hypothesis is None:
                continue
            action = "promote to paper-trading candidate" if idx == 0 and (experiment.score or 0) > 0 else "keep as research-only"
            confidence = 0.7 if idx == 0 else 0.55
            insights.append(
                ResearchInsight(
                    hypothesis_id=experiment.hypothesis_id,
                    insight=f"{hypothesis.statement} => {experiment.summary}",
                    action=action,
                    confidence=confidence,
                    metadata={"rank": idx + 1, "score": experiment.score},
                )
            )
        return insights


SUBAGENT_TEMPLATES = {
    "scout": {
        "goal": "Find markets, data slices, anomalies, or external catalysts worth testing.",
        "inputs": ["objective", "market universe", "time window", "constraints"],
        "outputs": ["candidate markets", "anomaly list", "ranked hypotheses"],
    },
    "experimenter": {
        "goal": "Turn a hypothesis into a measurable experiment or backtest.",
        "inputs": ["hypothesis", "datasets", "strategy parameters"],
        "outputs": ["metrics", "failure modes", "evidence bundle"],
    },
    "analyst": {
        "goal": "Compare experiment outputs and extract what is real versus noise.",
        "inputs": ["experiment results", "market context", "sample size"],
        "outputs": ["ranked insights", "confidence estimate", "next experiments"],
    },
    "reporter": {
        "goal": "Synthesize findings into a short operator-facing memo.",
        "inputs": ["insights", "artifacts", "objective"],
        "outputs": ["TLDR", "recommended actions", "artifact links"],
    },
}


def build_default_subagent_templates(domain: str = "polymarket", objective: str = "discover durable trading edges") -> Dict[str, Any]:
    return {
        "domain": domain,
        "objective": objective,
        "roles": copy.deepcopy(SUBAGENT_TEMPLATES),
    }


class PolymarketReportWriter(SimpleMarkdownReportWriter):
    def __init__(self, output_dir: str = "data/research"):
        super().__init__(output_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, result):
        report = super().write(result)
        subagent_path = self.output_dir / f"subagents_{result.cycle_id}.json"
        subagent_path.write_text(
            json.dumps(build_default_subagent_templates(result.context.domain, result.context.objective), indent=2),
            encoding="utf-8",
        )
        report["subagents_path"] = str(subagent_path)
        return report


def run_polymarket_research_cycle(
    config_path: str | Path,
    data_path: str | Path,
    output_dir: str | Path,
    objective: str,
    max_hypotheses: int = 3,
    max_markets: int = 5,
):
    config_path = Path(config_path)
    with config_path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    context = ResearchContext(
        domain="polymarket",
        objective=objective,
        constraints={"max_markets": max_markets},
        inputs={"data_path": str(data_path)},
    )
    reporter = PolymarketReportWriter(str(output_dir))
    loop = ResearchLoop(
        generator=PolymarketHypothesisGenerator(),
        runner=PolymarketExperimentRunner(config=config, data_path=data_path),
        analyzer=PolymarketResultAnalyzer(),
        reporter=reporter,
        artifact_store=JsonArtifactStore(str(output_dir)),
        config=ResearchLoopConfig(max_hypotheses=max_hypotheses, output_dir=str(output_dir), save_json=True),
    )
    result = loop.run(context)
    return {
        "cycle_id": result.cycle_id,
        "objective": objective,
        "report": result.report,
        "top_insight": asdict(result.insights[0]) if result.insights else None,
        "experiments": [asdict(experiment) for experiment in result.experiments],
    }
