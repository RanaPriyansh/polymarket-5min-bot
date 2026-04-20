from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml

from baseline_evidence import build_baseline_evidence


TRIAL_FAMILY_PATTERN = re.compile(r"^[a-z0-9_]+$")


@dataclass(frozen=True)
class BakeoffTrialSpec:
    family: str
    label: str


@dataclass(frozen=True)
class BakeoffSpec:
    experiment_id: str
    max_loops: int
    sleep_seconds: int
    runtime_root: str
    trials: list[BakeoffTrialSpec]


@dataclass(frozen=True)
class TrialOutcome:
    family: str
    label: str
    settled_count: int = 0
    resolved_count: int = 0
    realized_pnl: float = 0.0
    fill_count: int = 0
    toxic_skip_count: int = 0
    mark_to_market_pnl: float = 0.0
    max_drawdown: float = 0.0
    runtime_dir: str = ""
    evidence: dict[str, Any] | None = None


def _require_non_blank(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} must be non-blank")
    return text


def _validate_trial_family(value: Any, *, field_name: str) -> str:
    family = _require_non_blank(value, field_name)
    if not TRIAL_FAMILY_PATTERN.fullmatch(family):
        raise ValueError(f"{field_name} must match {TRIAL_FAMILY_PATTERN.pattern}")
    return family


def _resolve_trial_runtime_dir(runtime_root: str | Path, family: str) -> Path:
    trials_root = (Path(runtime_root) / "trials").resolve()
    runtime_dir = (trials_root / family).resolve()
    runtime_dir.relative_to(trials_root)
    return runtime_dir


def load_bakeoff_spec(path: str | Path) -> BakeoffSpec:
    with open(path, "r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)

    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ValueError("Bakeoff spec top-level YAML must be a mapping")

    experiment_id = _require_non_blank(payload.get("experiment_id"), "experiment_id")
    runtime_root = _require_non_blank(
        payload.get("runtime_root", "data/experiments/multi-family-bakeoff"),
        "runtime_root",
    )

    max_loops_raw = payload.get("max_loops", 20)
    max_loops = int(20 if max_loops_raw is None else max_loops_raw)
    if max_loops <= 0:
        raise ValueError("max_loops must be greater than 0")

    sleep_seconds_raw = payload.get("sleep_seconds", 5)
    sleep_seconds = int(5 if sleep_seconds_raw is None else sleep_seconds_raw)
    if sleep_seconds < 0:
        raise ValueError("sleep_seconds must be greater than or equal to 0")

    trials_payload = payload.get("trials") or []
    if not isinstance(trials_payload, list):
        raise ValueError("trials must be a list")

    trials: list[BakeoffTrialSpec] = []
    seen_families: set[str] = set()
    seen_labels: set[str] = set()
    for index, trial_payload in enumerate(trials_payload):
        if not isinstance(trial_payload, dict):
            raise ValueError(f"trial at index {index} must be a mapping")

        family = _validate_trial_family(
            trial_payload.get("family"),
            field_name=f"trial family at index {index}",
        )
        if family in seen_families:
            raise ValueError(f"Duplicate trial family: {family}")
        seen_families.add(family)

        label = str(trial_payload.get("label") or "candidate").strip() or "candidate"
        if label in seen_labels:
            raise ValueError(f"Duplicate trial label: {label}")
        seen_labels.add(label)

        trials.append(BakeoffTrialSpec(family=family, label=label))

    if not trials:
        raise ValueError("trials must contain at least one trial")
    baseline_count = sum(1 for trial in trials if trial.label == "baseline")
    if len(trials) > 1 and baseline_count != 1:
        raise ValueError("multi-trial bakeoff specs must contain exactly one baseline label")

    return BakeoffSpec(
        experiment_id=experiment_id,
        max_loops=max_loops,
        sleep_seconds=sleep_seconds,
        runtime_root=runtime_root,
        trials=trials,
    )


def build_trial_runtime_dir(spec: BakeoffSpec, trial: BakeoffTrialSpec) -> str:
    return str(_resolve_trial_runtime_dir(spec.runtime_root, trial.family))


def build_trial_command(
    *,
    python_bin: str,
    trial: BakeoffTrialSpec,
    runtime_dir: str | Path,
    max_loops: int,
    sleep_seconds: int,
    cli_path: str = str(Path(__file__).resolve().parent / "cli.py"),
    mode: str = "paper",
) -> list[str]:
    return [
        str(python_bin),
        cli_path,
        "run",
        "--mode",
        str(mode),
        "--runtime-dir",
        str(runtime_dir),
        "--strategies",
        trial.family,
        "--max-loops",
        str(int(max_loops)),
        "--sleep-seconds",
        str(int(sleep_seconds)),
    ]


def collect_trial_outcome(
    runtime_dir: str | Path,
    strategy_family: str,
    *,
    label: str | None = None,
) -> TrialOutcome:
    evidence = build_baseline_evidence(runtime_dir, strategy_family=strategy_family)
    current_run = dict(evidence.get("current_run", {}) or {})
    strategy_metrics = dict(evidence.get("strategy_metrics", {}) or {})
    risk = dict(evidence.get("risk", {}) or {})

    return TrialOutcome(
        family=strategy_family,
        label=str(label or strategy_family),
        settled_count=int(current_run.get("slot_settled_count", 0) or 0),
        resolved_count=int(current_run.get("resolved_trade_count", 0) or 0),
        realized_pnl=float(risk.get("realized_pnl_total", strategy_metrics.get("realized_pnl", 0.0)) or 0.0),
        fill_count=int(current_run.get("fill_event_count", 0) or 0),
        toxic_skip_count=int(strategy_metrics.get("toxic_book_skips", 0) or 0),
        mark_to_market_pnl=float(risk.get("realized_pnl_total", 0.0) or 0.0)
        + float(risk.get("unrealized_pnl_total", 0.0) or 0.0),
        max_drawdown=float(risk.get("max_drawdown", 0.0) or 0.0),
        runtime_dir=str(Path(runtime_dir)),
        evidence=evidence,
    )


def score_trial(outcome: TrialOutcome) -> tuple[int, int, float, float, int, float, int]:
    return (
        int(outcome.settled_count),
        int(outcome.resolved_count),
        float(outcome.realized_pnl),
        float(outcome.mark_to_market_pnl),
        int(outcome.fill_count),
        -float(outcome.max_drawdown),
        -int(outcome.toxic_skip_count),
    )


def rank_trials(outcomes: Iterable[TrialOutcome]) -> list[TrialOutcome]:
    return sorted(outcomes, key=score_trial, reverse=True)


def _score_no_settlement_candidate(outcome: TrialOutcome) -> tuple[int, float, float, float, int]:
    return (
        int(outcome.fill_count),
        float(outcome.realized_pnl),
        float(outcome.mark_to_market_pnl),
        -float(outcome.max_drawdown),
        -int(outcome.toxic_skip_count),
    )


def _has_attributable_trading_evidence(outcome: TrialOutcome) -> bool:
    return (
        int(outcome.fill_count) > 0
        or float(outcome.realized_pnl) != 0.0
        or float(outcome.mark_to_market_pnl) != 0.0
    )


def _select_promotion_candidate(spec: BakeoffSpec, ranked: list[TrialOutcome]) -> TrialOutcome | None:
    baseline_trial = next((trial for trial in spec.trials if trial.label == "baseline"), None)
    if baseline_trial is None:
        return ranked[0] if ranked else None

    baseline_outcome = next((outcome for outcome in ranked if outcome.family == baseline_trial.family), None)
    if baseline_outcome is None:
        return ranked[0] if ranked else None

    candidates = [
        outcome
        for outcome in ranked
        if outcome.family != baseline_trial.family and _has_attributable_trading_evidence(outcome)
    ]
    if not candidates:
        return None

    settled_advantage_candidates = [
        outcome for outcome in candidates if outcome.settled_count > baseline_outcome.settled_count
    ]
    if settled_advantage_candidates:
        return rank_trials(settled_advantage_candidates)[0]

    risk_qualified_candidates = [
        outcome
        for outcome in candidates
        if outcome.max_drawdown <= baseline_outcome.max_drawdown
        and outcome.toxic_skip_count <= baseline_outcome.toxic_skip_count
    ]
    if not risk_qualified_candidates:
        return None

    strongest_no_settlement = max(risk_qualified_candidates, key=_score_no_settlement_candidate)
    if _score_no_settlement_candidate(strongest_no_settlement) > _score_no_settlement_candidate(baseline_outcome):
        return strongest_no_settlement
    return None


def _build_promotion_summary(spec: BakeoffSpec, ranked: list[TrialOutcome]) -> dict[str, Any]:
    winner = ranked[0] if ranked else None
    baseline = next((trial for trial in spec.trials if trial.label == "baseline"), None)
    baseline_family = baseline.family if baseline else None
    promotion_candidate = _select_promotion_candidate(spec, ranked)

    if winner is None:
        return {
            "promotion_recommendation": "insufficient_evidence",
            "promoted_family": None,
            "baseline_family": baseline_family,
            "recommendation_text": "Promotion recommendation: insufficient evidence",
            "winner_line": "Winner: none",
        }

    if baseline_family is None:
        return {
            "promotion_recommendation": "promote",
            "promoted_family": winner.family,
            "baseline_family": None,
            "recommendation_text": f"Promotion recommendation: promote {winner.family}",
            "winner_line": f"Winner: {winner.family}",
        }

    if promotion_candidate is None:
        return {
            "promotion_recommendation": "keep_baseline",
            "promoted_family": None,
            "baseline_family": baseline_family,
            "recommendation_text": f"Promotion recommendation: keep baseline {baseline_family}",
            "winner_line": f"Winner: {winner.family}",
        }

    return {
        "promotion_recommendation": "promote",
        "promoted_family": promotion_candidate.family,
        "baseline_family": baseline_family,
        "recommendation_text": f"Promotion recommendation: promote {promotion_candidate.family} over baseline {baseline_family}",
        "winner_line": f"Winner: {winner.family}",
    }


def render_bakeoff_report(spec: BakeoffSpec, outcomes: Iterable[TrialOutcome]) -> str:
    ranked = rank_trials(outcomes)
    promotion_summary = _build_promotion_summary(spec, ranked)

    lines = [
        f"Experiment: {spec.experiment_id}",
        promotion_summary["winner_line"],
        promotion_summary["recommendation_text"],
        "Trials:",
    ]
    for outcome in ranked:
        lines.append(
            f"- {outcome.family} [{outcome.label}]: "
            f"settled={outcome.settled_count} "
            f"resolved={outcome.resolved_count} "
            f"realized_pnl={outcome.realized_pnl:.4f} "
            f"mtm_pnl={outcome.mark_to_market_pnl:.4f} "
            f"fills={outcome.fill_count} "
            f"toxic_skips={outcome.toxic_skip_count} "
            f"max_drawdown={outcome.max_drawdown:.4f}"
        )
    return "\n".join(lines)


def write_bakeoff_artifacts(
    experiment_dir: str | Path,
    spec: BakeoffSpec,
    outcomes: Iterable[TrialOutcome],
) -> dict[str, str]:
    experiment_path = Path(experiment_dir)
    experiment_path.mkdir(parents=True, exist_ok=True)
    ranked = rank_trials(outcomes)
    promotion_summary = _build_promotion_summary(spec, ranked)
    report = render_bakeoff_report(spec, ranked)
    winner = ranked[0] if ranked else None

    payload = {
        "experiment_id": spec.experiment_id,
        "runtime_root": spec.runtime_root,
        "promotion_recommendation": promotion_summary["promotion_recommendation"],
        "promoted_family": promotion_summary["promoted_family"],
        "baseline_family": promotion_summary["baseline_family"],
        "winner": None
        if winner is None
        else {
            "family": winner.family,
            "label": winner.label,
            "settled_count": winner.settled_count,
            "resolved_count": winner.resolved_count,
            "realized_pnl": winner.realized_pnl,
            "fill_count": winner.fill_count,
            "toxic_skip_count": winner.toxic_skip_count,
            "mark_to_market_pnl": winner.mark_to_market_pnl,
            "max_drawdown": winner.max_drawdown,
            "runtime_dir": winner.runtime_dir,
        },
        "ranked_outcomes": [
            {
                "family": outcome.family,
                "label": outcome.label,
                "settled_count": outcome.settled_count,
                "resolved_count": outcome.resolved_count,
                "realized_pnl": outcome.realized_pnl,
                "fill_count": outcome.fill_count,
                "toxic_skip_count": outcome.toxic_skip_count,
                "mark_to_market_pnl": outcome.mark_to_market_pnl,
                "max_drawdown": outcome.max_drawdown,
                "runtime_dir": outcome.runtime_dir,
                "evidence": outcome.evidence,
            }
            for outcome in ranked
        ],
        "report": report,
        "spec": {
            "experiment_id": spec.experiment_id,
            "max_loops": spec.max_loops,
            "sleep_seconds": spec.sleep_seconds,
            "runtime_root": spec.runtime_root,
            "trials": [{"family": trial.family, "label": trial.label} for trial in spec.trials],
        },
    }

    summary_json = experiment_path / "summary.json"
    summary_md = experiment_path / "summary.md"
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    summary_md.write_text(report + "\n", encoding="utf-8")
    return {
        "summary_json": str(summary_json),
        "summary_md": str(summary_md),
    }
