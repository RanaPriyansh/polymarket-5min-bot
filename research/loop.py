from __future__ import annotations

import json
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Protocol

from research.gate import build_gate_inputs, compute_gate_state


@dataclass
class ResearchContext:
    source: str
    runtime_dir: str | None = None
    artifact_dir: str | None = None
    sample_limit: int | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchHypothesis:
    hypothesis_id: str
    title: str
    thesis: str
    confidence: float
    evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchExperimentResult:
    experiment_id: str
    hypothesis_id: str
    summary: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchInsight:
    title: str
    observation: str
    recommendation: str
    confidence: float
    evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchCycleResult:
    cycle_id: str
    created_at: float
    source: str
    summary: str
    insights: List[ResearchInsight]
    raw_context: Dict[str, Any] = field(default_factory=dict)
    context: ResearchContext | None = None
    hypotheses: List[ResearchHypothesis] = field(default_factory=list)
    experiments: List[ResearchExperimentResult] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    top_recommendation: str | None = None


class ResearchAdapter(Protocol):
    def run(self) -> ResearchCycleResult:
        ...


def make_cycle_id(source: str = "runtime") -> str:
    return f"cycle-{source}-{int(time.time())}-{uuid.uuid4().hex[:6]}"


class ResearchLoop:
    def __init__(self, artifact_dir: str | Path, retention_limit: int = 48):
        self.artifact_dir = Path(artifact_dir)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.retention_limit = retention_limit
        self.latest_json_path = self.artifact_dir / "latest.json"
        self.latest_md_path = self.artifact_dir / "latest.md"

    def run_cycle(self, adapter: ResearchAdapter, *, runtime_dir: str | None = None) -> ResearchCycleResult:
        # Contradiction-first gate check (AC-5, AC-9)
        gate_state = "GREEN"
        gate_reasons: List[str] = []
        if runtime_dir is not None:
            gate_inputs = build_gate_inputs(runtime_dir)
            gate_state, gate_reasons = compute_gate_state(gate_inputs)

        result = adapter.run()

        # Apply gate emission rules
        if gate_state == "RED":
            # Block all output — emit contradiction report only
            result.experiments = []
            result.hypotheses = []
            result.top_recommendation = None
            result.next_actions = [
                f"[GATE RED] Fix contradictions before research can proceed: {'; '.join(gate_reasons)}"
            ]
            # Write contradiction report
            contradiction_report = {
                "gate_state": "RED",
                "detected_ts": time.time(),
                "cycle_id": result.cycle_id,
                "contradictions": gate_reasons,
            }
            contradiction_path = self.artifact_dir / f"contradiction-{int(time.time())}.json"
            try:
                with open(contradiction_path, "w") as _f:
                    json.dump(contradiction_report, _f, indent=2)
            except Exception:
                pass
        elif gate_state == "YELLOW":
            # Allow at most one hypothesis with confidence < 0.5
            result.experiments = []
            result.top_recommendation = None
            if result.hypotheses:
                h = result.hypotheses[0]
                h.confidence = min(h.confidence, 0.45)
                result.hypotheses = [h]
            if result.insights:
                result.insights[0].confidence = min(result.insights[0].confidence, 0.45)

        # Attach gate_state to the result for downstream consumers
        result.raw_context["gate_state"] = gate_state
        result.raw_context["gate_reasons"] = gate_reasons

        payload = self._result_payload(result)
        markdown = self._to_markdown(result)

        previous_latest = self._read_json(self.latest_json_path)
        previous_signature = self._dedupe_signature(previous_latest)
        current_signature = self._dedupe_signature(payload)

        if previous_signature != current_signature:
            json_path = self.artifact_dir / f"{result.cycle_id}.json"
            md_path = self.artifact_dir / f"{result.cycle_id}.md"
            json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            md_path.write_text(markdown, encoding="utf-8")
            self._prune_timestamped_artifacts()

        self.latest_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        self.latest_md_path.write_text(markdown, encoding="utf-8")
        return result

    def _result_payload(self, result: ResearchCycleResult) -> Dict[str, Any]:
        payload = asdict(result)
        if result.context is not None:
            payload["context"] = asdict(result.context)
        return payload

    def _to_markdown(self, result: ResearchCycleResult) -> str:
        lines = [
            f"# Autoresearch Cycle {result.cycle_id}",
            "",
            f"Source: {result.source}",
            f"Created at: {result.created_at:.3f}",
            "",
            f"Summary: {result.summary}",
        ]
        if result.top_recommendation:
            lines.extend(["", f"Top recommendation: {result.top_recommendation}"])
        if result.next_actions:
            lines.extend(["", "## Next actions"])
            for action in result.next_actions:
                lines.append(f"- {action}")
        if result.hypotheses:
            lines.extend(["", "## Hypotheses"])
            for idx, hypothesis in enumerate(result.hypotheses, start=1):
                lines.extend([
                    "",
                    f"### {idx}. {hypothesis.title}",
                    f"Thesis: {hypothesis.thesis}",
                    f"Confidence: {hypothesis.confidence:.0%}",
                    f"Evidence: {json.dumps(hypothesis.evidence, sort_keys=True)}",
                ])
        if result.experiments:
            lines.extend(["", "## Experiments"])
            for idx, experiment in enumerate(result.experiments, start=1):
                lines.extend([
                    "",
                    f"### {idx}. {experiment.experiment_id}",
                    f"Hypothesis: {experiment.hypothesis_id}",
                    f"Summary: {experiment.summary}",
                    f"Metrics: {json.dumps(experiment.metrics, sort_keys=True)}",
                    f"Evidence: {json.dumps(experiment.evidence, sort_keys=True)}",
                ])
        lines.extend(["", "## Insights"])
        for idx, insight in enumerate(result.insights, start=1):
            lines.extend([
                "",
                f"### {idx}. {insight.title}",
                f"Observation: {insight.observation}",
                f"Recommendation: {insight.recommendation}",
                f"Confidence: {insight.confidence:.0%}",
                f"Evidence: {json.dumps(insight.evidence, sort_keys=True)}",
            ])
        return "\n".join(lines)

    def _prune_timestamped_artifacts(self) -> None:
        json_files = self._timestamped_files("*.json")
        md_files = self._timestamped_files("*.md")
        for files in (json_files, md_files):
            overflow = len(files) - self.retention_limit
            if overflow > 0:
                for path in files[:overflow]:
                    path.unlink(missing_ok=True)

    def _timestamped_files(self, pattern: str) -> List[Path]:
        files = [
            path for path in self.artifact_dir.glob(pattern)
            if path.name not in {"latest.json", "latest.md"}
        ]
        return sorted(files, key=lambda p: p.stat().st_mtime)

    @staticmethod
    def _dedupe_signature(payload: Dict[str, Any] | None) -> str:
        if not payload:
            return ""
        normalized = dict(payload)
        normalized.pop("cycle_id", None)
        normalized.pop("created_at", None)
        return json.dumps(normalized, sort_keys=True)

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any] | None:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl_tail(path: Path, limit: int | None = None) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    if limit is None:
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows

    tail: deque[Dict[str, Any]] = deque(maxlen=limit)
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            tail.append(parsed)
    return list(tail)
