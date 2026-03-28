from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol
import json
import uuid


@dataclass
class ResearchContext:
    domain: str
    objective: str
    constraints: Dict[str, Any] = field(default_factory=dict)
    inputs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchHypothesis:
    id: str
    statement: str
    rationale: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchExperimentResult:
    hypothesis_id: str
    status: str
    score: Optional[float] = None
    summary: str = ""
    evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchInsight:
    hypothesis_id: str
    insight: str
    action: str = ""
    confidence: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchCycleResult:
    cycle_id: str
    context: ResearchContext
    hypotheses: List[ResearchHypothesis]
    experiments: List[ResearchExperimentResult]
    insights: List[ResearchInsight]
    report: Dict[str, Any]
    started_at: str
    completed_at: str


@dataclass
class ResearchLoopConfig:
    max_hypotheses: int = 3
    output_dir: str = "data/research"
    save_json: bool = True


class HypothesisGenerator(Protocol):
    def generate(self, context: ResearchContext, limit: int) -> List[ResearchHypothesis]: ...


class ExperimentRunner(Protocol):
    def run(self, hypothesis: ResearchHypothesis, context: ResearchContext) -> ResearchExperimentResult: ...


class ResultAnalyzer(Protocol):
    def analyze(
        self,
        context: ResearchContext,
        hypotheses: List[ResearchHypothesis],
        experiments: List[ResearchExperimentResult],
    ) -> List[ResearchInsight]: ...


class ReportWriter(Protocol):
    def write(self, result: ResearchCycleResult) -> Dict[str, Any]: ...


class EventSink(Protocol):
    def publish(self, event_type: str, payload: Dict[str, Any]) -> None: ...


class JsonArtifactStore:
    def __init__(self, output_dir: str = "data/research"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_cycle(self, result: ResearchCycleResult) -> Path:
        path = self.output_dir / f"cycle_{result.cycle_id}.json"
        payload = asdict(result)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path


class SimpleMarkdownReportWriter:
    def __init__(self, output_dir: str = "data/research"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, result: ResearchCycleResult) -> Dict[str, Any]:
        report_path = self.output_dir / f"report_{result.cycle_id}.md"
        lines = [
            f"# Research Cycle {result.cycle_id}",
            "",
            f"- Domain: {result.context.domain}",
            f"- Objective: {result.context.objective}",
            f"- Started: {result.started_at}",
            f"- Completed: {result.completed_at}",
            "",
            "## Hypotheses",
        ]
        for hyp in result.hypotheses:
            lines.append(f"- [{hyp.id}] {hyp.statement}")
        lines.append("")
        lines.append("## Experiments")
        for exp in result.experiments:
            lines.append(
                f"- [{exp.hypothesis_id}] status={exp.status} score={exp.score} summary={exp.summary}"
            )
        lines.append("")
        lines.append("## Insights")
        for insight in result.insights:
            lines.append(
                f"- [{insight.hypothesis_id}] {insight.insight} -> {insight.action}"
            )
        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {"path": str(report_path), "type": "markdown"}


class ResearchLoop:
    def __init__(
        self,
        generator: HypothesisGenerator,
        runner: ExperimentRunner,
        analyzer: ResultAnalyzer,
        reporter: ReportWriter,
        artifact_store: Optional[JsonArtifactStore] = None,
        event_sink: Optional[EventSink] = None,
        config: Optional[ResearchLoopConfig] = None,
    ):
        self.generator = generator
        self.runner = runner
        self.analyzer = analyzer
        self.reporter = reporter
        self.artifact_store = artifact_store
        self.event_sink = event_sink
        self.config = config or ResearchLoopConfig()

    def run(self, context: ResearchContext) -> ResearchCycleResult:
        cycle_id = uuid.uuid4().hex[:8]
        started_at = datetime.now(timezone.utc).isoformat()
        self._publish("cycle.started", {"cycle_id": cycle_id, "context": asdict(context)})

        hypotheses = self.generator.generate(context, self.config.max_hypotheses)
        self._publish("cycle.hypotheses_generated", {"cycle_id": cycle_id, "count": len(hypotheses)})

        experiments: List[ResearchExperimentResult] = []
        for hypothesis in hypotheses:
            result = self.runner.run(hypothesis, context)
            experiments.append(result)
            self._publish(
                "cycle.experiment_completed",
                {
                    "cycle_id": cycle_id,
                    "hypothesis_id": hypothesis.id,
                    "status": result.status,
                    "score": result.score,
                },
            )

        insights = self.analyzer.analyze(context, hypotheses, experiments)
        completed_at = datetime.now(timezone.utc).isoformat()
        result = ResearchCycleResult(
            cycle_id=cycle_id,
            context=context,
            hypotheses=hypotheses,
            experiments=experiments,
            insights=insights,
            report={},
            started_at=started_at,
            completed_at=completed_at,
        )
        result.report = self.reporter.write(result)

        if self.artifact_store and self.config.save_json:
            artifact_path = self.artifact_store.save_cycle(result)
            result.report["artifact_path"] = str(artifact_path)

        self._publish("cycle.completed", {"cycle_id": cycle_id, "report": result.report})
        return result

    def _publish(self, event_type: str, payload: Dict[str, Any]) -> None:
        if self.event_sink:
            self.event_sink.publish(event_type, payload)
