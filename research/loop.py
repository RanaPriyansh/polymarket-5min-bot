from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Protocol


@dataclass
class ResearchInsight:
    title: str
    observation: str
    recommendation: str
    confidence: float
    evidence: Dict = field(default_factory=dict)


@dataclass
class ResearchCycleResult:
    cycle_id: str
    created_at: float
    source: str
    summary: str
    insights: List[ResearchInsight]
    raw_context: Dict = field(default_factory=dict)


class ResearchAdapter(Protocol):
    def run(self) -> ResearchCycleResult:
        ...


def make_cycle_id(source: str = "polymarket") -> str:
    import uuid
    import time
    return f"cycle-{source}-{int(time.time())}-{uuid.uuid4().hex[:6]}"


class ResearchLoop:
    def __init__(self, artifact_dir: str | Path):
        self.artifact_dir = Path(artifact_dir)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)

    def run_cycle(self, adapter: ResearchAdapter) -> ResearchCycleResult:
        result = adapter.run()
        json_path = self.artifact_dir / f"{result.cycle_id}.json"
        md_path = self.artifact_dir / f"{result.cycle_id}.md"
        json_path.write_text(
            json.dumps(
                {
                    **asdict(result),
                    "insights": [asdict(i) for i in result.insights],
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        md_path.write_text(self._to_markdown(result), encoding="utf-8")
        return result

    def _to_markdown(self, result: ResearchCycleResult) -> str:
        lines = [
            f"# Autoresearch Cycle {result.cycle_id}",
            "",
            f"Source: {result.source}",
            "",
            f"Summary: {result.summary}",
            "",
            "## Insights",
        ]
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


def make_cycle_id(prefix: str = "runtime") -> str:
    return f"{prefix}-{int(time.time())}"
