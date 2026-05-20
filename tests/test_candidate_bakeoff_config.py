from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config.yaml"
CANDIDATE_BAKEOFF_PATH = PROJECT_ROOT / "configs" / "candidate-edge-bakeoff.yaml"
ALLOWED_TRIAL_STATES = {"candidate_only", "disabled"}


def test_candidate_edge_bakeoff_trials_are_candidate_eligible():
    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    spec = yaml.safe_load(CANDIDATE_BAKEOFF_PATH.read_text(encoding="utf-8"))

    candidates = {str(item) for item in (cfg.get("strategies", {}) or {}).get("candidates", []) or []}
    states = (cfg.get("strategies", {}) or {}).get("states", {}) or {}

    violations = []
    for trial in spec.get("trials", []) or []:
        family = str(trial["family"])
        state = str(states.get(family, ""))
        if family not in candidates or state not in ALLOWED_TRIAL_STATES:
            violations.append(f"{family}:{state or '<missing>'}")

    assert not violations, (
        "candidate-edge-bakeoff.yaml contains trial families that cannot run under "
        f"--allow-candidate-trial: {', '.join(violations)}"
    )
