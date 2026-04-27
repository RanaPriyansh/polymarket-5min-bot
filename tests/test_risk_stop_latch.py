import json

from click.testing import CliRunner

from cli import cli
from risk_latch import (
    LATCH_FILENAME,
    apply_startup_risk_latch,
    manual_reset_risk_latch,
    persist_runtime_risk_stop,
    write_risk_stop_latch,
)
from runtime_telemetry import RuntimeTelemetry
from status_utils import runtime_health_payload


def test_circuit_breaker_writes_persistent_latch(tmp_path):
    latch = write_risk_stop_latch(
        tmp_path,
        run_id="paper-123",
        stop_reason="circuit_breaker",
        gate_state="RED",
        gate_reasons=["circuit_breaker_dd"],
        risk_report={"capital": 447.5, "max_drawdown": 0.12, "daily_pnl": -52.5},
    )

    path = tmp_path / LATCH_FILENAME
    assert path.exists()
    on_disk = json.loads(path.read_text())
    assert on_disk == latch
    assert on_disk["run_id"] == "paper-123"
    assert on_disk["stop_reason"] == "circuit_breaker"
    assert on_disk["gate_state"] == "RED"
    assert on_disk["pid"] > 0
    assert on_disk["equity"] == 447.5
    assert on_disk["drawdown"] == 0.12
    assert on_disk["new_orders_paused"] is True


def test_runtime_risk_stop_path_persists_latch_and_pauses_status(tmp_path):
    runtime = RuntimeTelemetry(tmp_path)
    risk_report = {"capital": 440.0, "max_drawdown": 0.12, "daily_pnl": -60.0}

    latch = persist_runtime_risk_stop(
        runtime,
        tmp_path,
        run_id="paper-runtime-1",
        mode="paper",
        loop_count=7,
        stop_reason="drawdown_limit",
        risk_report=risk_report,
        gate_snapshot={"gate_state": "RED", "gate_reasons": ["drawdown_limit"]},
    )

    on_disk = json.loads((tmp_path / LATCH_FILENAME).read_text())
    status = runtime.read_status()
    assert on_disk == latch
    assert on_disk["stop_reason"] == "drawdown_limit"
    assert status["phase"] == "stopping"
    assert status["stop_reason"] == "drawdown_limit"
    assert status["new_orders_paused"] is True
    assert status["risk_latch_present"] is True
    assert status["risk_latch_reason"] == "drawdown_limit"


def test_max_risk_stop_path_persists_latch_without_loosening_limits(tmp_path):
    runtime = RuntimeTelemetry(tmp_path)

    persist_runtime_risk_stop(
        runtime,
        tmp_path,
        run_id="paper-runtime-2",
        mode="paper",
        loop_count=3,
        stop_reason="max_risk_stop",
        risk_report={"capital": 500.0, "max_drawdown": 0.0, "daily_pnl": 0.0},
        gate_snapshot={"gate_state": "RED", "gate_reasons": ["max_risk_stop"]},
    )

    on_disk = json.loads((tmp_path / LATCH_FILENAME).read_text())
    assert on_disk["stop_reason"] == "max_risk_stop"
    assert on_disk["new_orders_paused"] is True
    assert runtime.read_status()["pause_policy"] == "persistent_risk_stop_latch"


def test_startup_with_latch_pauses_new_orders(tmp_path):
    write_risk_stop_latch(
        tmp_path,
        run_id="old-run",
        stop_reason="drawdown_limit",
        gate_state="RED",
        risk_report={"capital": 450.0, "max_drawdown": 0.10},
    )
    runtime = RuntimeTelemetry(tmp_path)

    applied = apply_startup_risk_latch(runtime, run_id="new-run", mode="paper", strategies=["toxicity_mm"])

    assert applied is True
    status = runtime.read_status()
    assert status["phase"] == "paused"
    assert status["new_orders_paused"] is True
    assert status["new_order_pause"] is True
    assert status["risk_latch_present"] is True
    assert status["risk_latch_reason"] == "drawdown_limit"
    assert status["revived_after_risk_stop"] is True
    assert (tmp_path / LATCH_FILENAME).exists()


def test_healthcheck_reports_but_cannot_clear_latch(tmp_path):
    RuntimeTelemetry(tmp_path).update_status(run_id="run-1", phase="running", heartbeat_ts=9999999999)
    write_risk_stop_latch(tmp_path, run_id="run-1", stop_reason="daily_loss", gate_state="RED")

    payload = runtime_health_payload(tmp_path, max_heartbeat_age=180)

    assert payload["risk_latch_present"] is True
    assert payload["risk_latch_reason"] == "daily_loss"
    assert (tmp_path / LATCH_FILENAME).exists()


def test_manual_reset_archives_latch_and_requires_reason(tmp_path):
    write_risk_stop_latch(tmp_path, run_id="run-1", stop_reason="max_risk", gate_state="RED")
    original_bytes = (tmp_path / LATCH_FILENAME).read_bytes()

    try:
        manual_reset_risk_latch(tmp_path, reason="")
        assert False, "manual reset without reason should fail"
    except ValueError:
        pass
    assert (tmp_path / LATCH_FILENAME).exists()

    archive_path = manual_reset_risk_latch(tmp_path, reason="human approved clean restart after review")

    assert not (tmp_path / LATCH_FILENAME).exists()
    assert archive_path.exists()
    archived = json.loads(archive_path.read_text())
    assert archive_path.read_bytes() == original_bytes
    metadata = json.loads(archive_path.with_name(f"reset-{archive_path.name}.json").read_text())
    assert archived["stop_reason"] == "max_risk"
    assert "reset_reason" not in archived
    assert metadata["reset_reason"] == "human approved clean restart after review"
    assert metadata["archived_from"] == str(tmp_path / LATCH_FILENAME)


def test_manual_reset_archives_exact_corrupt_latch_bytes(tmp_path):
    corrupt_bytes = b'{"stop_reason": "drawdown_limit", this is not valid json\n\xff'
    latch_path = tmp_path / LATCH_FILENAME
    latch_path.write_bytes(corrupt_bytes)

    archive_path = manual_reset_risk_latch(tmp_path, reason="human approved reset of corrupt latch")

    assert not latch_path.exists()
    assert archive_path.read_bytes() == corrupt_bytes
    metadata = json.loads(archive_path.with_name(f"reset-{archive_path.name}.json").read_text())
    assert metadata["reset_reason"] == "human approved reset of corrupt latch"
    assert metadata["original_stop_reason"] == "unreadable_risk_stop_latch"


def test_reset_risk_latch_cli_archives_latch(tmp_path):
    write_risk_stop_latch(tmp_path, run_id="run-1", stop_reason="circuit_breaker", gate_state="RED")
    runner = CliRunner()

    missing = runner.invoke(cli, ["reset-risk-latch", "--runtime-dir", str(tmp_path)])
    assert missing.exit_code != 0
    assert (tmp_path / LATCH_FILENAME).exists()

    result = runner.invoke(
        cli,
        [
            "reset-risk-latch",
            "--runtime-dir",
            str(tmp_path),
            "--reason",
            "human approved clean restart after evidence review",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Archived risk latch" in result.output
    assert not (tmp_path / LATCH_FILENAME).exists()
    archives = list((tmp_path / "risk_latch_archive").glob("risk_stop_latch-*.json"))
    assert len(archives) == 1
