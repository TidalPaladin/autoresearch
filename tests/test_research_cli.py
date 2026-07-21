from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from project.research.runtime import (
    StudyConfig,
    read_notification_event,
    record_terminal_event,
    register_managed_root,
    write_notification_event,
)

THREAD_ID = "019f8098-aa66-7011-bc23-c3b3a78f7501"


def run_cli(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(Path(__file__).parents[1])
    return subprocess.run(
        [sys.executable, "scripts/research.py", *args],
        cwd=Path(__file__).parents[1],
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )


@pytest.fixture
def study_file(tmp_path: Path) -> Path:
    path = tmp_path / "study.yaml"
    path.write_text(
        f"id: study-a\nlog_root: {tmp_path / 'logs'}\n",
        encoding="utf-8",
    )
    return path


def test_notify_recovers_missing_notification_as_json(study_file: Path, tmp_path: Path) -> None:
    study = StudyConfig.load(study_file)
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )
    Path(terminal.terminal_state_path).with_name("notification.json").unlink()

    result = run_cli(tmp_path, "notify", str(study_file), "run-a", "--format", "json")

    assert result.returncode == 0
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["state"] == "pending"
    assert payload["run_id"] == "run-a"


def test_notify_text_is_pipe_safe_and_has_no_color(study_file: Path, tmp_path: Path) -> None:
    study = StudyConfig.load(study_file)
    record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )

    result = run_cli(tmp_path, "notify", str(study_file), "run-a")

    assert result.returncode == 0
    assert "PENDING  research notify" in result.stdout
    assert "\x1b[" not in result.stdout
    assert result.stderr == ""


def test_quiet_and_verbose_are_mutually_exclusive(study_file: Path, tmp_path: Path) -> None:
    result = run_cli(
        tmp_path,
        "notify",
        str(study_file),
        "run-a",
        "--quiet",
        "--verbose",
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert "not allowed" in result.stderr


def test_no_color_overrides_always(study_file: Path, tmp_path: Path) -> None:
    study = StudyConfig.load(study_file)
    record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )
    result = run_cli(
        tmp_path,
        "notify",
        str(study_file),
        "run-a",
        "--color",
        "always",
        "--no-color",
    )

    assert result.returncode == 0
    assert "\x1b[" not in result.stdout


def test_notify_validation_problem_uses_exit_one(study_file: Path, tmp_path: Path) -> None:
    register_managed_root(tmp_path / "logs")
    result = run_cli(tmp_path, "notify", str(study_file), "missing-run")

    assert result.returncode == 1
    assert result.stdout == ""
    assert "terminal state" in result.stderr


def test_invalid_invocation_uses_exit_two(tmp_path: Path) -> None:
    result = run_cli(tmp_path, "notify-worker")

    assert result.returncode == 2
    assert result.stdout == ""


def test_worker_empty_sweep_is_deterministic_json(tmp_path: Path) -> None:
    result = run_cli(
        tmp_path,
        "notify-worker",
        "--once",
        "--root",
        str(tmp_path / "missing"),
        "--format",
        "json",
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert json.loads(result.stdout) == {
        "accepted": 0,
        "discovered": 0,
        "due": 0,
        "failed": 0,
        "problems": [],
        "retrying": 0,
        "skipped": 0,
    }


def test_worker_unix_transport_requires_socket(tmp_path: Path) -> None:
    result = run_cli(tmp_path, "notify-worker", "--once", "--transport", "unix")

    assert result.returncode == 2
    assert result.stdout == ""
    assert "--socket is required" in result.stderr


def test_worker_delivery_problem_uses_exit_one_and_clean_json(tmp_path: Path) -> None:
    register_managed_root(tmp_path / "logs")
    path = tmp_path / "logs" / "study-a" / "runs" / "run-a" / "notification.json"
    path.parent.mkdir(parents=True)
    path.write_text("not-json", encoding="utf-8")

    result = run_cli(
        tmp_path,
        "notify-worker",
        "--once",
        "--root",
        str(tmp_path / "logs"),
        "--format",
        "json",
    )

    assert result.returncode == 1
    assert result.stderr == ""
    assert json.loads(result.stdout)["failed"] == 1


def test_notify_requeues_failed_event(study_file: Path, tmp_path: Path) -> None:
    study = StudyConfig.load(study_file)
    _, event = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )
    failed = event.with_delivery_failure(
        attempted_at=event.occurred_at,
        error="exhausted",
        next_attempt_at=None,
        exhausted=True,
    )
    write_notification_event(failed, study.log_root)

    failed_result = run_cli(tmp_path, "notify", str(study_file), "run-a", "--quiet")
    requeued_result = run_cli(
        tmp_path,
        "notify",
        str(study_file),
        "run-a",
        "--requeue",
        "--format",
        "json",
    )

    assert failed_result.returncode == 1
    assert failed_result.stdout.count("\n") == 1
    assert requeued_result.returncode == 0
    assert json.loads(requeued_result.stdout)["state"] == "pending"
    path = Path(event.terminal_state_path).with_name("notification.json")
    assert read_notification_event(path, study.log_root).attempt_count == 0


def test_register_root_reports_created_then_existing_as_json(tmp_path: Path) -> None:
    root = tmp_path / "logs"

    first = run_cli(
        tmp_path,
        "register-root",
        "--root",
        str(root),
        "--format",
        "json",
    )
    second = run_cli(
        tmp_path,
        "register-root",
        "--root",
        str(root),
        "--format",
        "json",
    )

    assert first.returncode == 0
    assert first.stderr == ""
    assert json.loads(first.stdout) == {
        "created": True,
        "marker": str(root.resolve() / ".autoresearch-root.json"),
        "root": str(root.resolve()),
    }
    assert second.returncode == 0
    assert json.loads(second.stdout)["created"] is False


def test_register_root_text_is_pipe_safe_and_quiet(tmp_path: Path) -> None:
    result = run_cli(
        tmp_path,
        "register-root",
        "--root",
        str(tmp_path / "logs"),
        "--quiet",
        "--color",
        "always",
        "--no-color",
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.count("\n") == 1
    assert "REGISTERED  research register-root" in result.stdout
    assert "\x1b[" not in result.stdout


def test_register_root_rejects_repository_root_with_exit_one(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    (repository / ".git").mkdir(parents=True)

    result = run_cli(tmp_path, "register-root", "--root", str(repository))

    assert result.returncode == 1
    assert result.stdout == ""
    assert "repository root" in result.stderr
