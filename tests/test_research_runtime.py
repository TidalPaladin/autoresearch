from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from project.research import runtime
from project.research.runtime import (
    NotificationEvent,
    StateValidationError,
    StudyConfig,
    ensure_notification,
    read_notification_event,
    read_terminal_event,
    record_terminal_event,
)

EVENT_ID = "12345678-1234-5678-9234-567812345678"
THREAD_ID = "019f8098-aa66-7011-bc23-c3b3a78f7501"
OCCURRED_AT = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)


@pytest.fixture
def study(tmp_path: Path) -> StudyConfig:
    return StudyConfig(id="vit-small-baseline-v1", log_root=tmp_path / "logs" / "research")


def test_record_writes_terminal_before_notification(
    study: StudyConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_write = runtime._atomic_write_json

    def fail_notification(path: Path, payload: dict[str, object]) -> None:
        if path.name == "notification.json":
            raise OSError("simulated queue failure")
        original_write(path, payload)

    monkeypatch.setattr(runtime, "_atomic_write_json", fail_notification)

    with pytest.raises(OSError, match="queue failure"):
        record_terminal_event(
            study,
            "pretrain-baseline-seed0",
            attempt=1,
            status="completed",
            event_id=EVENT_ID,
            occurred_at=OCCURRED_AT,
            originating_thread_id=THREAD_ID,
        )

    run_dir = study.run_dir("pretrain-baseline-seed0")
    assert (run_dir / "terminal.json").is_file()
    assert not (run_dir / "notification.json").exists()


def test_record_is_idempotent_by_event_id_and_uses_environment_thread(
    study: StudyConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CODEX_THREAD_ID", THREAD_ID)
    first = record_terminal_event(
        study,
        "pretrain-baseline-seed0",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
    )
    second = record_terminal_event(
        study,
        "pretrain-baseline-seed0",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
    )

    assert first == second
    assert first[0].originating_thread_id == THREAD_ID
    assert UUID(first[0].event_id).version == 4 or first[0].event_id == EVENT_ID
    assert not (study.run_dir("pretrain-baseline-seed0") / "attempts").exists()


def test_record_archives_prior_pair_before_replacement(study: StudyConfig) -> None:
    first_terminal, _ = record_terminal_event(
        study,
        "pretrain-baseline-seed0",
        attempt=1,
        status="failed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )
    second_event_id = "22345678-1234-5678-9234-567812345678"
    second_terminal, _ = record_terminal_event(
        study,
        "pretrain-baseline-seed0",
        attempt=2,
        status="completed",
        event_id=second_event_id,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )

    archive = (
        study.run_dir("pretrain-baseline-seed0")
        / "attempts"
        / f"{first_terminal.attempt}-{first_terminal.event_id}"
    )
    assert read_terminal_event(archive / "terminal.json", study.log_root) == first_terminal
    assert (archive / "notification.json").is_file()
    assert second_terminal.event_id == second_event_id


@pytest.mark.parametrize("identifier", ["../escape", "a/b", ".", "..", "", "white space"])
def test_rejects_unsafe_identifiers(tmp_path: Path, identifier: str) -> None:
    with pytest.raises(StateValidationError):
        StudyConfig(id=identifier, log_root=tmp_path)


def test_rejects_symlink_escape(tmp_path: Path) -> None:
    managed_root = tmp_path / "logs"
    managed_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (managed_root / "study-a").symlink_to(outside, target_is_directory=True)
    study = StudyConfig(id="study-a", log_root=managed_root)

    with pytest.raises(StateValidationError, match="managed root"):
        record_terminal_event(
            study,
            "run-a",
            attempt=1,
            status="completed",
            originating_thread_id=THREAD_ID,
        )


def test_rejects_malformed_and_mismatched_state(study: StudyConfig) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )
    notification_path = Path(terminal.terminal_state_path).with_name("notification.json")
    payload = json.loads(notification_path.read_text())
    payload["run_id"] = "other-run"
    notification_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateValidationError, match="does not match"):
        read_notification_event(notification_path, study.log_root, terminal=terminal)

    notification_path.write_text("{not-json", encoding="utf-8")
    with pytest.raises(StateValidationError, match="valid JSON"):
        read_notification_event(notification_path, study.log_root)


def test_rejects_non_absolute_or_outside_terminal_path(study: StudyConfig) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )
    notification_path = Path(terminal.terminal_state_path).with_name("notification.json")
    payload = json.loads(notification_path.read_text())
    payload["terminal_state_path"] = "terminal.json"
    notification_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateValidationError, match="absolute"):
        read_notification_event(notification_path, study.log_root)

    payload["terminal_state_path"] = str(study.log_root.parent / "terminal.json")
    notification_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(StateValidationError, match="managed root"):
        read_notification_event(notification_path, study.log_root)


def test_ensure_notification_recovers_missing_file_and_requeues_failed(
    study: StudyConfig,
) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )
    notification_path = Path(terminal.terminal_state_path).with_name("notification.json")
    notification_path.unlink()

    recovered = ensure_notification(study, "run-a")
    assert recovered.state == "pending"
    assert recovered.event_id == terminal.event_id

    failed = NotificationEvent.from_terminal(terminal).with_delivery_failure(
        attempted_at=OCCURRED_AT,
        error="permanent",
        next_attempt_at=None,
        exhausted=True,
    )
    runtime.write_notification_event(failed, study.log_root)
    requeued = ensure_notification(study, "run-a", requeue=True)
    assert requeued.state == "pending"
    assert requeued.attempt_count == 0
    assert requeued.last_error is None


def test_study_load_resolves_relative_root_and_rejects_bad_yaml(tmp_path: Path) -> None:
    source = tmp_path / "study.yaml"
    source.write_text("id: study-a\nlog_root: logs/research\n", encoding="utf-8")
    config = StudyConfig.load(source, base_dir=tmp_path)
    assert config.log_root == (tmp_path / "logs" / "research").resolve()

    source.write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    with pytest.raises(StateValidationError, match="JSON object"):
        StudyConfig.load(source)

    source.write_text("id: study-a\nlog_root: [bad\n", encoding="utf-8")
    with pytest.raises(StateValidationError, match="configuration"):
        StudyConfig.load(source)

    with pytest.raises(OSError, match="could not read"):
        StudyConfig.load(tmp_path / "missing.yaml")


def test_study_rejects_root_and_invalid_log_root_value(tmp_path: Path) -> None:
    with pytest.raises(StateValidationError, match="filesystem root"):
        StudyConfig(id="study-a", log_root=Path("/"))

    source = tmp_path / "study.yaml"
    source.write_text("id: study-a\nlog_root: 42\n", encoding="utf-8")
    with pytest.raises(StateValidationError, match="path string"):
        StudyConfig.load(source)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", True, "schema version"),
        ("schema_version", 2, "schema version"),
        ("event_id", 1, "UUID"),
        ("event_id", "not-a-uuid", "UUID"),
        ("event_id", "{12345678-1234-5678-9234-567812345678}", "canonical"),
        ("attempt", 0, "positive integer"),
        ("attempt", True, "positive integer"),
        ("status", "running", "terminal status"),
        ("occurred_at", 1, "ISO 8601"),
        ("occurred_at", "2026-07-20T12:00:00", "UTC offset"),
        ("occurred_at", "not-a-time", "ISO 8601"),
        ("originating_thread_id", "", "thread_id"),
        ("originating_thread_id", "bad\nthread", "control"),
        ("terminal_state_path", 1, "must be a string"),
    ],
)
def test_terminal_field_validation(
    study: StudyConfig, field: str, value: object, message: str
) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )
    path = Path(terminal.terminal_state_path)
    payload = json.loads(path.read_text())
    payload[field] = value
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateValidationError, match=message):
        read_terminal_event(path, study.log_root)


def test_terminal_rejects_missing_and_extra_fields(study: StudyConfig) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )
    path = Path(terminal.terminal_state_path)
    payload = json.loads(path.read_text())
    del payload["status"]
    payload["raw_log"] = "untrusted"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateValidationError, match=r"missing.*unexpected"):
        read_terminal_event(path, study.log_root)


def test_terminal_path_must_match_study_and_run_identifiers(study: StudyConfig) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )
    path = Path(terminal.terminal_state_path)
    payload = json.loads(path.read_text())
    payload["run_id"] = "run-b"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateValidationError, match="study and run"):
        read_terminal_event(path, study.log_root)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"state": "unknown"}, "delivery state"),
        ({"attempt_count": -1}, "non-negative"),
        ({"attempt_count": True}, "non-negative"),
        ({"last_error": 1}, "last_error"),
        ({"last_error": "bad\nerror"}, "sanitized"),
        ({"last_error": "x" * 501}, "sanitized"),
        ({"state": "accepted"}, "acceptance metadata"),
        ({"state": "failed"}, "failed notification"),
        ({"accepted_rpc_method": "turn/start"}, "unaccepted"),
        ({"attempt_count": 1}, "retry metadata"),
    ],
)
def test_notification_field_validation(
    study: StudyConfig, changes: dict[str, object], message: str
) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )
    path = Path(terminal.terminal_state_path).with_name("notification.json")
    payload = json.loads(path.read_text())
    payload.update(changes)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateValidationError, match=message):
        read_notification_event(path, study.log_root)


def test_requeue_rejects_nonfailed_event(study: StudyConfig) -> None:
    _, event = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )
    with pytest.raises(StateValidationError, match="only failed"):
        event.requeued()


def test_duplicate_event_rejects_changed_fields(study: StudyConfig) -> None:
    record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="failed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )
    with pytest.raises(StateValidationError, match="different terminal fields"):
        record_terminal_event(
            study,
            "run-a",
            attempt=1,
            status="completed",
            event_id=EVENT_ID,
            occurred_at=OCCURRED_AT,
            originating_thread_id=THREAD_ID,
        )


def test_atomic_write_removes_temporary_file_after_serialization_error(tmp_path: Path) -> None:
    target = tmp_path / "state.json"
    with pytest.raises(TypeError):
        runtime._atomic_write_json(target, {"bad": object()})
    assert not target.exists()
    assert list(tmp_path.glob("*.tmp")) == []


def test_archive_collision_is_rejected(study: StudyConfig) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="failed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )
    run_dir = study.run_dir("run-a")
    archive = run_dir / "attempts" / f"{terminal.attempt}-{terminal.event_id}"
    archive.mkdir(parents=True)
    collision = replace(terminal, status="completed")
    runtime._atomic_write_json(archive / "terminal.json", collision.to_dict())

    with pytest.raises(StateValidationError, match="archive collision"):
        record_terminal_event(
            study,
            "run-a",
            attempt=2,
            status="completed",
            originating_thread_id=THREAD_ID,
        )
