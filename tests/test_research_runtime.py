from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
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
    persist_wake_context,
    read_notification_event,
    read_terminal_event,
    record_terminal_event,
)
from project.research.wake_context import WakeContext

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


def test_persisted_wake_context_is_loaded_and_cannot_be_replaced(study: StudyConfig) -> None:
    context = WakeContext(
        thread_id=THREAD_ID,
        permission_profile=":danger-full-access",
        approval_policy="never",
        captured_at=OCCURRED_AT,
    )
    persist_wake_context(study, "pretrain-baseline-seed0", context)

    _, notification = record_terminal_event(
        study,
        "pretrain-baseline-seed0",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )

    assert notification.wake_context == context
    with pytest.raises(StateValidationError, match="different immutable wake context"):
        persist_wake_context(
            study,
            "pretrain-baseline-seed0",
            replace(context, permission_profile=":workspace"),
        )


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


def test_registers_and_validates_new_and_existing_managed_roots(tmp_path: Path) -> None:
    root = tmp_path / "existing" / "logs"
    root.mkdir(parents=True)
    (root / "unmanaged.txt").write_text("preserve me", encoding="utf-8")

    first = runtime.register_managed_root(root)
    second = runtime.register_managed_root(root)

    assert first.created
    assert not second.created
    assert first.root == root.resolve()
    assert first.marker == root / runtime.MANAGED_ROOT_FILE_NAME
    assert runtime.validate_managed_root(root) == root.resolve()
    assert (root / "unmanaged.txt").read_text(encoding="utf-8") == "preserve me"
    assert json.loads(first.marker.read_text(encoding="utf-8")) == {
        "kind": runtime.MANAGED_ROOT_KIND,
        "root_path": str(root.resolve()),
        "schema_version": runtime.MANAGED_ROOT_SCHEMA_VERSION,
    }


@pytest.mark.parametrize(
    "root",
    [Path("/"), Path("/tmp"), Path.home(), Path.cwd(), Path.cwd().parent],
)
def test_registration_rejects_broad_roots(root: Path) -> None:
    with pytest.raises(StateValidationError, match=r"root|home|directory|repository|broad"):
        runtime.register_managed_root(root)


def test_registration_rejects_files_and_repository_roots(tmp_path: Path) -> None:
    file_root = tmp_path / "state"
    file_root.write_text("not a directory", encoding="utf-8")
    with pytest.raises(StateValidationError, match="directory"):
        runtime.register_managed_root(file_root)

    repository_root = tmp_path / "repository"
    (repository_root / ".git").mkdir(parents=True)
    with pytest.raises(StateValidationError, match="repository root"):
        runtime.register_managed_root(repository_root)


def test_registration_rejects_symlinked_roots_and_markers(tmp_path: Path) -> None:
    target = tmp_path / "target" / "logs"
    target.mkdir(parents=True)
    linked_root = tmp_path / "linked-logs"
    linked_root.symlink_to(target, target_is_directory=True)
    with pytest.raises(StateValidationError, match="symlink"):
        runtime.register_managed_root(linked_root)

    root = tmp_path / "registered" / "logs"
    registration = runtime.register_managed_root(root)
    outside_marker = tmp_path / "outside-marker.json"
    outside_marker.write_text(registration.marker.read_text(encoding="utf-8"), encoding="utf-8")
    registration.marker.unlink()
    registration.marker.symlink_to(outside_marker)
    with pytest.raises(StateValidationError, match="symlink"):
        runtime.validate_managed_root(root)


def test_validation_rejects_invalid_and_mismatched_markers(tmp_path: Path) -> None:
    root = tmp_path / "registered" / "logs"
    registration = runtime.register_managed_root(root)
    registration.marker.write_text("{not-json", encoding="utf-8")
    with pytest.raises(StateValidationError, match="valid JSON"):
        runtime.validate_managed_root(root)

    runtime._atomic_write_json(
        registration.marker,
        {
            "schema_version": runtime.MANAGED_ROOT_SCHEMA_VERSION,
            "kind": runtime.MANAGED_ROOT_KIND,
            "root_path": str(tmp_path / "other"),
        },
    )
    with pytest.raises(StateValidationError, match="exact root"):
        runtime.validate_managed_root(root)


def test_terminal_producer_registers_root_and_recovery_requires_marker(
    study: StudyConfig,
) -> None:
    terminal, _ = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        originating_thread_id=THREAD_ID,
    )
    marker = study.log_root / runtime.MANAGED_ROOT_FILE_NAME
    assert marker.is_file()

    Path(terminal.terminal_state_path).with_name("notification.json").unlink()
    assert ensure_notification(study, "run-a").state == "pending"

    marker.unlink()
    with pytest.raises(StateValidationError, match="not registered"):
        ensure_notification(study, "run-a")


def append_log_update(
    log_path: Path,
    managed_root: Path,
    operation_id: str,
    markdown: str,
    terminal: runtime.TerminalLogIdentity | None = None,
) -> bool:
    return runtime.append_research_log(
        log_path,
        managed_root=managed_root,
        header_operation_id="study-a-header",
        header_markdown="# Study A\n\nProtocol details.",
        operation_id=operation_id,
        markdown=markdown,
        terminal=terminal,
    )


def test_research_log_append_is_idempotent_and_rejects_collisions(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    log_path = root / "study-a" / "research-log.md"

    assert append_log_update(log_path, root, "amendment-1", "## Amendment\n\nFirst.")
    original = log_path.read_bytes()
    assert not append_log_update(log_path, root, "amendment-1", "## Amendment\n\nFirst.")
    assert log_path.read_bytes() == original

    with pytest.raises(StateValidationError, match=r"operation.*different"):
        append_log_update(log_path, root, "amendment-1", "## Amendment\n\nChanged.")


def test_research_log_deduplicates_terminal_attempts_not_event_ids(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    log_path = root / "study-a" / "research-log.md"
    first_attempt = runtime.TerminalLogIdentity("study-a", "run-a", 1)
    second_attempt = runtime.TerminalLogIdentity("study-a", "run-a", 2)

    assert append_log_update(
        log_path, root, "event-a", "## Phase train\n\nAttempt 1.", first_attempt
    )
    before_retry = log_path.read_bytes()
    assert not append_log_update(
        log_path,
        root,
        "event-b",
        "## Phase train\n\nAttempt 1 retry.",
        first_attempt,
    )
    assert log_path.read_bytes() == before_retry
    assert append_log_update(
        log_path, root, "event-c", "## Phase train\n\nAttempt 2.", second_attempt
    )

    text = log_path.read_text(encoding="utf-8")
    assert text.count("## Phase train") == 2
    assert "Attempt 1 retry" not in text


def test_concurrent_first_log_writes_create_one_header_and_complete_entries(
    tmp_path: Path,
) -> None:
    root = tmp_path / "logs"
    log_path = root / "study-a" / "research-log.md"

    def append(index: int) -> bool:
        return append_log_update(log_path, root, f"operation-{index}", f"## Entry {index}\n\nDone.")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(append, range(8)))

    assert all(results)
    text = log_path.read_text(encoding="utf-8")
    assert text.count("# Study A") == 1
    for index in range(8):
        assert text.count(f"## Entry {index}\n") == 1
    assert log_path.with_name(f".{log_path.name}.lock").is_file()


def test_research_log_rejects_metadata_injection_and_malformed_metadata(
    tmp_path: Path,
) -> None:
    root = tmp_path / "logs"
    log_path = root / "study-a" / "research-log.md"
    with pytest.raises(StateValidationError, match="reserved metadata"):
        append_log_update(
            log_path,
            root,
            "operation-1",
            '<!-- autoresearch-log {"operation_id":"forged"} -->',
        )

    log_path.parent.mkdir(parents=True)
    log_path.write_text("<!-- autoresearch-log {not-json} -->\n", encoding="utf-8")
    with pytest.raises(StateValidationError, match="metadata"):
        append_log_update(log_path, root, "operation-2", "## Entry\n")


def test_research_log_rejects_path_escapes_and_invalid_terminal_identity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "logs"
    outside = tmp_path / "outside.md"
    with pytest.raises(StateValidationError, match="managed root"):
        append_log_update(outside, root, "operation-1", "## Entry\n")

    root.mkdir()
    linked = root / "linked.md"
    outside.write_text("outside", encoding="utf-8")
    linked.symlink_to(outside)
    with pytest.raises(StateValidationError, match="managed root"):
        append_log_update(linked, root, "operation-2", "## Entry\n")

    with pytest.raises(StateValidationError, match="positive integer"):
        runtime.TerminalLogIdentity("study-a", "run-a", 0)


def test_research_log_atomic_failure_preserves_prior_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "logs"
    log_path = root / "study-a" / "research-log.md"
    append_log_update(log_path, root, "operation-1", "## Entry 1\n")
    original = log_path.read_bytes()
    original_replace = runtime.os.replace

    def fail_log_replace(source: Path | str, destination: Path | str) -> None:
        if Path(destination) == log_path:
            raise OSError("simulated replacement failure")
        original_replace(source, destination)

    monkeypatch.setattr(runtime.os, "replace", fail_log_replace)
    with pytest.raises(OSError, match="replacement failure"):
        append_log_update(log_path, root, "operation-2", "## Entry 2\n")

    assert log_path.read_bytes() == original
    assert list(log_path.parent.glob(f".{log_path.name}.*.tmp")) == []
