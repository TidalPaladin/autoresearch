from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from random import Random
from typing import Any

import pytest
from notify_wake import (
    AppServerError,
    DeliveryOutcome,
    DeliveryState,
    MessageTransport,
    WakeContext,
)

from project.research import codex_notifications
from project.research.codex_notifications import (
    SweepResult,
    build_wake_prompt,
    deliver_notification,
    enter_research_notify_wait,
    goal_wait_path,
    next_notification_attempt_at,
    notification_lock_path,
    sweep_notifications,
)
from project.research.runtime import (
    NotificationEvent,
    StateValidationError,
    StudyConfig,
    notification_namespace,
    notification_path_for_event,
    persist_wake_context,
    read_notification_event,
    record_terminal_event,
    register_managed_root,
    write_notification_event,
)

NOW = datetime(2026, 7, 29, 19, 0, tzinfo=UTC)
THREAD_ID = "019fa9c6-3613-7e60-a328-bf6f5c62c7bd"
EVENT_ID = "22345678-1234-5678-9234-567812345678"
PERMISSION_PROFILE = ":danger-full-access"


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


class ScriptedTransport(MessageTransport):
    def __init__(
        self,
        handler: Callable[[dict[str, Any]], list[dict[str, Any]]],
        *,
        fail_after_method: str | None = None,
    ) -> None:
        self._handler = handler
        self._fail_after_method = fail_after_method
        self._responses: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    async def send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)
        for response in self._handler(message):
            self._responses.put_nowait(response)
        if message.get("method") == self._fail_after_method:
            raise ConnectionError("lost acknowledgment")

    async def receive(self) -> dict[str, Any]:
        return await self._responses.get()

    async def close(self) -> None:
        self.closed = True


def context() -> WakeContext:
    return WakeContext(
        thread_id=THREAD_ID,
        permission_profile=PERMISSION_PROFILE,
        approval_policy="never",
        captured_at=NOW,
        goal_snapshot=None,
    )


def goal(*, status: str, updated_at: int) -> dict[str, Any]:
    return {
        "threadId": THREAD_ID,
        "objective": "wait for the research controller",
        "status": status,
        "tokenBudget": 10_000,
        "tokensUsed": 100,
        "timeUsedSeconds": 10,
        "createdAt": 1,
        "updatedAt": updated_at,
    }


def handler(
    *,
    selected_goal: dict[str, Any] | None = None,
    history_event_id: str | None = None,
) -> Callable[[dict[str, Any]], list[dict[str, Any]]]:
    current_goal = None if selected_goal is None else dict(selected_goal)

    def respond(message: dict[str, Any]) -> list[dict[str, Any]]:
        nonlocal current_goal
        if "id" not in message:
            return []
        request_id = message["id"]
        method = message.get("method")
        if method == "initialize":
            return [{"id": request_id, "result": {"userAgent": "fake"}}]
        if method == "thread/resume":
            return [
                {
                    "id": request_id,
                    "result": {
                        "thread": {
                            "id": THREAD_ID,
                            "status": {"type": "idle"},
                            "turns": [],
                        },
                        "activePermissionProfile": {"id": PERMISSION_PROFILE},
                        "approvalPolicy": "never",
                    },
                }
            ]
        if method == "thread/goal/get":
            return [{"id": request_id, "result": {"goal": current_goal}}]
        if method == "thread/goal/set":
            assert current_goal is not None
            current_goal = {
                **current_goal,
                "status": message["params"]["status"],
                "updatedAt": current_goal["updatedAt"] + 1,
            }
            return [{"id": request_id, "result": {"goal": current_goal}}]
        if method == "thread/read":
            turns = []
            if history_event_id is not None:
                turns = [
                    {
                        "id": "history-turn",
                        "status": "completed",
                        "items": [
                            {
                                "type": "userMessage",
                                "clientId": history_event_id,
                                "content": [],
                            }
                        ],
                    }
                ]
            return [
                {
                    "id": request_id,
                    "result": {
                        "thread": {
                            "id": THREAD_ID,
                            "status": {"type": "idle"},
                            "turns": turns,
                        }
                    },
                }
            ]
        if method == "turn/start":
            return [{"id": request_id, "result": {"turn": {"id": "wake-turn"}}}]
        raise AssertionError(f"unexpected method: {method}")

    return respond


def prepared_event(tmp_path: Path) -> tuple[StudyConfig, NotificationEvent]:
    study = StudyConfig(id="study-a", log_root=tmp_path / "logs")
    register_managed_root(study.log_root)
    persist_wake_context(study, "run-a", context())
    _terminal, event = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=NOW,
        originating_thread_id=THREAD_ID,
    )
    return study, event


def test_new_events_use_only_the_version_two_namespace(tmp_path: Path) -> None:
    study, event = prepared_event(tmp_path)
    namespace = notification_namespace(study.log_root)
    path = notification_path_for_event(study.log_root, EVENT_ID)

    assert namespace == study.log_root / ".notify-wake" / "v2"
    assert path.is_file()
    assert event.state == "pending"
    assert not (study.run_dir("run-a") / "notification.json").exists()
    assert not (study.run_dir("run-a") / "wake-context.json").exists()


def test_adapter_metadata_and_prompt_are_deterministic(tmp_path: Path) -> None:
    study, event = prepared_event(tmp_path)
    result = SweepResult(retrying=1, problems=("retry due",))

    assert "Study: study-a" in build_wake_prompt(event)
    assert "Run: run-a" in build_wake_prompt(event)
    assert notification_lock_path(study.log_root, THREAD_ID).parent.name == ".thread-locks"
    assert result.exit_code == 1
    assert result.to_dict()["problems"] == ["retry due"]
    assert SweepResult().exit_code == 0


def test_direct_delivery_accepts_and_missing_context_is_permanent(tmp_path: Path) -> None:
    _study, event = prepared_event(tmp_path)
    accepted_transport = ScriptedTransport(handler())

    accepted = run(deliver_notification(event, accepted_transport))

    assert accepted.rpc_method == "turn/start"
    assert accepted.turn_id == "wake-turn"

    missing_transport = ScriptedTransport(handler())
    with pytest.raises(AppServerError, match="no version-2 wake context") as error:
        run(deliver_notification(replace(event, wake_context=None), missing_transport))

    assert error.value.permanent
    assert missing_transport.closed


def test_direct_delivery_surfaces_shared_nonacceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _study, event = prepared_event(tmp_path)

    async def blocked(*_args: object, **_kwargs: object) -> DeliveryOutcome:
        return DeliveryOutcome(
            state=DeliveryState.BLOCKED,
            error="manual goal block",
        )

    monkeypatch.setattr(codex_notifications, "deliver_wake", blocked)
    with pytest.raises(AppServerError, match="manual goal block") as error:
        run(deliver_notification(event, ScriptedTransport(handler())))

    assert error.value.permanent


def test_sweep_starts_root_without_model_override(tmp_path: Path) -> None:
    study, _event = prepared_event(tmp_path)
    transport = ScriptedTransport(handler())

    result = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: asyncio.sleep(0, result=transport),
            now=lambda: NOW,
            random=Random(0),
        )
    )

    assert result.accepted == 1
    persisted = read_notification_event(
        notification_path_for_event(study.log_root, EVENT_ID),
        study.log_root,
    )
    assert persisted.state == "accepted"
    start = next(message for message in transport.sent if message.get("method") == "turn/start")
    assert "model" not in start["params"]
    assert "effort" not in start["params"]


def test_lost_acknowledgment_reconciles_without_duplicate_start(tmp_path: Path) -> None:
    study, _event = prepared_event(tmp_path)
    first = ScriptedTransport(handler(), fail_after_method="turn/start")
    uncertain = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: asyncio.sleep(0, result=first),
            now=lambda: NOW,
            random=Random(0),
        )
    )
    assert uncertain.retrying == 1
    persisted = read_notification_event(
        notification_path_for_event(study.log_root, EVENT_ID),
        study.log_root,
    )
    assert persisted.state == "uncertain"

    history = ScriptedTransport(handler(history_event_id=EVENT_ID))
    accepted = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: asyncio.sleep(0, result=history),
            now=lambda: NOW + timedelta(seconds=1),
            random=Random(0),
        )
    )
    assert accepted.accepted == 1
    assert not any(message.get("method") == "turn/start" for message in history.sent)


def test_manually_blocked_goal_is_preserved(tmp_path: Path) -> None:
    study, _event = prepared_event(tmp_path)
    transport = ScriptedTransport(handler(selected_goal=goal(status="blocked", updated_at=3)))

    result = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: asyncio.sleep(0, result=transport),
            now=lambda: NOW,
            random=Random(0),
        )
    )

    assert result.failed == 1
    assert not any(message.get("method") == "thread/goal/set" for message in transport.sent)


@pytest.mark.parametrize(
    ("delivery_kind", "expected_count", "expected_state"),
    [
        ("accepted", "skipped", "accepted"),
        ("blocked", "failed", "blocked"),
        ("future", "skipped", "retry_due"),
    ],
)
def test_sweep_handles_nondue_and_terminal_delivery_states(
    tmp_path: Path,
    delivery_kind: str,
    expected_count: str,
    expected_state: str,
) -> None:
    study, event = prepared_event(tmp_path)
    if delivery_kind == "accepted":
        selected = event.with_acceptance(
            accepted_at=NOW,
            rpc_method="turn/start",
            turn_id="wake-turn",
        )
    elif delivery_kind == "blocked":
        selected = event.with_delivery_failure(
            attempted_at=NOW,
            error="manual recovery",
            next_attempt_at=None,
            exhausted=True,
        )
    else:
        selected = event.with_delivery_failure(
            attempted_at=NOW,
            error="transient",
            next_attempt_at=NOW + timedelta(minutes=5),
            exhausted=False,
        )
    write_notification_event(selected, study.log_root)

    result = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
            now=lambda: NOW,
        )
    )

    assert getattr(result, expected_count) == 1
    assert (
        read_notification_event(
            notification_path_for_event(study.log_root, EVENT_ID),
            study.log_root,
        ).state
        == expected_state
    )


def test_sweep_blocks_event_without_wake_context(tmp_path: Path) -> None:
    study, _event = prepared_event(tmp_path)
    context_path = (
        notification_namespace(study.log_root)
        / "contexts"
        / "study-a"
        / "run-a"
        / "wake-context.json"
    )
    context_path.unlink()

    result = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
            now=lambda: NOW,
        )
    )

    assert result.failed == 1
    persisted = read_notification_event(
        notification_path_for_event(study.log_root, EVENT_ID),
        study.log_root,
    )
    assert persisted.state == "blocked"
    assert persisted.last_error == "notification has no version-2 wake context"


def test_sweep_retries_transport_errors_and_invalid_leases(tmp_path: Path) -> None:
    study, _event = prepared_event(tmp_path)

    async def unavailable() -> MessageTransport:
        raise ConnectionError("daemon unavailable\nretry")

    first = run(
        sweep_notifications(
            study.log_root,
            connect=unavailable,
            now=lambda: NOW,
            random=Random(0),
        )
    )
    assert first.retrying == 1
    assert "daemon unavailable retry" in first.problems[0]

    event = read_notification_event(
        notification_path_for_event(study.log_root, EVENT_ID),
        study.log_root,
    )
    write_notification_event(
        replace(
            event,
            delivery=event.delivery.schedule_retry(
                attempted_at=NOW,
                error="retry now",
                next_attempt_at=NOW,
                increment_attempt=False,
            ),
        ),
        study.log_root,
    )
    lease_path = goal_wait_path(study.log_root, THREAD_ID)
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    lease_path.write_text('{"schema_version":2}\n', encoding="utf-8")

    second = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: asyncio.sleep(
                0,
                result=ScriptedTransport(handler()),
            ),
            now=lambda: NOW,
            random=Random(0),
        )
    )
    assert second.retrying == 1
    assert "goal-wait lease is invalid" in second.problems[0]


def test_sweep_blocks_permanent_adapter_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    study, _event = prepared_event(tmp_path)

    async def blocked(*_args: object, **_kwargs: object) -> DeliveryOutcome:
        return DeliveryOutcome(
            state=DeliveryState.BLOCKED,
            error="authority mismatch",
        )

    monkeypatch.setattr(codex_notifications, "deliver_wake", blocked)
    result = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: asyncio.sleep(0, result=ScriptedTransport(handler())),
            now=lambda: NOW,
        )
    )

    assert result.failed == 1
    persisted = read_notification_event(
        notification_path_for_event(study.log_root, EVENT_ID),
        study.log_root,
    )
    assert persisted.state == "blocked"
    assert persisted.last_error == "authority mismatch"


def test_sweep_schedules_shared_retry_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    study, _event = prepared_event(tmp_path)

    async def retry(*_args: object, **_kwargs: object) -> DeliveryOutcome:
        return DeliveryOutcome(
            state=DeliveryState.RETRY_DUE,
            error="daemon busy",
        )

    monkeypatch.setattr(codex_notifications, "deliver_wake", retry)
    result = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: asyncio.sleep(0, result=ScriptedTransport(handler())),
            now=lambda: NOW,
            random=Random(0),
        )
    )

    assert result.retrying == 1
    persisted = read_notification_event(
        notification_path_for_event(study.log_root, EVENT_ID),
        study.log_root,
    )
    assert persisted.state == "retry_due"
    assert persisted.last_error == "daemon busy"


def test_sweep_reports_malformed_v2_event_and_missing_root(tmp_path: Path) -> None:
    missing_result = run(
        sweep_notifications(
            tmp_path / "missing",
            connect=lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
        )
    )
    assert missing_result == SweepResult()

    study, _event = prepared_event(tmp_path)
    path = notification_path_for_event(study.log_root, EVENT_ID)
    path.write_text("not-json", encoding="utf-8")
    malformed_result = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
            now=lambda: NOW,
        )
    )

    assert malformed_result.failed == 1
    assert malformed_result.due == 1
    assert "not valid JSON" in malformed_result.problems[0]


def test_retry_deadline_uses_only_valid_version_two_events(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    assert next_notification_attempt_at(missing) is None

    study, event = prepared_event(tmp_path)
    deadline = NOW + timedelta(seconds=30)
    retrying = event.with_delivery_failure(
        attempted_at=NOW,
        error="retry",
        next_attempt_at=deadline,
        exhausted=False,
    )
    write_notification_event(retrying, study.log_root)
    malformed = (
        notification_namespace(study.log_root)
        / "events"
        / "32345678-1234-5678-9234-567812345678"
        / "notification.json"
    )
    malformed.parent.mkdir(parents=True)
    malformed.write_text("not-json", encoding="utf-8")

    assert next_notification_attempt_at(study.log_root) == deadline


def test_owned_notify_wait_is_durable_under_v2_namespace(tmp_path: Path) -> None:
    study = StudyConfig(id="study-a", log_root=tmp_path / "logs")
    register_managed_root(study.log_root)
    transport = ScriptedTransport(handler(selected_goal=goal(status="active", updated_at=2)))

    lease = run(
        enter_research_notify_wait(
            study.log_root,
            context=context(),
            loop_id="research:study-a",
            source_ids=("controller:study-a",),
            transport=transport,
            verify_loop_identity=lambda loop_id, source_ids: (
                loop_id == "research:study-a" and source_ids == ("controller:study-a",)
            ),
        )
    )

    assert lease.state == "owned"
    assert goal_wait_path(study.log_root, THREAD_ID).is_file()


def test_sweep_ignores_legacy_run_notifications(tmp_path: Path) -> None:
    study = StudyConfig(id="study-a", log_root=tmp_path / "logs")
    register_managed_root(study.log_root)
    legacy = study.run_dir("run-a") / "notification.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text('{"schema_version":1,"state":"pending"}\n', encoding="utf-8")

    result = run(
        sweep_notifications(
            study.log_root,
            connect=lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
            now=lambda: NOW,
        )
    )

    assert result.discovered == 0
    assert legacy.is_file()


def test_version_one_delivery_state_is_rejected(tmp_path: Path) -> None:
    study, _event = prepared_event(tmp_path)
    path = notification_path_for_event(study.log_root, EVENT_ID)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["delivery"]["schema_version"] = 1
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateValidationError, match="cutover required"):
        read_notification_event(path, study.log_root)
