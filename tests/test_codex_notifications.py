from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Coroutine
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from random import Random
from typing import Any

import pytest

from project.research.codex_notifications import (
    MAX_DELIVERY_ATTEMPTS,
    AppServerProtocolError,
    JsonlStdioTransport,
    RpcClient,
    UnixWebSocketTransport,
    build_wake_prompt,
    deliver_notification,
    notification_lock_path,
    stdio_connector,
    sweep_notifications,
    unix_connector,
)
from project.research.runtime import (
    NotificationEvent,
    StateValidationError,
    StudyConfig,
    read_notification_event,
    record_terminal_event,
    write_notification_event,
)

EVENT_ID = "12345678-1234-5678-9234-567812345678"
THREAD_ID = "019f8098-aa66-7011-bc23-c3b3a78f7501"
NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)


def run_async[**P](function: Callable[P, Coroutine[Any, Any, None]]) -> Callable[P, None]:
    @wraps(function)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> None:
        asyncio.run(function(*args, **kwargs))

    return wrapped


class ScriptedTransport:
    def __init__(self, handler: Callable[[dict[str, Any]], list[dict[str, Any]]]) -> None:
        self.handler = handler
        self.sent: list[dict[str, Any]] = []
        self.incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.closed = False

    async def send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)
        for response in self.handler(message):
            await self.incoming.put(response)

    async def receive(self) -> dict[str, Any]:
        return await self.incoming.get()

    async def close(self) -> None:
        self.closed = True


class GatedStartTransport(ScriptedTransport):
    def __init__(self, started: asyncio.Event, release: asyncio.Event) -> None:
        super().__init__(app_server_handler(status="idle", turns=[]))
        self.started = started
        self.release = release

    async def send(self, message: dict[str, Any]) -> None:
        if message.get("method") == "turn/start":
            self.sent.append(message)
            self.started.set()
            await self.release.wait()
            await self.incoming.put({"id": message["id"], "result": {"turn": {"id": "new-turn"}}})
            return
        await super().send(message)


def notification(tmp_path: Path) -> NotificationEvent:
    study = StudyConfig(id="study-a", log_root=tmp_path / "logs")
    _, event = record_terminal_event(
        study,
        "run-a",
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=NOW,
        originating_thread_id=THREAD_ID,
    )
    return event


def thread(status: str, turns: list[dict[str, Any]]) -> dict[str, Any]:
    return {"id": THREAD_ID, "status": {"type": status}, "turns": turns}


def app_server_handler(
    *, status: str, turns: list[dict[str, Any]], steer_error: bool = False
) -> Callable[[dict[str, Any]], list[dict[str, Any]]]:
    def handle(message: dict[str, Any]) -> list[dict[str, Any]]:
        if "id" not in message:
            return []
        request_id = message["id"]
        method = message["method"]
        interleaved = {"method": "thread/status/changed", "params": {}}
        if method == "initialize":
            return [interleaved, {"id": request_id, "result": {"userAgent": "fake"}}]
        if method == "thread/resume":
            return [{"id": request_id, "result": {"thread": thread(status, turns)}}]
        if method == "thread/read":
            return [{"id": request_id, "result": {"thread": thread(status, turns)}}]
        if method == "turn/start":
            return [{"id": request_id, "result": {"turn": {"id": "new-turn"}}}]
        if method == "turn/steer":
            if steer_error:
                return [
                    {
                        "id": request_id,
                        "error": {"code": -32602, "message": "expected turn changed"},
                    }
                ]
            return [{"id": request_id, "result": {"turnId": "active-turn"}}]
        raise AssertionError(f"unexpected method: {method}")

    return handle


@run_async
async def test_idle_thread_starts_turn_after_fresh_read(tmp_path: Path) -> None:
    event = notification(tmp_path)
    transport = ScriptedTransport(app_server_handler(status="idle", turns=[]))

    accepted = await deliver_notification(event, transport, request_timeout=0.2)

    methods = [message.get("method") for message in transport.sent]
    assert methods == ["initialize", "initialized", "thread/resume", "thread/read", "turn/start"]
    start = transport.sent[-1]["params"]
    assert start["clientUserMessageId"] == EVENT_ID
    assert start["input"] == [{"type": "text", "text": build_wake_prompt(event)}]
    assert accepted.rpc_method == "turn/start"
    assert accepted.turn_id == "new-turn"


@run_async
async def test_active_thread_steers_sole_in_progress_turn(tmp_path: Path) -> None:
    event = notification(tmp_path)
    turns = [
        {"id": "completed-turn", "status": "completed"},
        {"id": "active-turn", "status": "inProgress"},
    ]
    transport = ScriptedTransport(app_server_handler(status="active", turns=turns))

    accepted = await deliver_notification(event, transport, request_timeout=0.2)

    steer = transport.sent[-1]
    assert steer["method"] == "turn/steer"
    assert steer["params"]["expectedTurnId"] == "active-turn"
    assert accepted.turn_id == "active-turn"


@pytest.mark.parametrize(
    ("status", "turns"),
    [
        ("notLoaded", []),
        ("systemError", []),
        ("active", []),
        ("active", [{"id": "a", "status": "inProgress"}, {"id": "b", "status": "inProgress"}]),
        ("idle", [{"id": "a", "status": "inProgress"}]),
    ],
)
@run_async
async def test_unknown_racy_or_nonsteerable_state_remains_unaccepted(
    tmp_path: Path, status: str, turns: list[dict[str, Any]]
) -> None:
    event = notification(tmp_path)
    transport = ScriptedTransport(app_server_handler(status=status, turns=turns))

    with pytest.raises(AppServerProtocolError):
        await deliver_notification(event, transport, request_timeout=0.2)

    assert not any(
        message.get("method") in {"turn/start", "turn/steer"} for message in transport.sent
    )


@run_async
async def test_expected_turn_rpc_race_is_not_accepted(tmp_path: Path) -> None:
    event = notification(tmp_path)
    turns = [{"id": "active-turn", "status": "inProgress"}]
    transport = ScriptedTransport(
        app_server_handler(status="active", turns=turns, steer_error=True)
    )

    with pytest.raises(AppServerProtocolError, match="expected turn changed"):
        await deliver_notification(event, transport, request_timeout=0.2)


@run_async
async def test_rpc_client_rejects_server_requests_without_autoapproval() -> None:
    def handler(message: dict[str, Any]) -> list[dict[str, Any]]:
        if message.get("method") == "initialize":
            request_id = message["id"]
            return [
                {
                    "id": "approval-1",
                    "method": "item/commandExecution/requestApproval",
                    "params": {},
                },
                {"id": request_id, "result": {}},
            ]
        return []

    transport = ScriptedTransport(handler)
    async with RpcClient(transport, request_timeout=0.2) as client:
        await client.request("initialize", {"clientInfo": {"name": "test", "version": "1"}})

    rejection = next(message for message in transport.sent if message.get("id") == "approval-1")
    assert rejection["error"]["code"] == -32601
    assert "result" not in rejection


@run_async
async def test_rpc_timeout_is_reported(tmp_path: Path) -> None:
    event = notification(tmp_path)
    transport = ScriptedTransport(lambda _message: [])

    with pytest.raises(AppServerProtocolError, match="timed out"):
        await deliver_notification(event, transport, request_timeout=0.01)


def test_prompt_contains_only_validated_fields_not_terminal_content(tmp_path: Path) -> None:
    event = notification(tmp_path)
    terminal_path = Path(event.terminal_state_path)
    terminal_path.write_text(
        terminal_path.read_text() + "RAW_SECRET_LOG SHOCKING_STACK_TRACE",
        encoding="utf-8",
    )

    prompt = build_wake_prompt(event)

    assert "Study: study-a" in prompt
    assert "Run: run-a" in prompt
    assert "Status: completed" in prompt
    assert str(terminal_path) in prompt
    assert "RAW_SECRET_LOG" not in prompt
    assert "STACK_TRACE" not in prompt


@run_async
async def test_sweep_accepts_once_and_deduplicates(tmp_path: Path) -> None:
    event = notification(tmp_path)
    calls = 0

    async def connect() -> ScriptedTransport:
        nonlocal calls
        calls += 1
        return ScriptedTransport(app_server_handler(status="idle", turns=[]))

    first = await sweep_notifications(
        tmp_path / "logs", connect=connect, now=lambda: NOW, random=Random(0)
    )
    second = await sweep_notifications(
        tmp_path / "logs", connect=connect, now=lambda: NOW, random=Random(0)
    )

    persisted = read_notification_event(
        Path(event.terminal_state_path).with_name("notification.json"), tmp_path / "logs"
    )
    assert first.accepted == 1
    assert second.due == 0
    assert calls == 1
    assert persisted.state == "accepted"
    assert persisted.accepted_rpc_method == "turn/start"
    assert notification_lock_path(tmp_path / "logs", THREAD_ID).is_file()


@run_async
async def test_acceptance_is_persisted_only_after_server_response(tmp_path: Path) -> None:
    event = notification(tmp_path)
    path = Path(event.terminal_state_path).with_name("notification.json")
    started = asyncio.Event()
    release = asyncio.Event()

    async def connect() -> ScriptedTransport:
        return GatedStartTransport(started, release)

    sweep = asyncio.create_task(
        sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    assert read_notification_event(path, tmp_path / "logs").state == "pending"
    release.set()
    result = await asyncio.wait_for(sweep, timeout=1)

    assert result.accepted == 1
    assert read_notification_event(path, tmp_path / "logs").state == "accepted"


@run_async
async def test_concurrent_sweeps_serialize_delivery_per_thread(tmp_path: Path) -> None:
    notification(tmp_path)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def connect() -> ScriptedTransport:
        nonlocal calls
        calls += 1
        return GatedStartTransport(started, release)

    first = asyncio.create_task(
        sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    second = asyncio.create_task(
        sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    )
    await asyncio.sleep(0.02)
    release.set()
    first_result, second_result = await asyncio.wait_for(asyncio.gather(first, second), timeout=1)

    assert first_result.accepted + second_result.accepted == 1
    assert calls == 1


@run_async
async def test_sweep_schedules_one_full_jitter_retry_per_run(tmp_path: Path) -> None:
    event = notification(tmp_path)

    async def connect() -> ScriptedTransport:
        return ScriptedTransport(lambda _message: [])

    result = await sweep_notifications(
        tmp_path / "logs",
        connect=connect,
        now=lambda: NOW,
        random=Random(0),
        request_timeout=0.01,
    )
    persisted = read_notification_event(
        Path(event.terminal_state_path).with_name("notification.json"), tmp_path / "logs"
    )

    assert result.retrying == 1
    assert persisted.attempt_count == 1
    assert persisted.last_attempt_at == NOW
    assert NOW < persisted.next_attempt_at <= NOW + timedelta(seconds=5)  # type: ignore[operator]

    not_due = await sweep_notifications(
        tmp_path / "logs",
        connect=connect,
        now=lambda: NOW,
        random=Random(0),
        request_timeout=0.01,
    )
    assert not_due.due == 0
    assert (
        read_notification_event(
            Path(event.terminal_state_path).with_name("notification.json"), tmp_path / "logs"
        ).attempt_count
        == 1
    )


@run_async
async def test_sweep_fails_after_maximum_attempts(tmp_path: Path) -> None:
    event = notification(tmp_path)
    path = Path(event.terminal_state_path).with_name("notification.json")
    current = event
    for index in range(MAX_DELIVERY_ATTEMPTS - 1):
        current = current.with_delivery_failure(
            attempted_at=NOW - timedelta(minutes=10 - index),
            error="retry",
            next_attempt_at=NOW,
            exhausted=False,
        )
    from project.research.runtime import write_notification_event

    write_notification_event(current, tmp_path / "logs")

    async def connect() -> ScriptedTransport:
        return ScriptedTransport(lambda _message: [])

    result = await sweep_notifications(
        tmp_path / "logs",
        connect=connect,
        now=lambda: NOW,
        random=Random(0),
        request_timeout=0.01,
    )

    assert result.failed == 1
    assert read_notification_event(path, tmp_path / "logs").state == "failed"


@run_async
async def test_stdio_transport_integrates_with_fake_jsonl_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = tmp_path / "fake_server.py"
    server.write_text(
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    message = json.loads(line)\n"
        "    print(json.dumps({'method': 'notice', 'params': {}}), flush=True)\n"
        "    print(json.dumps({'id': message['id'], 'result': {'echo': message['method']}}), flush=True)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        JsonlStdioTransport,
        "command",
        ("python", str(server)),
    )
    transport = await JsonlStdioTransport.connect(tmp_path / "daemon.sock")
    async with RpcClient(transport, request_timeout=1) as client:
        result = await client.request("initialize", {})

    assert result == {"echo": "initialize"}


@run_async
async def test_unix_websocket_transport_integrates_with_fake_server(tmp_path: Path) -> None:
    websockets = pytest.importorskip("websockets.asyncio.server")
    socket_path = tmp_path / "app-server.sock"

    async def echo(websocket: Any) -> None:
        message = json.loads(await websocket.recv())
        await websocket.send(json.dumps({"id": message["id"], "result": {"ok": True}}))

    async with websockets.unix_serve(echo, path=socket_path):
        transport = await UnixWebSocketTransport.connect(socket_path)
        async with RpcClient(transport, request_timeout=1) as client:
            result = await client.request("initialize", {})

    assert result == {"ok": True}


@run_async
async def test_deliver_rejects_missing_thread_and_nonpending_event(tmp_path: Path) -> None:
    event = notification(tmp_path)
    missing_thread = replace(event, originating_thread_id=None)
    missing_transport = ScriptedTransport(lambda _message: [])
    with pytest.raises(AppServerProtocolError, match="no originating") as error:
        await deliver_notification(missing_thread, missing_transport)
    assert error.value.permanent
    assert missing_transport.closed

    accepted = event.with_acceptance(
        accepted_at=NOW, rpc_method="turn/start", turn_id="accepted-turn"
    )
    accepted_transport = ScriptedTransport(lambda _message: [])
    with pytest.raises(AppServerProtocolError, match="only pending"):
        await deliver_notification(accepted, accepted_transport)
    assert accepted_transport.closed


def malformed_response_handler(fault: str) -> Callable[[dict[str, Any]], list[dict[str, Any]]]:
    def handle(message: dict[str, Any]) -> list[dict[str, Any]]:
        if "id" not in message:
            return []
        request_id = message["id"]
        method = message["method"]
        if method == "initialize":
            return [{"id": request_id, "result": {}}]
        if method == "thread/resume":
            if fault == "resume-missing":
                return [{"id": request_id, "result": {}}]
            return [{"id": request_id, "result": {"thread": thread("idle", [])}}]
        if method == "thread/read":
            response_thread: dict[str, Any] = thread("idle", [])
            if fault == "wrong-thread":
                response_thread["id"] = "other-thread"
            elif fault == "bad-status":
                response_thread["status"] = "idle"
            elif fault == "missing-turns":
                response_thread["turns"] = None
            return [{"id": request_id, "result": {"thread": response_thread}}]
        if method == "turn/start":
            result: dict[str, Any] = {"turn": {"id": "new-turn"}}
            if fault == "bad-start":
                result = {"turn": {}}
            return [{"id": request_id, "result": result}]
        raise AssertionError(method)

    return handle


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        ("resume-missing", "missing thread"),
        ("wrong-thread", "unexpected thread"),
        ("bad-status", "unknown thread status"),
        ("missing-turns", "missing turns"),
        ("bad-start", "accepted turn ID"),
    ],
)
@run_async
async def test_deliver_rejects_malformed_lifecycle_responses(
    tmp_path: Path, fault: str, message: str
) -> None:
    event = notification(tmp_path)
    transport = ScriptedTransport(malformed_response_handler(fault))
    with pytest.raises(AppServerProtocolError, match=message):
        await deliver_notification(event, transport, request_timeout=0.2)


@run_async
async def test_deliver_rejects_unexpected_steer_turn_id(tmp_path: Path) -> None:
    event = notification(tmp_path)
    turns = [{"id": "active-turn", "status": "inProgress"}]

    def handle(message: dict[str, Any]) -> list[dict[str, Any]]:
        responses = app_server_handler(status="active", turns=turns)(message)
        if message.get("method") == "turn/steer":
            return [{"id": message["id"], "result": {"turnId": "other-turn"}}]
        return responses

    with pytest.raises(AppServerProtocolError, match="unexpected turn ID"):
        await deliver_notification(event, ScriptedTransport(handle), request_timeout=0.2)


@pytest.mark.parametrize(
    ("response", "message"),
    [
        ({"error": "plain error"}, "plain error"),
        ({"result": []}, "non-object result"),
    ],
)
@run_async
async def test_rpc_rejects_nonstandard_error_and_result(
    response: dict[str, Any], message: str
) -> None:
    def handle(request: dict[str, Any]) -> list[dict[str, Any]]:
        return [{"id": request["id"], **response}]

    transport = ScriptedTransport(handle)
    async with RpcClient(transport, request_timeout=0.2) as client:
        with pytest.raises(AppServerProtocolError, match=message):
            await client.request("test", {})


@pytest.mark.parametrize("message", ["not-json", "[]", '{"id": 1, "result": 2}'])
@run_async
async def test_stdio_transport_rejects_invalid_messages(tmp_path: Path, message: str) -> None:
    server = tmp_path / "bad_server.py"
    server.write_text(
        f"import sys\nsys.stdin.readline()\nprint({message!r}, flush=True)\n",
        encoding="utf-8",
    )
    original = JsonlStdioTransport.command
    JsonlStdioTransport.command = ("python", str(server))
    try:
        transport = await JsonlStdioTransport.connect()
        async with RpcClient(transport, request_timeout=0.2) as client:
            with pytest.raises(AppServerProtocolError):
                await client.request("test", {})
    finally:
        JsonlStdioTransport.command = original


@run_async
async def test_stdio_transport_reports_proxy_eof(tmp_path: Path) -> None:
    server = tmp_path / "closed_server.py"
    server.write_text(
        "import sys\nprint('daemon unavailable', file=sys.stderr)\n", encoding="utf-8"
    )
    original = JsonlStdioTransport.command
    JsonlStdioTransport.command = ("python", str(server))
    try:
        transport = await JsonlStdioTransport.connect()
        with pytest.raises(AppServerProtocolError, match="daemon unavailable"):
            await transport.receive()
        await transport.close()
    finally:
        JsonlStdioTransport.command = original


@run_async
async def test_unix_transport_reports_missing_socket(tmp_path: Path) -> None:
    with pytest.raises(AppServerProtocolError, match="could not connect"):
        await UnixWebSocketTransport.connect(tmp_path / "missing.sock")


@run_async
async def test_sweep_handles_empty_root_root_safety_and_naive_clock(tmp_path: Path) -> None:
    async def connect() -> ScriptedTransport:
        raise AssertionError("no delivery should be attempted")

    assert (await sweep_notifications(tmp_path / "missing", connect=connect)).discovered == 0
    with pytest.raises(StateValidationError, match="filesystem root"):
        await sweep_notifications(Path("/"), connect=connect)
    tmp_path.mkdir(exist_ok=True)
    with pytest.raises(StateValidationError, match="offset-aware"):
        await sweep_notifications(
            tmp_path, connect=connect, now=lambda: datetime(2026, 7, 20, 12, 0)
        )


@run_async
async def test_sweep_marks_missing_thread_permanently_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    study = StudyConfig(id="study-a", log_root=tmp_path / "logs")
    _, event = record_terminal_event(
        study, "run-a", attempt=1, status="completed", originating_thread_id=None
    )

    async def connect() -> ScriptedTransport:
        raise AssertionError("invalid event must not connect")

    result = await sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    persisted = read_notification_event(
        Path(event.terminal_state_path).with_name("notification.json"), tmp_path / "logs"
    )
    assert result.failed == 1
    assert persisted.state == "failed"
    assert persisted.attempt_count == 1


@run_async
async def test_sweep_reports_malformed_and_already_failed_state(tmp_path: Path) -> None:
    malformed = tmp_path / "logs" / "study-a" / "runs" / "run-a" / "notification.json"
    malformed.parent.mkdir(parents=True)
    malformed.write_text("not-json", encoding="utf-8")

    async def connect() -> ScriptedTransport:
        raise AssertionError("invalid event must not connect")

    malformed_result = await sweep_notifications(
        tmp_path / "logs", connect=connect, now=lambda: NOW
    )
    assert malformed_result.failed == 1
    assert malformed_result.due == 1

    malformed.unlink()
    event = notification(tmp_path)
    failed = event.with_delivery_failure(
        attempted_at=NOW, error="failed", next_attempt_at=None, exhausted=True
    )
    write_notification_event(failed, tmp_path / "logs")
    failed_result = await sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    assert failed_result.failed == 1
    assert failed_result.due == 0
    assert failed_result.exit_code == 1
    assert failed_result.to_dict()["failed"] == 1


@run_async
async def test_sweep_replaces_invalid_notification_with_failed_state(tmp_path: Path) -> None:
    event = notification(tmp_path)
    path = Path(event.terminal_state_path).with_name("notification.json")
    path.write_text("not-json", encoding="utf-8")

    async def connect() -> ScriptedTransport:
        raise AssertionError("invalid event must not connect")

    result = await sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    persisted = read_notification_event(path, tmp_path / "logs")

    assert result.failed == 1
    assert persisted.state == "failed"
    assert persisted.last_error is not None


@run_async
async def test_accepted_ledger_deduplicates_reconstructed_pending_event(tmp_path: Path) -> None:
    event = notification(tmp_path)
    calls = 0

    async def connect() -> ScriptedTransport:
        nonlocal calls
        calls += 1
        return ScriptedTransport(app_server_handler(status="idle", turns=[]))

    await sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    write_notification_event(event, tmp_path / "logs")

    async def must_not_connect() -> ScriptedTransport:
        raise AssertionError("accepted event should be deduplicated")

    deduplicated = await sweep_notifications(
        tmp_path / "logs", connect=must_not_connect, now=lambda: NOW
    )
    assert deduplicated.accepted == 1
    assert calls == 1


@run_async
async def test_sweep_fails_invalid_accepted_ledger(tmp_path: Path) -> None:
    event = notification(tmp_path)

    async def connect() -> ScriptedTransport:
        return ScriptedTransport(app_server_handler(status="idle", turns=[]))

    await sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    write_notification_event(event, tmp_path / "logs")
    ledger = notification_lock_path(tmp_path / "logs", THREAD_ID).with_suffix(".accepted.json")
    ledger.write_text("not-json", encoding="utf-8")
    result = await sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    persisted = read_notification_event(
        Path(event.terminal_state_path).with_name("notification.json"), tmp_path / "logs"
    )
    assert result.failed == 1
    assert persisted.state == "failed"


@run_async
async def test_sweep_sanitizes_and_bounds_delivery_errors(tmp_path: Path) -> None:
    event = notification(tmp_path)

    async def connect() -> ScriptedTransport:
        raise OSError("line one\nline two\x00" + "x" * 600)

    result = await sweep_notifications(tmp_path / "logs", connect=connect, now=lambda: NOW)
    persisted = read_notification_event(
        Path(event.terminal_state_path).with_name("notification.json"), tmp_path / "logs"
    )
    assert result.retrying == 1
    assert persisted.last_error is not None
    assert "\n" not in persisted.last_error
    assert len(persisted.last_error) == 500


@run_async
async def test_connector_factories_delegate_to_transport_classes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = ScriptedTransport(lambda _message: [])

    async def stdio_connect(socket_path: Path | None = None) -> ScriptedTransport:
        assert socket_path == tmp_path / "daemon.sock"
        return expected

    async def unix_connect_fake(socket_path: Path) -> ScriptedTransport:
        assert socket_path == tmp_path / "daemon.sock"
        return expected

    monkeypatch.setattr(JsonlStdioTransport, "connect", stdio_connect)
    monkeypatch.setattr(UnixWebSocketTransport, "connect", unix_connect_fake)
    assert await stdio_connector(tmp_path / "daemon.sock")() is expected
    assert await unix_connector(tmp_path / "daemon.sock")() is expected
