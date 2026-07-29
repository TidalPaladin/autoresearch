from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from websockets.asyncio.server import unix_serve

from project.research.runtime import (
    StudyConfig,
    persist_wake_context,
    read_notification_event,
    record_terminal_event,
)
from project.research.wake_context import WakeContext

REPOSITORY_ROOT = Path(__file__).parents[1]
RESEARCH_SCRIPT = REPOSITORY_ROOT / "scripts" / "research.py"
EVENT_ID = "12345678-1234-5678-9234-567812345678"
THREAD_ID = "subscription-free-terminal-content-sentinel"
PERMISSION_PROFILE = ":ci-notify-loop"
APPROVAL_POLICY = "never"
ACCEPTED_TURN_ID = "fake-notify-turn"
OCCURRED_AT = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
REMOVED_ENVIRONMENT_MARKERS = ("CHATGPT", "CODEX", "OPENAI")


def _isolated_environment(tmp_path: Path) -> dict[str, str]:
    empty_bin = tmp_path / "empty-bin"
    empty_bin.mkdir()
    environment = {
        key: value
        for key, value in os.environ.items()
        if not any(marker in key.upper() for marker in REMOVED_ENVIRONMENT_MARKERS)
    }
    environment["PATH"] = str(empty_bin)
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT)
    assert shutil.which("codex", path=environment["PATH"]) is None
    assert not any(
        marker in key.upper() for key in environment for marker in REMOVED_ENVIRONMENT_MARKERS
    )
    return environment


def _persist_terminal_state(tmp_path: Path, run_id: str) -> tuple[StudyConfig, Path, Path]:
    study = StudyConfig(id="notify-loop", log_root=tmp_path / "state")
    persist_wake_context(
        study,
        run_id,
        WakeContext(
            thread_id=THREAD_ID,
            permission_profile=PERMISSION_PROFILE,
            approval_policy=APPROVAL_POLICY,
            captured_at=OCCURRED_AT,
            goal_snapshot=None,
        ),
    )
    terminal, _notification = record_terminal_event(
        study,
        run_id,
        attempt=1,
        status="completed",
        event_id=EVENT_ID,
        occurred_at=OCCURRED_AT,
        originating_thread_id=THREAD_ID,
    )
    terminal_path = Path(terminal.terminal_state_path)
    return study, terminal_path, terminal_path.with_name("notification.json")


async def _run_worker(
    *,
    root: Path,
    socket_path: Path,
    environment: Mapping[str, str],
) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(RESEARCH_SCRIPT),
        "notify-worker",
        "--once",
        "--root",
        str(root),
        "--socket",
        str(socket_path),
        "--format",
        "json",
        cwd=REPOSITORY_ROOT,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = await process.communicate()
    assert process.returncode is not None
    return process.returncode, stdout_bytes.decode(), stderr_bytes.decode()


def _response_for(message: dict[str, Any]) -> dict[str, Any] | None:
    method = message.get("method")
    request_id = message.get("id")
    if request_id is None:
        return None
    if method == "initialize":
        result: dict[str, Any] = {"userAgent": "subscription-free-fake"}
    elif method == "thread/resume":
        result = {
            "thread": {
                "id": THREAD_ID,
                "status": {"type": "idle"},
                "turns": [],
            },
            "activePermissionProfile": {"id": PERMISSION_PROFILE},
            "approvalPolicy": APPROVAL_POLICY,
        }
    elif method == "thread/goal/get":
        result = {"goal": None}
    elif method == "thread/read":
        result = {
            "thread": {
                "id": THREAD_ID,
                "status": {"type": "idle"},
                "turns": [],
            }
        }
    elif method == "turn/start":
        result = {"turn": {"id": ACCEPTED_TURN_ID}}
    else:
        return {
            "id": request_id,
            "error": {"code": -32601, "message": f"unexpected method: {method}"},
        }
    return {"id": request_id, "result": result}


async def _exercise_success(tmp_path: Path) -> None:
    study, terminal_path, notification_path = _persist_terminal_state(tmp_path, "success")
    terminal_before = terminal_path.read_bytes()
    socket_path = tmp_path / "fake.sock"
    messages: list[dict[str, Any]] = []

    async def fake_app_server(websocket: Any) -> None:
        async for raw_message in websocket:
            message = json.loads(raw_message)
            messages.append(message)
            response = _response_for(message)
            if response is not None:
                await websocket.send(json.dumps(response))

    server = await unix_serve(
        fake_app_server,
        path=str(socket_path),
        compression=None,
        server_header=None,
    )
    try:
        return_code, stdout, stderr = await _run_worker(
            root=study.log_root,
            socket_path=socket_path,
            environment=_isolated_environment(tmp_path),
        )
    finally:
        server.close()
        await server.wait_closed()

    assert return_code == 0
    assert stderr == ""
    assert json.loads(stdout) == {
        "accepted": 1,
        "discovered": 1,
        "due": 1,
        "failed": 0,
        "problems": [],
        "retrying": 0,
        "skipped": 0,
    }
    assert terminal_path.read_bytes() == terminal_before
    persisted = read_notification_event(notification_path, study.log_root)
    assert persisted.state == "accepted"
    assert persisted.attempt_count == 1
    assert persisted.accepted_rpc_method == "turn/start"
    assert persisted.accepted_turn_id == ACCEPTED_TURN_ID

    methods = [message["method"] for message in messages]
    assert methods == [
        "initialize",
        "initialized",
        "thread/resume",
        "thread/goal/get",
        "thread/read",
        "turn/start",
    ]
    start = next(message for message in messages if message["method"] == "turn/start")
    expected_prompt = (
        "Research run completed.\n"
        "Study: notify-loop\n"
        "Run: success\n"
        "Status: completed\n"
        f"Terminal state: {terminal_path}\n\n"
        "Inspect the terminal state and continue the study protocol."
    )
    assert start["params"] == {
        "threadId": THREAD_ID,
        "input": [{"type": "text", "text": expected_prompt}],
        "clientUserMessageId": EVENT_ID,
    }
    assert "model" not in start["params"]
    assert "effort" not in start["params"]


def test_notify_worker_completes_subscription_free_fake_server_loop(tmp_path: Path) -> None:
    asyncio.run(_exercise_success(tmp_path))


async def _exercise_outage(tmp_path: Path) -> None:
    study, terminal_path, notification_path = _persist_terminal_state(tmp_path, "outage")
    terminal_before = terminal_path.read_bytes()

    return_code, stdout, stderr = await _run_worker(
        root=study.log_root,
        socket_path=tmp_path / "unavailable.sock",
        environment=_isolated_environment(tmp_path),
    )

    assert return_code == 1
    assert stderr == ""
    payload = json.loads(stdout)
    assert {key: value for key, value in payload.items() if key != "problems"} == {
        "accepted": 0,
        "discovered": 1,
        "due": 1,
        "failed": 0,
        "retrying": 1,
        "skipped": 0,
    }
    assert len(payload["problems"]) == 1
    assert str(notification_path) in payload["problems"][0]
    assert str(tmp_path / "unavailable.sock") in payload["problems"][0]
    assert terminal_path.read_bytes() == terminal_before
    persisted = read_notification_event(notification_path, study.log_root)
    assert persisted.state == "retry_due"
    assert persisted.status == "completed"
    assert persisted.attempt_count == 1
    assert persisted.last_attempt_at is not None
    assert persisted.next_attempt_at is not None
    assert persisted.next_attempt_at >= persisted.last_attempt_at
    assert persisted.accepted_at is None


def test_notify_worker_preserves_terminal_truth_during_app_server_outage(
    tmp_path: Path,
) -> None:
    asyncio.run(_exercise_outage(tmp_path))
