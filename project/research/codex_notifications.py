"""Deliver queued research events to a persistent Codex app-server daemon."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from random import Random
from typing import Any, Protocol, cast

from filelock import FileLock
from filelock import Timeout as FileLockTimeout
from websockets.asyncio.client import ClientConnection, unix_connect

from project.research.runtime import (
    MAX_LAST_ERROR_LENGTH,
    NOTIFICATION_FILE_NAME,
    SCHEMA_VERSION,
    STATE_LOCK_NAME,
    TERMINAL_FILE_NAME,
    NotificationEvent,
    StateValidationError,
    _atomic_write_json,
    read_notification_event,
    read_terminal_event,
    validate_managed_root,
    write_notification_event,
)

APP_SERVER_BASELINE = "0.144.5"
CLIENT_NAME = "autoresearch_notification_template"
CLIENT_TITLE = "Autoresearch Notification Template"
CLIENT_VERSION = "1.0.0"
TERMINAL_WAKE_MODEL = "gpt-5.6-luna"
TERMINAL_WAKE_EFFORT = "medium"
DEFAULT_REQUEST_TIMEOUT = 15.0
RETRY_BASE_SECONDS = 5.0
RETRY_FACTOR = 2.0
RETRY_CAP_SECONDS = 300.0
MAX_DELIVERY_ATTEMPTS = 8
SERVER_REQUEST_REJECTION_CODE = -32601
SERVER_REQUEST_REJECTION_MESSAGE = "This client does not handle server requests"
CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]+")

JsonObject = dict[str, Any]


class MessageTransport(Protocol):
    """One-message-at-a-time JSON transport used by the RPC dispatcher."""

    async def send(self, message: JsonObject) -> None: ...

    async def receive(self) -> JsonObject: ...

    async def close(self) -> None: ...


class AppServerProtocolError(RuntimeError):
    """A connection or protocol outcome that was not accepted by app-server."""

    def __init__(self, message: str, *, permanent: bool = False) -> None:
        super().__init__(message)
        self.permanent = permanent


class JsonlStdioTransport:
    """JSONL transport through ``codex app-server proxy``.

    The proxy connects to an existing daemon control socket. This class owns the
    short-lived proxy process, never the daemon.
    """

    command: tuple[str, ...] = ("codex", "app-server", "proxy")

    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self._process = process

    @classmethod
    async def connect(cls, socket_path: Path | None = None) -> JsonlStdioTransport:
        command = [*cls.command]
        if socket_path is not None:
            command.extend(("--sock", str(socket_path.expanduser().resolve(strict=False))))
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as error:
            raise AppServerProtocolError(f"could not start app-server proxy: {error}") from error
        if process.stdin is None or process.stdout is None:
            process.kill()
            await process.wait()
            raise AppServerProtocolError("app-server proxy did not expose stdin and stdout")
        return cls(process)

    async def send(self, message: JsonObject) -> None:
        stream = self._process.stdin
        if stream is None or stream.is_closing():
            raise AppServerProtocolError("app-server proxy stdin is closed")
        try:
            stream.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")
            await stream.drain()
        except (BrokenPipeError, ConnectionError, OSError) as error:
            raise AppServerProtocolError(f"could not write to app-server proxy: {error}") from error

    async def receive(self) -> JsonObject:
        stream = self._process.stdout
        if stream is None:
            raise AppServerProtocolError("app-server proxy stdout is unavailable")
        line = await stream.readline()
        if not line:
            detail = ""
            if self._process.stderr is not None:
                try:
                    raw_error = await asyncio.wait_for(self._process.stderr.read(2048), timeout=0.1)
                    detail = raw_error.decode(errors="replace").strip()
                except TimeoutError:
                    pass
            suffix = f": {detail}" if detail else ""
            raise AppServerProtocolError(f"app-server proxy closed the connection{suffix}")
        return _decode_message(line)

    async def close(self) -> None:
        stream = self._process.stdin
        if stream is not None and not stream.is_closing():
            stream.close()
            with suppress(BrokenPipeError, ConnectionError):
                await stream.wait_closed()
        try:
            await asyncio.wait_for(self._process.wait(), timeout=1.0)
        except TimeoutError:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=1.0)
            except TimeoutError:
                self._process.kill()
                await self._process.wait()


class UnixWebSocketTransport:
    """One-JSON-message-per-frame transport over a local Unix socket."""

    def __init__(self, connection: ClientConnection) -> None:
        self._connection = connection

    @classmethod
    async def connect(cls, socket_path: Path) -> UnixWebSocketTransport:
        path = socket_path.expanduser().resolve(strict=False)
        try:
            connection = await unix_connect(path=str(path), uri="ws://localhost")
        except (OSError, ValueError) as error:
            raise AppServerProtocolError(
                f"could not connect to app-server Unix socket {path}: {error}"
            ) from error
        return cls(connection)

    async def send(self, message: JsonObject) -> None:
        try:
            await self._connection.send(json.dumps(message, separators=(",", ":")))
        except (ConnectionError, OSError) as error:
            raise AppServerProtocolError(
                f"could not write to app-server socket: {error}"
            ) from error

    async def receive(self) -> JsonObject:
        try:
            message = await self._connection.recv()
        except (ConnectionError, OSError) as error:
            raise AppServerProtocolError(f"app-server socket read failed: {error}") from error
        return _decode_message(message)

    async def close(self) -> None:
        await self._connection.close()


def _decode_message(message: str | bytes) -> JsonObject:
    try:
        decoded = json.loads(message)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise AppServerProtocolError(f"app-server returned invalid JSON: {error}") from error
    if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
        raise AppServerProtocolError("app-server message must be a JSON object")
    return cast(JsonObject, decoded)


class RpcClient:
    """Bidirectional request dispatcher for the headerless JSON-RPC wire format."""

    def __init__(self, transport: MessageTransport, *, request_timeout: float) -> None:
        self._transport = transport
        self._request_timeout = request_timeout
        self._next_request_id = 1
        self._pending: dict[int, asyncio.Future[JsonObject]] = {}
        self._reader: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self._reader_error: AppServerProtocolError | None = None

    async def __aenter__(self) -> RpcClient:
        self._reader = asyncio.create_task(self._read_messages())
        return self

    async def __aexit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: object,
    ) -> None:
        if self._reader is not None:
            self._reader.cancel()
            with suppress(asyncio.CancelledError):
                await self._reader
        await self._transport.close()

    async def request(self, method: str, params: JsonObject) -> JsonObject:
        if self._reader_error is not None:
            raise self._reader_error
        request_id = self._next_request_id
        self._next_request_id += 1
        future: asyncio.Future[JsonObject] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._send({"id": request_id, "method": method, "params": params})
            response = await asyncio.wait_for(future, timeout=self._request_timeout)
        except TimeoutError as error:
            raise AppServerProtocolError(f"{method} timed out") from error
        finally:
            self._pending.pop(request_id, None)
        if "error" in response:
            rpc_error = response["error"]
            if isinstance(rpc_error, dict):
                code = rpc_error.get("code", "unknown")
                message = rpc_error.get("message", "unknown app-server error")
            else:
                code = "unknown"
                message = rpc_error
            raise AppServerProtocolError(f"{method} failed ({code}): {message}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise AppServerProtocolError(f"{method} returned a non-object result")
        return cast(JsonObject, result)

    async def notify(self, method: str, params: JsonObject) -> None:
        await self._send({"method": method, "params": params})

    async def _send(self, message: JsonObject) -> None:
        async with self._send_lock:
            await self._transport.send(message)

    async def _read_messages(self) -> None:
        try:
            while True:
                message = await self._transport.receive()
                message_id = message.get("id")
                method = message.get("method")
                if message_id is not None and isinstance(method, str):
                    await self._send(
                        {
                            "id": message_id,
                            "error": {
                                "code": SERVER_REQUEST_REJECTION_CODE,
                                "message": SERVER_REQUEST_REJECTION_MESSAGE,
                            },
                        }
                    )
                    continue
                if message_id is None:
                    continue
                if not isinstance(message_id, int):
                    continue
                future = self._pending.get(message_id)
                if future is not None and not future.done():
                    future.set_result(message)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            protocol_error = (
                error
                if isinstance(error, AppServerProtocolError)
                else AppServerProtocolError(f"app-server dispatcher failed: {error}")
            )
            self._reader_error = protocol_error
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(protocol_error)


@dataclass(frozen=True, slots=True)
class Acceptance:
    rpc_method: str
    turn_id: str


def build_wake_prompt(event: NotificationEvent) -> str:
    """Build the fixed trusted wake prompt without reading terminal contents."""

    return (
        "Research run completed.\n"
        f"Study: {event.study_id}\n"
        f"Run: {event.run_id}\n"
        f"Status: {event.status}\n"
        f"Terminal state: {event.terminal_state_path}\n\n"
        "Inspect the terminal state and continue the study protocol."
    )


def _thread_from_result(result: JsonObject, method: str, expected_thread_id: str) -> JsonObject:
    thread = result.get("thread")
    if not isinstance(thread, dict):
        raise AppServerProtocolError(f"{method} response is missing thread state")
    if thread.get("id") != expected_thread_id:
        raise AppServerProtocolError(f"{method} returned an unexpected thread")
    return cast(JsonObject, thread)


async def deliver_notification(
    event: NotificationEvent,
    transport: MessageTransport,
    *,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> Acceptance:
    """Attempt one app-server delivery and return only after server acceptance."""

    thread_id = event.originating_thread_id
    if thread_id is None:
        await transport.close()
        raise AppServerProtocolError(
            "notification has no originating Codex thread ID", permanent=True
        )
    if event.state != "pending":
        await transport.close()
        raise AppServerProtocolError("only pending notifications can be delivered", permanent=True)

    async with RpcClient(transport, request_timeout=request_timeout) as client:
        await client.request(
            "initialize",
            {
                "clientInfo": {
                    "name": CLIENT_NAME,
                    "title": CLIENT_TITLE,
                    "version": CLIENT_VERSION,
                }
            },
        )
        await client.notify("initialized", {})
        resumed = await client.request("thread/resume", {"threadId": thread_id})
        _thread_from_result(resumed, "thread/resume", thread_id)
        fresh = await client.request("thread/read", {"threadId": thread_id, "includeTurns": True})
        thread = _thread_from_result(fresh, "thread/read", thread_id)
        status = thread.get("status")
        if not isinstance(status, dict) or not isinstance(status.get("type"), str):
            raise AppServerProtocolError("thread/read returned an unknown thread status")
        status_type = status["type"]
        turns = thread.get("turns")
        if not isinstance(turns, list):
            raise AppServerProtocolError("thread/read response is missing turns")
        in_progress = [
            turn
            for turn in turns
            if isinstance(turn, dict)
            and turn.get("status") == "inProgress"
            and isinstance(turn.get("id"), str)
        ]
        input_items = [{"type": "text", "text": build_wake_prompt(event)}]
        if status_type == "idle":
            if in_progress:
                raise AppServerProtocolError("thread status changed while preparing turn/start")
            result = await client.request(
                "turn/start",
                {
                    "threadId": thread_id,
                    "input": input_items,
                    "clientUserMessageId": event.event_id,
                    "model": TERMINAL_WAKE_MODEL,
                    "effort": TERMINAL_WAKE_EFFORT,
                },
            )
            turn = result.get("turn")
            if not isinstance(turn, dict) or not isinstance(turn.get("id"), str):
                raise AppServerProtocolError("turn/start response is missing the accepted turn ID")
            return Acceptance(rpc_method="turn/start", turn_id=turn["id"])
        if status_type == "active":
            if len(in_progress) != 1:
                raise AppServerProtocolError(
                    "active thread does not have exactly one steerable in-progress turn"
                )
            expected_turn_id = cast(str, in_progress[0]["id"])
            result = await client.request(
                "turn/steer",
                {
                    "threadId": thread_id,
                    "input": input_items,
                    "expectedTurnId": expected_turn_id,
                    "clientUserMessageId": event.event_id,
                },
            )
            turn_id = result.get("turnId")
            if not isinstance(turn_id, str) or turn_id != expected_turn_id:
                raise AppServerProtocolError("turn/steer returned an unexpected turn ID")
            return Acceptance(rpc_method="turn/steer", turn_id=turn_id)
        raise AppServerProtocolError(f"thread is not deliverable in state {status_type!r}")


@dataclass(frozen=True, slots=True)
class SweepResult:
    discovered: int = 0
    due: int = 0
    accepted: int = 0
    retrying: int = 0
    failed: int = 0
    skipped: int = 0
    problems: tuple[str, ...] = ()

    @property
    def exit_code(self) -> int:
        return 1 if self.failed or self.retrying or self.problems else 0

    def to_dict(self) -> dict[str, object]:
        return {
            "discovered": self.discovered,
            "due": self.due,
            "accepted": self.accepted,
            "retrying": self.retrying,
            "failed": self.failed,
            "skipped": self.skipped,
            "problems": list(self.problems),
        }


def notification_lock_path(root: Path, thread_id: str) -> Path:
    """Return the stable per-thread delivery lock path."""

    digest = hashlib.sha256(thread_id.encode()).hexdigest()
    return root.expanduser().resolve(strict=False) / ".notification-locks" / f"{digest}.lock"


def _accepted_ledger_path(lock_path: Path) -> Path:
    return lock_path.with_suffix(".accepted.json")


def _read_accepted_ledger(path: Path, thread_id: str) -> dict[str, JsonObject]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise StateValidationError(f"accepted-event ledger is not valid JSON: {error}") from error
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("thread_id") != thread_id
        or not isinstance(payload.get("events"), dict)
    ):
        raise StateValidationError(f"accepted-event ledger is invalid: {path}")
    events = payload["events"]
    if not all(isinstance(key, str) and isinstance(value, dict) for key, value in events.items()):
        raise StateValidationError(f"accepted-event ledger contains invalid entries: {path}")
    return cast(dict[str, JsonObject], events)


def _write_accepted_ledger(path: Path, thread_id: str, events: dict[str, JsonObject]) -> None:
    _atomic_write_json(
        path,
        {
            "schema_version": SCHEMA_VERSION,
            "thread_id": thread_id,
            "events": events,
        },
    )


def _sanitize_error(error: BaseException) -> str:
    text = CONTROL_CHARACTERS.sub(" ", str(error)).strip()
    text = " ".join(text.split())
    return (text or error.__class__.__name__)[:MAX_LAST_ERROR_LENGTH]


def _is_due(event: NotificationEvent, now: datetime) -> bool:
    return event.state == "pending" and (
        event.next_attempt_at is None or event.next_attempt_at <= now
    )


@asynccontextmanager
async def _async_file_lock(path: Path) -> AsyncIterator[None]:
    """Acquire a cross-process lock without blocking the event-loop thread."""

    lock = FileLock(str(path), thread_local=False)
    while True:
        try:
            await asyncio.to_thread(lock.acquire, timeout=0)
            break
        except FileLockTimeout:
            await asyncio.sleep(0.01)
    try:
        yield
    finally:
        lock.release()


async def _deliver_path(
    path: Path,
    root: Path,
    *,
    connect: Callable[[], Awaitable[MessageTransport]],
    now: datetime,
    random: Random,
    request_timeout: float,
) -> tuple[str, str | None]:
    try:
        initial = read_notification_event(path, root)
    except (OSError, StateValidationError) as error:
        problem = f"{path}: {_sanitize_error(error)}"
        try:
            terminal_path = path.with_name(TERMINAL_FILE_NAME)
            terminal = read_terminal_event(terminal_path, root)
            if Path(terminal.terminal_state_path) != terminal_path.resolve(strict=False):
                raise StateValidationError(
                    "terminal_state_path does not identify the current terminal file"
                )
            failed = NotificationEvent.from_terminal(terminal).with_delivery_failure(
                attempted_at=now,
                error=_sanitize_error(error),
                next_attempt_at=None,
                exhausted=True,
            )
            with FileLock(str(path.parent / STATE_LOCK_NAME)):
                write_notification_event(failed, root)
        except (OSError, StateValidationError):
            return "failed", problem
        return "failed", problem
    if initial.state == "accepted":
        return "skipped", None
    if initial.state == "failed":
        return "failed", f"{path}: notification requires explicit requeue"
    if not _is_due(initial, now):
        return "skipped", None
    if initial.originating_thread_id is None:
        failed = initial.with_delivery_failure(
            attempted_at=now,
            error="notification has no originating Codex thread ID",
            next_attempt_at=None,
            exhausted=True,
        )
        with FileLock(str(path.parent / STATE_LOCK_NAME)):
            write_notification_event(failed, root)
        return "failed", f"{path}: notification has no originating Codex thread ID"

    thread_id = initial.originating_thread_id
    lock_path = notification_lock_path(root, thread_id)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    async with _async_file_lock(lock_path):
        try:
            with FileLock(str(path.parent / STATE_LOCK_NAME)):
                event = read_notification_event(path, root)
                terminal = read_terminal_event(Path(event.terminal_state_path), root)
                if event.as_terminal() != terminal:
                    raise StateValidationError("notification does not match terminal state")
            if event.state != "pending" or not _is_due(event, now):
                return "skipped", None
            ledger_path = _accepted_ledger_path(lock_path)
            ledger = _read_accepted_ledger(ledger_path, thread_id)
            prior = ledger.get(event.event_id)
            if prior is not None:
                accepted_at_value = prior.get("accepted_at")
                rpc_method = prior.get("rpc_method")
                turn_id = prior.get("turn_id")
                if not all(
                    isinstance(value, str) for value in (accepted_at_value, rpc_method, turn_id)
                ):
                    raise StateValidationError("accepted-event ledger entry is invalid")
                accepted_at = datetime.fromisoformat(
                    cast(str, accepted_at_value).replace("Z", "+00:00")
                )
                if accepted_at.tzinfo is None or accepted_at.utcoffset() is None:
                    raise StateValidationError("accepted-event ledger timestamp lacks a UTC offset")
                accepted_at = accepted_at.astimezone(UTC)
                accepted = replace(
                    event,
                    state="accepted",
                    last_attempt_at=accepted_at,
                    next_attempt_at=None,
                    last_error=None,
                    accepted_at=accepted_at,
                    accepted_rpc_method=cast(str, rpc_method),
                    accepted_turn_id=cast(str, turn_id),
                )
                with FileLock(str(path.parent / STATE_LOCK_NAME)):
                    write_notification_event(accepted, root)
                return "accepted", None

            transport = await connect()
            acceptance = await deliver_notification(
                event, transport, request_timeout=request_timeout
            )
            accepted_at = now
            accepted = event.with_acceptance(
                accepted_at=accepted_at,
                rpc_method=acceptance.rpc_method,
                turn_id=acceptance.turn_id,
            )
            ledger[event.event_id] = {
                "accepted_at": accepted_at.isoformat(),
                "rpc_method": acceptance.rpc_method,
                "turn_id": acceptance.turn_id,
            }
            _write_accepted_ledger(ledger_path, thread_id, ledger)
            with FileLock(str(path.parent / STATE_LOCK_NAME)):
                write_notification_event(accepted, root)
            return "accepted", None
        except BaseException as error:
            if isinstance(error, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            current = read_notification_event(path, root)
            attempt_count = current.attempt_count + 1
            permanent = isinstance(
                error, (StateValidationError, AppServerProtocolError)
            ) and getattr(error, "permanent", isinstance(error, StateValidationError))
            exhausted = permanent or attempt_count >= MAX_DELIVERY_ATTEMPTS
            delay_cap = min(
                RETRY_CAP_SECONDS,
                RETRY_BASE_SECONDS * (RETRY_FACTOR**current.attempt_count),
            )
            next_attempt_at = (
                None if exhausted else now + timedelta(seconds=random.uniform(0.0, delay_cap))
            )
            updated = current.with_delivery_failure(
                attempted_at=now,
                error=_sanitize_error(error),
                next_attempt_at=next_attempt_at,
                exhausted=exhausted,
            )
            with FileLock(str(path.parent / STATE_LOCK_NAME)):
                write_notification_event(updated, root)
            outcome = "failed" if exhausted else "retrying"
            return outcome, f"{path}: {updated.last_error}"


def _notification_paths(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob(NOTIFICATION_FILE_NAME)
        if path.parent.parent.name == "runs" and "attempts" not in path.parts
    )


async def sweep_notifications(
    root: Path,
    *,
    connect: Callable[[], Awaitable[MessageTransport]],
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    random: Random | None = None,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> SweepResult:
    """Process each due current notification at most once in this sweep."""

    requested_root = root.expanduser()
    if not requested_root.exists() and not requested_root.is_symlink():
        return SweepResult()
    managed_root = validate_managed_root(requested_root)
    selected_now = now()
    if selected_now.tzinfo is None or selected_now.utcoffset() is None:
        raise StateValidationError("worker clock must return an offset-aware datetime")
    selected_now = selected_now.astimezone(UTC)
    generator = random or Random()
    paths = _notification_paths(managed_root)
    counts = {
        "accepted": 0,
        "retrying": 0,
        "failed": 0,
        "skipped": 0,
    }
    due = 0
    problems: list[str] = []
    for path in paths:
        try:
            event = read_notification_event(path, managed_root)
            if _is_due(event, selected_now):
                due += 1
        except (OSError, StateValidationError):
            due += 1
        outcome, problem = await _deliver_path(
            path,
            managed_root,
            connect=connect,
            now=selected_now,
            random=generator,
            request_timeout=request_timeout,
        )
        counts[outcome] += 1
        if problem is not None:
            problems.append(problem)
    return SweepResult(
        discovered=len(paths),
        due=due,
        accepted=counts["accepted"],
        retrying=counts["retrying"],
        failed=counts["failed"],
        skipped=counts["skipped"],
        problems=tuple(problems),
    )


def stdio_connector(socket_path: Path | None = None) -> Callable[[], Awaitable[MessageTransport]]:
    async def connect() -> MessageTransport:
        return await JsonlStdioTransport.connect(socket_path)

    return connect


def unix_connector(socket_path: Path) -> Callable[[], Awaitable[MessageTransport]]:
    async def connect() -> MessageTransport:
        return await UnixWebSocketTransport.connect(socket_path)

    return connect
