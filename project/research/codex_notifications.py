"""Research event delivery through the shared notify-wake v2 runtime."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from random import Random
from typing import cast

from filelock import FileLock
from filelock import Timeout as FileLockTimeout
from notify_wake import (
    AppServerError,
    DeliveryPolicy,
    DeliveryState,
    MessageTransport,
    NotifyWaitLease,
    UnixWebSocketTransport,
    WakeContext,
    WakeRequest,
    capture_wake_context,
    deliver_wake,
    enter_notify_wait,
    reconcile_wake,
)
from notify_wake.models import normalize_datetime

from project.research.runtime import (
    MAX_LAST_ERROR_LENGTH,
    NOTIFICATION_FILE_NAME,
    STATE_LOCK_NAME,
    NotificationEvent,
    StateValidationError,
    _atomic_write_json,
    _load_json,
    notification_namespace,
    read_notification_event,
    validate_managed_root,
    write_notification_event,
)

APP_SERVER_BASELINE = "0.146.0"
DEFAULT_REQUEST_TIMEOUT = 15.0
RETRY_BASE_SECONDS = 5.0
RETRY_FACTOR = 2.0
RETRY_CAP_SECONDS = 300.0
GOAL_WAIT_DIRECTORY = "goal-waits"
AppServerProtocolError = AppServerError


@dataclass(frozen=True, slots=True)
class Acceptance:
    rpc_method: str
    turn_id: str


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


def build_wake_prompt(event: NotificationEvent) -> str:
    """Build one fixed prompt from validated research identifiers."""

    return (
        "Research run completed.\n"
        f"Study: {event.study_id}\n"
        f"Run: {event.run_id}\n"
        f"Status: {event.status}\n"
        f"Terminal state: {event.terminal_state_path}\n\n"
        "Inspect the terminal state and continue the study protocol."
    )


def notification_lock_path(root: Path, thread_id: str) -> Path:
    """Return the v2 per-thread delivery lock."""

    digest = hashlib.sha256(thread_id.encode()).hexdigest()
    return notification_namespace(root) / ".thread-locks" / f"{digest}.lock"


def goal_wait_path(root: Path, thread_id: str) -> Path:
    """Return the exact shared goal-wait lease path for one task."""

    digest = hashlib.sha256(thread_id.encode()).hexdigest()
    return notification_namespace(root) / GOAL_WAIT_DIRECTORY / f"{digest}.json"


def _read_goal_wait(root: Path, thread_id: str) -> NotifyWaitLease | None:
    path = goal_wait_path(root, thread_id)
    if not path.exists():
        return None
    try:
        return NotifyWaitLease.from_dict(_load_json(path))
    except ValueError as error:
        raise StateValidationError(f"goal-wait lease is invalid: {error}") from error


def _write_goal_wait(root: Path, lease: NotifyWaitLease) -> None:
    _atomic_write_json(goal_wait_path(root, lease.thread_id), lease.to_dict())


async def enter_research_notify_wait(
    root: Path,
    *,
    context: WakeContext,
    loop_id: str,
    source_ids: tuple[str, ...],
    transport: MessageTransport,
    verify_loop_identity: Callable[[str, tuple[str, ...]], bool],
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> NotifyWaitLease:
    """Block one exact active goal after its research controller is armed."""

    managed_root = validate_managed_root(root)
    async with _async_file_lock(notification_lock_path(managed_root, context.thread_id)):
        return await enter_notify_wait(
            context=context,
            loop_id=loop_id,
            source_ids=source_ids,
            transport=transport,
            persist_lease=lambda lease: _write_goal_wait(managed_root, lease),
            verify_loop_identity=verify_loop_identity,
            request_timeout=request_timeout,
        )


async def deliver_notification(
    event: NotificationEvent,
    transport: MessageTransport,
    *,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> Acceptance:
    """Deliver one event through the shared runtime without repository RPC code."""

    if event.wake_context is None:
        await transport.close()
        raise AppServerProtocolError(
            "notification has no version-2 wake context",
            permanent=True,
        )
    outcome = await deliver_wake(
        WakeRequest(
            event_id=event.event_id,
            prompt=build_wake_prompt(event),
            context=event.wake_context,
            policy=DeliveryPolicy.RESEARCH_COMPATIBILITY,
        ),
        transport,
        persist_request_boundary=lambda _method, _sent_at: None,
        request_timeout=request_timeout,
    )
    if (
        outcome.state == DeliveryState.ACCEPTED
        and outcome.rpc_method is not None
        and outcome.turn_id is not None
    ):
        return Acceptance(outcome.rpc_method, outcome.turn_id)
    raise AppServerProtocolError(
        outcome.error or f"wake delivery ended in {outcome.state}",
        permanent=outcome.state == DeliveryState.BLOCKED,
        request_may_have_reached=outcome.state == DeliveryState.UNCERTAIN,
    )


def _sanitize_error(error: BaseException) -> str:
    text = " ".join(str(error).split())
    return (text or error.__class__.__name__)[:MAX_LAST_ERROR_LENGTH]


def _is_due(event: NotificationEvent, now: datetime) -> bool:
    if event.state in {"in_flight", "uncertain"}:
        return True
    return event.state in {"pending", "retry_due"} and (
        event.next_attempt_at is None or event.next_attempt_at <= now
    )


@asynccontextmanager
async def _async_file_lock(path: Path) -> AsyncIterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _retry_event(
    event: NotificationEvent,
    *,
    attempted_at: datetime,
    error: str,
    random: Random,
) -> NotificationEvent:
    projected_attempts = event.attempt_count + (
        0 if event.state in {"in_flight", "uncertain"} else 1
    )
    exponent = max(projected_attempts - 1, 0)
    delay_cap = min(RETRY_CAP_SECONDS, RETRY_BASE_SECONDS * (RETRY_FACTOR**exponent))
    next_attempt_at = attempted_at + timedelta(seconds=random.uniform(0.0, delay_cap))
    delivery = event.delivery.schedule_retry(
        attempted_at=attempted_at,
        error=error,
        next_attempt_at=next_attempt_at,
        increment_attempt=event.state not in {"in_flight", "uncertain"},
    )
    return replace(event, delivery=delivery)


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
        return "failed", f"{path}: {_sanitize_error(error)}"
    if initial.state == "accepted":
        return "skipped", None
    if initial.state == "blocked":
        return "failed", f"{path}: notification requires explicit requeue"
    if not _is_due(initial, now):
        return "skipped", None
    if initial.wake_context is None:
        blocked = replace(
            initial,
            delivery=initial.delivery.mark_blocked(
                attempted_at=now,
                error="notification has no version-2 wake context",
            ),
        )
        write_notification_event(blocked, root)
        return "failed", f"{path}: notification has no version-2 wake context"

    thread_id = initial.delivery.thread_id
    async with _async_file_lock(notification_lock_path(root, thread_id)):
        event = read_notification_event(path, root)
        if not _is_due(event, now):
            return "skipped", None
        try:
            lease = _read_goal_wait(root, thread_id)
            transport = await connect()

            def persist_boundary(rpc_method: str, sent_at: datetime) -> None:
                current = read_notification_event(path, root)
                updated = replace(
                    current,
                    delivery=current.delivery.mark_in_flight(
                        sent_at,
                        rpc_method=rpc_method,
                    ),
                )
                with FileLock(str(path.parent / STATE_LOCK_NAME)):
                    write_notification_event(updated, root)

            request = WakeRequest(
                event_id=event.event_id,
                prompt=build_wake_prompt(event),
                context=cast(WakeContext, event.wake_context),
            )
            if event.delivery.requires_history_reconciliation:
                attempted_rpc_method = event.delivery.attempted_rpc_method
                if attempted_rpc_method is None:
                    raise StateValidationError("uncertain notification lacks attempted_rpc_method")
                outcome = await reconcile_wake(
                    request,
                    transport,
                    attempted_rpc_method=attempted_rpc_method,
                    lease=lease,
                    persist_lease=lambda selected: _write_goal_wait(root, selected),
                    now=lambda: now,
                    request_timeout=request_timeout,
                )
            else:
                outcome = await deliver_wake(
                    request,
                    transport,
                    persist_request_boundary=persist_boundary,
                    lease=lease,
                    persist_lease=lambda selected: _write_goal_wait(root, selected),
                    now=lambda: now,
                    request_timeout=request_timeout,
                )

            current = read_notification_event(path, root)
            if outcome.state == DeliveryState.ACCEPTED:
                if outcome.rpc_method is None or outcome.turn_id is None:
                    raise StateValidationError("accepted wake lacks turn metadata")
                updated = replace(
                    current,
                    delivery=current.delivery.mark_accepted(
                        accepted_at=now,
                        rpc_method=outcome.rpc_method,
                        turn_id=outcome.turn_id,
                    ),
                )
                result = "accepted"
                problem = None
            elif outcome.state == DeliveryState.UNCERTAIN:
                sent_at = current.delivery.request_sent_at or now
                updated = replace(
                    current,
                    delivery=current.delivery.mark_uncertain(
                        sent_at=sent_at,
                        reason=outcome.error or "wake acknowledgment is uncertain",
                    ),
                )
                result = "retrying"
                problem = f"{path}: {outcome.error}"
            elif outcome.state == DeliveryState.BLOCKED:
                if current.delivery.requires_history_reconciliation:
                    delivery = current.delivery.mark_reconciliation_blocked(
                        attempted_at=now,
                        error=outcome.error or "history reconciliation is blocked",
                    )
                else:
                    delivery = current.delivery.mark_blocked(
                        attempted_at=now,
                        error=outcome.error or "wake delivery is blocked",
                    )
                updated = replace(current, delivery=delivery)
                result = "failed"
                problem = f"{path}: {delivery.last_error}"
            else:
                updated = _retry_event(
                    current,
                    attempted_at=now,
                    error=outcome.error or "wake delivery retry is due",
                    random=random,
                )
                result = "failed" if updated.state == "blocked" else "retrying"
                problem = f"{path}: {updated.last_error}"
        except BaseException as error:
            if isinstance(error, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            current = read_notification_event(path, root)
            if isinstance(error, AppServerError) and error.permanent:
                updated = replace(
                    current,
                    delivery=current.delivery.mark_blocked(
                        attempted_at=now,
                        error=_sanitize_error(error),
                    ),
                )
            else:
                updated = _retry_event(
                    current,
                    attempted_at=now,
                    error=_sanitize_error(error),
                    random=random,
                )
            result = "failed" if updated.state == "blocked" else "retrying"
            problem = f"{path}: {updated.last_error}"
        with FileLock(str(path.parent / STATE_LOCK_NAME)):
            write_notification_event(updated, root)
        return result, problem


def _notification_paths(root: Path) -> list[Path]:
    events_root = notification_namespace(root) / "events"
    if not events_root.exists():
        return []
    return sorted(events_root.glob(f"*/{NOTIFICATION_FILE_NAME}"))


def next_notification_attempt_at(root: Path) -> datetime | None:
    """Return the earliest retry in the exact version-2 namespace."""

    requested_root = root.expanduser()
    if not requested_root.exists() and not requested_root.is_symlink():
        return None
    managed_root = validate_managed_root(requested_root)
    retry_deadlines: list[datetime] = []
    for path in _notification_paths(managed_root):
        try:
            event = read_notification_event(path, managed_root)
        except (OSError, StateValidationError):
            continue
        if event.state == "retry_due" and event.next_attempt_at is not None:
            retry_deadlines.append(event.next_attempt_at)
    return min(retry_deadlines) if retry_deadlines else None


async def sweep_notifications(
    root: Path,
    *,
    connect: Callable[[], Awaitable[MessageTransport]],
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    random: Random | None = None,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> SweepResult:
    """Process every due v2 notification at most once."""

    requested_root = root.expanduser()
    if not requested_root.exists() and not requested_root.is_symlink():
        return SweepResult()
    managed_root = validate_managed_root(requested_root)
    selected_now = normalize_datetime(now(), "worker clock")
    generator = random or Random()
    paths = _notification_paths(managed_root)
    counts = {"accepted": 0, "retrying": 0, "failed": 0, "skipped": 0}
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


def unix_connector(socket_path: Path) -> Callable[[], Awaitable[MessageTransport]]:
    async def connect() -> MessageTransport:
        return await UnixWebSocketTransport.connect(socket_path)

    return connect


__all__ = [
    "APP_SERVER_BASELINE",
    "Acceptance",
    "AppServerProtocolError",
    "MessageTransport",
    "SweepResult",
    "UnixWebSocketTransport",
    "build_wake_prompt",
    "capture_wake_context",
    "deliver_notification",
    "enter_research_notify_wait",
    "goal_wait_path",
    "next_notification_attempt_at",
    "notification_lock_path",
    "sweep_notifications",
    "unix_connector",
]
