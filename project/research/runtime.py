"""Durable terminal-event and notification queue persistence.

This module owns local state only. It never connects to Codex and never changes a
terminal run result because notification delivery failed.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID, uuid4

import yaml
from filelock import FileLock

SCHEMA_VERSION = 1
STATE_LOCK_NAME = ".state.lock"
TERMINAL_FILE_NAME = "terminal.json"
NOTIFICATION_FILE_NAME = "notification.json"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
TERMINAL_STATUSES = frozenset({"completed", "failed", "crashed", "timed_out", "cancelled"})
DELIVERY_STATES = frozenset({"pending", "accepted", "failed"})
MAX_LAST_ERROR_LENGTH = 500
MANAGED_ROOT_SCHEMA_VERSION = 1
MANAGED_ROOT_FILE_NAME = ".autoresearch-root.json"
MANAGED_ROOT_KIND = "autoresearch-managed-root"
MANAGED_ROOT_FIELDS = frozenset({"schema_version", "kind", "root_path"})
RESEARCH_LOG_SCHEMA_VERSION = 1
RESEARCH_LOG_MARKER_PREFIX = "<!-- autoresearch-log "
RESEARCH_LOG_MARKER_SUFFIX = " -->"
RESEARCH_LOG_METADATA_FIELDS = frozenset(
    {"schema_version", "kind", "operation_id", "content_sha256", "terminal"}
)

TerminalStatus = Literal["completed", "failed", "crashed", "timed_out", "cancelled"]
DeliveryState = Literal["pending", "accepted", "failed"]


class StateValidationError(ValueError):
    """Persisted or requested research state violates the schema or path contract."""


@dataclass(frozen=True, slots=True)
class ManagedRootRegistration:
    """Result of idempotently registering one exact managed root."""

    root: Path
    marker: Path
    created: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "created": self.created,
            "marker": str(self.marker),
            "root": str(self.root),
        }


def _validate_identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_PATTERN.fullmatch(value):
        raise StateValidationError(f"{field_name} is not a safe identifier: {value!r}")
    return value


def _validate_thread_id(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > 512:
        raise StateValidationError("originating_thread_id must be a non-empty string or null")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise StateValidationError("originating_thread_id must not contain control characters")
    return value


def _validate_event_id(value: object) -> str:
    if not isinstance(value, str):
        raise StateValidationError("event_id must be a UUID string")
    try:
        parsed = UUID(value)
    except ValueError as error:
        raise StateValidationError("event_id must be a UUID string") from error
    if str(parsed) != value.lower():
        raise StateValidationError("event_id must use canonical UUID text")
    return value.lower()


def _validate_attempt(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise StateValidationError("attempt must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class TerminalLogIdentity:
    """Stable identity for one terminal run attempt in the research log."""

    study_id: str
    run_id: str
    attempt: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "study_id", _validate_identifier(self.study_id, "study id"))
        object.__setattr__(self, "run_id", _validate_identifier(self.run_id, "run id"))
        object.__setattr__(self, "attempt", _validate_attempt(self.attempt))

    def to_dict(self) -> dict[str, object]:
        return {
            "attempt": self.attempt,
            "run_id": self.run_id,
            "study_id": self.study_id,
        }


@dataclass(frozen=True, slots=True)
class _ResearchLogRecord:
    kind: Literal["header", "entry"]
    operation_id: str
    content_sha256: str
    terminal: TerminalLogIdentity | None
    markdown: str


def _normalize_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise StateValidationError(f"{field_name} must include a UTC offset")
    return value.astimezone(UTC)


def _parse_datetime(value: object, field_name: str, *, optional: bool = False) -> datetime | None:
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise StateValidationError(f"{field_name} must be an ISO 8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise StateValidationError(f"{field_name} must be an ISO 8601 string") from error
    return _normalize_utc(parsed, field_name)


def _isoformat(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _require_mapping(payload: object, source: Path) -> dict[str, Any]:
    if not isinstance(payload, dict) or not all(isinstance(key, str) for key in payload):
        raise StateValidationError(f"state in {source} must be a JSON object")
    return cast(dict[str, Any], payload)


def _require_keys(payload: dict[str, Any], expected: frozenset[str], source: Path) -> None:
    present = frozenset(payload)
    if present == expected:
        return
    missing = sorted(expected - present)
    extra = sorted(present - expected)
    details: list[str] = []
    if missing:
        details.append(f"missing {missing}")
    if extra:
        details.append(f"unexpected {extra}")
    raise StateValidationError(f"invalid fields in {source}: {', '.join(details)}")


def _resolved_managed_path(path: Path, managed_root: Path, field_name: str) -> Path:
    if not path.is_absolute():
        raise StateValidationError(f"{field_name} must be absolute")
    root = managed_root.expanduser().resolve(strict=False)
    resolved = path.expanduser().resolve(strict=False)
    if resolved == root or not resolved.is_relative_to(root):
        raise StateValidationError(f"{field_name} must remain inside the managed root {root}")
    return resolved


def _lexical_absolute(path: Path) -> Path:
    return Path(os.path.abspath(path.expanduser()))


def _validated_root_target(root: Path) -> Path:
    lexical = _lexical_absolute(root)
    resolved = lexical.resolve(strict=False)
    if lexical != resolved:
        raise StateValidationError("managed root must not contain symlink path components")
    if resolved == Path(resolved.anchor):
        raise StateValidationError("managed root must not be a filesystem root")
    if resolved.parent == Path(resolved.anchor):
        raise StateValidationError("managed root must not be a top-level directory")

    home = Path.home().resolve(strict=False)
    if resolved == home or home.is_relative_to(resolved):
        raise StateValidationError("managed root must not be a home directory or its parent")

    current = Path.cwd().resolve(strict=False)
    if resolved == current or current.is_relative_to(resolved):
        if (resolved / ".git").exists():
            raise StateValidationError("managed root must not be a repository root")
        raise StateValidationError("managed root must not be a broad working-directory parent")
    if (resolved / ".git").exists():
        raise StateValidationError("managed root must not be a repository root")
    if resolved.exists() and not resolved.is_dir():
        raise StateValidationError("managed root must be a directory")
    return resolved


def _managed_root_marker(root: Path) -> Path:
    return root / MANAGED_ROOT_FILE_NAME


@dataclass(frozen=True, slots=True)
class StudyConfig:
    """Minimal validated study configuration used by the template runtime."""

    id: str
    log_root: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _validate_identifier(self.id, "study id"))
        root = self.log_root.expanduser().resolve(strict=False)
        if root == Path(root.anchor):
            raise StateValidationError("log_root must not be a filesystem root")
        object.__setattr__(self, "log_root", root)

    @classmethod
    def load(cls, path: Path | str, *, base_dir: Path | None = None) -> StudyConfig:
        """Load a minimal study YAML file.

        Relative log roots resolve from ``base_dir`` or the current working
        directory. This keeps the committed example usable from the repository
        root while allowing callers to supply an explicit project root.
        """

        source = Path(path).expanduser().resolve(strict=False)
        try:
            raw = yaml.safe_load(source.read_text(encoding="utf-8"))
        except OSError as error:
            raise OSError(f"could not read study configuration {source}: {error}") from error
        except yaml.YAMLError as error:
            raise StateValidationError(
                f"could not read study configuration {source}: {error}"
            ) from error
        payload = _require_mapping(raw, source)
        _require_keys(payload, frozenset({"id", "log_root"}), source)
        log_root_value = payload["log_root"]
        if not isinstance(log_root_value, str) or not log_root_value:
            raise StateValidationError("log_root must be a non-empty path string")
        log_root = Path(os.path.expandvars(log_root_value)).expanduser()
        if not log_root.is_absolute():
            log_root = (base_dir or Path.cwd()) / log_root
        return cls(id=_validate_identifier(payload["id"], "study id"), log_root=log_root)

    def run_dir(self, run_id: str) -> Path:
        """Return the validated current-state directory for a run."""

        validated_run = _validate_identifier(run_id, "run id")
        candidate = self.log_root / self.id / "runs" / validated_run
        return _resolved_managed_path(candidate.absolute(), self.log_root, "run path")


TERMINAL_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "study_id",
        "run_id",
        "attempt",
        "status",
        "occurred_at",
        "originating_thread_id",
        "terminal_state_path",
    }
)


@dataclass(frozen=True, slots=True)
class TerminalEvent:
    """A durable terminal outcome. Notification state cannot change these fields."""

    schema_version: int
    event_id: str
    study_id: str
    run_id: str
    attempt: int
    status: TerminalStatus
    occurred_at: datetime
    originating_thread_id: str | None
    terminal_state_path: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.schema_version, int)
            or isinstance(self.schema_version, bool)
            or self.schema_version != SCHEMA_VERSION
        ):
            raise StateValidationError(f"unsupported state schema version: {self.schema_version!r}")
        object.__setattr__(self, "event_id", _validate_event_id(self.event_id))
        object.__setattr__(self, "study_id", _validate_identifier(self.study_id, "study id"))
        object.__setattr__(self, "run_id", _validate_identifier(self.run_id, "run id"))
        object.__setattr__(self, "attempt", _validate_attempt(self.attempt))
        if self.status not in TERMINAL_STATUSES:
            raise StateValidationError(f"invalid terminal status: {self.status!r}")
        object.__setattr__(self, "occurred_at", _normalize_utc(self.occurred_at, "occurred_at"))
        object.__setattr__(
            self, "originating_thread_id", _validate_thread_id(self.originating_thread_id)
        )
        if not isinstance(self.terminal_state_path, str):
            raise StateValidationError("terminal_state_path must be a string")

    @classmethod
    def from_dict(cls, payload: dict[str, Any], source: Path, managed_root: Path) -> TerminalEvent:
        _require_keys(payload, TERMINAL_FIELDS, source)
        terminal_path_value = payload["terminal_state_path"]
        if not isinstance(terminal_path_value, str):
            raise StateValidationError("terminal_state_path must be a string")
        terminal_path = Path(terminal_path_value)
        resolved_terminal = _resolved_managed_path(
            terminal_path, managed_root, "terminal_state_path"
        )
        status = payload["status"]
        if not isinstance(status, str) or status not in TERMINAL_STATUSES:
            raise StateValidationError(f"invalid terminal status: {status!r}")
        occurred_at = _parse_datetime(payload["occurred_at"], "occurred_at")
        assert occurred_at is not None
        event = cls(
            schema_version=payload["schema_version"],
            event_id=payload["event_id"],
            study_id=payload["study_id"],
            run_id=payload["run_id"],
            attempt=payload["attempt"],
            status=cast(TerminalStatus, status),
            occurred_at=occurred_at,
            originating_thread_id=payload["originating_thread_id"],
            terminal_state_path=str(resolved_terminal),
        )
        expected_terminal = (
            managed_root.resolve(strict=False)
            / event.study_id
            / "runs"
            / event.run_id
            / TERMINAL_FILE_NAME
        ).resolve(strict=False)
        if resolved_terminal != expected_terminal:
            raise StateValidationError(
                "terminal_state_path does not match the study and run identifiers"
            )
        return event

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "study_id": self.study_id,
            "run_id": self.run_id,
            "attempt": self.attempt,
            "status": self.status,
            "occurred_at": _isoformat(self.occurred_at),
            "originating_thread_id": self.originating_thread_id,
            "terminal_state_path": self.terminal_state_path,
        }


NOTIFICATION_FIELDS = TERMINAL_FIELDS | frozenset(
    {
        "state",
        "attempt_count",
        "last_attempt_at",
        "next_attempt_at",
        "last_error",
        "accepted_at",
        "accepted_rpc_method",
        "accepted_turn_id",
    }
)


@dataclass(frozen=True, slots=True)
class NotificationEvent:
    """Durable delivery state for one terminal event."""

    schema_version: int
    event_id: str
    study_id: str
    run_id: str
    attempt: int
    status: TerminalStatus
    occurred_at: datetime
    originating_thread_id: str | None
    terminal_state_path: str
    state: DeliveryState
    attempt_count: int
    last_attempt_at: datetime | None
    next_attempt_at: datetime | None
    last_error: str | None
    accepted_at: datetime | None
    accepted_rpc_method: str | None
    accepted_turn_id: str | None

    def __post_init__(self) -> None:
        terminal = self.as_terminal()
        if self.state not in DELIVERY_STATES:
            raise StateValidationError(f"invalid delivery state: {self.state!r}")
        if not isinstance(self.attempt_count, int) or isinstance(self.attempt_count, bool):
            raise StateValidationError("attempt_count must be a non-negative integer")
        if self.attempt_count < 0:
            raise StateValidationError("attempt_count must be a non-negative integer")
        for field_name in ("last_attempt_at", "next_attempt_at", "accepted_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _normalize_utc(value, field_name))
        if self.last_error is not None:
            if not isinstance(self.last_error, str) or not self.last_error:
                raise StateValidationError("last_error must be a non-empty string or null")
            if len(self.last_error) > MAX_LAST_ERROR_LENGTH or any(
                ord(character) < 32 or ord(character) == 127 for character in self.last_error
            ):
                raise StateValidationError(
                    "last_error must be sanitized and at most 500 characters"
                )
        for field_name in ("accepted_rpc_method", "accepted_turn_id"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value):
                raise StateValidationError(f"{field_name} must be a non-empty string or null")
        if self.state == "accepted":
            if self.accepted_at is None or self.accepted_rpc_method not in {
                "turn/start",
                "turn/steer",
            }:
                raise StateValidationError("accepted notification is missing acceptance metadata")
            if self.accepted_turn_id is None:
                raise StateValidationError("accepted notification is missing accepted_turn_id")
            if (
                self.last_attempt_at != self.accepted_at
                or self.next_attempt_at is not None
                or self.last_error is not None
            ):
                raise StateValidationError(
                    "accepted notification has inconsistent delivery metadata"
                )
        elif any(
            value is not None
            for value in (self.accepted_at, self.accepted_rpc_method, self.accepted_turn_id)
        ):
            raise StateValidationError(
                "unaccepted notification must not contain acceptance metadata"
            )
        if self.state == "failed" and (
            self.attempt_count < 1
            or self.last_attempt_at is None
            or self.next_attempt_at is not None
            or self.last_error is None
        ):
            raise StateValidationError("failed notification has inconsistent delivery metadata")
        if self.state == "pending":
            retry_metadata = (self.last_attempt_at, self.next_attempt_at, self.last_error)
            if self.attempt_count == 0 and any(value is not None for value in retry_metadata):
                raise StateValidationError(
                    "new pending notification must not contain retry metadata"
                )
            if self.attempt_count > 0 and any(value is None for value in retry_metadata):
                raise StateValidationError(
                    "retried pending notification has incomplete retry metadata"
                )
        del terminal

    def as_terminal(self) -> TerminalEvent:
        return TerminalEvent(
            schema_version=self.schema_version,
            event_id=self.event_id,
            study_id=self.study_id,
            run_id=self.run_id,
            attempt=self.attempt,
            status=self.status,
            occurred_at=self.occurred_at,
            originating_thread_id=self.originating_thread_id,
            terminal_state_path=self.terminal_state_path,
        )

    @classmethod
    def from_terminal(cls, terminal: TerminalEvent) -> NotificationEvent:
        return cls(
            schema_version=terminal.schema_version,
            event_id=terminal.event_id,
            study_id=terminal.study_id,
            run_id=terminal.run_id,
            attempt=terminal.attempt,
            status=terminal.status,
            occurred_at=terminal.occurred_at,
            originating_thread_id=terminal.originating_thread_id,
            terminal_state_path=terminal.terminal_state_path,
            state="pending",
            attempt_count=0,
            last_attempt_at=None,
            next_attempt_at=None,
            last_error=None,
            accepted_at=None,
            accepted_rpc_method=None,
            accepted_turn_id=None,
        )

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        source: Path,
        managed_root: Path,
        *,
        terminal: TerminalEvent | None = None,
    ) -> NotificationEvent:
        _require_keys(payload, NOTIFICATION_FIELDS, source)
        terminal_payload = {key: payload[key] for key in TERMINAL_FIELDS}
        parsed_terminal = TerminalEvent.from_dict(terminal_payload, source, managed_root)
        state = payload["state"]
        if not isinstance(state, str) or state not in DELIVERY_STATES:
            raise StateValidationError(f"invalid delivery state: {state!r}")
        parsed = cls(
            schema_version=parsed_terminal.schema_version,
            event_id=parsed_terminal.event_id,
            study_id=parsed_terminal.study_id,
            run_id=parsed_terminal.run_id,
            attempt=parsed_terminal.attempt,
            status=parsed_terminal.status,
            occurred_at=parsed_terminal.occurred_at,
            originating_thread_id=parsed_terminal.originating_thread_id,
            terminal_state_path=parsed_terminal.terminal_state_path,
            state=cast(DeliveryState, state),
            attempt_count=payload["attempt_count"],
            last_attempt_at=_parse_datetime(
                payload["last_attempt_at"], "last_attempt_at", optional=True
            ),
            next_attempt_at=_parse_datetime(
                payload["next_attempt_at"], "next_attempt_at", optional=True
            ),
            last_error=payload["last_error"],
            accepted_at=_parse_datetime(payload["accepted_at"], "accepted_at", optional=True),
            accepted_rpc_method=payload["accepted_rpc_method"],
            accepted_turn_id=payload["accepted_turn_id"],
        )
        if terminal is not None and parsed.as_terminal() != terminal:
            raise StateValidationError(f"notification in {source} does not match terminal state")
        return parsed

    def to_dict(self) -> dict[str, object]:
        return {
            **self.as_terminal().to_dict(),
            "state": self.state,
            "attempt_count": self.attempt_count,
            "last_attempt_at": _isoformat(self.last_attempt_at),
            "next_attempt_at": _isoformat(self.next_attempt_at),
            "last_error": self.last_error,
            "accepted_at": _isoformat(self.accepted_at),
            "accepted_rpc_method": self.accepted_rpc_method,
            "accepted_turn_id": self.accepted_turn_id,
        }

    def with_delivery_failure(
        self,
        *,
        attempted_at: datetime,
        error: str,
        next_attempt_at: datetime | None,
        exhausted: bool,
    ) -> NotificationEvent:
        return replace(
            self,
            state="failed" if exhausted else "pending",
            attempt_count=self.attempt_count + 1,
            last_attempt_at=attempted_at,
            next_attempt_at=None if exhausted else next_attempt_at,
            last_error=error,
            accepted_at=None,
            accepted_rpc_method=None,
            accepted_turn_id=None,
        )

    def with_acceptance(
        self, *, accepted_at: datetime, rpc_method: str, turn_id: str
    ) -> NotificationEvent:
        return replace(
            self,
            state="accepted",
            attempt_count=self.attempt_count + 1,
            last_attempt_at=accepted_at,
            next_attempt_at=None,
            last_error=None,
            accepted_at=accepted_at,
            accepted_rpc_method=rpc_method,
            accepted_turn_id=turn_id,
        )

    def requeued(self) -> NotificationEvent:
        if self.state != "failed":
            raise StateValidationError("only failed notifications can be requeued")
        return replace(
            self,
            state="pending",
            attempt_count=0,
            last_attempt_at=None,
            next_attempt_at=None,
            last_error=None,
            accepted_at=None,
            accepted_rpc_method=None,
            accepted_turn_id=None,
        )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise
    except OSError:
        raise
    except (UnicodeError, json.JSONDecodeError) as error:
        raise StateValidationError(f"state in {path} is not valid JSON: {error}") from error
    return _require_mapping(raw, path)


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    """Atomically replace JSON through a synced same-directory temporary file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def validate_managed_root(root: Path) -> Path:
    """Return the canonical root after validating its exact registration marker."""

    resolved = _validated_root_target(root)
    if not resolved.exists():
        raise StateValidationError(f"managed root does not exist: {resolved}")
    marker = _managed_root_marker(resolved)
    if marker.is_symlink():
        raise StateValidationError("managed-root marker must not be a symlink")
    if not marker.exists():
        raise StateValidationError(f"managed root is not registered: {resolved}")
    if not marker.is_file():
        raise StateValidationError(f"managed-root marker must be a file: {marker}")
    payload = _load_json(marker)
    _require_keys(payload, MANAGED_ROOT_FIELDS, marker)
    schema_version = payload["schema_version"]
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != MANAGED_ROOT_SCHEMA_VERSION
    ):
        raise StateValidationError(f"unsupported managed-root schema version: {schema_version!r}")
    if payload["kind"] != MANAGED_ROOT_KIND:
        raise StateValidationError(f"invalid managed-root marker kind: {payload['kind']!r}")
    if payload["root_path"] != str(resolved):
        raise StateValidationError("managed-root marker does not identify the exact root")
    return resolved


def register_managed_root(root: Path) -> ManagedRootRegistration:
    """Atomically register one safe root without scanning or replacing its contents."""

    resolved = _validated_root_target(root)
    resolved.mkdir(parents=True, exist_ok=True)
    resolved = _validated_root_target(resolved)
    marker = _managed_root_marker(resolved)
    if marker.is_symlink():
        raise StateValidationError("managed-root marker must not be a symlink")
    if marker.exists():
        validate_managed_root(resolved)
        return ManagedRootRegistration(root=resolved, marker=marker, created=False)
    _atomic_write_json(
        marker,
        {
            "schema_version": MANAGED_ROOT_SCHEMA_VERSION,
            "kind": MANAGED_ROOT_KIND,
            "root_path": str(resolved),
        },
    )
    return ManagedRootRegistration(root=resolved, marker=marker, created=True)


def _atomic_write_text(path: Path, content: str) -> None:
    """Atomically replace UTF-8 text through a synced same-directory file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def _normalize_markdown(markdown: str, field_name: str) -> str:
    if not isinstance(markdown, str) or not markdown.strip():
        raise StateValidationError(f"{field_name} must be non-empty Markdown")
    if RESEARCH_LOG_MARKER_PREFIX in markdown:
        raise StateValidationError(f"{field_name} contains reserved metadata")
    return f"{markdown.rstrip()}\n"


def _research_log_marker(
    *,
    kind: Literal["header", "entry"],
    operation_id: str,
    markdown: str,
    terminal: TerminalLogIdentity | None,
) -> str:
    metadata = {
        "schema_version": RESEARCH_LOG_SCHEMA_VERSION,
        "kind": kind,
        "operation_id": operation_id,
        "content_sha256": hashlib.sha256(markdown.encode()).hexdigest(),
        "terminal": None if terminal is None else terminal.to_dict(),
    }
    encoded = json.dumps(metadata, separators=(",", ":"), sort_keys=True)
    return f"{RESEARCH_LOG_MARKER_PREFIX}{encoded}{RESEARCH_LOG_MARKER_SUFFIX}"


def _parse_terminal_log_identity(value: object, source: Path) -> TerminalLogIdentity | None:
    if value is None:
        return None
    payload = _require_mapping(value, source)
    _require_keys(payload, frozenset({"study_id", "run_id", "attempt"}), source)
    return TerminalLogIdentity(
        study_id=payload["study_id"],
        run_id=payload["run_id"],
        attempt=payload["attempt"],
    )


def _parse_research_log(log_path: Path, content: str) -> list[_ResearchLogRecord]:
    lines = content.splitlines(keepends=True)
    marker_indices = [
        index for index, line in enumerate(lines) if line.startswith(RESEARCH_LOG_MARKER_PREFIX)
    ]
    if not marker_indices or marker_indices[0] != 0:
        raise StateValidationError(f"research log metadata is missing or misplaced: {log_path}")
    records: list[_ResearchLogRecord] = []
    for position, marker_index in enumerate(marker_indices):
        marker_line = lines[marker_index].removesuffix("\n")
        if not marker_line.endswith(RESEARCH_LOG_MARKER_SUFFIX):
            raise StateValidationError(f"research log metadata is malformed: {log_path}")
        encoded = marker_line[len(RESEARCH_LOG_MARKER_PREFIX) : -len(RESEARCH_LOG_MARKER_SUFFIX)]
        try:
            raw_metadata = json.loads(encoded)
        except json.JSONDecodeError as error:
            raise StateValidationError(f"research log metadata is malformed: {error}") from error
        metadata = _require_mapping(raw_metadata, log_path)
        _require_keys(metadata, RESEARCH_LOG_METADATA_FIELDS, log_path)
        if metadata["schema_version"] != RESEARCH_LOG_SCHEMA_VERSION:
            raise StateValidationError(
                f"unsupported research-log schema version: {metadata['schema_version']!r}"
            )
        kind = metadata["kind"]
        if kind not in {"header", "entry"}:
            raise StateValidationError(f"invalid research-log record kind: {kind!r}")
        operation_id = _validate_identifier(metadata["operation_id"], "operation id")
        digest = metadata["content_sha256"]
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise StateValidationError("research-log content digest is invalid")
        body_end = (
            marker_indices[position + 1] if position + 1 < len(marker_indices) else len(lines)
        )
        body = "".join(lines[marker_index + 1 : body_end])
        if not body.endswith("\n\n"):
            raise StateValidationError("research-log record is not a complete Markdown block")
        markdown = body[:-1]
        if hashlib.sha256(markdown.encode()).hexdigest() != digest:
            raise StateValidationError("research-log record content does not match its metadata")
        records.append(
            _ResearchLogRecord(
                kind=cast(Literal["header", "entry"], kind),
                operation_id=operation_id,
                content_sha256=digest,
                terminal=_parse_terminal_log_identity(metadata["terminal"], log_path),
                markdown=markdown,
            )
        )
    if records[0].kind != "header" or any(record.kind == "header" for record in records[1:]):
        raise StateValidationError("research log must contain exactly one leading header")
    return records


def _render_research_log_record(
    *,
    kind: Literal["header", "entry"],
    operation_id: str,
    markdown: str,
    terminal: TerminalLogIdentity | None,
) -> str:
    marker = _research_log_marker(
        kind=kind,
        operation_id=operation_id,
        markdown=markdown,
        terminal=terminal,
    )
    return f"{marker}\n{markdown}\n"


def append_research_log(
    log_path: Path,
    *,
    managed_root: Path,
    header_operation_id: str,
    header_markdown: str,
    operation_id: str,
    markdown: str,
    terminal: TerminalLogIdentity | None = None,
) -> bool:
    """Append one complete Markdown update under a stable sibling lock.

    Return ``False`` when the operation or terminal attempt is already present.
    """

    resolved_path = _resolved_managed_path(log_path.absolute(), managed_root, "research log path")
    normalized_header = _normalize_markdown(header_markdown, "header_markdown")
    normalized_markdown = _normalize_markdown(markdown, "markdown")
    validated_header_id = _validate_identifier(header_operation_id, "header operation id")
    validated_operation_id = _validate_identifier(operation_id, "operation id")
    if validated_header_id == validated_operation_id:
        raise StateValidationError("header and entry operation IDs must differ")
    lock_path = resolved_path.with_name(f".{resolved_path.name}.lock")
    if lock_path.is_symlink():
        raise StateValidationError("research-log lock must not be a symlink")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(lock_path)):
        if resolved_path.exists():
            try:
                content = resolved_path.read_text(encoding="utf-8")
            except UnicodeError as error:
                raise StateValidationError(f"research log is not valid UTF-8: {error}") from error
            records = _parse_research_log(resolved_path, content)
            header = records[0]
            expected_header_digest = hashlib.sha256(normalized_header.encode()).hexdigest()
            if (
                header.operation_id != validated_header_id
                or header.content_sha256 != expected_header_digest
                or header.terminal is not None
            ):
                raise StateValidationError("research log header differs from the requested header")
        else:
            content = _render_research_log_record(
                kind="header",
                operation_id=validated_header_id,
                markdown=normalized_header,
                terminal=None,
            )
            records = []

        expected_digest = hashlib.sha256(normalized_markdown.encode()).hexdigest()
        for record in records:
            if record.operation_id == validated_operation_id:
                if (
                    record.kind == "entry"
                    and record.content_sha256 == expected_digest
                    and record.terminal == terminal
                ):
                    return False
                raise StateValidationError(
                    f"operation {validated_operation_id!r} already exists with different content"
                )
        if terminal is not None and any(record.terminal == terminal for record in records):
            return False

        content += _render_research_log_record(
            kind="entry",
            operation_id=validated_operation_id,
            markdown=normalized_markdown,
            terminal=terminal,
        )
        _atomic_write_text(resolved_path, content)
        return True


def read_terminal_event(path: Path, managed_root: Path) -> TerminalEvent:
    """Read and validate a terminal state file."""

    resolved_path = _resolved_managed_path(path.absolute(), managed_root, "terminal file path")
    return TerminalEvent.from_dict(_load_json(resolved_path), resolved_path, managed_root)


def read_notification_event(
    path: Path, managed_root: Path, *, terminal: TerminalEvent | None = None
) -> NotificationEvent:
    """Read and validate a queued notification and optional matching terminal event."""

    resolved_path = _resolved_managed_path(path.absolute(), managed_root, "notification file path")
    return NotificationEvent.from_dict(
        _load_json(resolved_path), resolved_path, managed_root, terminal=terminal
    )


def write_notification_event(event: NotificationEvent, managed_root: Path) -> None:
    """Persist notification delivery state without modifying terminal state."""

    validated_root = validate_managed_root(managed_root)
    terminal_path = _resolved_managed_path(
        Path(event.terminal_state_path), validated_root, "terminal_state_path"
    )
    _atomic_write_json(terminal_path.with_name(NOTIFICATION_FILE_NAME), event.to_dict())


def _archive_current_pair(run_dir: Path, managed_root: Path, terminal: TerminalEvent) -> None:
    notification_path = run_dir / NOTIFICATION_FILE_NAME
    if notification_path.exists():
        notification = read_notification_event(notification_path, managed_root, terminal=terminal)
    else:
        notification = NotificationEvent.from_terminal(terminal)
    archive_dir = run_dir / "attempts" / f"{terminal.attempt}-{terminal.event_id}"
    _resolved_managed_path(archive_dir.absolute(), managed_root, "archive path")
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived_terminal_path = archive_dir / TERMINAL_FILE_NAME
    archived_notification_path = archive_dir / NOTIFICATION_FILE_NAME
    if archived_terminal_path.exists():
        archived = read_terminal_event(archived_terminal_path, managed_root)
        if archived != terminal:
            raise StateValidationError(f"archive collision at {archive_dir}")
    else:
        _atomic_write_json(archived_terminal_path, terminal.to_dict())
    if archived_notification_path.exists():
        archived_notification = read_notification_event(
            archived_notification_path, managed_root, terminal=terminal
        )
        if archived_notification != notification:
            raise StateValidationError(f"archive collision at {archive_dir}")
    else:
        _atomic_write_json(archived_notification_path, notification.to_dict())


def record_terminal_event(
    study: StudyConfig,
    run_id: str,
    *,
    attempt: int,
    status: TerminalStatus,
    event_id: str | None = None,
    occurred_at: datetime | None = None,
    originating_thread_id: str | None = None,
) -> tuple[TerminalEvent, NotificationEvent]:
    """Write terminal state first, then queue its notification.

    Repeating the same event ID with identical terminal fields is idempotent.
    A different current event is archived before replacement.
    """

    validated_run = _validate_identifier(run_id, "run id")
    register_managed_root(study.log_root)
    run_dir = study.run_dir(validated_run)
    run_dir.mkdir(parents=True, exist_ok=True)
    terminal_path = _resolved_managed_path(
        (run_dir / TERMINAL_FILE_NAME).absolute(), study.log_root, "terminal_state_path"
    )
    selected_thread_id = (
        originating_thread_id
        if originating_thread_id is not None
        else os.environ.get("CODEX_THREAD_ID")
    )
    terminal = TerminalEvent(
        schema_version=SCHEMA_VERSION,
        event_id=_validate_event_id(event_id or str(uuid4())),
        study_id=study.id,
        run_id=validated_run,
        attempt=_validate_attempt(attempt),
        status=status,
        occurred_at=_normalize_utc(occurred_at or datetime.now(UTC), "occurred_at"),
        originating_thread_id=_validate_thread_id(selected_thread_id),
        terminal_state_path=str(terminal_path),
    )
    lock = FileLock(str(run_dir / STATE_LOCK_NAME))
    with lock:
        if terminal_path.exists():
            current = read_terminal_event(terminal_path, study.log_root)
            if current.event_id == terminal.event_id:
                if current != terminal:
                    raise StateValidationError(
                        f"event {terminal.event_id} already exists with different terminal fields"
                    )
                notification_path = run_dir / NOTIFICATION_FILE_NAME
                if notification_path.exists():
                    notification = read_notification_event(
                        notification_path, study.log_root, terminal=current
                    )
                else:
                    notification = NotificationEvent.from_terminal(current)
                    _atomic_write_json(notification_path, notification.to_dict())
                return current, notification
            _archive_current_pair(run_dir, study.log_root, current)

        notification = NotificationEvent.from_terminal(terminal)
        _atomic_write_json(terminal_path, terminal.to_dict())
        _atomic_write_json(run_dir / NOTIFICATION_FILE_NAME, notification.to_dict())
        return terminal, notification


def ensure_notification(
    study: StudyConfig, run_id: str, *, requeue: bool = False
) -> NotificationEvent:
    """Validate or reconstruct one run notification, optionally requeueing failure."""

    validate_managed_root(study.log_root)
    run_dir = study.run_dir(run_id)
    terminal_path = run_dir / TERMINAL_FILE_NAME
    notification_path = run_dir / NOTIFICATION_FILE_NAME
    if not terminal_path.exists():
        raise StateValidationError(f"terminal state does not exist: {terminal_path}")
    lock = FileLock(str(run_dir / STATE_LOCK_NAME))
    with lock:
        terminal = read_terminal_event(terminal_path, study.log_root)
        if Path(terminal.terminal_state_path) != terminal_path.resolve(strict=False):
            raise StateValidationError(
                "terminal_state_path does not identify the current terminal file"
            )
        if notification_path.exists():
            notification = read_notification_event(
                notification_path, study.log_root, terminal=terminal
            )
        else:
            notification = NotificationEvent.from_terminal(terminal)
            _atomic_write_json(notification_path, notification.to_dict())
        if requeue:
            notification = notification.requeued()
            _atomic_write_json(notification_path, notification.to_dict())
        return notification
