"""Outer process-group supervision for one local PyTorch run attempt."""

from __future__ import annotations

import hashlib
import json
import math
import os
import signal
import subprocess
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

SupervisorStatus = Literal["completed", "failed", "timed_out"]


@dataclass(frozen=True, slots=True)
class SupervisorResult:
    """Trusted terminal evidence after the complete process group is stopped."""

    status: SupervisorStatus
    returncode: int
    pid: int
    process_group_id: int
    operation_started_at: str
    occurred_at: str
    elapsed_seconds: float
    elapsed_basis: Literal["operation"] = "operation"


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
        directory_descriptor = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


class TorchSupervisor:
    """Start, time-limit, terminate, and reap one exact local process group."""

    def __init__(
        self,
        state_directory: Path,
        *,
        managed_root: Path,
        terminate_grace_seconds: float = 5.0,
    ) -> None:
        lexical_root = Path(os.path.abspath(managed_root.expanduser()))
        lexical_state = Path(os.path.abspath(state_directory.expanduser()))
        resolved_root = lexical_root.resolve(strict=False)
        resolved_state = lexical_state.resolve(strict=False)
        if lexical_root != resolved_root or lexical_state != resolved_state:
            raise ValueError("supervisor paths must not contain symlink components")
        if resolved_state == resolved_root or not resolved_state.is_relative_to(resolved_root):
            raise ValueError("supervisor state directory must be inside managed_root")
        if terminate_grace_seconds < 0 or not math.isfinite(terminate_grace_seconds):
            raise ValueError("terminate_grace_seconds must be finite and non-negative")
        self._directory = resolved_state
        self._state_path = self._directory / "terminal.json"
        self._terminate_grace_seconds = terminate_grace_seconds

    @property
    def state_path(self) -> Path:
        return self._state_path

    @staticmethod
    def _group_exists(process_group_id: int) -> bool:
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            return False
        return True

    def _stop_group(self, process: subprocess.Popen[bytes], process_group_id: int) -> None:
        if self._group_exists(process_group_id):
            with suppress(ProcessLookupError):
                os.killpg(process_group_id, signal.SIGTERM)
        deadline = time.monotonic() + self._terminate_grace_seconds
        while self._group_exists(process_group_id) and time.monotonic() < deadline:
            time.sleep(0.01)
        if self._group_exists(process_group_id):
            with suppress(ProcessLookupError):
                os.killpg(process_group_id, signal.SIGKILL)
        try:
            process.wait(timeout=max(self._terminate_grace_seconds, 0.1))
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive Popen fallback
            process.kill()
            process.wait()

    def run(
        self,
        command: Sequence[str],
        *,
        timeout_seconds: float,
        environment: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        on_terminal: Callable[[SupervisorResult], None] | None = None,
    ) -> SupervisorResult:
        """Run one no-shell command and call the terminal producer after cleanup."""

        selected_command = tuple(command)
        if not selected_command or any(not isinstance(item, str) or not item for item in command):
            raise ValueError("command must contain non-empty argument strings")
        if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
            raise ValueError("timeout_seconds must be positive and finite")
        self._directory.mkdir(parents=True, exist_ok=True)
        started_at = datetime.now(UTC)
        started_clock = time.monotonic()
        command_digest = hashlib.sha256("\0".join(selected_command).encode()).hexdigest()
        process = subprocess.Popen(
            selected_command,
            cwd=cwd,
            env=dict(environment) if environment is not None else None,
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        process_group_id = os.getpgid(process.pid)
        _atomic_write_json(
            self._directory / "launch.json",
            {
                "command_sha256": command_digest,
                "operation_started_at": started_at.isoformat(),
                "pid": process.pid,
                "process_group_id": process_group_id,
                "status": "running",
                "timeout_seconds": timeout_seconds,
            },
        )
        try:
            try:
                returncode = process.wait(timeout=timeout_seconds)
                status: SupervisorStatus = "completed" if returncode == 0 else "failed"
            except subprocess.TimeoutExpired:
                status = "timed_out"
                returncode = -signal.SIGTERM
            self._stop_group(process, process_group_id)
            if process.returncode is not None:
                returncode = process.returncode
        except BaseException:
            self._stop_group(process, process_group_id)
            raise
        occurred_at = datetime.now(UTC)
        result = SupervisorResult(
            status=status,
            returncode=returncode,
            pid=process.pid,
            process_group_id=process_group_id,
            operation_started_at=started_at.isoformat(),
            occurred_at=occurred_at.isoformat(),
            elapsed_seconds=max(0.0, time.monotonic() - started_clock),
        )
        _atomic_write_json(self._state_path, asdict(result))
        if on_terminal is not None:
            on_terminal(result)
        return result


__all__ = ["SupervisorResult", "TorchSupervisor"]
