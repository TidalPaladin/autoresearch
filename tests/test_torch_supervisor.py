from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from project.research import supervisor as supervisor_module
from project.research.supervisor import TorchSupervisor

pytestmark = pytest.mark.pytorch


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_for_exit(pid: int) -> None:
    deadline = time.monotonic() + 5
    while _process_exists(pid) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not _process_exists(pid)


def test_supervisor_records_success_after_reaping_process(tmp_path: Path) -> None:
    root = tmp_path / "run"
    supervisor = TorchSupervisor(root / "supervisor", managed_root=root)

    result = supervisor.run([sys.executable, "-c", "raise SystemExit(0)"], timeout_seconds=10)

    assert result.status == "completed"
    assert result.returncode == 0
    assert result.elapsed_seconds >= 0
    assert not _process_exists(result.pid)
    persisted = json.loads(supervisor.state_path.read_text(encoding="utf-8"))
    assert persisted["status"] == "completed"
    assert persisted["pid"] == result.pid


@pytest.mark.parametrize(
    ("parent_code", "timeout_seconds", "expected_status"),
    [
        ("raise SystemExit(3)", 10.0, "failed"),
        ("import time; time.sleep(30)", 0.1, "timed_out"),
    ],
)
def test_supervisor_cleans_process_group_on_failure_or_timeout(
    tmp_path: Path,
    parent_code: str,
    timeout_seconds: float,
    expected_status: str,
) -> None:
    root = tmp_path / expected_status
    child_pid_path = root / "child.pid"
    root.mkdir()
    script = (
        "import pathlib, subprocess, sys; "
        f"p=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
        f"pathlib.Path({str(child_pid_path)!r}).write_text(str(p.pid)); "
        f"{parent_code}"
    )
    supervisor = TorchSupervisor(
        root / "supervisor",
        managed_root=root,
        terminate_grace_seconds=0.05,
    )

    result = supervisor.run([sys.executable, "-c", script], timeout_seconds=timeout_seconds)

    assert result.status == expected_status
    child_pid = int(child_pid_path.read_text(encoding="utf-8"))
    _wait_for_exit(child_pid)
    assert not _process_exists(result.pid)


def test_supervisor_rejects_state_path_outside_managed_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="inside managed_root"):
        TorchSupervisor(tmp_path / "outside", managed_root=tmp_path / "managed")


def test_supervisor_rejects_symlink_invalid_grace_command_and_timeout(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        TorchSupervisor(linked_root / "state", managed_root=linked_root)
    with pytest.raises(ValueError, match="terminate_grace_seconds"):
        TorchSupervisor(
            tmp_path / "root" / "state", managed_root=tmp_path / "root", terminate_grace_seconds=-1
        )

    supervisor = TorchSupervisor(tmp_path / "valid" / "state", managed_root=tmp_path / "valid")
    with pytest.raises(ValueError, match="command"):
        supervisor.run([], timeout_seconds=1)
    with pytest.raises(ValueError, match="timeout_seconds"):
        supervisor.run([sys.executable], timeout_seconds=0)


def test_supervisor_calls_terminal_producer_after_persisting(tmp_path: Path) -> None:
    root = tmp_path / "callback"
    supervisor = TorchSupervisor(root / "state", managed_root=root)
    observed: list[tuple[str, bool]] = []

    def observe(result: Any) -> None:
        observed.append((result.status, supervisor.state_path.exists()))

    supervisor.run(
        [sys.executable, "-c", "raise SystemExit(0)"],
        timeout_seconds=10,
        on_terminal=observe,
    )
    assert observed == [("completed", True)]


def test_atomic_state_failure_removes_temporary_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "event.json"
    monkeypatch.setattr(os, "replace", lambda *_arguments: (_ for _ in ()).throw(OSError("fail")))
    with pytest.raises(OSError, match="fail"):
        supervisor_module._atomic_write_json(path, {"state": "test"})
    assert list(path.parent.glob(".event.json.*.tmp")) == []


def test_group_exists_reports_missing_process_group() -> None:
    assert not TorchSupervisor._group_exists(2**30)
