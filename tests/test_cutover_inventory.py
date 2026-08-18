from __future__ import annotations

import json
from pathlib import Path

import pytest
from notify_wake import CutoverError

from project.research.cutover import inventory_legacy_state, write_cutover_manifest
from project.research.runtime import StateValidationError, register_managed_root

SOURCE_COMMIT = "e9806cbe7a35ed1678dff526b80a16a4cefd72b5"


def _legacy_event(root: Path, name: str, payload: object) -> Path:
    path = root / ".notify-wake" / "v2" / "events" / name / "notification.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    return path


def test_inventory_is_non_migrating_and_manifest_hashes_legacy_files(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    register_managed_root(root)
    version_one = root / ".notify-wake" / "v1" / "accepted.json"
    version_one.parent.mkdir(parents=True)
    version_one.write_text('{"state":"accepted"}\n', encoding="utf-8")
    terminal = root / ".notify-wake" / "v2" / "events" / "event-a" / "terminal.json"
    terminal.parent.mkdir(parents=True, exist_ok=True)
    terminal.write_text('{"event_id":"event-a"}\n', encoding="utf-8")

    before = {path: path.read_bytes() for path in (version_one, terminal)}
    inventory = inventory_legacy_state(root)

    assert inventory.file_count == 2
    assert inventory.live_identities == ()
    assert inventory.superseded_ids == ("event-a",)
    assert not inventory.manifest_path.exists()
    assert {path: path.read_bytes() for path in before} == before

    payload = write_cutover_manifest(inventory, source_commit=SOURCE_COMMIT)
    assert payload["source_commit"] == SOURCE_COMMIT
    assert payload["total_file_count"] == 2
    assert inventory.manifest_path.exists()
    assert {path: path.read_bytes() for path in before} == before


def test_inventory_ignores_current_timed_events_and_flags_possible_live_state(
    tmp_path: Path,
) -> None:
    root = tmp_path / "logs"
    register_managed_root(root)
    _legacy_event(
        root,
        "current",
        {
            "operation_started_at": "2026-08-18T00:00:00+00:00",
            "occurred_at": "2026-08-18T00:00:01+00:00",
            "elapsed_seconds": 1,
            "elapsed_basis": "operation",
        },
    )
    live = _legacy_event(
        root,
        "live",
        {"event_id": "live", "delivery": {"state": "pending"}},
    )

    inventory = inventory_legacy_state(root)

    assert inventory.groups["autoresearch_notification_without_timing"] == (live,)
    assert inventory.live_identities == ("event:live",)
    with pytest.raises(CutoverError, match="legacy work is live"):
        write_cutover_manifest(inventory, source_commit=SOURCE_COMMIT)


def test_inventory_flags_unreadable_evidence_and_rejects_symlinks(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    register_managed_root(root)
    unreadable = _legacy_event(root, "broken", "not an object")
    inventory = inventory_legacy_state(root)
    assert inventory.live_identities == (f"unreadable:{unreadable.relative_to(root)}",)

    version_one = root / ".notify-wake" / "v1"
    version_one.mkdir(parents=True)
    target = tmp_path / "target.json"
    target.write_text("{}\n", encoding="utf-8")
    (version_one / "linked.json").symlink_to(target)
    with pytest.raises(StateValidationError, match="symlink"):
        inventory_legacy_state(root)
