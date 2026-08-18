"""Non-migrating inventory for pre-timing autoresearch notification evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from notify_wake import create_cutover_manifest

from project.research.runtime import StateValidationError, validate_managed_root

TIMING_FIELDS = frozenset(
    {
        "operation_started_at",
        "occurred_at",
        "elapsed_seconds",
        "elapsed_basis",
    }
)
LIVE_DELIVERY_STATES = frozenset({"pending", "in_flight", "uncertain", "retry_due"})


@dataclass(frozen=True, slots=True)
class CutoverInventory:
    """Legacy files and possible live identities found without changing them."""

    managed_root: Path
    manifest_path: Path
    groups: dict[str, tuple[Path, ...]]
    dispositions: dict[str, str]
    superseded_ids: tuple[str, ...]
    live_identities: tuple[str, ...]

    @property
    def file_count(self) -> int:
        return sum(len(paths) for paths in self.groups.values())

    def to_dict(self) -> dict[str, object]:
        return {
            "managed_root": str(self.managed_root),
            "manifest_path": str(self.manifest_path),
            "file_count": self.file_count,
            "groups": {name: [str(path) for path in paths] for name, paths in self.groups.items()},
            "dispositions": dict(self.dispositions),
            "superseded_ids": list(self.superseded_ids),
            "live_identities": list(self.live_identities),
        }


def _regular_files(directory: Path) -> tuple[Path, ...]:
    if not directory.exists():
        return ()
    files = tuple(sorted(path for path in directory.rglob("*") if path.is_file()))
    if any(path.is_symlink() for path in files):
        raise StateValidationError(f"legacy inventory contains a symlink: {directory}")
    return files


def _load_legacy_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def inventory_legacy_state(managed_root: Path) -> CutoverInventory:
    """Inventory legacy evidence without modifying or migrating any file."""

    root = validate_managed_root(managed_root)
    namespace = root / ".notify-wake"
    version_two = namespace / "v2"
    groups: dict[str, tuple[Path, ...]] = {}
    dispositions: dict[str, str] = {}

    version_one_files = _regular_files(namespace / "v1")
    if version_one_files:
        groups["notify_wake_v1"] = version_one_files
        dispositions["notify_wake_v1"] = "preserved_legacy_v1_evidence"

    old_terminals: list[Path] = []
    old_notifications: list[Path] = []
    superseded_ids: set[str] = set()
    live_identities: set[str] = set()
    events_root = version_two / "events"
    for path in _regular_files(events_root):
        if path.name not in {"terminal.json", "notification.json"}:
            continue
        payload = _load_legacy_json(path)
        if payload is not None and TIMING_FIELDS.issubset(payload):
            continue
        selected = old_terminals if path.name == "terminal.json" else old_notifications
        selected.append(path)
        if payload is None:
            live_identities.add(f"unreadable:{path.relative_to(root)}")
            continue
        event_id = payload.get("event_id")
        if isinstance(event_id, str) and event_id:
            superseded_ids.add(event_id)
        delivery = payload.get("delivery")
        if isinstance(delivery, dict) and delivery.get("state") in LIVE_DELIVERY_STATES:
            live_identities.add(f"event:{event_id or path.parent.name}")

    if old_terminals:
        groups["autoresearch_terminal_without_timing"] = tuple(old_terminals)
        dispositions["autoresearch_terminal_without_timing"] = "cutover_required_no_timing_evidence"
    if old_notifications:
        groups["autoresearch_notification_without_timing"] = tuple(old_notifications)
        dispositions["autoresearch_notification_without_timing"] = (
            "cutover_required_no_timing_evidence"
        )

    return CutoverInventory(
        managed_root=root,
        manifest_path=version_two / "cutover-manifest.json",
        groups=groups,
        dispositions=dispositions,
        superseded_ids=tuple(sorted(superseded_ids)),
        live_identities=tuple(sorted(live_identities)),
    )


def write_cutover_manifest(inventory: CutoverInventory, *, source_commit: str) -> dict[str, Any]:
    """Hash legacy evidence into the fixed manifest path without migrating it."""

    return create_cutover_manifest(
        inventory.manifest_path,
        source_commit=source_commit,
        legacy_groups=inventory.groups,
        dispositions=inventory.dispositions,
        superseded_ids=inventory.superseded_ids,
        live_identities=inventory.live_identities,
    )


__all__ = ["CutoverInventory", "inventory_legacy_state", "write_cutover_manifest"]
