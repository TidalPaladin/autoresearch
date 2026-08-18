"""Manifest-gated optional Weights & Biases telemetry."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any, Literal, Protocol, cast

from project.research.pytorch import EpochMetrics

WandbMode = Literal["online", "offline", "disabled"]
METRIC_FIELDS = frozenset(
    {
        "epoch",
        "global_step",
        "loss",
        "accuracy",
        "sample_count",
        "cumulative_runtime_seconds",
    }
)
MANIFEST_FIELDS = METRIC_FIELDS | {"config"}


class WandbRun(Protocol):
    def log(self, payload: dict[str, object], *, step: int | None = None) -> None:
        """Write one telemetry payload."""

    def finish(self) -> None:
        """Close the run."""


class WandbClient(Protocol):
    def init(self, **arguments: Any) -> WandbRun:
        """Create one exact W&B run."""

        ...


class WandbTracker:
    """Rank-zero W&B adapter with an exact destination and data manifest."""

    def __init__(
        self,
        *,
        entity: str,
        project: str,
        mode: WandbMode,
        emitted_data_manifest: Mapping[str, str],
        rank: int,
        client: WandbClient | None = None,
    ) -> None:
        if not isinstance(entity, str) or not entity.strip():
            raise ValueError("W&B entity must be exact and non-empty")
        if not isinstance(project, str) or not project.strip():
            raise ValueError("W&B project must be exact and non-empty")
        if mode not in {"online", "offline", "disabled"}:
            raise ValueError("W&B mode must be online, offline, or disabled")
        if not emitted_data_manifest:
            raise ValueError("W&B emitted-data manifest must not be empty")
        unknown = sorted(set(emitted_data_manifest) - MANIFEST_FIELDS)
        if unknown:
            raise ValueError(f"unsafe W&B manifest field: {', '.join(unknown)}")
        if not all(
            isinstance(classification, str) and classification.strip()
            for classification in emitted_data_manifest.values()
        ):
            raise ValueError("every W&B manifest field requires a data classification")
        if not isinstance(rank, int) or isinstance(rank, bool) or rank < 0:
            raise ValueError("rank must be a non-negative integer")
        self.entity = entity
        self.project = project
        self.mode = mode
        self.emitted_data_manifest = dict(emitted_data_manifest)
        self.rank = rank
        self._client = client
        self._run: WandbRun | None = None

    def _selected_client(self) -> WandbClient:
        if self._client is not None:
            return self._client
        try:
            return cast(WandbClient, importlib.import_module("wandb"))
        except ImportError as error:
            raise RuntimeError("install the 'wandb' extra to use WandbTracker") from error

    def start(self, *, run_name: str, config: Mapping[str, object] | None) -> None:
        """Start the exact rank-zero run without implicit destination defaults."""

        if self.rank != 0 or self.mode == "disabled":
            return
        if not isinstance(run_name, str) or not run_name.strip():
            raise ValueError("W&B run name must be exact and non-empty")
        if config is not None and "config" not in self.emitted_data_manifest:
            raise ValueError("W&B config is not present in the emitted-data manifest")
        if self._run is not None:
            raise RuntimeError("W&B run is already started")
        self._run = self._selected_client().init(
            config=dict(config) if config is not None else None,
            entity=self.entity,
            mode=self.mode,
            name=run_name,
            project=self.project,
        )

    def log(self, metrics: EpochMetrics) -> None:
        """Write only manifest-listed aggregate metrics from rank zero."""

        if self.rank != 0 or self.mode == "disabled":
            return
        if self._run is None:
            raise RuntimeError("W&B run is not started")
        if not {"epoch", "global_step"}.issubset(self.emitted_data_manifest):
            raise ValueError("W&B metric manifest must include epoch and global_step")
        available: dict[str, object] = {
            "epoch": metrics.epoch,
            "global_step": metrics.global_step,
            "loss": metrics.loss,
            "accuracy": metrics.accuracy,
            "sample_count": metrics.sample_count,
            "cumulative_runtime_seconds": metrics.cumulative_runtime_seconds,
        }
        payload = {
            field: available[field]
            for field in sorted(self.emitted_data_manifest)
            if field in available
        }
        self._run.log(payload, step=metrics.global_step)

    def finish(self) -> None:
        """Finish the active rank-zero run."""

        if self._run is not None:
            self._run.finish()
            self._run = None


__all__ = ["WandbTracker"]
