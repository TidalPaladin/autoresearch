from __future__ import annotations

import importlib
from typing import Any, cast

import pytest

from project.research import wandb_tracking
from project.research.pytorch import EpochMetrics
from project.research.wandb_tracking import WandbTracker

pytestmark = pytest.mark.pytorch


class FakeRun:
    def __init__(self) -> None:
        self.logged: list[tuple[dict[str, object], int | None]] = []
        self.finished = False

    def log(self, payload: dict[str, object], *, step: int | None = None) -> None:
        self.logged.append((payload, step))

    def finish(self) -> None:
        self.finished = True


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.run = FakeRun()

    def init(self, **arguments: Any) -> FakeRun:
        self.calls.append(arguments)
        return self.run


def _metrics() -> EpochMetrics:
    return EpochMetrics(
        epoch=1,
        global_step=4,
        loss=0.25,
        accuracy=0.875,
        sample_count=32,
        cumulative_runtime_seconds=1.5,
        metric_state={"best_accuracy": 0.875},
    )


def test_rank_zero_wandb_uses_exact_destination_mode_and_manifest() -> None:
    client = FakeClient()
    tracker = WandbTracker(
        entity="research-team",
        project="synthetic-classification",
        mode="offline",
        emitted_data_manifest={
            "accuracy": "aggregate metric",
            "epoch": "progress counter",
            "global_step": "progress counter",
            "loss": "aggregate metric",
        },
        rank=0,
        client=client,
    )

    tracker.start(run_name="seed-7", config=None)
    tracker.log(_metrics())
    tracker.finish()

    assert client.calls == [
        {
            "config": None,
            "entity": "research-team",
            "mode": "offline",
            "name": "seed-7",
            "project": "synthetic-classification",
        }
    ]
    assert client.run.logged == [
        (
            {"accuracy": 0.875, "epoch": 1, "global_step": 4, "loss": 0.25},
            4,
        )
    ]
    assert client.run.finished


def test_nonzero_rank_never_initializes_or_writes_wandb() -> None:
    client = FakeClient()
    tracker = WandbTracker(
        entity="research-team",
        project="synthetic-classification",
        mode="offline",
        emitted_data_manifest={"loss": "aggregate metric"},
        rank=1,
        client=client,
    )

    tracker.start(run_name="rank-1", config=None)
    tracker.log(_metrics())
    tracker.finish()

    assert client.calls == []
    assert client.run.logged == []


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"entity": ""}, "entity"),
        ({"project": ""}, "project"),
        ({"mode": "automatic"}, "mode"),
        ({"emitted_data_manifest": {}}, "manifest"),
        ({"emitted_data_manifest": {"raw_sample": "data"}}, "manifest field"),
    ],
)
def test_wandb_tracker_rejects_incomplete_or_unsafe_contracts(
    arguments: dict[str, object],
    message: str,
) -> None:
    defaults: dict[str, object] = {
        "entity": "research-team",
        "project": "synthetic-classification",
        "mode": "offline",
        "emitted_data_manifest": {"loss": "aggregate metric"},
        "rank": 0,
        "client": FakeClient(),
    }
    defaults.update(arguments)

    with pytest.raises(ValueError, match=message):
        WandbTracker(**cast(Any, defaults))


@pytest.mark.parametrize(
    "arguments",
    [
        {"emitted_data_manifest": {"loss": ""}},
        {"rank": -1},
        {"rank": True},
    ],
)
def test_wandb_tracker_rejects_invalid_classifications_or_rank(
    arguments: dict[str, object],
) -> None:
    defaults: dict[str, object] = {
        "entity": "research-team",
        "project": "synthetic-classification",
        "mode": "offline",
        "emitted_data_manifest": {"loss": "aggregate metric"},
        "rank": 0,
        "client": FakeClient(),
    }
    defaults.update(arguments)
    with pytest.raises(ValueError):
        WandbTracker(**cast(Any, defaults))


def test_wandb_tracker_rejects_unmanifested_config_or_metric() -> None:
    tracker = WandbTracker(
        entity="research-team",
        project="synthetic-classification",
        mode="offline",
        emitted_data_manifest={"loss": "aggregate metric"},
        rank=0,
        client=FakeClient(),
    )

    with pytest.raises(ValueError, match="config"):
        tracker.start(run_name="seed-7", config={"seed": 7})
    tracker.start(run_name="seed-7", config=None)
    with pytest.raises(ValueError, match="metric"):
        tracker.log(_metrics())


def test_disabled_tracker_never_uses_rank_zero_client() -> None:
    client = FakeClient()
    tracker = WandbTracker(
        entity="research-team",
        project="synthetic-classification",
        mode="disabled",
        emitted_data_manifest={"loss": "aggregate metric"},
        rank=0,
        client=client,
    )
    tracker.start(run_name="", config={"unmanifested": True})
    tracker.log(_metrics())
    tracker.finish()
    assert client.calls == []


def test_wandb_tracker_rejects_blank_or_duplicate_run_name() -> None:
    tracker = WandbTracker(
        entity="research-team",
        project="synthetic-classification",
        mode="offline",
        emitted_data_manifest={"epoch": "counter", "global_step": "counter"},
        rank=0,
        client=FakeClient(),
    )
    with pytest.raises(ValueError, match="run name"):
        tracker.start(run_name="", config=None)
    tracker.start(run_name="run-1", config=None)
    with pytest.raises(RuntimeError, match="already started"):
        tracker.start(run_name="run-2", config=None)


def test_wandb_tracker_passes_manifested_config() -> None:
    client = FakeClient()
    tracker = WandbTracker(
        entity="research-team",
        project="synthetic-classification",
        mode="offline",
        emitted_data_manifest={"config": "public parameters"},
        rank=0,
        client=client,
    )
    tracker.start(run_name="run-1", config={"seed": 7})
    assert client.calls[0]["config"] == {"seed": 7}


def test_wandb_tracker_reports_missing_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = importlib.import_module

    def missing(name: str) -> Any:
        if name == "wandb":
            raise ImportError(name)
        return original(name)

    monkeypatch.setattr(wandb_tracking.importlib, "import_module", missing)
    tracker = WandbTracker(
        entity="research-team",
        project="synthetic-classification",
        mode="offline",
        emitted_data_manifest={"epoch": "counter"},
        rank=0,
    )
    with pytest.raises(RuntimeError, match="wandb"):
        tracker.start(run_name="run-1", config=None)
