from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import torch
from torch.utils.data import DataLoader, TensorDataset

from project.research import pytorch as pytorch_adapter
from project.research.pytorch import (
    CheckpointStore,
    DistributedContext,
    TrainingComponents,
    TrainingConfig,
    run_training,
    validate_training_components,
)

REPOSITORY_ROOT = Path(__file__).parents[1]
SYNTHETIC_SCRIPT = REPOSITORY_ROOT / "scripts" / "synthetic_classification.py"
pytestmark = pytest.mark.pytorch


def _components(seed: int = 7) -> TrainingComponents:
    torch.manual_seed(seed)
    features = torch.linspace(-1.0, 1.0, steps=128).reshape(32, 4)
    labels = (features.sum(dim=1) > 0).to(torch.long)
    loader = DataLoader(TensorDataset(features, labels), batch_size=8, shuffle=False)
    model = torch.nn.Linear(4, 2)
    return TrainingComponents(
        model=model,
        optimizer=torch.optim.SGD(model.parameters(), lr=0.1),
        train_loader=loader,
        criterion=torch.nn.CrossEntropyLoss(),
    )


def _state_dict(components: TrainingComponents) -> dict[str, torch.Tensor]:
    return {name: value.detach().clone() for name, value in components.model.state_dict().items()}


class RecordingTracker:
    def __init__(self) -> None:
        self.metrics: list[object] = []

    def log(self, metrics: object) -> None:
        self.metrics.append(metrics)


def _distributed_components(*, rank: int = 0, replicas: int = 2) -> TrainingComponents:
    components = _components()
    dataset = components.train_loader.dataset
    sampler = torch.utils.data.DistributedSampler(
        dataset,
        num_replicas=replicas,
        rank=rank,
        shuffle=False,
    )
    components.train_loader = DataLoader(dataset, batch_size=8, sampler=sampler)
    return components


def test_single_process_training_matches_epoch_boundary_resume(tmp_path: Path) -> None:
    context = DistributedContext(rank=0, local_rank=0, world_size=1, device="cpu", backend="gloo")
    uninterrupted = _components()
    uninterrupted_store = CheckpointStore(
        tmp_path / "uninterrupted" / "checkpoints",
        managed_root=tmp_path / "uninterrupted",
    )
    uninterrupted_metrics = run_training(
        TrainingConfig(epochs=4, seed=11),
        context,
        uninterrupted,
        uninterrupted_store,
    )

    partial = _components()
    resumed_root = tmp_path / "resumed"
    resumed_store = CheckpointStore(
        resumed_root / "checkpoints",
        managed_root=resumed_root,
    )
    run_training(
        TrainingConfig(epochs=2, seed=11),
        context,
        partial,
        resumed_store,
    )
    resumed = _components()
    resumed_metrics = run_training(
        TrainingConfig(epochs=4, seed=11, resume=True),
        context,
        resumed,
        resumed_store,
    )

    for name, value in _state_dict(uninterrupted).items():
        torch.testing.assert_close(value, _state_dict(resumed)[name], rtol=0, atol=0)
    assert resumed_metrics[-1].epoch == 4
    assert uninterrupted_metrics[-1].global_step == resumed_metrics[-1].global_step
    checkpoint = torch.load(resumed_store.path, map_location="cpu", weights_only=False)
    assert checkpoint["completed_epoch"] == 4
    assert checkpoint["scheduler"] is None
    assert checkpoint["cumulative_runtime_seconds"] >= 0
    assert set(checkpoint["random_state_by_rank"]) == {0}


@pytest.mark.parametrize(
    "context",
    [
        DistributedContext(rank=1, local_rank=0, world_size=1, device="cpu", backend="gloo"),
        DistributedContext(rank=0, local_rank=0, world_size=2, device="cpu", backend="nccl"),
        DistributedContext(rank=0, local_rank=0, world_size=1, device="cuda:0", backend="gloo"),
    ],
)
def test_distributed_context_rejects_inconsistent_identity(
    context: DistributedContext,
) -> None:
    with pytest.raises(ValueError):
        context.validate()


@pytest.mark.parametrize(
    "arguments",
    [
        {"epochs": 0, "seed": 1},
        {"epochs": True, "seed": 1},
        {"epochs": 1, "seed": -1},
        {"epochs": 1, "seed": True},
        {"epochs": 1, "seed": 1, "resume": 1},
    ],
)
def test_training_config_rejects_invalid_values(arguments: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        TrainingConfig(**arguments)


@pytest.mark.parametrize(
    "context",
    [
        DistributedContext(rank=0, local_rank=0, world_size=0, device="cpu", backend="gloo"),
        DistributedContext(rank=0, local_rank=2, world_size=2, device="cpu", backend="gloo"),
        DistributedContext(rank=0, local_rank=1, world_size=2, device="cuda:0", backend="nccl"),
    ],
)
def test_distributed_context_rejects_invalid_ranges_or_device(
    context: DistributedContext,
) -> None:
    with pytest.raises(ValueError):
        context.validate()


def test_single_process_context_initializes_as_noop_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = DistributedContext(rank=0, local_rank=0, world_size=1, device="cpu", backend="gloo")
    context.initialize("file:///unused")
    destroyed: list[bool] = []

    class Distributed:
        @staticmethod
        def is_initialized() -> bool:
            return True

        @staticmethod
        def destroy_process_group() -> None:
            destroyed.append(True)

    monkeypatch.setattr(
        pytorch_adapter, "_torch", lambda: type("Torch", (), {"distributed": Distributed})
    )
    context.close()
    assert destroyed == [True]


def test_distributed_context_rejects_invalid_timeout_or_unavailable_cuda(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cpu = DistributedContext(rank=0, local_rank=0, world_size=2, device="cpu", backend="gloo")
    with pytest.raises(ValueError, match="timeout_seconds"):
        cpu.initialize("file:///unused", timeout_seconds=0)

    class Cuda:
        @staticmethod
        def is_available() -> bool:
            return False

    monkeypatch.setattr(pytorch_adapter, "_torch", lambda: type("Torch", (), {"cuda": Cuda}))
    cuda = DistributedContext(rank=0, local_rank=0, world_size=2, device="cuda:0", backend="nccl")
    with pytest.raises(RuntimeError, match="unavailable"):
        cuda.initialize("file:///unused")


def test_distributed_training_requires_matching_distributed_sampler() -> None:
    context = DistributedContext(rank=0, local_rank=0, world_size=2, device="cpu", backend="gloo")

    with pytest.raises(ValueError, match="DistributedSampler"):
        validate_training_components(context, _components())


def test_distributed_training_rejects_mismatched_sampler_identity() -> None:
    context = DistributedContext(rank=0, local_rank=0, world_size=2, device="cpu", backend="gloo")
    with pytest.raises(ValueError, match="rank and world size"):
        validate_training_components(context, _distributed_components(rank=1))


def test_distributed_training_requires_matching_initialized_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context = DistributedContext(rank=0, local_rank=0, world_size=2, device="cpu", backend="gloo")
    components = _distributed_components()
    store = CheckpointStore(tmp_path / "run" / "checkpoints", managed_root=tmp_path / "run")

    monkeypatch.setattr(torch.distributed, "is_initialized", lambda: False)
    with pytest.raises(RuntimeError, match="must be initialized"):
        run_training(TrainingConfig(epochs=1, seed=1), context, components, store)

    monkeypatch.setattr(torch.distributed, "is_initialized", lambda: True)
    monkeypatch.setattr(torch.distributed, "get_rank", lambda: 1)
    monkeypatch.setattr(torch.distributed, "get_world_size", lambda: 2)
    with pytest.raises(RuntimeError, match="does not match"):
        run_training(TrainingConfig(epochs=1, seed=1), context, components, store)


def test_checkpoint_store_rejects_paths_outside_managed_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="inside managed_root"):
        CheckpointStore(tmp_path / "outside", managed_root=tmp_path / "managed")


def test_checkpoint_store_rejects_symlinked_path(tmp_path: Path) -> None:
    root = tmp_path / "root"
    target = tmp_path / "target"
    target.mkdir()
    root.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        CheckpointStore(root / "checkpoints", managed_root=root)


@pytest.mark.parametrize(
    ("completed_epoch", "global_step", "runtime", "message"),
    [
        (0, 0, 0.0, "counters"),
        (1, -1, 0.0, "counters"),
        (1, 0, -1.0, "runtime"),
        (1, 0, float("nan"), "runtime"),
    ],
)
def test_checkpoint_save_rejects_invalid_progress(
    tmp_path: Path,
    completed_epoch: int,
    global_step: int,
    runtime: float,
    message: str,
) -> None:
    root = tmp_path / "root"
    store = CheckpointStore(root / "checkpoints", managed_root=root)
    context = DistributedContext(rank=0, local_rank=0, world_size=1, device="cpu", backend="gloo")
    with pytest.raises(ValueError, match=message):
        store.save(
            _components(),
            context,
            completed_epoch=completed_epoch,
            global_step=global_step,
            metric_state={},
            cumulative_runtime_seconds=runtime,
            random_state_by_rank={0: {}},
        )


def test_checkpoint_atomic_failure_removes_temporary_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    store = CheckpointStore(root / "checkpoints", managed_root=root)
    context = DistributedContext(rank=0, local_rank=0, world_size=1, device="cpu", backend="gloo")
    monkeypatch.setattr(os, "replace", lambda *_arguments: (_ for _ in ()).throw(OSError("fail")))
    with pytest.raises(OSError, match="fail"):
        store.save(
            _components(),
            context,
            completed_epoch=1,
            global_step=1,
            metric_state={},
            cumulative_runtime_seconds=0.0,
            random_state_by_rank={0: {}},
        )
    assert list(store.path.parent.glob(".last.*.tmp")) == []


def test_nonzero_rank_cannot_write_checkpoint(tmp_path: Path) -> None:
    context = DistributedContext(rank=1, local_rank=1, world_size=2, device="cpu", backend="gloo")
    store = CheckpointStore(tmp_path / "root" / "checkpoints", managed_root=tmp_path / "root")

    assert (
        store.save(
            _components(),
            context,
            completed_epoch=1,
            global_step=4,
            metric_state={},
            cumulative_runtime_seconds=1.0,
            random_state_by_rank={1: {}},
        )
        is None
    )
    assert not store.path.exists()


def test_restore_returns_none_and_rejects_malformed_checkpoints(tmp_path: Path) -> None:
    root = tmp_path / "root"
    store = CheckpointStore(root / "checkpoints", managed_root=root)
    context = DistributedContext(rank=0, local_rank=0, world_size=1, device="cpu", backend="gloo")
    components = _components()
    assert store.restore(components, context) is None
    store.path.parent.mkdir(parents=True)

    torch.save({"unexpected": True}, store.path)
    with pytest.raises(ValueError, match="fields"):
        store.restore(components, context)


def test_restore_validates_schema_scheduler_rank_and_metric_state(tmp_path: Path) -> None:
    root = tmp_path / "root"
    store = CheckpointStore(root / "checkpoints", managed_root=root)
    context = DistributedContext(rank=0, local_rank=0, world_size=1, device="cpu", backend="gloo")
    components = _components()
    store.save(
        components,
        context,
        completed_epoch=1,
        global_step=1,
        metric_state={"loss": 1.0},
        cumulative_runtime_seconds=1.0,
        random_state_by_rank={0: pytorch_adapter._capture_random_state()},
    )
    original = torch.load(store.path, weights_only=False)

    cases = [
        ({**original, "schema_version": 99}, "schema"),
        ({**original, "scheduler": {"state": 1}}, "scheduler"),
        ({**original, "random_state_by_rank": {}}, "random state"),
        ({**original, "metric_state": []}, "metric state"),
    ]
    for payload, message in cases:
        torch.save(payload, store.path)
        with pytest.raises(ValueError, match=message):
            store.restore(_components(), context)


def test_restore_rejects_incomplete_or_unavailable_random_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="incomplete"):
        pytorch_adapter._restore_random_state({})
    state = pytorch_adapter._capture_random_state()
    state["torch_cuda"] = [torch.get_rng_state()]
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    with pytest.raises(ValueError, match="CPU-only"):
        pytorch_adapter._restore_random_state(state)


def test_training_uses_scheduler_tracker_and_rejects_empty_loader(tmp_path: Path) -> None:
    context = DistributedContext(rank=0, local_rank=0, world_size=1, device="cpu", backend="gloo")
    components = _components()
    components.scheduler = torch.optim.lr_scheduler.StepLR(components.optimizer, step_size=1)
    tracker = RecordingTracker()
    root = tmp_path / "tracked"
    store = CheckpointStore(root / "checkpoints", managed_root=root)
    metrics = run_training(
        TrainingConfig(epochs=1, seed=2),
        context,
        components,
        store,
        tracker=tracker,
    )
    assert tracker.metrics == [metrics[0]]

    resumed = _components()
    resumed.scheduler = torch.optim.lr_scheduler.StepLR(resumed.optimizer, step_size=1)
    assert (
        run_training(
            TrainingConfig(epochs=1, seed=2, resume=True),
            context,
            resumed,
            store,
        )
        == ()
    )

    empty = _components()
    empty.train_loader = DataLoader(
        TensorDataset(torch.empty(0, 4), torch.empty(0, dtype=torch.long))
    )
    empty_root = tmp_path / "empty"
    with pytest.raises(ValueError, match="no samples"):
        run_training(
            TrainingConfig(epochs=1, seed=2),
            context,
            empty,
            CheckpointStore(empty_root / "checkpoints", managed_root=empty_root),
        )


def test_two_process_gloo_synthetic_demo(tmp_path: Path) -> None:
    output = tmp_path / "gloo-demo"
    result = subprocess.run(
        [
            sys.executable,
            str(SYNTHETIC_SCRIPT),
            "--backend",
            "gloo",
            "--world-size",
            "2",
            "--epochs",
            "2",
            "--output",
            str(output),
        ],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        check=False,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stderr
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert summary["backend"] == "gloo"
    assert summary["world_size"] == 2
    assert summary["completed_epoch"] == 2
    checkpoint = torch.load(
        output / "checkpoints" / "last.pt",
        map_location="cpu",
        weights_only=False,
    )
    assert set(checkpoint["random_state_by_rank"]) == {0, 1}
