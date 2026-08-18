"""Optional single-node PyTorch training and epoch-checkpoint interfaces."""

from __future__ import annotations

import importlib
import math
import os
import pickle
import random
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal, Protocol, cast

Backend = Literal["gloo", "nccl"]


def _torch() -> Any:
    try:
        return importlib.import_module("torch")
    except ImportError as error:
        raise RuntimeError("install the 'pytorch' extra to use the PyTorch adapter") from error


@dataclass(frozen=True, slots=True)
class TrainingConfig:
    """Bounded epoch-training configuration."""

    epochs: int
    seed: int
    resume: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.epochs, int) or isinstance(self.epochs, bool) or self.epochs < 1:
            raise ValueError("epochs must be a positive integer")
        if not isinstance(self.seed, int) or isinstance(self.seed, bool) or self.seed < 0:
            raise ValueError("seed must be a non-negative integer")
        if not isinstance(self.resume, bool):
            raise ValueError("resume must be a boolean")


@dataclass(frozen=True, slots=True)
class DistributedContext:
    """Explicit identity for one rank in one single-node process group."""

    rank: int
    local_rank: int
    world_size: int
    device: str
    backend: Backend

    @property
    def is_rank_zero(self) -> bool:
        return self.rank == 0

    @property
    def is_distributed(self) -> bool:
        return self.world_size > 1

    def validate(self) -> None:
        if self.world_size < 1:
            raise ValueError("world_size must be positive")
        if not 0 <= self.rank < self.world_size:
            raise ValueError("rank must be inside world_size")
        if not 0 <= self.local_rank < self.world_size:
            raise ValueError("local_rank must be inside world_size")
        if self.device == "cpu":
            if self.backend != "gloo":
                raise ValueError("CPU distributed training requires the Gloo backend")
            return
        if self.device != f"cuda:{self.local_rank}":
            raise ValueError("CUDA device must match the explicit local_rank")
        if self.backend != "nccl":
            raise ValueError("CUDA distributed training requires the NCCL backend")

    def initialize(self, init_method: str, *, timeout_seconds: float = 120.0) -> None:
        """Initialize this exact process group when more than one rank is used."""

        self.validate()
        if not self.is_distributed:
            return
        if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
            raise ValueError("timeout_seconds must be positive and finite")
        torch = _torch()
        if self.device.startswith("cuda:"):
            if not torch.cuda.is_available() or torch.cuda.device_count() <= self.local_rank:
                raise RuntimeError(f"CUDA device {self.local_rank} is unavailable")
            torch.cuda.set_device(self.local_rank)
        if torch.distributed.is_initialized():
            raise RuntimeError("a PyTorch process group is already initialized")
        torch.distributed.init_process_group(
            backend=self.backend,
            init_method=init_method,
            rank=self.rank,
            world_size=self.world_size,
            timeout=timedelta(seconds=timeout_seconds),
        )

    def close(self) -> None:
        """Destroy this rank's initialized process group."""

        torch = _torch()
        if torch.distributed.is_initialized():
            torch.distributed.destroy_process_group()


@dataclass(slots=True)
class TrainingComponents:
    """PyTorch objects supplied by a downstream model and dataset adapter."""

    model: Any
    optimizer: Any
    train_loader: Any
    criterion: Any
    scheduler: Any | None = None


@dataclass(frozen=True, slots=True)
class EpochMetrics:
    """Aggregated metrics for one fully completed epoch."""

    epoch: int
    global_step: int
    loss: float
    accuracy: float
    sample_count: int
    cumulative_runtime_seconds: float
    metric_state: Mapping[str, float]


@dataclass(frozen=True, slots=True)
class CheckpointState:
    """Recovered epoch boundary and cumulative study state."""

    completed_epoch: int
    global_step: int
    metric_state: dict[str, float]
    cumulative_runtime_seconds: float


class MetricsTracker(Protocol):
    def log(self, metrics: EpochMetrics) -> None:
        """Write one rank-zero epoch metric payload."""


def validate_training_components(
    context: DistributedContext,
    components: TrainingComponents,
) -> None:
    """Reject an implicit or mismatched distributed sampler contract."""

    context.validate()
    if not context.is_distributed:
        return
    torch = _torch()
    sampler: Any = getattr(components.train_loader, "sampler", None)
    if not isinstance(sampler, torch.utils.data.DistributedSampler):
        raise ValueError("distributed training requires a DistributedSampler")
    if sampler.rank != context.rank or sampler.num_replicas != context.world_size:
        raise ValueError("DistributedSampler rank and world size must match the context")


def _capture_random_state() -> dict[str, object]:
    torch = _torch()
    cuda_state: list[Any] | None = None
    # CUDA RNG persistence is exercised by the guarded two-device smoke test.
    if torch.cuda.is_available():  # pragma: no cover - requires a CUDA host
        cuda_state = list(torch.cuda.get_rng_state_all())
    return {
        "python": random.getstate(),
        "torch_cpu": torch.get_rng_state(),
        "torch_cuda": cuda_state,
    }


def _restore_random_state(state: Mapping[str, object]) -> None:
    torch = _torch()
    python_state = state.get("python")
    cpu_state = state.get("torch_cpu")
    if python_state is None or cpu_state is None:
        raise ValueError("checkpoint random state is incomplete")
    random.setstate(cast(tuple[Any, ...], python_state))
    torch.set_rng_state(cpu_state)
    cuda_state = state.get("torch_cuda")
    if cuda_state is not None:
        if not torch.cuda.is_available():
            raise ValueError("checkpoint contains CUDA random state on a CPU-only host")
        torch.cuda.set_rng_state_all(cuda_state)  # pragma: no cover - requires a CUDA host


def _all_rank_random_states(context: DistributedContext) -> dict[int, object]:
    local_state = _capture_random_state()
    if not context.is_distributed:
        return {context.rank: local_state}
    # The distributed branch is covered by the two-process Gloo integration test.
    # Coverage data from the spawned ranks is intentionally not merged into this gate.
    torch = _torch()  # pragma: no cover - exercised in spawned ranks
    device = torch.device(context.device)  # pragma: no cover - exercised in spawned ranks
    serialized = pickle.dumps(  # pragma: no cover - exercised in spawned ranks
        local_state, protocol=pickle.HIGHEST_PROTOCOL
    )
    local_size = torch.tensor(  # pragma: no cover - exercised in spawned ranks
        [len(serialized)], dtype=torch.int64, device=device
    )
    gathered_sizes = [  # pragma: no cover - exercised in spawned ranks
        torch.zeros_like(local_size) for _ in range(context.world_size)
    ]
    torch.distributed.all_gather(gathered_sizes, local_size)  # pragma: no cover
    sizes = [int(size.item()) for size in gathered_sizes]  # pragma: no cover
    maximum_size = max(sizes)  # pragma: no cover
    local_bytes = torch.zeros(  # pragma: no cover - exercised in spawned ranks
        maximum_size, dtype=torch.uint8, device=device
    )
    if serialized:  # pragma: no cover - exercised in spawned ranks
        local_bytes[: len(serialized)] = torch.tensor(  # pragma: no cover
            list(serialized),
            dtype=torch.uint8,
            device=device,
        )
    gathered_bytes = [  # pragma: no cover - exercised in spawned ranks
        torch.zeros_like(local_bytes) for _ in range(context.world_size)
    ]
    torch.distributed.all_gather(gathered_bytes, local_bytes)  # pragma: no cover
    return {  # pragma: no cover - exercised in spawned ranks
        rank: pickle.loads(bytes(buffer[: sizes[rank]].cpu().tolist()))
        for rank, buffer in enumerate(gathered_bytes)
    }


class CheckpointStore:
    """Atomic rank-zero checkpoint persistence inside one managed run root."""

    CHECKPOINT_SCHEMA_VERSION = 1
    CHECKPOINT_FIELDS = frozenset(
        {
            "schema_version",
            "model",
            "optimizer",
            "scheduler",
            "completed_epoch",
            "global_step",
            "metric_state",
            "cumulative_runtime_seconds",
            "random_state_by_rank",
        }
    )

    def __init__(self, directory: Path, *, managed_root: Path) -> None:
        lexical_root = Path(os.path.abspath(managed_root.expanduser()))
        lexical_directory = Path(os.path.abspath(directory.expanduser()))
        resolved_root = lexical_root.resolve(strict=False)
        resolved_directory = lexical_directory.resolve(strict=False)
        if lexical_root != resolved_root or lexical_directory != resolved_directory:
            raise ValueError("checkpoint paths must not contain symlink components")
        if resolved_directory == resolved_root or not resolved_directory.is_relative_to(
            resolved_root
        ):
            raise ValueError("checkpoint directory must be inside managed_root")
        self._managed_root = resolved_root
        self._directory = resolved_directory
        self._path = self._directory / "last.pt"

    @property
    def path(self) -> Path:
        return self._path

    def save(
        self,
        components: TrainingComponents,
        context: DistributedContext,
        *,
        completed_epoch: int,
        global_step: int,
        metric_state: Mapping[str, float],
        cumulative_runtime_seconds: float,
        random_state_by_rank: Mapping[int, object],
    ) -> Path | None:
        """Atomically save one completed epoch on rank zero only."""

        if not context.is_rank_zero:
            return None
        if completed_epoch < 1 or global_step < 0:
            raise ValueError("checkpoint progress counters are invalid")
        if cumulative_runtime_seconds < 0 or not math.isfinite(cumulative_runtime_seconds):
            raise ValueError("cumulative runtime must be finite and non-negative")
        torch = _torch()
        payload = {
            "schema_version": self.CHECKPOINT_SCHEMA_VERSION,
            "model": components.model.state_dict(),
            "optimizer": components.optimizer.state_dict(),
            "scheduler": (
                components.scheduler.state_dict() if components.scheduler is not None else None
            ),
            "completed_epoch": completed_epoch,
            "global_step": global_step,
            "metric_state": dict(metric_state),
            "cumulative_runtime_seconds": float(cumulative_runtime_seconds),
            "random_state_by_rank": dict(random_state_by_rank),
        }
        self._directory.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self._directory,
            prefix=".last.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                torch.save(payload, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, self._path)
            directory_descriptor = os.open(
                self._directory,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise
        return self._path

    def restore(
        self,
        components: TrainingComponents,
        context: DistributedContext,
    ) -> CheckpointState | None:
        """Restore the last completed epoch and this rank's random state."""

        if not self._path.exists():
            return None
        torch = _torch()
        payload = torch.load(self._path, map_location=context.device, weights_only=False)
        if not isinstance(payload, dict) or set(payload) != self.CHECKPOINT_FIELDS:
            raise ValueError("checkpoint fields are invalid")
        if payload["schema_version"] != self.CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("checkpoint schema version is unsupported")
        components.model.load_state_dict(payload["model"])
        components.optimizer.load_state_dict(payload["optimizer"])
        scheduler_state = payload["scheduler"]
        if scheduler_state is not None:
            if components.scheduler is None:
                raise ValueError("checkpoint scheduler state has no matching scheduler")
            components.scheduler.load_state_dict(scheduler_state)
        rank_states = payload["random_state_by_rank"]
        if not isinstance(rank_states, dict) or context.rank not in rank_states:
            raise ValueError(f"checkpoint has no random state for rank {context.rank}")
        _restore_random_state(rank_states[context.rank])
        metric_state = payload["metric_state"]
        if not isinstance(metric_state, dict):
            raise ValueError("checkpoint metric state is invalid")
        return CheckpointState(
            completed_epoch=int(payload["completed_epoch"]),
            global_step=int(payload["global_step"]),
            metric_state={str(key): float(value) for key, value in metric_state.items()},
            cumulative_runtime_seconds=float(payload["cumulative_runtime_seconds"]),
        )


def run_training(
    config: TrainingConfig,
    context: DistributedContext,
    components: TrainingComponents,
    checkpoint_store: CheckpointStore,
    *,
    tracker: MetricsTracker | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[EpochMetrics, ...]:
    """Train through completed epochs and checkpoint each epoch boundary."""

    validate_training_components(context, components)
    torch = _torch()
    if context.is_distributed:
        if not torch.distributed.is_initialized():
            raise RuntimeError("distributed context must be initialized before training")
        if (
            torch.distributed.get_rank() != context.rank
            or torch.distributed.get_world_size() != context.world_size
        ):
            raise RuntimeError("initialized process group does not match the context")

    random.seed(config.seed + context.rank)
    torch.manual_seed(config.seed + context.rank)
    device = torch.device(context.device)
    components.model.to(device)
    restored = checkpoint_store.restore(components, context) if config.resume else None
    completed_epoch = restored.completed_epoch if restored is not None else 0
    global_step = restored.global_step if restored is not None else 0
    cumulative_runtime = restored.cumulative_runtime_seconds if restored is not None else 0.0
    metric_state = dict(restored.metric_state) if restored is not None else {}
    if completed_epoch >= config.epochs:
        return ()

    training_model = components.model
    if context.is_distributed:
        # The two-process Gloo test covers DDP without merging child coverage.
        training_model = torch.nn.parallel.DistributedDataParallel(  # pragma: no cover
            components.model,
            device_ids=[context.local_rank] if context.device.startswith("cuda:") else None,
        )

    results: list[EpochMetrics] = []
    for epoch in range(completed_epoch + 1, config.epochs + 1):
        sampler: Any = getattr(components.train_loader, "sampler", None)
        if context.is_distributed:
            sampler.set_epoch(epoch)  # pragma: no cover - exercised in spawned ranks
        training_model.train()
        epoch_started = clock()
        loss_sum = 0.0
        correct = 0
        sample_count = 0
        for features, labels in components.train_loader:
            features = features.to(device)
            labels = labels.to(device)
            components.optimizer.zero_grad(set_to_none=True)
            logits = training_model(features)
            loss = components.criterion(logits, labels)
            loss.backward()
            components.optimizer.step()
            batch_size = int(labels.shape[0])
            loss_sum += float(loss.detach().item()) * batch_size
            correct += int((logits.detach().argmax(dim=1) == labels).sum().item())
            sample_count += batch_size
            global_step += 1
        if components.scheduler is not None:
            components.scheduler.step()

        totals = torch.tensor(
            [loss_sum, float(correct), float(sample_count)],
            dtype=torch.float64,
            device=device,
        )
        if context.is_distributed:
            torch.distributed.all_reduce(totals)  # pragma: no cover - spawned ranks
        total_loss, total_correct, total_samples = (float(value) for value in totals.tolist())
        if total_samples <= 0:
            raise ValueError("training loader produced no samples")
        cumulative_runtime += max(0.0, clock() - epoch_started)
        metric_state["best_accuracy"] = max(
            metric_state.get("best_accuracy", 0.0),
            total_correct / total_samples,
        )
        metrics = EpochMetrics(
            epoch=epoch,
            global_step=global_step,
            loss=total_loss / total_samples,
            accuracy=total_correct / total_samples,
            sample_count=int(total_samples),
            cumulative_runtime_seconds=cumulative_runtime,
            metric_state=dict(metric_state),
        )
        random_states = _all_rank_random_states(context)
        checkpoint_store.save(
            components,
            context,
            completed_epoch=epoch,
            global_step=global_step,
            metric_state=metric_state,
            cumulative_runtime_seconds=cumulative_runtime,
            random_state_by_rank=random_states,
        )
        if tracker is not None and context.is_rank_zero:
            tracker.log(metrics)
        if context.is_distributed:
            torch.distributed.barrier()  # pragma: no cover - exercised in spawned ranks
        results.append(metrics)
    return tuple(results)


__all__ = [
    "CheckpointStore",
    "DistributedContext",
    "EpochMetrics",
    "TrainingComponents",
    "TrainingConfig",
    "run_training",
    "validate_training_components",
]
