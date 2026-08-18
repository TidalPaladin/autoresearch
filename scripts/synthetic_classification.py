#!/usr/bin/env python3
"""Run a deterministic synthetic classification demonstration."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

import torch
from torch.utils.data import DataLoader, DistributedSampler, TensorDataset

from project.research.pytorch import (
    CheckpointStore,
    DistributedContext,
    TrainingComponents,
    TrainingConfig,
    run_training,
)


def _write_summary(path: Path, payload: dict[str, object]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=".summary.",
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
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def _worker(
    rank: int,
    world_size: int,
    backend: str,
    epochs: int,
    output_text: str,
    init_method: str,
) -> None:
    output = Path(output_text)
    device = "cpu" if backend == "gloo" else f"cuda:{rank}"
    context = DistributedContext(
        rank=rank,
        local_rank=rank,
        world_size=world_size,
        device=device,
        backend=backend,  # type: ignore[arg-type]
    )
    context.initialize(init_method)
    try:
        torch.manual_seed(7)
        features = torch.linspace(-2.0, 2.0, steps=256).reshape(64, 4)
        labels = (features[:, 0] + features[:, 1] > 0).to(torch.long)
        dataset = TensorDataset(features, labels)
        sampler = DistributedSampler(
            dataset,
            num_replicas=world_size,
            rank=rank,
            shuffle=True,
            seed=19,
        )
        loader = DataLoader(dataset, batch_size=8, sampler=sampler)
        model = torch.nn.Linear(4, 2)
        components = TrainingComponents(
            model=model,
            optimizer=torch.optim.SGD(model.parameters(), lr=0.1),
            train_loader=loader,
            criterion=torch.nn.CrossEntropyLoss(),
        )
        store = CheckpointStore(output / "checkpoints", managed_root=output)
        metrics = run_training(
            TrainingConfig(epochs=epochs, seed=23),
            context,
            components,
            store,
        )
        if context.is_rank_zero:
            final = metrics[-1]
            _write_summary(
                output / "summary.json",
                {
                    "accuracy": final.accuracy,
                    "backend": backend,
                    "completed_epoch": final.epoch,
                    "global_step": final.global_step,
                    "loss": final.loss,
                    "world_size": world_size,
                },
            )
    finally:
        context.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("gloo", "nccl"), default="gloo")
    parser.add_argument("--world-size", type=int, default=1)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--output", type=Path, default=Path("logs/research/synthetic-demo"))
    parser.add_argument("--cuda-smoke", action="store_true")
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    if arguments.world_size < 1:
        raise SystemExit("world size must be positive")
    if arguments.backend == "nccl" and torch.cuda.device_count() < arguments.world_size:
        if arguments.cuda_smoke:
            print(
                "SKIP: CUDA smoke requires at least "
                f"{arguments.world_size} CUDA devices; found {torch.cuda.device_count()}."
            )
            return 0
        raise SystemExit("the requested NCCL ranks do not have matching CUDA devices")
    output = arguments.output.expanduser().resolve(strict=False)
    output.mkdir(parents=True, exist_ok=True)
    init_path = output / ".distributed-init"
    init_path.unlink(missing_ok=True)
    init_method = f"file://{init_path}"
    torch.multiprocessing.spawn(  # pyright: ignore[reportPrivateImportUsage]
        _worker,
        args=(
            arguments.world_size,
            arguments.backend,
            arguments.epochs,
            str(output),
            init_method,
        ),
        nprocs=arguments.world_size,
        join=True,
    )
    init_path.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
