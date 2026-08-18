# PyTorch Autoresearch Template

This repository is a concrete PyTorch implementation of the domain-neutral
`$autoresearch` skill. It provides single-node distributed training, completed-epoch
recovery, optional W&B telemetry, local process supervision, durable research events,
and `$notify-wake` event production.

Downstream projects supply real models, datasets, data loaders, objectives, and study
definitions. Mid-epoch recovery and multi-node elastic training are out of scope.

The distribution remains named `python-template` so a new project can choose its own
name. Hatch VCS derives versions from Git. This change does not add a static version or
release tag.

## Clone and initialize

Clone the template and its shallow skills submodule:

```bash
git clone --recurse-submodules https://github.com/TidalPaladin/autoresearch.git
cd autoresearch
uv sync --frozen --all-groups
make check
```

If the repository is already cloned, initialize the skill tree explicitly:

```bash
git submodule update --init --depth 1 .agents/skills
```

The repository requires Python 3.12 or later and `uv==0.11.28`.

## Shared skill and template boundary

The canonical domain-neutral skill is
`.agents/skills/autoresearch/SKILL.md`. The `.agents/skills` directory is an HTTPS,
shallow submodule of [`TidalPaladin/skills`](https://github.com/TidalPaladin/skills).
The gitlink and `notify-wake-runtime==1.0.0` source dependency use the same exact skills
commit.

The shared skill owns research discipline, study definitions, provenance, local logs,
resource controls, promotion rules, sparse monitoring, goal handling, and adapter
requirements. The shared `$notify-wake` runtime owns app-server transport, authority
capture, delivery state, reconciliation, retries, root-task delivery, and owned goal
waits.

This template owns these PyTorch-specific mechanisms:

- one-host Gloo on CPU and NCCL on CUDA
- explicit rank, local-rank, world-size, device, backend, and sampler validation
- completed-epoch checkpointing and resume
- process-group supervision and cleanup
- rank-zero research state, terminal events, log entries, and telemetry
- optional manifest-gated W&B tracking
- deterministic synthetic-classification tests and demonstration code
- research event production and attention predicates.

Do not copy or edit the skill inside this repository. Make shared policy changes in the
skills repository, merge them, then update all exact pins together.

### Update the shared source

Use a merged commit from the skills repository's `main` branch:

```bash
git -C .agents/skills fetch origin main
git -C .agents/skills checkout <merged-skills-sha>
```

Set the same 40-character SHA in these locations:

- the `.agents/skills` gitlink
- `[tool.uv.sources].notify-wake-runtime` in `pyproject.toml`
- `NOTIFY_WAKE_RUNTIME` in `Makefile`
- the locked Git source in `uv.lock`.

Then refresh and validate:

```bash
uv lock
make source-link-check
git diff --submodule=log
```

`scripts/validate_source_links.py` fails if the submodule is absent, uninitialized,
dirty, at the wrong commit, or inconsistent with any runtime pin.

## PyTorch interfaces

Install the optional training dependencies with exact locked versions:

```bash
uv sync --frozen --all-groups --extra pytorch --extra wandb
```

The package exports these typed interfaces from `project.research`:

- `TrainingConfig`
- `DistributedContext`
- `TrainingComponents`
- `EpochMetrics`
- `CheckpointStore`
- `TorchSupervisor`
- `WandbTracker`

`TrainingComponents` accepts downstream model, optimizer, loader, loss, and optional
scheduler objects. A distributed loader must use a `DistributedSampler` whose rank and
replica count match `DistributedContext`. Call `DistributedContext.initialize()` before
distributed training and `close()` after all ranks have stopped.

`CheckpointStore` writes `last.pt` through a synced same-directory temporary file and
atomic replacement. Rank zero writes only after an epoch completes. Each checkpoint
contains the model, optimizer, optional scheduler, completed epoch, global step, metric
state, cumulative runtime, and random state for every rank. Resume starts after the last
completed epoch.

Run the deterministic CPU demonstration with one or two processes:

```bash
uv run --extra pytorch python scripts/synthetic_classification.py \
  --backend gloo --world-size 2 --epochs 2
```

The guarded NCCL target requires at least two visible CUDA devices. It reports `SKIP`
and exits successfully when the host does not meet that requirement:

```bash
make cuda-smoke
```

## W&B contract

W&B is optional and has no standing authorization. The caller must supply an exact
entity, project, mode (`online`, `offline`, or `disabled`), and emitted-data manifest.
Only rank zero initializes or writes a run. The tracker emits only manifest-listed
aggregate fields. Tests use an injected fake client and never contact W&B.

Review the destination, data classification, account, credentials, retention, and
external-write authorization before selecting `online` mode.

## Notification eligibility

Prepare notification state before launching the supervised process. A launch is eligible
only when either condition is true:

- the user explicitly requested a durable wake, or
- the recorded estimate is strictly greater than 600 seconds.

Exactly 600 seconds and unknown estimates use an ordinary bounded wait or status check.
The persisted plan is immutable and records the estimate basis.

Capture the live Codex wake context through the shared runtime, then persist it with the
eligible plan. The template does not implement app-server RPCs or infer missing
authority.

## Terminal-first event production

Terminal truth and notification delivery are separate operations:

```python
from datetime import UTC, datetime
from pathlib import Path

from project.research.runtime import (
    StudyConfig,
    queue_terminal_notification,
    record_terminal_event,
)

study = StudyConfig.load(
    Path("research/studies/example.yaml"),
    base_dir=Path.cwd(),
)
started_at = datetime.now(UTC)

# Run the supervised operation, then record its terminal outcome.
terminal = record_terminal_event(
    study,
    "pretrain-baseline-seed0",
    attempt=1,
    status="completed",
    operation_started_at=started_at,
    elapsed_basis="operation",
)

# Queue only when the persisted plan authorized delivery and the context matches.
notification = queue_terminal_notification(terminal, wake_context)
```

`record_terminal_event()` writes source truth only. It stores immutable operation-start
time, occurrence time, elapsed seconds, and elapsed basis. `queue_terminal_notification()`
creates delivery state only for an eligible prepared watch. `ensure_notification()` may
reconstruct only a notification authorized by the persisted launch plan.

Every wake includes the fixed text `Elapsed before notification: <seconds> seconds`.
Retries reuse the terminal event's elapsed value. Wake payloads contain only identifiers,
status, timing evidence, and the absolute terminal path. They exclude raw logs, errors,
stack traces, model output, and training output.

Rank zero is the only writer for checkpoints, progress state, research-log entries,
terminal events, and W&B telemetry.

## Legacy cutover inventory

Pre-timing adapter records are rejected with a cutover-required error. Inventory them
without migration or mutation:

```bash
uv run python scripts/research.py cutover-inventory \
  --root logs/research --format json
```

The output always reports the fixed manifest path:
`<managed-root>/.notify-wake/v2/cutover-manifest.json`. After verifying that no live
legacy identity exists, write an immutable evidence manifest with the exact merged skills
commit:

```bash
uv run python scripts/research.py cutover-inventory \
  --root logs/research \
  --write-manifest \
  --source-commit <merged-skills-sha>
```

This command hashes and classifies legacy evidence. It does not transform, delete,
requeue, or migrate a legacy record. It refuses unreadable evidence and delivery states
that may still be live.

## Notification worker

Repository code requires an existing Codex app-server daemon. It does not start, restart,
or stop the daemon. Deliver due events once through the daemon's Unix socket:

```bash
uv run python scripts/research.py notify-worker --once --root logs/research
```

Use `--socket /absolute/path/to/app-server.sock` for an explicit socket. Inspect or
recover an authorized run notification with:

```bash
uv run python scripts/research.py notify research/studies/example.yaml <run-id>
```

Delivery uses the shared `research_compatibility` policy. Notification failure changes
only delivery state. It cannot change a terminal training result. The shared owned-goal
lease may reactivate only the exact blocked goal revision that it owns.

## Local state

Runtime state stays under the managed research root:

```text
logs/research/
  .autoresearch-root.json
  .notify-wake/
    v2/
      .notify-wake-root.json
      contexts/<study-id>/<run-id>/
        wake-context.json
        notification-plan.json
      current/<study-id>/<run-id>.json
      events/<event-id>/
        terminal.json
        notification.json
  <study-id>/
    research-log.md
    runs/<run-id>/
```

State paths must be absolute after resolution, remain inside the declared root, and not
use symlink components. Generated research state, logs, credentials, and app-server
schemas stay out of Git.

## Validation

Run the repository gates with the required uv version:

```bash
make check
make test-pytorch
make test-notify-loop
make package-check
make cuda-smoke
git diff --check
```

`make check` validates the shared source link, formatting, Ruff, Basedpyright, core tests
with at least 90 percent branch coverage, and dependency advisories. `make test-pytorch`
type-checks the optional entry points, enforces a separate 90 percent branch-coverage
gate for the optional adapters, and runs a two-process Gloo test. CI adds a required
hosted `PyTorch / Python 3.14` job and includes it in the stable `Required` aggregate.

## Create a downstream project

1. Rename `project/` and update its imports.
2. Change the distribution name in `pyproject.toml` and the package smoke test.
3. Replace the example study definition.
4. Implement the real model and data-loader factory behind `TrainingComponents`.
5. Define explicit resource limits, attention predicates, emitted-data manifests, and
   promotion rules.
6. Keep the shared skills gitlink and runtime pin exact and synchronized.
7. Run every validation gate before publication.
