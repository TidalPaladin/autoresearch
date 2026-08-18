# AGENTS.md

This repository is the PyTorch implementation template for the canonical domain-neutral
`$autoresearch` skill.

## Source ownership

- Treat `.agents/skills` as a read-only shallow submodule of
  `https://github.com/TidalPaladin/skills.git`.
- Keep the `.agents/skills` gitlink, the `notify-wake-runtime` source in
  `pyproject.toml`, the Makefile package smoke-test source, and `uv.lock` pinned to the
  same exact merged skills commit.
- Make shared research-policy changes in the skills repository. Do not copy or
  synchronize skill files from this template.
- Run `make source-link-check` after every submodule or shared-runtime update.

## Template scope

- Keep the implementation limited to one host: Gloo on CPU and NCCL on CUDA.
- Require explicit rank, local rank, world size, device, backend, and
  `DistributedSampler` identity.
- Checkpoint only after a completed epoch. Persist model, optimizer, optional scheduler,
  global step, metric state, cumulative runtime, and every rank's random state through
  atomic replacement.
- Resume only from the last completed epoch. Do not add mid-epoch or multi-node elastic
  recovery without a separately reviewed design.
- Keep real models and data loaders downstream. Maintain the deterministic synthetic
  classification example as the repository demonstration.
- After child spawn, supervise the full local process group until every process is
  stopped and reaped. Persist terminal evidence only after cleanup.
- Make rank zero the only writer for checkpoints, progress state, research logs,
  terminal events, and telemetry.

## W&B

- W&B has no standing authorization.
- Require an exact entity, project, mode, and emitted-data manifest.
- Emit only manifest-listed aggregate fields. Do not emit samples, logs, errors, model
  output, training output, or undeclared configuration.
- Use injected fake or offline clients in tests. Tests must not contact W&B.

## Notify-wake boundary

- Keep research event production and attention predicates in this repository.
- Use `notify-wake-runtime` for app-server transport, authority capture, reconciliation,
  delivery state, retries, root delivery, and owned goal waits. Do not implement local
  app-server compatibility code.
- Persist an immutable launch plan before process start. Authorize a durable wake only
  for an explicit request or an estimated runtime strictly greater than 600 seconds.
  Exactly 600 seconds and unknown estimates use a bounded wait or status check.
- Write and sync terminal truth before notification state. `record_terminal_event()` must
  not create a notification. `queue_terminal_notification()` requires an eligible plan
  and matching prepared wake context.
- Preserve operation-start time, occurrence time, elapsed seconds, and elapsed basis in
  terminal evidence. Keep elapsed seconds fixed across retries.
- Exclude raw logs, errors, stack traces, model output, and training output from wakes.
- Reject pre-timing records with a cutover-required error. Inventory and hash them with
  the non-migrating cutover command. Never infer or synthesize missing timing evidence.
- Use the default `research_compatibility` policy unless strict non-atomicity blocking is
  explicitly required. Preserve authority-mismatch and manual-goal blockers.

## Tests and quality

- Follow TDD for fixes. Confirm a focused regression fails before changing production
  code, then rerun it after the fix.
- Keep core and optional-adapter branch coverage above 90 percent.
- Run these gates before handoff:

```bash
make check
make test-pytorch
make test-notify-loop
make package-check
make cuda-smoke
git diff --check
```

- `make cuda-smoke` must remain guarded and report a skip when fewer than two CUDA
  devices are visible.
- The CI `Quality` job alone initializes the skills submodule for source-link
  validation. Other lightweight jobs must not initialize it unless their checks require
  the skill tree.
- Keep the required hosted `PyTorch / Python 3.14` job in the stable `Required`
  aggregate.

## Documentation

- Update README.md when the shared-source procedure, PyTorch interfaces, state layout,
  notification contract, or validation commands change.
- Use concise ASD-STE100-style technical prose.
