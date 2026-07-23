# Python Autoresearch Template

This repository is a Python project template for recoverable empirical research. It provides durable terminal-event recording and a separate Codex notification worker. Projects created from the template supply their own training, supervision, heartbeat, metrics, and experiment-domain logic.

The package remains named `project` and the distribution remains named `python-template` so a new project can choose its own names. Versions continue to come from Git through Hatch VCS. The template does not contain a static release version or release tag.

## Create a project from the template

1. Rename `project/` to the import package name.
2. Change `project.research` imports in `scripts/research.py` and the tests.
3. Change `name = "python-template"` in `pyproject.toml`.
4. Update the package name used by `make package-check`.
5. Replace the example study ID in `research/studies/example.yaml`.
6. Run `uv sync --frozen --all-groups`, then `make check`.

The repository requires Python 3.12 or later and `uv==0.11.28`. Direct dependencies and build tools are pinned in `pyproject.toml`; `uv.lock` makes the full environment reproducible.

## Notification architecture

Training and Codex communication are separate processes:

```text
project worker
  -> registers the exact managed root
  -> writes terminal.json
  -> writes notification.json with state pending
  -> notification worker reads the event
  -> app-server resumes or steers the originating Codex task
```

`project/research/runtime.py` performs local file operations only. It writes terminal state before notification state and never waits for Codex. `project/research/codex_notifications.py` owns queue delivery and app-server communication. `scripts/research.py` exposes acknowledgement and one-shot worker commands.

A notification error cannot change a terminal status such as `completed`, `failed`, or `timed_out`. It changes only delivery metadata in `notification.json`. Notification discovery starts only after the worker validates the root's registration marker.

## Study and state layout

A study file contains the validated study ID and managed log root:

```yaml
id: example
log_root: logs/research
```

Relative log roots resolve from the current working directory. Run commands from the repository root or load the configuration with an explicit `base_dir` in Python.

Current state uses this layout:

```text
logs/research/
  .autoresearch-root.json
  .notification-locks/
    <sha256-thread-id>.lock
    <sha256-thread-id>.accepted.json
  <study-id>/
    .research-log.md.lock
    research-log.md
    runs/
      <run-id>/
        .state.lock
        terminal.json
        notification.json
        attempts/
          <attempt>-<event-id>/
            terminal.json
            notification.json
```

`.autoresearch-root.json` contains an exact schema version, marker kind, and canonical absolute root path. `record_terminal_event` creates it through an atomic same-directory replacement before producing queue state. An existing root without this marker is never scanned. Registration rejects files, filesystem and top-level roots, home directories and their parents, repository roots, broad working-directory parents, symlinked paths, and malformed or mismatched markers.

The current terminal and notification pair always describes one event. Recording a different event archives the prior pair before replacement. Repeating the same event ID with the same terminal fields is idempotent. Identifiers cannot contain separators, whitespace, or traversal components. Persisted paths must be absolute, remain under the declared log root after symlink resolution, and match between the terminal and notification records.

### Register an existing root

Roots created by a terminal-event producer are registered automatically. Register an existing queue before its first worker sweep:

```bash
uv run python scripts/research.py register-root --root logs/research
```

The command is idempotent and does not scan or replace existing queue contents. A nonexistent worker root remains an empty successful sweep, but an existing unregistered root returns validation exit code `1`. Inspect registration as JSON with `--format json`; the document contains `created`, `root`, and `marker`.

Runtime state under `logs/research/` is ignored by Git. Do not commit generated terminal files, notification files, locks, accepted-event ledgers, logs, credentials, or app-server schemas.

## Record terminal state from project code

Call `record_terminal_event` from an outer supervisor after it has classified the process outcome. The supervisor remains responsible for timeout enforcement, child exits, signals, checkpoints, and domain-specific recovery.

```python
from datetime import UTC, datetime
from pathlib import Path

from project.research.runtime import StudyConfig, record_terminal_event

study = StudyConfig.load(
    Path("research/studies/example.yaml"),
    base_dir=Path.cwd(),
)

terminal, notification = record_terminal_event(
    study,
    "pretrain-baseline-seed0",
    attempt=1,
    status="completed",
    occurred_at=datetime.now(UTC),
)
```

The originating task defaults to `CODEX_THREAD_ID`. Pass `originating_thread_id=` when the host exposes the ID through another trusted source. A missing task ID produces durable terminal state, but the notification worker marks delivery failed because it has no safe destination.

If the process stops after `terminal.json` is synced but before `notification.json` is queued, reconstruct the pending event with:

```bash
uv run python scripts/research.py notify research/studies/example.yaml pretrain-baseline-seed0
```

## Append the research log

Use `append_research_log` as the single-writer primitive for a shared Markdown study log. It acquires the stable `.research-log.md.lock`, re-reads after locking, creates the header with the first entry, and replaces the log through a synced same-directory temporary file.

```python
from pathlib import Path

from project.research.runtime import TerminalLogIdentity, append_research_log

append_research_log(
    Path("logs/research/example/research-log.md"),
    managed_root=Path("logs/research"),
    header_operation_id="example-header",
    header_markdown="# Example study\n\nFixed protocol.",
    operation_id="pretrain-baseline-seed0-attempt-1",
    markdown="## Completed\n\nHeadline metrics and provenance.",
    terminal=TerminalLogIdentity("example", "pretrain-baseline-seed0", 1),
)
```

The helper stores internal HTML-comment metadata with each complete Markdown block. Replaying the same operation or terminal attempt returns `False` without changing the file. Reusing an operation ID with different content fails validation. Terminal deduplication uses `study_id`, `run_id`, and `attempt`, so separate attempts remain separate even when their phase or event ID matches.

## Run a persistent Codex app-server daemon

Repository code requires an existing daemon. It never starts, restarts, or stops app-server. The operator owns daemon lifecycle and authentication.

With Codex CLI 0.145.0, start and inspect the managed local daemon outside this repository:

```bash
codex app-server daemon start
codex app-server daemon version
```

The implementation baseline is app-server schema 0.145.0, including the
persistent-goal APIs used to reactivate blocked research goals. Later compatible
versions can work, but unknown statuses, fields, protocol errors, and lifecycle
races remain queued instead of being guessed around. Review the current
[Codex App Server documentation](https://learn.chatgpt.com/docs/app-server.md)
before changing the client.

## Deliver through the daemon Unix socket

The worker connects directly with a WebSocket handshake over the daemon's
local Unix socket. By default it discovers the running socket through
`codex app-server daemon version`:

```bash
uv run python scripts/research.py notify-worker --once \
  --root logs/research
```

Use `--socket /absolute/path/to/app-server.sock` to select a non-default daemon
socket. The client disables WebSocket compression and the user-agent header and
accepts app-server messages up to 16 MiB. Do not expose an unauthenticated
app-server listener on a shared or public network. TCP WebSocket support is
experimental and is not implemented by this template.

## CLI behavior

Register an exact worker root or validate its existing marker:

```bash
uv run python scripts/research.py register-root --root <path>
```

Inspect, reconstruct, or explicitly requeue one run:

```bash
uv run python scripts/research.py notify <study.yaml> <run-id>
uv run python scripts/research.py notify <study.yaml> <run-id> --requeue
```

Process each due current event at most once:

```bash
uv run python scripts/research.py notify-worker --once \
  [--root logs/research] \
  [--socket PATH]
```

All commands support `--format text|json`, `--color auto|always|never`, `--no-color`, and mutually exclusive `--quiet` or `--verbose`. Primary text or JSON goes to stdout. Warnings and diagnostics go to stderr. JSON is deterministic and never contains ANSI color.

Exit codes are:

| Code | Meaning |
| ---: | --- |
| `0` | The command succeeded. For a worker sweep, every due event was accepted. |
| `1` | Validation or delivery problems remain, including a scheduled retry or failed event. |
| `2` | The invocation is invalid, or the CLI encountered a runtime or I/O failure. |

`notify` is a state acknowledgement interface. It can reconstruct a missing notification and reset a failed event with `--requeue`, but it cannot record app-server acceptance. Only `notify-worker` writes `accepted` after a successful `turn/start` or `turn/steer` response.

## Delivery lifecycle

| State | Worker action | Next state |
| --- | --- | --- |
| `pending`, not due | Skip this sweep | `pending` |
| `pending`, blocked goal | Set the goal to `active`, then deliver by task state | Continue below |
| `pending`, idle task | Send `turn/start` | `accepted` after the response |
| `pending`, active task | Send `turn/steer` with the newest in-progress turn's expected ID | `accepted` after the response |
| `pending`, connection or protocol failure | Record one attempt and full-jitter backoff | `pending` |
| `pending`, permanently invalid or eighth failed attempt | Stop automatic delivery | `failed` |
| `failed`, explicit `notify --requeue` | Reset delivery metadata | `pending` |
| `accepted` | Deduplicate by event ID under the task lock | `accepted` |

Backoff starts at 5 seconds, doubles per attempt, caps at 300 seconds, and uses full jitter. Each worker sweep records at most one attempt per due event. Acceptance is also recorded in a per-task ledger, and the event ID is sent as `clientUserMessageId` for app-server deduplication.

Before waking the task, the worker queries its persistent goal. It reactivates
only a `blocked` goal. Explicit `paused`, `complete`, `usageLimited`, and
`budgetLimited` states are preserved, and a task without a goal is still
deliverable.

The wake message contains only validated identifiers, terminal status, and the absolute `terminal.json` path:

```text
Research run completed.
Study: <study-id>
Run: <run-id>
Status: <terminal-status>
Terminal state: <absolute-terminal-json-path>

Inspect the terminal state and continue the study protocol.
```

Raw logs, stack traces, error text, model output, and training output never enter the prompt.

## Event-driven wakeups and scheduled fallback

Prefer a host controller or local non-model watcher that invokes the one-shot
worker after `notification.json` is durable. Do not keep a ChatGPT or Codex turn
open to sleep or poll terminal files. The watcher must not change terminal
training state or make training wait for Codex.

The generic template implements terminal events and leaves trainer and
supervisor mechanics to the domain adapter. Long-running adapters should add
one-shot lifecycle records for the first recovery-confirming
train-validation-checkpoint cycle, supervisor loss without terminal state, and
trainer-progress stalls. Drive their controller from durable file events,
process-exit handles, and explicit progress deadlines. Routine progress,
heartbeats, notification retries, and acceptance writes must not wake Codex or
retrigger delivery. After a transport failure, keep events queued and gate more
automatic attempts until daemon socket replacement or a due sparse recovery
check.

When an event source is unavailable, a scheduled task can run the worker as a
sparse fallback:

```bash
uv run python scripts/research.py notify-worker --once --root logs/research
```

Create and own that schedule in the ChatGPT desktop app. Explicitly select
GPT-5.6 Luna with medium reasoning once in the scheduled-task configuration;
do not inherit a higher-cost chat default. A scheduled task inside the
originating chat retains that chat's context. Keep the computer on, the app
running, and the repository available. This template does not create or modify
schedules. See the [scheduled tasks documentation](https://learn.chatgpt.com/docs/automations.md).

App-server `turn/start` also accepts `model` and `effort` overrides, so a
dedicated idle monitor wake can select `gpt-5.6-luna` and `medium`
automatically. `turn/steer` cannot replace the model of an active turn. The
template notifier sends the override when starting an idle task; adapters inherit
that automatic event-wake selection.

Usage reporting is opportunistic. During an existing monitoring, terminal, or
handoff report, sample current Codex rate-limit telemetry once when available
and include the observation time, used and remaining percentages, reset time,
and change from the prior report. Do not create a separate scheduled task, wake,
wait, or polling loop for usage alone. The sample does not count as a research
monitoring check.

When an authorized pull request includes terminal comparative results, refresh
its body after pushing the result commit. Add a `## Findings` table generated
from the committed structured summary with every evaluated variant or
preregistered aggregate, key hyperparameters, primary and convergence metrics,
per-run wall time or another predefined resource measure, and promotion
decision. Report total study wall span and summed run time or compute cost
separately, mark censored results, and distinguish active from wall time and
nominal from effective hyperparameters. Omit this section for protocol-only
changes and studies that are still active.

## Security and failure boundaries

- Treat study YAML, persisted JSON, file paths, app-server messages, and daemon errors as untrusted input.
- Validate the exact managed-root marker before recursive notification discovery. Do not infer ownership from an existing directory or its contents.
- Keep the daemon and Unix socket local. Apply filesystem permissions appropriate to the host.
- Never put secrets, raw samples, logs, stack traces, or training output in a wake prompt.
- Never let notification delivery change a terminal training result.
- Never let the training process wait for Codex availability.
- Use fake Unix-socket servers in tests. Automated tests must not resume, steer, or wake a real Codex task.
- Do not add daemon lifecycle management to the training process, supervisor, or notification worker.

## Downstream adapter contract

The generic package does not implement training, external trackers, process supervision, or monitoring schedules. A downstream adapter must implement these protocol rules:

- Declare an emitted-data-class manifest for each tracker operation, including launch, summary, backfill, configuration, and provenance. An online write requires the exact destination and approval for every emitted class. Otherwise the complete operation remains local, and provenance records the requested and effective modes.
- Own the child process group after spawn. On heartbeat or state-write failure, cancellation, interrupt, or another exceptional exit, terminate the group, escalate when required, and reap every child before releasing GPU or other resource locks.
- Change a run's polling counter and `next_check_at` only when that run is due. A wake for a terminal run clears only that run's poll and leaves every unrelated run's counters and schedule unchanged.
- Use registered managed roots and `append_research_log` semantics for notification state and the shared study log.

## Development commands

```bash
make format         # rewrite Python formatting
make lint           # Ruff lint checks
make types          # Basedpyright
make test           # pytest with branch coverage, minimum 90 percent
make audit          # pip-audit across all locked groups
make check          # all non-rewriting gates
make package-check  # build and import the wheel in an isolated environment
```

## Canonical autoresearch skill

The maintained skill is `.agents/skills/autoresearch/` in this repository. Use it for experiment planning, launch, recovery, monitoring, comparison, and notification handling. Copy that directory into downstream repositories so each project carries the contract it implements:

```bash
mkdir -p /path/to/downstream/.agents/skills/autoresearch
cp -R .agents/skills/autoresearch/. \
  /path/to/downstream/.agents/skills/autoresearch/
```

Synchronize downstream copies from this repository and validate the result with:

```bash
uv run python "${CODEX_HOME:-${HOME}/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" \
  .agents/skills/autoresearch
```

The separately installed `~/.codex/skills/autoresearch` copy is deprecated. Do not edit it as a source. Remove it only after every consumer uses a repository copy or another installation synchronized from this canonical directory.
