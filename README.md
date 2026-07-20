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
  -> writes terminal.json
  -> writes notification.json with state pending
  -> notification worker reads the event
  -> app-server resumes or steers the originating Codex task
```

`project/research/runtime.py` performs local file operations only. It writes terminal state before notification state and never waits for Codex. `project/research/codex_notifications.py` owns queue delivery and app-server communication. `scripts/research.py` exposes acknowledgement and one-shot worker commands.

A notification error cannot change a terminal status such as `completed`, `failed`, or `timed_out`. It changes only delivery metadata in `notification.json`.

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
  .notification-locks/
    <sha256-thread-id>.lock
    <sha256-thread-id>.accepted.json
  <study-id>/
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

The current pair always describes one event. Recording a different event archives the prior pair before replacement. Repeating the same event ID with the same terminal fields is idempotent. Identifiers cannot contain separators, whitespace, or traversal components. Persisted paths must be absolute, remain under the declared log root after symlink resolution, and match between the terminal and notification records.

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

## Run a persistent Codex app-server daemon

Repository code requires an existing daemon. It never starts, restarts, or stops app-server. The operator owns daemon lifecycle and authentication.

With Codex CLI 0.144.5, start and inspect the managed local daemon outside this repository:

```bash
codex app-server daemon start
codex app-server daemon version
```

The implementation baseline is app-server schema 0.144.5. Later compatible versions can work, but unknown statuses, fields, protocol errors, and lifecycle races remain queued instead of being guessed around. Review the current [Codex App Server documentation](https://learn.chatgpt.com/docs/app-server.md) before changing the client.

## Deliver through the stdio proxy

The default transport launches a short-lived `codex app-server proxy` process. The proxy carries JSONL between this worker and the existing daemon control socket. Closing the proxy does not stop the daemon.

```bash
uv run python scripts/research.py notify-worker --once \
  --root logs/research \
  --transport stdio
```

Use `--socket /absolute/path/to/app-server.sock` to select a non-default daemon socket.

## Deliver through a Unix socket

The Unix transport connects directly with a WebSocket handshake over the local Unix socket. Read the managed socket path from the daemon and pass it explicitly:

```bash
APP_SERVER_SOCKET="$(codex app-server daemon version | \
  uv run python -c 'import json, sys; print(json.load(sys.stdin)["socketPath"])')"

uv run python scripts/research.py notify-worker --once \
  --root logs/research \
  --transport unix \
  --socket "$APP_SERVER_SOCKET"
```

Prefer stdio or a local Unix socket. Do not expose an unauthenticated app-server listener on a shared or public network. TCP WebSocket support is experimental and is not implemented by this template.

## CLI behavior

Inspect, reconstruct, or explicitly requeue one run:

```bash
uv run python scripts/research.py notify <study.yaml> <run-id>
uv run python scripts/research.py notify <study.yaml> <run-id> --requeue
```

Process each due current event at most once:

```bash
uv run python scripts/research.py notify-worker --once \
  [--root logs/research] \
  [--transport stdio|unix] \
  [--socket PATH]
```

Both commands support `--format text|json`, `--color auto|always|never`, `--no-color`, and mutually exclusive `--quiet` or `--verbose`. Primary text or JSON goes to stdout. Warnings and diagnostics go to stderr. JSON is deterministic and never contains ANSI color.

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
| `pending`, idle task | Send `turn/start` | `accepted` after the response |
| `pending`, one active turn | Send `turn/steer` with its expected turn ID | `accepted` after the response |
| `pending`, connection or protocol failure | Record one attempt and full-jitter backoff | `pending` |
| `pending`, permanently invalid or eighth failed attempt | Stop automatic delivery | `failed` |
| `failed`, explicit `notify --requeue` | Reset delivery metadata | `pending` |
| `accepted` | Deduplicate by event ID under the task lock | `accepted` |

Backoff starts at 5 seconds, doubles per attempt, caps at 300 seconds, and uses full jitter. Each worker sweep records at most one attempt per due event. Acceptance is also recorded in a per-task ledger, and the event ID is sent as `clientUserMessageId` for app-server deduplication.

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

## Scheduled one-shot polling

A scheduled task can run the worker periodically when maintaining a standalone controller is unnecessary:

```bash
uv run python scripts/research.py notify-worker --once --root logs/research
```

Create and own that schedule in the ChatGPT desktop app. A scheduled task inside the originating chat retains that chat's context and is suited to polling long-running local work. Keep the computer on, the app running, and the repository available. This template does not create or modify schedules. See the [scheduled tasks documentation](https://learn.chatgpt.com/docs/automations.md).

## Security and failure boundaries

- Treat study YAML, persisted JSON, file paths, app-server messages, and daemon errors as untrusted input.
- Keep the daemon and Unix socket local. Apply filesystem permissions appropriate to the host.
- Never put secrets, raw samples, logs, stack traces, or training output in a wake prompt.
- Never let notification delivery change a terminal training result.
- Never let the training process wait for Codex availability.
- Use fake JSONL and Unix-socket servers in tests. Automated tests must not resume, steer, or wake a real Codex task.
- Do not add daemon lifecycle management to the training process, supervisor, or notification worker.

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

The project-specific autoresearch skill is stored at `.agents/skills/autoresearch/`. Use it for experiment planning, launch, recovery, monitoring, comparison, and notification handling.
