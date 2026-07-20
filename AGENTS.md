# AGENTS.md

## Scope

This repository is a reusable Python autoresearch template. Keep the generic `project` package and `python-template` distribution unless the user asks to initialize a downstream project. Keep training, supervision, heartbeat, metrics, and experiment-domain logic out of the template.

## Autoresearch workflow

- Read and use `.agents/skills/autoresearch/SKILL.md` for every empirical research study, experiment launch, recovery, comparison, or terminal-event workflow.
- Require an active goal with study completion criteria before launching an experiment.
- Recover existing state before creating or changing a study.
- Treat local research logs as canonical and external trackers as optional telemetry.

## Development commands

- Install: `uv sync --frozen --all-groups`
- Format: `make format`
- Lint: `make lint`
- Type check: `make types`
- Test with coverage: `make test`
- Audit locked dependencies: `make audit`
- Run all non-rewriting gates: `make check`
- Verify the built wheel: `make package-check`
- Validate the project skill: `uv run python "${CODEX_HOME:-${HOME}/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" .agents/skills/autoresearch`

Use `uv==0.11.28`. Pin direct dependencies and commit `uv.lock`. Preserve Hatch VCS versioning. Do not add a static project version or create release tags as part of ordinary maintenance.

## Tests

- Follow TDD. Add a failing regression or behavior test before production code.
- Keep branch coverage at or above 90%.
- Test state corruption, path escapes, crash windows, retries, protocol races, and I/O failures, not only successful delivery.
- Use fake JSONL and WebSocket-over-Unix servers. Tests must not connect to or wake a real Codex task.
- Run tests on Python 3.12 and 3.14 for changes to the runtime or notifier.

## State safety

- Write and sync `terminal.json` before `notification.json`.
- Use same-directory atomic replacement, file and directory sync, and stable sibling locks.
- Validate identifiers, schemas, timestamps, matching fields, absolute managed paths, and resolved symlink containment before acting.
- Archive a different prior current event before replacement. Deduplicate retries by event ID.
- Never delete unmanaged or historical experiment artifacts during autonomous work.
- Keep generated state under `logs/research/` and out of Git.

Notification failure must never change terminal training status. The runtime records terminal truth. Only notification delivery state can move between `pending`, `accepted`, and `failed`.

## App-server ownership

- Require an existing persistent Codex app-server daemon.
- Do not start, restart, or stop the daemon from repository code.
- Keep app-server communication out of training and supervisor processes.
- Use `codex app-server proxy` for JSONL stdio or a local WebSocket-over-Unix connection.
- Mark acceptance only after app-server accepts `turn/start` or `turn/steer`.
- Leave unknown states, turn races, protocol errors, and connection failures queued for bounded retry.
- Never auto-approve an app-server request.
- Send only the fixed trusted wake prompt. Do not include raw logs, errors, stack traces, training output, or model output.

## Git and publication

Do not commit, tag, push, create a pull request, publish artifacts, create scheduled tasks, or modify external trackers without explicit user authorization.
