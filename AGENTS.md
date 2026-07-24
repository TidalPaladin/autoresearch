# AGENTS.md

## Scope

This repository is a reusable Python autoresearch template. Keep the generic `project` package and `python-template` distribution unless the user asks to initialize a downstream project. Keep training, supervision, heartbeat, metrics, and experiment-domain logic out of the template.

## Autoresearch workflow

- Treat `.agents/skills/autoresearch/` as the canonical skill source. The personal `~/.codex/skills/autoresearch` copy is deprecated and must not be edited as a source.
- Read and use `.agents/skills/autoresearch/SKILL.md` for every empirical research study, experiment launch, recovery, comparison, or terminal-event workflow.
- Require an active goal with study completion criteria before launching an experiment.
- Recover existing state before creating or changing a study.
- Treat local research logs as canonical and W&B as the standing-authorized online telemetry destination. Keep other external trackers separately authorized.

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
- Use fake WebSocket-over-Unix servers. Tests must not connect to or wake a real Codex task.
- Run tests on Python 3.12 and 3.14 for changes to the runtime or notifier.

## State safety

- Register each existing research root explicitly with `scripts/research.py register-root --root <path>` before notification discovery. Producers register new roots automatically.
- Require the exact atomic `.autoresearch-root.json` marker before scanning an existing root. Reject filesystem, home, repository, broad parent, malformed, and symlinked roots.
- Write and sync `terminal.json` before `notification.json`.
- Capture the live originating thread's effective permission-profile identity and approval policy before child spawn. Persist JSON `null` when app-server explicitly reports no named profile, and store both values in an immutable per-run `wake-context.json`.
- Use same-directory atomic replacement, file and directory sync, and stable sibling locks.
- Validate identifiers, schemas, timestamps, matching fields, absolute managed paths, and resolved symlink containment before acting.
- Archive a different prior current event before replacement. Deduplicate retries by event ID.
- Append shared Markdown research logs through the locked runtime helper. Deduplicate all updates by operation ID and terminal entries by study, run, and attempt.
- Never delete unmanaged or historical experiment artifacts during autonomous work.
- Keep generated state under `logs/research/` and out of Git.

Notification failure must never change terminal training status. The runtime records terminal truth. Only notification delivery state can move between `pending`, `accepted`, and `failed`.

## Adapter conformance

- Define the exact destination and emitted data classes for every W&B operation. Standing authorization covers declared non-sensitive metrics, configs, and provenance; fail preflight instead of silently launching a scientific run offline when the destination or manifest is incomplete.
- After spawning a child, own its process group until it is terminated and reaped. Perform this cleanup after every exceptional exit and before releasing GPU or other resource locks.
- Advance a run's monitoring counters or `next_check_at` only when that run is due. A terminal wake must leave unrelated run counters and schedules unchanged and clear only the terminal run's poll.
- Use terminal events as the primary wake path. Never keep a Codex turn open to sleep or poll; a local non-model watcher may wake Codex only for terminal events, exceptional safety conditions, or due sparse watchdog checks.
- For long-running adapters, define one-shot first-cycle, supervisor-loss, and progress-stall lifecycle events. Use durable file events, process-exit handles, and explicit deadlines; never wake for routine progress, heartbeat, notification retry, or acceptance writes.
- Gate delivery after a transport failure until an external readiness signal such as daemon socket replacement or a due sparse recovery check. Notification backoff must not become a polling loop.
- Pin read-only scheduled monitoring to GPT-5.6 Luna with medium reasoning when model selection is available. Record the effective model and any fallback.
- During a research report turn that is already running, sample current Codex rate-limit telemetry once if available and include a compact usage snapshot. Never create a separate schedule, wake, wait, or polling loop for usage reporting, and do not advance research monitoring counters for it.
- Treat token-use limits as monitoring-only limits. Count only intervals spent polling or inspecting live experiment state. Exclude initial study setup, implementation, tests, launch preparation and execution, result analysis, and all code or configuration changes during a study. Never use aggregate goal or task token usage to block research. If monitoring-only usage cannot be isolated, report it as unavailable.
- After verifying supervisor identities and durable startup state for a dispatched round, immediately return the persistent goal to its event-wait state when the goal API and higher-priority policy permit it. Do the same after a nonterminal lifecycle event when no immediate mutation remains. Do not leave the goal active for repeated no-event continuations.

## App-server ownership

- Require an existing persistent Codex app-server daemon.
- Do not start, restart, or stop the daemon from repository code.
- Keep app-server communication out of training and supervisor processes.
- Connect directly to the running daemon's local Unix socket, discovering it through `codex app-server daemon version` unless the operator supplies an explicit path.
- Resume with the run's exact captured permission profile and approval policy and verify the returned effective context before querying the originating thread goal. Never hardcode a broader profile or fall back to app-server defaults.
- Treat an explicit `activePermissionProfile: null` as a match only for a captured null profile. Treat an absent field or mismatched wake context as a permanent delivery failure. Transition only `blocked` goals to `active`; preserve `paused`, `complete`, `usageLimited`, and `budgetLimited` states.
- Mark acceptance only after app-server accepts `turn/start` or `turn/steer`.
- For an idle dedicated monitor turn, prefer a `turn/start` override of `model: gpt-5.6-luna` and `effort: medium`. A `turn/steer` request inherits the active turn's model.
- Leave unknown states, turn races, protocol errors, and connection failures queued for bounded retry.
- Never auto-approve an app-server request.
- Send only the fixed trusted wake prompt. Do not include raw logs, errors, stack traces, training output, or model output.

## Git and publication

Treat non-destructive Git operations in the primary repository as
standing-authorized, including study branches, commits, fetches, and pushes to
non-protected branches. Treat tandem-repository branches and commits as
standing-authorized local operations; a clean exact-SHA local commit satisfies
autoresearch provenance, but pushing a tandem repository requires explicit
permission. Pull requests, protected-branch pushes, history rewrites, tags,
non-W&B publication, artifact deletion, and scheduled-task changes retain their
separate authorization requirements. Online W&B operations are
standing-authorized for declared non-sensitive research data.
When an authorized pull request contains terminal comparative research results, update its body after the result commit is pushed with a `## Findings` table generated from the committed structured summary. Include every evaluated variant or preregistered aggregate, key hyperparameters, primary and convergence metrics, elapsed wall time, decision, total study span, and summed run time or compute cost; mark censored values and distinguish active from wall time. Omit the section for protocol-only changes and active studies.
