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
- Test compatibility without coverage: `make test-compat`
- Test the subscription-free notification loop: `make test-notify-loop`
- Audit locked dependencies: `make audit`
- Run all non-rewriting gates: `make check`
- Verify one wheel and source distribution: `make package-check`
- Validate the project skill: `uv run python "${CODEX_HOME:-${HOME}/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" .agents/skills/autoresearch`

Use `uv==0.11.28`. Pin direct dependencies and commit `uv.lock`. Preserve Hatch VCS versioning. Do not add a static project version or create release tags as part of ordinary maintenance.

## Tests

- Follow TDD. Add a failing regression or behavior test before production code.
- Keep branch coverage at or above 90%.
- Test state corruption, path escapes, crash windows, retries, protocol races, and I/O failures, not only successful delivery.
- Use fake WebSocket-over-Unix servers. Tests must not connect to or wake a real Codex task.
- Run tests on Python 3.12 and 3.14 for changes to the runtime or notifier.

## Continuous integration

- Use GitHub Actions on `ubuntu-24.04`; do not restore CircleCI or Codecov configuration.
- Keep `Required` as the single stable required context. It must fail when Quality, either Python test leg, Notify loop, or Package fails, is cancelled, or is skipped.
- Keep CI free of repository secrets and Codex subscriptions. The focused notification test must use an explicit temporary fake Unix socket, an absolute Python executable, no `codex` executable on `PATH`, and no Codex, ChatGPT, or OpenAI environment variables.
- Keep `production-package.yml` and `dependency-health.yml` manual-only until each workflow exists on `master` and passes exact-ref dispatch validation. Then add `17 3 * * 1` and `23 4 * * 2`, respectively, in a separate reviewed change.
- Retain production packages, checksums, security audit evidence, and deprecation evidence for 7 days. Security findings and incomplete scans both fail; deprecation findings remain informational.
- Do not add advisory exceptions by default. Every future exception must name the advisory, evidence, owner, and expiry or review date.
- Enable the `Require CI on master` ruleset only after the matching-head `Required` check succeeds on `master`. Require that context and an up-to-date branch, with no review-count rule or bypass actor.
- No trusted callback or exact-run non-model watcher is configured for GitHub Actions. Do not claim automatic Codex wake delivery; resume the task manually and validate the exact run, attempt, ref, head SHA, workflow blob, jobs, and artifacts.

## State safety

- Register each existing research root explicitly with `scripts/research.py register-root --root <path>` before notification discovery. Producers register new roots automatically.
- Require the exact atomic `.autoresearch-root.json` marker before scanning an existing root. Reject filesystem, home, repository, broad parent, malformed, and symlinked roots.
- Use `$notify-wake` as the single source for app-server delivery, delivery-state, authority, reconciliation, and owned goal-wait behavior. Keep only research event production, trusted prompts, registered roots, controllers, and retry timing in this repository.
- Write and sync each v2 `terminal.json` before its `notification.json` under `<managed-root>/.notify-wake/v2/`. Treat version-1 state as inert history and reject it with a cutover-required error.
- Capture the live originating thread's effective permission-profile identity and approval policy before child spawn. When `CODEX_PERMISSION_PROFILE` is unset, omit the override and persist the non-null profile ID resolved by app-server, including an implicit built-in ID. Fail before dispatch if app-server does not report a selectable profile, and store the resolved context in an immutable per-run `wake-context.json`.
- Use same-directory atomic replacement, file and directory sync, and stable sibling locks.
- Validate identifiers, schemas, timestamps, matching fields, absolute managed paths, and resolved symlink containment before acting.
- Preserve every v2 event by stable event ID and advance only the run's current-event pointer.
- Append shared Markdown research logs through the locked runtime helper. Deduplicate all updates by operation ID and terminal entries by study, run, and attempt.
- Never delete unmanaged or historical experiment artifacts during autonomous work.
- Keep generated state under `logs/research/` and out of Git.

Notification failure must never change terminal training status. The runtime records terminal truth. Only notification delivery state can move among the v2 shared states.

## Adapter conformance

- Define the exact destination and emitted data classes for every W&B operation. Standing authorization covers declared non-sensitive metrics, configs, and provenance; fail preflight instead of silently launching a scientific run offline when the destination or manifest is incomplete.
- After spawning a child, own its process group until it is terminated and reaped. Perform this cleanup after every exceptional exit and before releasing GPU or other resource locks.
- Advance a run's monitoring counters or `next_check_at` only when that run is due. A terminal wake must leave unrelated run counters and schedules unchanged and clear only the terminal run's poll.
- Use terminal events as the primary wake path. Never keep a Codex turn open to sleep or poll; a local non-model watcher may wake Codex only for terminal events, exceptional safety conditions, or due sparse watchdog checks.
- For long-running adapters, define one-shot first-cycle, supervisor-loss, and progress-stall lifecycle events. Use durable file events, process-exit handles, and explicit deadlines; never wake for routine progress, heartbeat, notification retry, or acceptance writes.
- Schedule transient retries from the earliest durable `next_attempt_at` in the non-model controller. Fresh due events remain deliverable while another event backs off; never use a notification-set-wide transport latch. Retry writes must not recursively trigger delivery.
- Persist controller startup, sweep outcomes, isolated problems, and shutdown or failure locally.
- Keep read-only and notification-recovery commands independent of training datasets and launch-only environment variables. Validate those prerequisites at preflight or launch and name unresolved variables explicitly.
- Use GPT-5.6 Luna with medium reasoning only for read-only scheduled checks, dedicated relay tasks or model-selectable subagents, and other low-value non-mutating work. Never change the model of the active root conversation. The root model retains launches, recovery, goal changes, scientific decisions, and code changes.
- During a research report turn that is already running, sample current Codex rate-limit telemetry once if available and include a compact usage snapshot. Never create a separate schedule, wake, wait, or polling loop for usage reporting, and do not advance research monitoring counters for it.
- Treat token-use limits as monitoring-only limits. Count only intervals spent polling or inspecting live experiment state. Exclude initial study setup, implementation, tests, launch preparation and execution, result analysis, and all code or configuration changes during a study. Never use aggregate goal or task token usage to block research. If monitoring-only usage cannot be isolated, report it as unavailable.
- After verifying supervisor identities and durable startup state, call the shared owned goal-wait lifecycle when the goal is active, no immediate work remains, and the goal API permits blocking. Do the same after a nonterminal event. A wake may reactivate only the exact blocked goal revision owned by that wait lease. Treat every unmatched, changed, or uncertain blocked goal as manually blocked.

## App-server ownership

- Pin `notify-wake-runtime==1.0.0` by exact Git SHA and require the schema-conforming Codex 0.146.0 app-server baseline.
- Require an existing daemon. Repository code must not start, restart, or stop it, and training or supervisor processes must not communicate with it.
- Call the shared runtime for socket discovery, context capture, delivery, reconciliation, and goal wait. Do not add repository-local RPC clients or response compatibility.
- Use the default `research_compatibility` policy. Its goal transition and idle-start check are best-effort because Codex 0.146.0 has no compare-and-set or atomic idle-start operation. Use `strict` only as an explicit opt-in.
- Never include `model` or `effort` in a root `turn/start`. Send only the fixed trusted wake prompt, without raw logs, errors, stack traces, training output, or model output.

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
