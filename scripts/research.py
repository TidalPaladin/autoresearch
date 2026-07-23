#!/usr/bin/env python3
"""Inspect and deliver durable autoresearch terminal notifications."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NoReturn, TextIO, cast

from project.research.codex_notifications import (
    SweepResult,
    sweep_notifications,
    unix_connector,
)
from project.research.runtime import (
    ManagedRootRegistration,
    NotificationEvent,
    StateValidationError,
    StudyConfig,
    ensure_notification,
    register_managed_root,
)

OutputFormat = Literal["text", "json"]
ColorMode = Literal["auto", "always", "never"]
EXIT_SUCCESS = 0
EXIT_PROBLEMS = 1
EXIT_RUNTIME_ERROR = 2


class InvocationError(ValueError):
    """Command arguments conflict after argparse validation."""


@dataclass(frozen=True, slots=True)
class OutputOptions:
    format: OutputFormat
    color: ColorMode
    no_color: bool
    quiet: bool
    verbose: bool

    def uses_color(self, stream: TextIO) -> bool:
        if self.no_color or self.format != "text" or self.color == "never":
            return False
        return self.color == "always" or stream.isatty()


def _add_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--color", choices=("auto", "always", "never"), default="auto")
    parser.add_argument("--no-color", action="store_true", help="disable ANSI color")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("--quiet", action="store_true", help="print only the summary")
    verbosity.add_argument("--verbose", action="store_true", help="include delivery metadata")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="research",
        description="Manage durable autoresearch notifications without changing terminal run status.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    notify = commands.add_parser(
        "notify", help="validate, reconstruct, report, or explicitly requeue one event"
    )
    notify.add_argument("study", type=Path, metavar="STUDY_YAML")
    notify.add_argument("run_id", metavar="RUN_ID")
    notify.add_argument("--requeue", action="store_true", help="requeue a failed event")
    _add_output_arguments(notify)

    worker = commands.add_parser("notify-worker", help="deliver due queued events once")
    worker.add_argument("--once", action="store_true", required=True)
    worker.add_argument("--root", type=Path, default=Path("logs/research"))
    worker.add_argument("--socket", type=Path, help="daemon Unix socket path")
    _add_output_arguments(worker)

    register_root = commands.add_parser(
        "register-root", help="register one exact root for notification discovery"
    )
    register_root.add_argument("--root", type=Path, required=True)
    _add_output_arguments(register_root)
    return parser


def _output_options(arguments: argparse.Namespace) -> OutputOptions:
    return OutputOptions(
        format=cast(OutputFormat, arguments.format),
        color=cast(ColorMode, arguments.color),
        no_color=arguments.no_color,
        quiet=arguments.quiet,
        verbose=arguments.verbose,
    )


def _styled(text: str, code: str, *, enabled: bool) -> str:
    return f"\x1b[{code}m{text}\x1b[0m" if enabled else text


def _render_notification(event: NotificationEvent, options: OutputOptions, stream: TextIO) -> None:
    if options.format == "json":
        json.dump(event.to_dict(), stream, indent=2, sort_keys=True)
        stream.write("\n")
        return
    color = options.uses_color(stream)
    status_code = {"pending": "1;33", "accepted": "1;32", "failed": "1;31"}[event.state]
    status = _styled(event.state.upper(), status_code, enabled=color)
    stream.write(f"{status}  research notify  {event.study_id}/{event.run_id}\n")
    if options.quiet:
        return
    stream.write("\nDelivery\n")
    rows: list[tuple[str, object]] = [
        ("State", event.state),
        ("Event", event.event_id),
        ("Attempt", event.attempt),
        ("Terminal status", event.status),
    ]
    if options.verbose:
        rows.extend(
            (
                ("Terminal state", event.terminal_state_path),
                ("Delivery attempts", event.attempt_count),
                (
                    "Next attempt",
                    event.next_attempt_at.isoformat() if event.next_attempt_at else "none",
                ),
                ("Accepted method", event.accepted_rpc_method or "none"),
                ("Accepted turn", event.accepted_turn_id or "none"),
            )
        )
    width = max(len(label) for label, _value in rows)
    for label, value in rows:
        stream.write(f"  {label + ':':<{width + 2}}{value}\n")


def _render_sweep(result: SweepResult, options: OutputOptions, stream: TextIO) -> None:
    if options.format == "json":
        json.dump(result.to_dict(), stream, indent=2, sort_keys=True)
        stream.write("\n")
        return
    color = options.uses_color(stream)
    passing = result.exit_code == EXIT_SUCCESS
    status = _styled("PASS" if passing else "WARN", "1;32" if passing else "1;33", enabled=color)
    stream.write(f"{status}  research notify-worker  {result.accepted}/{result.due} due accepted\n")
    if options.quiet:
        return
    stream.write("\nSweep\n")
    rows = [
        ("Discovered", result.discovered),
        ("Due", result.due),
        ("Accepted", result.accepted),
        ("Retrying", result.retrying),
        ("Failed", result.failed),
        ("Skipped", result.skipped),
    ]
    width = max(len(label) for label, _value in rows)
    for label, value in rows:
        stream.write(f"  {label + ':':<{width + 2}}{value:,}\n")
    if options.verbose and result.problems:
        stream.write("\nProblems\n")
        for problem in result.problems:
            stream.write(f"  {problem}\n")


def _render_registration(
    registration: ManagedRootRegistration, options: OutputOptions, stream: TextIO
) -> None:
    if options.format == "json":
        json.dump(registration.to_dict(), stream, indent=2, sort_keys=True)
        stream.write("\n")
        return
    color = options.uses_color(stream)
    status = _styled("REGISTERED", "1;32", enabled=color)
    stream.write(f"{status}  research register-root  {registration.root}\n")
    if options.quiet:
        return
    stream.write("\nRegistration\n")
    rows = [
        ("Root", registration.root),
        ("Marker", registration.marker),
        ("State", "created" if registration.created else "existing"),
    ]
    width = max(len(label) for label, _value in rows)
    for label, value in rows:
        stream.write(f"  {label + ':':<{width + 2}}{value}\n")


def _run_notify(arguments: argparse.Namespace) -> int:
    study = StudyConfig.load(arguments.study)
    event = ensure_notification(study, arguments.run_id, requeue=arguments.requeue)
    _render_notification(event, _output_options(arguments), sys.stdout)
    return EXIT_PROBLEMS if event.state == "failed" else EXIT_SUCCESS


def _run_register_root(arguments: argparse.Namespace) -> int:
    registration = register_managed_root(arguments.root)
    _render_registration(registration, _output_options(arguments), sys.stdout)
    return EXIT_SUCCESS


async def _run_worker_async(arguments: argparse.Namespace) -> int:
    connector = unix_connector(resolve_daemon_socket(arguments.socket))
    result = await sweep_notifications(arguments.root, connect=connector)
    options = _output_options(arguments)
    _render_sweep(result, options, sys.stdout)
    if result.problems and options.format == "text" and not options.verbose:
        for problem in result.problems:
            print(f"research warning: {problem}", file=sys.stderr)
    return result.exit_code


def _daemon_version_output() -> str:
    result = subprocess.run(
        ("codex", "app-server", "daemon", "version"),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "could not inspect the Codex app-server daemon")
    return result.stdout


def resolve_daemon_socket(
    explicit_socket: Path | None,
    *,
    daemon_version: Callable[[], str] = _daemon_version_output,
) -> Path:
    """Resolve the running daemon's Unix control socket."""
    if explicit_socket is not None:
        return explicit_socket.expanduser().resolve(strict=False)
    try:
        payload = json.loads(daemon_version())
    except json.JSONDecodeError as error:
        raise RuntimeError("Codex daemon version output is not valid JSON") from error
    socket_path = (
        payload.get("socketPath")
        if isinstance(payload, dict) and payload.get("status") == "running"
        else None
    )
    if not isinstance(socket_path, str) or not socket_path or not Path(socket_path).is_absolute():
        raise RuntimeError("Codex app-server daemon did not report an absolute running socket path")
    return Path(socket_path).resolve(strict=False)


def _runtime_failure(message: str) -> NoReturn:
    print(f"research failed: {message}", file=sys.stderr)
    raise SystemExit(EXIT_RUNTIME_ERROR)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "notify":
            return _run_notify(arguments)
        if arguments.command == "notify-worker":
            return asyncio.run(_run_worker_async(arguments))
        if arguments.command == "register-root":
            return _run_register_root(arguments)
        raise InvocationError(f"unsupported command: {arguments.command}")
    except StateValidationError as error:
        print(f"research validation failed: {error}", file=sys.stderr)
        return EXIT_PROBLEMS
    except InvocationError as error:
        parser.error(str(error))
    except OSError as error:
        _runtime_failure(str(error))
    except Exception as error:
        _runtime_failure(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
