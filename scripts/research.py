#!/usr/bin/env python3
"""Inspect and deliver durable autoresearch terminal notifications."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NoReturn, TextIO, cast

from project.research.codex_notifications import (
    SweepResult,
    stdio_connector,
    sweep_notifications,
    unix_connector,
)
from project.research.runtime import (
    NotificationEvent,
    StateValidationError,
    StudyConfig,
    ensure_notification,
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
    worker.add_argument("--transport", choices=("stdio", "unix"), default="stdio")
    worker.add_argument("--socket", type=Path, help="daemon Unix socket path")
    _add_output_arguments(worker)
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


def _run_notify(arguments: argparse.Namespace) -> int:
    study = StudyConfig.load(arguments.study)
    event = ensure_notification(study, arguments.run_id, requeue=arguments.requeue)
    _render_notification(event, _output_options(arguments), sys.stdout)
    return EXIT_PROBLEMS if event.state == "failed" else EXIT_SUCCESS


async def _run_worker_async(arguments: argparse.Namespace) -> int:
    if arguments.transport == "unix":
        if arguments.socket is None:
            raise InvocationError("--socket is required with --transport unix")
        connector = unix_connector(arguments.socket)
    else:
        connector = stdio_connector(arguments.socket)
    result = await sweep_notifications(arguments.root, connect=connector)
    options = _output_options(arguments)
    _render_sweep(result, options, sys.stdout)
    if result.problems and options.format == "text" and not options.verbose:
        for problem in result.problems:
            print(f"research warning: {problem}", file=sys.stderr)
    return result.exit_code


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
