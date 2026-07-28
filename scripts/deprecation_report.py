#!/usr/bin/env python3
"""Report direct dependency, runtime, and warning deprecations."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

PYTHON_VERSIONS_URL = "https://devguide.python.org/versions/"
PYPI_JSON_URL = "https://pypi.org/pypi/{package}/json"
CHECKED_PYTHON_MINORS = ("3.12", "3.14")
SUPPORTED_PYTHON_STATUSES = frozenset({"feature", "prerelease", "bugfix", "security"})
EXACT_PIN_PATTERN = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)"
    r"(?:\[[A-Za-z0-9_,.-]+\])?"
    r"==(?P<version>[^;\s]+)"
    r"(?:\s*;\s*.+)?$"
)
UV_VERSION_PATTERN = re.compile(r"^==(?P<version>[^,\s]+)$")
NORMALIZED_NAME_PATTERN = re.compile(r"[-_.]+")
WARNING_NAMES = ("PendingDeprecationWarning", "DeprecationWarning")
NETWORK_TIMEOUT_SECONDS = 10
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
SCHEMA_VERSION = 1
EXIT_SUCCESS = 0
EXIT_RUNTIME_ERROR = 2

Fetch = Callable[[str], bytes]
Report = dict[str, Any]


class ReportError(RuntimeError):
    """Dependency reporting could not produce complete, interpretable evidence."""


@dataclass(frozen=True, slots=True)
class DependencyPin:
    """One exact direct dependency pin from project metadata."""

    scope: str
    name: str
    version: str

    @property
    def normalized_name(self) -> str:
        return NORMALIZED_NAME_PATTERN.sub("-", self.name).lower()


class _VersionsTableParser(HTMLParser):
    """Collect text cells from HTML tables without depending on an HTML package."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._row is not None and self._cell is not None:
            value = " ".join("".join(self._cell).split())
            self._row.append(value)
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None


def _default_fetch(url: str) -> bytes:
    request = Request(
        url,
        headers={"User-Agent": "autoresearch-deprecation-report/1"},
    )
    with urlopen(request, timeout=NETWORK_TIMEOUT_SECONDS) as response:
        payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ReportError(f"response from {url} exceeds {MAX_RESPONSE_BYTES:,} bytes")
    return payload


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReportError(f"{field_name} must be a table or object")
    return value


def _string_list(value: object, field_name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ReportError(f"{field_name} must be a list of dependency strings")
    return value


def _parse_pin(requirement: str, scope: str) -> DependencyPin:
    match = EXACT_PIN_PATTERN.fullmatch(requirement.strip())
    if match is None:
        raise ReportError(f"{scope} dependency {requirement!r} must be exactly pinned with ==")
    return DependencyPin(
        scope=scope,
        name=match.group("name"),
        version=match.group("version"),
    )


def _project_metadata(pyproject_path: Path) -> tuple[list[DependencyPin], str, str]:
    try:
        payload = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as error:
        raise ReportError(f"could not parse {pyproject_path}: {error}") from error

    build_system = _mapping(payload.get("build-system"), "build-system")
    project = _mapping(payload.get("project"), "project")
    dependency_groups = _mapping(payload.get("dependency-groups"), "dependency-groups")
    uv = _mapping(_mapping(payload.get("tool"), "tool").get("uv"), "tool.uv")

    raw_requires_python = project.get("requires-python")
    if not isinstance(raw_requires_python, str) or not raw_requires_python:
        raise ReportError("project.requires-python must be a non-empty string")
    raw_uv_version = uv.get("required-version")
    if not isinstance(raw_uv_version, str):
        raise ReportError("tool.uv.required-version must be a string")
    uv_match = UV_VERSION_PATTERN.fullmatch(raw_uv_version)
    if uv_match is None:
        raise ReportError("uv required-version must be exactly pinned with ==")

    pins = [
        *(
            _parse_pin(requirement, "build")
            for requirement in _string_list(build_system.get("requires"), "build-system.requires")
        ),
        *(
            _parse_pin(requirement, "runtime")
            for requirement in _string_list(project.get("dependencies"), "project.dependencies")
        ),
    ]
    for group_name in sorted(dependency_groups):
        requirements = _string_list(
            dependency_groups[group_name],
            f"dependency-groups.{group_name}",
        )
        pins.extend(_parse_pin(requirement, "development") for requirement in requirements)

    unique_pins = sorted(
        set(pins),
        key=lambda pin: (pin.scope, pin.normalized_name, pin.version),
    )
    if not unique_pins:
        raise ReportError("pyproject.toml contains no direct dependencies")
    return unique_pins, raw_requires_python, uv_match.group("version")


def _fetch_bytes(url: str, fetch: Fetch) -> bytes:
    try:
        payload = fetch(url)
    except ReportError:
        raise
    except Exception as error:
        raise ReportError(str(error)) from error
    if not isinstance(payload, bytes):
        raise ReportError(f"fetcher returned non-bytes content for {url}")
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ReportError(f"response from {url} exceeds {MAX_RESPONSE_BYTES:,} bytes")
    return payload


def _json_object(payload: bytes, url: str) -> Mapping[str, Any]:
    try:
        parsed = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReportError(f"{url} did not return valid JSON: {error}") from error
    return _mapping(parsed, f"response from {url}")


def _dependency_record(pin: DependencyPin, fetch: Fetch) -> tuple[Report, list[Report]]:
    url = PYPI_JSON_URL.format(package=pin.normalized_name)
    payload = _json_object(_fetch_bytes(url, fetch), url)
    info = _mapping(payload.get("info"), f"{url} info")
    latest_version = info.get("version")
    if not isinstance(latest_version, str) or not latest_version:
        raise ReportError(f"{url} info.version must be a non-empty string")
    releases = _mapping(payload.get("releases"), f"{url} releases")
    release_files = releases.get(pin.version)
    if not isinstance(release_files, list) or not release_files:
        raise ReportError(f"{url} has no release files for pinned version {pin.version}")

    yanked_files: list[Report] = []
    findings: list[Report] = []
    for index, raw_file in enumerate(release_files):
        release_file = _mapping(raw_file, f"{url} releases.{pin.version}[{index}]")
        filename = release_file.get("filename")
        yanked = release_file.get("yanked")
        reason = release_file.get("yanked_reason")
        if not isinstance(filename, str) or not filename:
            raise ReportError(f"{url} release filename must be a non-empty string")
        if not isinstance(yanked, bool):
            raise ReportError(f"{url} release yanked field must be a boolean")
        if reason is not None and not isinstance(reason, str):
            raise ReportError(f"{url} release yanked_reason must be a string or null")
        if yanked:
            selected_reason = reason or "no reason reported"
            yanked_files.append({"filename": filename, "reason": selected_reason})
            findings.append(
                {
                    "filename": filename,
                    "kind": "yanked-file",
                    "package": pin.normalized_name,
                    "reason": selected_reason,
                    "version": pin.version,
                }
            )

    return (
        {
            "latest_version": latest_version,
            "name": pin.normalized_name,
            "pinned_version": pin.version,
            "scope": pin.scope,
            "yanked_files": yanked_files,
        },
        findings,
    )


def _python_version_statuses(html: bytes) -> dict[str, str]:
    try:
        text = html.decode()
    except UnicodeDecodeError as error:
        raise ReportError(f"{PYTHON_VERSIONS_URL} is not UTF-8: {error}") from error
    parser = _VersionsTableParser()
    try:
        parser.feed(text)
        parser.close()
    except Exception as error:
        raise ReportError(f"could not parse {PYTHON_VERSIONS_URL}: {error}") from error

    branch_index: int | None = None
    status_index: int | None = None
    statuses: dict[str, str] = {}
    for row in parser.rows:
        normalized = [cell.casefold() for cell in row]
        if "branch" in normalized and "status" in normalized:
            branch_index = normalized.index("branch")
            status_index = normalized.index("status")
            continue
        if branch_index is None or status_index is None:
            continue
        if max(branch_index, status_index) >= len(row):
            continue
        branch = row[branch_index].strip()
        status = row[status_index].strip().casefold()
        if branch and status:
            statuses[branch] = status
    return statuses


def _deprecation_warnings(warnings_path: Path) -> list[str]:
    try:
        lines = warnings_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise ReportError(f"could not read warning report {warnings_path}: {error}") from error
    return sorted(
        {
            line.strip()
            for line in lines
            if any(warning_name in line for warning_name in WARNING_NAMES)
        }
    )


def collect_report(
    pyproject_path: Path,
    warnings_path: Path,
    *,
    fetch: Fetch = _default_fetch,
    checked_at: datetime | None = None,
) -> Report:
    """Collect complete deprecation evidence or raise ``ReportError``."""

    selected_at = checked_at or datetime.now(UTC)
    if selected_at.tzinfo is None or selected_at.utcoffset() is None:
        raise ReportError("checked_at must include a UTC offset")
    selected_at = selected_at.astimezone(UTC)
    pins, requires_python, uv_version = _project_metadata(pyproject_path)

    dependencies: list[Report] = []
    findings: list[Report] = []
    for pin in pins:
        dependency, dependency_findings = _dependency_record(pin, fetch)
        dependencies.append(dependency)
        findings.extend(dependency_findings)

    version_statuses = _python_version_statuses(_fetch_bytes(PYTHON_VERSIONS_URL, fetch))
    python_versions: list[Report] = []
    for minor in CHECKED_PYTHON_MINORS:
        status = version_statuses.get(minor)
        if status is None:
            raise ReportError(f"official versions table has no status for Python {minor}")
        python_versions.append({"minor": minor, "status": status})
        if status not in SUPPORTED_PYTHON_STATUSES:
            findings.append(
                {
                    "kind": "unsupported-python",
                    "minor": minor,
                    "status": status,
                }
            )

    return {
        "schema_version": SCHEMA_VERSION,
        "checked_at": selected_at.isoformat(),
        "sources": {
            "pypi": "https://pypi.org/",
            "python_versions": PYTHON_VERSIONS_URL,
        },
        "pyproject_path": str(pyproject_path.resolve(strict=False)),
        "warnings_path": str(warnings_path.resolve(strict=False)),
        "requires_python": requires_python,
        "required_uv_version": uv_version,
        "direct_dependencies": dependencies,
        "python_versions": python_versions,
        "deprecation_warnings": _deprecation_warnings(warnings_path),
        "findings": sorted(findings, key=lambda finding: json.dumps(finding, sort_keys=True)),
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    """Render a compact GitHub job summary from a validated report."""

    dependencies = report["direct_dependencies"]
    python_versions = report["python_versions"]
    findings = report["findings"]
    warnings = report["deprecation_warnings"]
    finding_word = "finding" if len(findings) == 1 else "findings"
    warning_word = "warning" if len(warnings) == 1 else "warnings"
    lines = [
        "# Dependency deprecation report",
        "",
        f"- Checked: `{report['checked_at']}`",
        f"- Direct pins inspected: {len(dependencies)}",
        f"- Required uv: `{report['required_uv_version']}`",
        f"- Findings: {len(findings)} {finding_word}",
        f"- Deprecation warnings: {len(warnings)} {warning_word}",
        "",
        "## Python support",
        "",
        "| Python | Status |",
        "| --- | --- |",
    ]
    lines.extend(f"| {entry['minor']} | {entry['status']} |" for entry in python_versions)
    lines.extend(["", "## Findings", ""])
    if findings:
        lines.extend(
            f"- `{finding['kind']}`: `{json.dumps(finding, sort_keys=True)}`"
            for finding in findings
        )
    else:
        lines.append("- None")
    lines.extend(["", "## Deprecation warnings", ""])
    if warnings:
        lines.extend(f"- `{warning}`" for warning in warnings)
    else:
        lines.append("- None")
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dependency-report",
        description="Report yanked direct pins, Python support, and deprecation warnings.",
    )
    parser.add_argument("--pyproject", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--warnings-file", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def _write_report(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def main(
    argv: Sequence[str] | None = None,
    *,
    fetch: Fetch = _default_fetch,
    checked_at: datetime | None = None,
) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        report = collect_report(
            arguments.pyproject,
            arguments.warnings_file,
            fetch=fetch,
            checked_at=checked_at,
        )
        json_report = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if arguments.output is not None:
            _write_report(arguments.output, json_report)
        if arguments.summary is not None:
            _write_report(arguments.summary, render_markdown(report))
    except (OSError, ReportError) as error:
        print(f"dependency-report failed: {error}", file=sys.stderr)
        return EXIT_RUNTIME_ERROR

    if arguments.format == "json":
        sys.stdout.write(json_report)
    else:
        finding_count = len(report["findings"])
        warning_count = len(report["deprecation_warnings"])
        status = "WARN" if finding_count or warning_count else "PASS"
        print(
            f"{status}  dependency-report  "
            f"{finding_count} findings, {warning_count} deprecation warnings"
        )
    return EXIT_SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())
