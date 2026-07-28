#!/usr/bin/env python3
"""Classify pip-audit output as clean, findings, or incomplete."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

SCANNER_NAME = "pip-audit"
SCANNER_VERSION = "2.10.1"
INCLUDED_GROUPS = "all locked dependency groups"
EXIT_CLEAN = 0
EXIT_FINDINGS = 1
EXIT_INCOMPLETE = 2

AuditClassification = Literal["clean", "findings", "incomplete"]
Metadata = dict[str, Any]


def _vulnerability_count(report_path: Path) -> tuple[int | None, str | None]:
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except OSError as error:
        return None, f"could not read audit report: {error}"
    except (UnicodeError, json.JSONDecodeError) as error:
        return None, f"audit report is not valid JSON: {error}"
    if not isinstance(payload, Mapping):
        return None, "audit report root is not an object"
    dependencies = payload.get("dependencies")
    if not isinstance(dependencies, list):
        return None, "audit report dependencies field is not a list"
    vulnerability_count = 0
    for index, dependency in enumerate(dependencies):
        if not isinstance(dependency, Mapping):
            return None, f"audit report dependency {index} is not an object"
        vulnerabilities = dependency.get("vulns")
        if not isinstance(vulnerabilities, list):
            return None, f"audit report dependency {index} vulnerabilities are not a list"
        if not all(isinstance(vulnerability, Mapping) for vulnerability in vulnerabilities):
            return None, f"audit report dependency {index} has an invalid vulnerability"
        vulnerability_count += len(vulnerabilities)
    return vulnerability_count, None


def _file_digest(path: Path, label: str) -> tuple[str | None, str | None]:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest(), None
    except OSError as error:
        return None, f"could not read {label}: {error}"


def classify_audit(
    report_path: Path,
    *,
    scanner_exit_code: int,
    service: str,
    checked_at: datetime | None = None,
    requirements: Path | None = None,
    lockfile: Path | None = None,
    command: str | None = None,
) -> Metadata:
    """Return metadata that preserves findings versus incomplete-scan evidence."""

    selected_at = checked_at or datetime.now(UTC)
    if selected_at.tzinfo is None or selected_at.utcoffset() is None:
        raise ValueError("checked_at must include a UTC offset")
    vulnerability_count, report_error = _vulnerability_count(report_path)
    requirements_digest, requirements_error = (
        _file_digest(requirements, "exported requirements")
        if requirements is not None
        else (None, None)
    )
    lockfile_digest, lockfile_error = (
        _file_digest(lockfile, "lockfile") if lockfile is not None else (None, None)
    )
    evidence_errors = [
        error for error in (report_error, requirements_error, lockfile_error) if error is not None
    ]
    classification: AuditClassification
    classification_error: str | None = "; ".join(evidence_errors) or None
    if evidence_errors:
        classification = "incomplete"
    elif scanner_exit_code == EXIT_CLEAN and vulnerability_count == 0:
        classification = "clean"
    elif scanner_exit_code == EXIT_FINDINGS and vulnerability_count is not None:
        if vulnerability_count > 0:
            classification = "findings"
        else:
            classification = "incomplete"
            classification_error = "scanner exit code does not match the report vulnerability count"
    else:
        classification = "incomplete"
        classification_error = (
            "scanner exit code does not match a complete clean or findings report"
        )
    selected_command = command or (
        f"pip-audit=={SCANNER_VERSION} --vulnerability-service {service}"
    )
    metadata: Metadata = {
        "schema_version": 1,
        "advisory_database_revision": "live provider response; revision unavailable",
        "advisory_service": service,
        "checked_at": selected_at.astimezone(UTC).isoformat(),
        "classification": classification,
        "command": selected_command,
        "included_groups": INCLUDED_GROUPS,
        "lockfile": str(lockfile) if lockfile is not None else None,
        "lockfile_sha256": lockfile_digest,
        "report": str(report_path),
        "requirements": str(requirements) if requirements is not None else None,
        "requirements_sha256": requirements_digest,
        "scanner": SCANNER_NAME,
        "scanner_exit_code": scanner_exit_code,
        "scanner_version": SCANNER_VERSION,
        "vulnerability_count": vulnerability_count,
    }
    if classification_error is not None:
        metadata["error"] = classification_error
    return metadata


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="classify-audit",
        description="Classify one pip-audit JSON report and persist scan metadata.",
    )
    parser.add_argument("--service", choices=("pypi", "osv"), required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--scanner-exit-code", type=int, required=True)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--lockfile", type=Path, required=True)
    parser.add_argument("--command")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    checked_at: datetime | None = None,
) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        metadata = classify_audit(
            arguments.report,
            scanner_exit_code=arguments.scanner_exit_code,
            service=arguments.service,
            checked_at=checked_at,
            requirements=arguments.requirements,
            lockfile=arguments.lockfile,
            command=arguments.command,
        )
        arguments.metadata.write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except (OSError, ValueError) as error:
        print(f"classify-audit failed: {error}", file=sys.stderr)
        return EXIT_INCOMPLETE

    print(f"{metadata['classification'].upper()}  classify-audit  {metadata['advisory_service']}")
    return {
        "clean": EXIT_CLEAN,
        "findings": EXIT_FINDINGS,
        "incomplete": EXIT_INCOMPLETE,
    }[metadata["classification"]]


if __name__ == "__main__":
    raise SystemExit(main())
