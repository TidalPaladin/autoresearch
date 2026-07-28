from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from scripts import classify_audit

CHECKED_AT = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)


def _write_report(tmp_path: Path, vulnerabilities: list[dict[str, object]]) -> Path:
    report_path = tmp_path / "audit.json"
    report_path.write_text(
        json.dumps(
            {
                "dependencies": [
                    {
                        "name": "example",
                        "version": "1.0",
                        "vulns": vulnerabilities,
                    }
                ],
                "fixes": [],
            }
        ),
        encoding="utf-8",
    )
    return report_path


def test_clean_audit_metadata_records_complete_scan(tmp_path: Path) -> None:
    report_path = _write_report(tmp_path, [])

    metadata = classify_audit.classify_audit(
        report_path,
        scanner_exit_code=0,
        service="pypi",
        checked_at=CHECKED_AT,
    )

    assert metadata["classification"] == "clean"
    assert metadata["vulnerability_count"] == 0
    assert metadata["scanner_exit_code"] == 0
    assert metadata["checked_at"] == CHECKED_AT.isoformat()


def test_finding_audit_metadata_is_distinct_from_incomplete_scan(tmp_path: Path) -> None:
    report_path = _write_report(
        tmp_path,
        [{"id": "PYSEC-2026-1", "fix_versions": ["1.1"], "aliases": []}],
    )

    metadata = classify_audit.classify_audit(
        report_path,
        scanner_exit_code=1,
        service="osv",
        checked_at=CHECKED_AT,
    )

    assert metadata["classification"] == "findings"
    assert metadata["vulnerability_count"] == 1
    assert metadata["scanner_exit_code"] == 1


def test_invalid_or_inconsistent_audit_is_incomplete(tmp_path: Path) -> None:
    invalid_report = tmp_path / "invalid.json"
    invalid_report.write_text("not-json", encoding="utf-8")
    empty_findings_report = _write_report(tmp_path, [])

    invalid = classify_audit.classify_audit(
        invalid_report,
        scanner_exit_code=2,
        service="pypi",
        checked_at=CHECKED_AT,
    )
    inconsistent = classify_audit.classify_audit(
        empty_findings_report,
        scanner_exit_code=1,
        service="pypi",
        checked_at=CHECKED_AT,
    )
    missing_evidence = classify_audit.classify_audit(
        empty_findings_report,
        scanner_exit_code=0,
        service="pypi",
        checked_at=CHECKED_AT,
        requirements=tmp_path / "missing-requirements.txt",
    )

    assert invalid["classification"] == "incomplete"
    assert "valid JSON" in invalid["error"]
    assert inconsistent["classification"] == "incomplete"
    assert "does not match" in inconsistent["error"]
    assert missing_evidence["classification"] == "incomplete"
    assert "exported requirements" in missing_evidence["error"]


def test_cli_persists_metadata_and_fails_for_findings(
    tmp_path: Path,
) -> None:
    report_path = _write_report(
        tmp_path,
        [{"id": "PYSEC-2026-1", "fix_versions": [], "aliases": []}],
    )
    metadata_path = tmp_path / "metadata.json"
    requirements_path = tmp_path / "locked-requirements.txt"
    lockfile_path = tmp_path / "uv.lock"
    requirements_path.write_text("example==1.0 --hash=sha256:abc\n", encoding="utf-8")
    lockfile_path.write_text("version = 1\nrevision = 3\n", encoding="utf-8")

    exit_code = classify_audit.main(
        [
            "--service",
            "pypi",
            "--report",
            str(report_path),
            "--metadata",
            str(metadata_path),
            "--scanner-exit-code",
            "1",
            "--requirements",
            str(requirements_path),
            "--lockfile",
            str(lockfile_path),
        ],
        checked_at=CHECKED_AT,
    )

    assert exit_code == 1
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["classification"] == "findings"
    assert metadata["requirements"] == str(requirements_path)
    assert metadata["requirements_sha256"]
    assert metadata["lockfile"] == str(lockfile_path)
    assert metadata["lockfile_sha256"]
