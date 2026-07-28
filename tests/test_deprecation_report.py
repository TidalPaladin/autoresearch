from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts import deprecation_report

CHECKED_AT = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
PYTHON_VERSIONS_URL = "https://devguide.python.org/versions/"
PYPROJECT = """\
[build-system]
requires = [
    "builder==4.0",
]
build-backend = "builder.api"

[project]
name = "report-test"
requires-python = ">=3.12"
dependencies = [
    "runtime-lib==1.0",
]

[dependency-groups]
dev = [
    "dev-tool==2.0",
]

[tool.uv]
required-version = "==0.11.28"
"""
PYTHON_VERSIONS_HTML = """\
<table>
  <thead><tr><th>Branch</th><th>Status</th></tr></thead>
  <tbody>
    <tr><td>3.14</td><td>bugfix</td></tr>
    <tr><td>3.12</td><td>security</td></tr>
  </tbody>
</table>
"""


def _pypi_payload(
    package: str,
    pinned_version: str,
    *,
    latest_version: str | None = None,
    yanked: bool = False,
) -> bytes:
    return json.dumps(
        {
            "info": {
                "name": package,
                "version": latest_version or pinned_version,
            },
            "releases": {
                pinned_version: [
                    {
                        "filename": f"{package}-{pinned_version}-py3-none-any.whl",
                        "yanked": yanked,
                        "yanked_reason": "broken metadata" if yanked else None,
                    }
                ]
            },
        }
    ).encode()


def _fixture_fetch(url: str) -> bytes:
    responses = {
        PYTHON_VERSIONS_URL: PYTHON_VERSIONS_HTML.encode(),
        "https://pypi.org/pypi/builder/json": _pypi_payload("builder", "4.0"),
        "https://pypi.org/pypi/dev-tool/json": _pypi_payload("dev-tool", "2.0"),
        "https://pypi.org/pypi/runtime-lib/json": _pypi_payload(
            "runtime-lib",
            "1.0",
            latest_version="2.0",
            yanked=True,
        ),
    }
    return responses[url]


def _write_inputs(tmp_path: Path, pyproject: str = PYPROJECT) -> tuple[Path, Path]:
    pyproject_path = tmp_path / "pyproject.toml"
    warnings_path = tmp_path / "deprecation-warnings.txt"
    pyproject_path.write_text(pyproject, encoding="utf-8")
    warnings_path.write_text(
        "tests/test_example.py:1: PendingDeprecationWarning: old pending path\n"
        "tests/test_example.py:2: DeprecationWarning: old path\n",
        encoding="utf-8",
    )
    return pyproject_path, warnings_path


def test_collect_report_inspects_all_direct_pins_and_informational_findings(
    tmp_path: Path,
) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path)

    report = deprecation_report.collect_report(
        pyproject_path,
        warnings_path,
        fetch=_fixture_fetch,
        checked_at=CHECKED_AT,
    )

    assert report["checked_at"] == CHECKED_AT.isoformat()
    assert report["required_uv_version"] == "0.11.28"
    assert report["requires_python"] == ">=3.12"
    assert report["python_versions"] == [
        {"minor": "3.12", "status": "security"},
        {"minor": "3.14", "status": "bugfix"},
    ]
    assert report["direct_dependencies"] == [
        {
            "latest_version": "4.0",
            "name": "builder",
            "pinned_version": "4.0",
            "scope": "build",
            "yanked_files": [],
        },
        {
            "latest_version": "2.0",
            "name": "dev-tool",
            "pinned_version": "2.0",
            "scope": "development",
            "yanked_files": [],
        },
        {
            "latest_version": "2.0",
            "name": "runtime-lib",
            "pinned_version": "1.0",
            "scope": "runtime",
            "yanked_files": [
                {
                    "filename": "runtime-lib-1.0-py3-none-any.whl",
                    "reason": "broken metadata",
                }
            ],
        },
    ]
    assert report["deprecation_warnings"] == [
        "tests/test_example.py:1: PendingDeprecationWarning: old pending path",
        "tests/test_example.py:2: DeprecationWarning: old path",
    ]
    assert report["findings"] == [
        {
            "filename": "runtime-lib-1.0-py3-none-any.whl",
            "kind": "yanked-file",
            "package": "runtime-lib",
            "reason": "broken metadata",
            "version": "1.0",
        }
    ]


def test_newer_release_alone_is_not_a_deprecation_finding(tmp_path: Path) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path)

    report = deprecation_report.collect_report(
        pyproject_path,
        warnings_path,
        fetch=_fixture_fetch,
        checked_at=CHECKED_AT,
    )

    runtime = next(
        dependency
        for dependency in report["direct_dependencies"]
        if dependency["name"] == "runtime-lib"
    )
    assert runtime["latest_version"] == "2.0"
    assert runtime["pinned_version"] == "1.0"
    assert not any(finding["kind"] == "newer-version" for finding in report["findings"])


def test_unsupported_python_status_is_an_informational_finding(tmp_path: Path) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path)

    def unsupported_python(url: str) -> bytes:
        if url == PYTHON_VERSIONS_URL:
            return PYTHON_VERSIONS_HTML.replace("security", "end-of-life").encode()
        return _fixture_fetch(url)

    report = deprecation_report.collect_report(
        pyproject_path,
        warnings_path,
        fetch=unsupported_python,
        checked_at=CHECKED_AT,
    )

    assert {
        "kind": "unsupported-python",
        "minor": "3.12",
        "status": "end-of-life",
    } in report["findings"]


@pytest.mark.parametrize(
    ("pyproject", "message"),
    [
        (
            PYPROJECT.replace("runtime-lib==1.0", "runtime-lib>=1.0"),
            "exactly pinned",
        ),
        (
            PYPROJECT.replace('required-version = "==0.11.28"', 'required-version = ">=0.11"'),
            "uv required-version",
        ),
    ],
)
def test_collect_report_rejects_unpinned_direct_tools(
    tmp_path: Path,
    pyproject: str,
    message: str,
) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path, pyproject)

    with pytest.raises(deprecation_report.ReportError, match=message):
        deprecation_report.collect_report(
            pyproject_path,
            warnings_path,
            fetch=_fixture_fetch,
            checked_at=CHECKED_AT,
        )


def test_collect_report_rejects_incomplete_python_versions_table(tmp_path: Path) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path)

    def missing_python_version(url: str) -> bytes:
        if url == PYTHON_VERSIONS_URL:
            return PYTHON_VERSIONS_HTML.replace(
                "<tr><td>3.14</td><td>bugfix</td></tr>",
                "",
            ).encode()
        return _fixture_fetch(url)

    with pytest.raises(deprecation_report.ReportError, match=r"Python 3\.14"):
        deprecation_report.collect_report(
            pyproject_path,
            warnings_path,
            fetch=missing_python_version,
            checked_at=CHECKED_AT,
        )


def test_collect_report_rejects_malformed_pypi_release_schema(tmp_path: Path) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path)

    def malformed_release(url: str) -> bytes:
        if url == "https://pypi.org/pypi/runtime-lib/json":
            return json.dumps({"info": {"version": "1.0"}, "releases": []}).encode()
        return _fixture_fetch(url)

    with pytest.raises(deprecation_report.ReportError, match="releases must be"):
        deprecation_report.collect_report(
            pyproject_path,
            warnings_path,
            fetch=malformed_release,
            checked_at=CHECKED_AT,
        )


def test_cli_writes_json_and_summary_but_findings_exit_successfully(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path)
    output_path = tmp_path / "report.json"
    summary_path = tmp_path / "summary.md"

    exit_code = deprecation_report.main(
        [
            "--pyproject",
            str(pyproject_path),
            "--warnings-file",
            str(warnings_path),
            "--output",
            str(output_path),
            "--summary",
            str(summary_path),
            "--format",
            "json",
        ],
        fetch=_fixture_fetch,
        checked_at=CHECKED_AT,
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out) == json.loads(output_path.read_text(encoding="utf-8"))
    assert "1 finding" in summary_path.read_text(encoding="utf-8")


def test_cli_network_failure_uses_runtime_error_exit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path)

    def unavailable(_url: str) -> bytes:
        raise OSError("network unavailable")

    exit_code = deprecation_report.main(
        [
            "--pyproject",
            str(pyproject_path),
            "--warnings-file",
            str(warnings_path),
        ],
        fetch=unavailable,
        checked_at=CHECKED_AT,
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "dependency-report failed: network unavailable\n"


def test_cli_output_failure_uses_runtime_error_exit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pyproject_path, warnings_path = _write_inputs(tmp_path)

    exit_code = deprecation_report.main(
        [
            "--pyproject",
            str(pyproject_path),
            "--warnings-file",
            str(warnings_path),
            "--output",
            str(tmp_path / "missing" / "report.json"),
        ],
        fetch=_fixture_fetch,
        checked_at=CHECKED_AT,
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err.startswith("dependency-report failed:")
    assert "No such file or directory" in captured.err
