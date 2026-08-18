#!/usr/bin/env python3
"""Validate the canonical skills submodule and shared runtime source pin."""

from __future__ import annotations

import argparse
import configparser
import json
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

SKILLS_PATH = Path(".agents/skills")
SKILL_PATH = SKILLS_PATH / "autoresearch" / "SKILL.md"
SKILLS_URL = "https://github.com/TidalPaladin/skills.git"
SUBMODULE_SECTION = 'submodule ".agents/skills"'
RUNTIME_PACKAGE = "notify-wake-runtime"
RUNTIME_SUBDIRECTORY = "notify-wake"
GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
GITLINK_PATTERN = re.compile(r"^160000 (?P<sha>[0-9a-f]{40}) [0-3]\t\.agents/skills\n?$")
MAKEFILE_PIN_PATTERN = re.compile(
    r"^NOTIFY_WAKE_RUNTIME\s*=.*skills\.git@(?P<sha>[0-9a-f]{40})"
    r"\\#subdirectory=notify-wake\s*$",
    re.MULTILINE,
)

RunGit = Callable[[Sequence[str], Path], str]


class SourceLinkError(RuntimeError):
    """A canonical source link is absent, mutable, or inconsistent."""


@dataclass(frozen=True, slots=True)
class SourceLinkResult:
    """Validated immutable source identities."""

    skills_path: str
    skills_sha: str
    skills_url: str
    runtime_sha: str


def _run_git(arguments: Sequence[str], cwd: Path) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=cwd,
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise SourceLinkError(f"Git command failed in {cwd}: {detail}")
    return completed.stdout


def _read_toml(path: Path) -> Mapping[str, Any]:
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as error:
        raise SourceLinkError(f"could not parse {path}: {error}") from error
    return value


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SourceLinkError(f"{name} must be a table")
    return value


def _submodule_configuration(repo_root: Path) -> str:
    path = repo_root / ".gitmodules"
    parser = configparser.ConfigParser(interpolation=None)
    try:
        with path.open(encoding="utf-8") as stream:
            parser.read_file(stream)
    except (OSError, UnicodeError, configparser.Error) as error:
        raise SourceLinkError(f"could not parse {path}: {error}") from error
    if not parser.has_section(SUBMODULE_SECTION):
        raise SourceLinkError(f"{SKILLS_PATH} submodule configuration is missing")
    section = parser[SUBMODULE_SECTION]
    if section.get("path") != SKILLS_PATH.as_posix():
        raise SourceLinkError(f"{SKILLS_PATH} submodule path is inconsistent")
    url = section.get("url")
    if url != SKILLS_URL:
        raise SourceLinkError(f"{SKILLS_PATH} must use {SKILLS_URL}")
    if section.getboolean("shallow", fallback=False) is not True:
        raise SourceLinkError(f"{SKILLS_PATH} must recommend a shallow checkout")
    return url


def _gitlink_sha(repo_root: Path, run_git: RunGit) -> str:
    output = run_git(("ls-files", "--stage", "--", SKILLS_PATH.as_posix()), repo_root)
    match = GITLINK_PATTERN.fullmatch(output)
    if match is None:
        raise SourceLinkError(f"{SKILLS_PATH} must be one exact gitlink")
    return match.group("sha")


def _pyproject_runtime_sha(repo_root: Path) -> str:
    payload = _read_toml(repo_root / "pyproject.toml")
    tool = _mapping(payload.get("tool"), "tool")
    uv = _mapping(tool.get("uv"), "tool.uv")
    sources = _mapping(uv.get("sources"), "tool.uv.sources")
    source = _mapping(sources.get(RUNTIME_PACKAGE), f"tool.uv.sources.{RUNTIME_PACKAGE}")
    if source.get("git") != SKILLS_URL:
        raise SourceLinkError(f"{RUNTIME_PACKAGE} must use {SKILLS_URL}")
    if source.get("subdirectory") != RUNTIME_SUBDIRECTORY:
        raise SourceLinkError(f"{RUNTIME_PACKAGE} must use the notify-wake subdirectory")
    revision = source.get("rev")
    if not isinstance(revision, str) or GIT_SHA_PATTERN.fullmatch(revision) is None:
        raise SourceLinkError(f"{RUNTIME_PACKAGE} runtime pin must be a full Git SHA")
    return revision


def _makefile_runtime_sha(repo_root: Path) -> str:
    path = repo_root / "Makefile"
    try:
        contents = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise SourceLinkError(f"could not read {path}: {error}") from error
    match = MAKEFILE_PIN_PATTERN.search(contents)
    if match is None:
        raise SourceLinkError("Makefile runtime pin is missing or malformed")
    return match.group("sha")


def _lock_runtime_sha(repo_root: Path) -> str:
    payload = _read_toml(repo_root / "uv.lock")
    packages = payload.get("package")
    if not isinstance(packages, list):
        raise SourceLinkError("uv.lock package list is missing")
    matches = [
        package
        for package in packages
        if isinstance(package, Mapping) and package.get("name") == RUNTIME_PACKAGE
    ]
    if len(matches) != 1:
        raise SourceLinkError(f"uv.lock must contain one {RUNTIME_PACKAGE} package")
    source = _mapping(matches[0].get("source"), f"uv.lock {RUNTIME_PACKAGE} source")
    git_source = source.get("git")
    if not isinstance(git_source, str):
        raise SourceLinkError(f"uv.lock {RUNTIME_PACKAGE} Git source is missing")
    parsed = urlsplit(git_source)
    if f"{parsed.scheme}://{parsed.netloc}{parsed.path}" != SKILLS_URL:
        raise SourceLinkError(f"uv.lock {RUNTIME_PACKAGE} source repository is inconsistent")
    query = parse_qs(parsed.query)
    revisions = query.get("rev", [])
    subdirectories = query.get("subdirectory", [])
    if len(revisions) != 1 or GIT_SHA_PATTERN.fullmatch(revisions[0]) is None:
        raise SourceLinkError(f"uv.lock {RUNTIME_PACKAGE} runtime pin is malformed")
    if subdirectories != [RUNTIME_SUBDIRECTORY]:
        raise SourceLinkError(f"uv.lock {RUNTIME_PACKAGE} subdirectory is inconsistent")
    if parsed.fragment != revisions[0]:
        raise SourceLinkError(f"uv.lock {RUNTIME_PACKAGE} resolved commit is inconsistent")
    return revisions[0]


def validate_source_links(
    repo_root: Path,
    *,
    run_git: RunGit = _run_git,
) -> SourceLinkResult:
    """Validate the initialized submodule and every shared-runtime source pin."""

    root = repo_root.resolve()
    skills_root = root / SKILLS_PATH
    if not skills_root.exists():
        raise SourceLinkError(f"{SKILLS_PATH} submodule is missing")
    if not (skills_root / ".git").exists() or not (root / SKILL_PATH).is_file():
        raise SourceLinkError(f"{SKILLS_PATH} submodule is not initialized")

    skills_url = _submodule_configuration(root)
    gitlink_sha = _gitlink_sha(root, run_git)
    checkout_sha = run_git(("rev-parse", "HEAD"), skills_root).strip()
    if checkout_sha != gitlink_sha:
        raise SourceLinkError(
            f"{SKILLS_PATH} checkout {checkout_sha} does not match gitlink {gitlink_sha}"
        )
    dirty = run_git(("status", "--porcelain", "--untracked-files=all"), skills_root)
    if dirty:
        raise SourceLinkError(f"{SKILLS_PATH} has local changes")

    runtime_pins = {
        "pyproject.toml": _pyproject_runtime_sha(root),
        "Makefile": _makefile_runtime_sha(root),
        "uv.lock": _lock_runtime_sha(root),
    }
    mismatches = {name: sha for name, sha in runtime_pins.items() if sha != gitlink_sha}
    if mismatches:
        details = ", ".join(f"{name}={sha}" for name, sha in sorted(mismatches.items()))
        raise SourceLinkError(
            f"notify-wake runtime pin must match skills gitlink {gitlink_sha}: {details}"
        )
    return SourceLinkResult(
        skills_path=SKILLS_PATH.as_posix(),
        skills_sha=gitlink_sha,
        skills_url=skills_url,
        runtime_sha=gitlink_sha,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(arguments)
    try:
        result = validate_source_links(args.root)
    except SourceLinkError as error:
        print(f"source-link validation failed: {error}", file=sys.stderr)
        return 1
    if args.format == "json":
        print(json.dumps(asdict(result), sort_keys=True))
    else:
        print(f"source links valid at {result.skills_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
