from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from scripts import validate_source_links

SKILLS_SHA = "e9806cbe7a35ed1678dff526b80a16a4cefd72b5"
OTHER_SHA = "0" * 40
SKILLS_URL = "https://github.com/TidalPaladin/skills.git"


class FakeGit:
    def __init__(
        self,
        *,
        gitlink_sha: str = SKILLS_SHA,
        checkout_sha: str = SKILLS_SHA,
        dirty: str = "",
    ) -> None:
        self.gitlink_sha = gitlink_sha
        self.checkout_sha = checkout_sha
        self.dirty = dirty

    def __call__(self, arguments: Sequence[str], cwd: Path) -> str:
        del cwd
        command = tuple(arguments)
        if command == ("ls-files", "--stage", "--", ".agents/skills"):
            return f"160000 {self.gitlink_sha} 0\t.agents/skills\n"
        if command == ("rev-parse", "HEAD"):
            return f"{self.checkout_sha}\n"
        if command == ("status", "--porcelain", "--untracked-files=all"):
            return self.dirty
        raise AssertionError(f"unexpected Git command: {command}")


def _write_fixture(
    root: Path,
    *,
    runtime_sha: str = SKILLS_SHA,
    initialize_submodule: bool = True,
) -> None:
    (root / ".gitmodules").write_text(
        '[submodule ".agents/skills"]\n'
        "\tpath = .agents/skills\n"
        f"\turl = {SKILLS_URL}\n"
        "\tshallow = true\n",
        encoding="utf-8",
    )
    (root / "pyproject.toml").write_text(
        "[tool.uv.sources]\n"
        'notify-wake-runtime = { git = "https://github.com/TidalPaladin/skills.git", '
        f'rev = "{runtime_sha}", subdirectory = "notify-wake" }}\n',
        encoding="utf-8",
    )
    (root / "Makefile").write_text(
        "NOTIFY_WAKE_RUNTIME = notify-wake-runtime @ "
        "git+https://github.com/TidalPaladin/skills.git@"
        f"{runtime_sha}\\#subdirectory=notify-wake\n",
        encoding="utf-8",
    )
    (root / "uv.lock").write_text(
        "version = 1\nrevision = 3\n"
        "[[package]]\n"
        'name = "notify-wake-runtime"\n'
        'version = "1.0.0"\n'
        'source = { git = "https://github.com/TidalPaladin/skills.git?'
        f'subdirectory=notify-wake&rev={runtime_sha}#{runtime_sha}" }}\n',
        encoding="utf-8",
    )
    if initialize_submodule:
        skill = root / ".agents" / "skills" / "autoresearch" / "SKILL.md"
        skill.parent.mkdir(parents=True)
        skill.write_text("# Autoresearch\n", encoding="utf-8")
        (root / ".agents" / "skills" / ".git").write_text(
            "gitdir: fixture\n",
            encoding="utf-8",
        )


def test_source_links_accept_matching_clean_initialized_checkout(tmp_path: Path) -> None:
    _write_fixture(tmp_path)

    result = validate_source_links.validate_source_links(tmp_path, run_git=FakeGit())

    assert result.skills_sha == SKILLS_SHA
    assert result.runtime_sha == SKILLS_SHA
    assert result.skills_url == SKILLS_URL


def test_source_links_reject_missing_submodule(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    submodule = tmp_path / ".agents" / "skills"
    for path in sorted(submodule.rglob("*"), reverse=True):
        path.unlink() if path.is_file() else path.rmdir()
    submodule.rmdir()

    with pytest.raises(validate_source_links.SourceLinkError, match="submodule is missing"):
        validate_source_links.validate_source_links(tmp_path, run_git=FakeGit())


def test_source_links_reject_uninitialized_submodule(tmp_path: Path) -> None:
    _write_fixture(tmp_path, initialize_submodule=False)
    (tmp_path / ".agents" / "skills").mkdir(parents=True)

    with pytest.raises(validate_source_links.SourceLinkError, match="not initialized"):
        validate_source_links.validate_source_links(tmp_path, run_git=FakeGit())


def test_source_links_reject_dirty_submodule(tmp_path: Path) -> None:
    _write_fixture(tmp_path)

    with pytest.raises(validate_source_links.SourceLinkError, match="has local changes"):
        validate_source_links.validate_source_links(
            tmp_path,
            run_git=FakeGit(dirty=" M autoresearch/SKILL.md\n"),
        )


def test_source_links_reject_wrong_checkout_sha(tmp_path: Path) -> None:
    _write_fixture(tmp_path)

    with pytest.raises(validate_source_links.SourceLinkError, match="does not match gitlink"):
        validate_source_links.validate_source_links(
            tmp_path,
            run_git=FakeGit(checkout_sha=OTHER_SHA),
        )


def test_source_links_reject_runtime_pin_mismatch(tmp_path: Path) -> None:
    _write_fixture(tmp_path, runtime_sha=OTHER_SHA)

    with pytest.raises(validate_source_links.SourceLinkError, match="runtime pin"):
        validate_source_links.validate_source_links(tmp_path, run_git=FakeGit())
