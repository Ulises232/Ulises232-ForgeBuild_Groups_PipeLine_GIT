import subprocess
from pathlib import Path

from buildtool.core.git_tasks_local import _create_or_switch


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)
    return result


def test_create_or_switch_uses_base_branch(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "dev@example.com")
    _git(repo, "config", "user.name", "Dev")

    (repo / "file.txt").write_text("root", encoding="utf-8")
    _git(repo, "add", "file.txt")
    _git(repo, "commit", "-m", "init")

    _git(repo, "switch", "-c", "v2.68")
    (repo / "file.txt").write_text("base branch", encoding="utf-8")
    _git(repo, "commit", "-am", "base update")

    ok, detail = _create_or_switch("v2.68_feature", repo, base="v2.68")
    assert ok
    assert detail in {"create_switch_base", "checkout_create_base", "switch"}

    tip_feature = _git(repo, "rev-parse", "v2.68_feature").stdout.strip()
    tip_base = _git(repo, "rev-parse", "v2.68").stdout.strip()
    assert tip_feature == tip_base

    ok_missing, message = _create_or_switch("v2.68_bugfix", repo, base="does-not-exist")
    assert not ok_missing
    assert message
