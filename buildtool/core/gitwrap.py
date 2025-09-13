
from __future__ import annotations
import subprocess, os
from dataclasses import dataclass

@dataclass
class GitResult:
    code: int
    out: str

def _run(cmd, cwd: str, env: dict | None = None) -> GitResult:
    try:
        proc = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            env={**os.environ, **(env or {})}
        )
        out = []
        assert proc.stdout is not None
        for line in proc.stdout:
            out.append(line)
        code = proc.wait()
        return GitResult(code, "".join(out))
    except Exception as e:
        return GitResult(999, f"{type(e).__name__}: {e}")

def _git(cwd: str, *args: str) -> GitResult:
    return _run(["git", *args], cwd=cwd)

def fetch(cwd: str) -> GitResult:
    return _git(cwd, "fetch", "--all", "--prune")

def is_repo_clean(cwd: str) -> bool:
    r = _git(cwd, "status", "--porcelain")
    return r.code == 0 and r.out.strip() == ""

def current_branch(cwd: str) -> str:
    r = _git(cwd, "rev-parse", "--abbrev-ref", "HEAD")
    return r.out.strip() if r.code == 0 else "(desconocida)"

def checkout(cwd: str, branch: str, create: bool=False, track: str | None=None) -> GitResult:
    if create:
        args = ["checkout", "-b", branch]
        if track: args += ["--track", track]
    else:
        args = ["checkout", branch]
    return _git(cwd, *args)

def create_branch(cwd: str, branch: str, base: str|None=None, push: bool=False) -> GitResult:
    args = ["checkout", "-b", branch] + ([base] if base else [])
    r = _git(cwd, *args)
    if r.code != 0: return r
    if push:
        return _git(cwd, "push", "-u", "origin", branch)
    return r

def delete_branch(cwd: str, branch: str, remote: bool=False, force: bool=False) -> GitResult:
    if remote:
        return _git(cwd, "push", "origin", "--delete", branch)
    else:
        return _git(cwd, "branch", "-D" if force else "-d", branch)

def remote_branch_exists(cwd: str, branch: str) -> bool:
    r = _git(cwd, "ls-remote", "--exit-code", "--heads", "origin", branch)
    return r.code == 0

def local_branch_exists(cwd: str, branch: str) -> bool:
    r = _git(cwd, "show-ref", "--verify", "--quiet", f"refs/heads/{branch}")
    return r.code == 0

def list_local_branches(cwd: str) -> list[str]:
    r = _git(cwd, "branch")
    if r.code != 0: return []
    out = []
    for ln in r.out.splitlines():
        s = ln.strip()
        if not s: continue
        if s.startswith("* "): s = s[2:]
        out.append(s.strip())
    return out

def list_remote_branches(cwd: str) -> list[str]:
    r = _git(cwd, "ls-remote", "--heads", "origin")
    if r.code != 0: return []
    out = []
    for ln in r.out.splitlines():
        if "\trefs/heads/" in ln:
            name = ln.split("\trefs/heads/", 1)[1].strip()
            if name: out.append(name)
    return out

def merge_into_current(cwd: str, source: str) -> GitResult:
    return _git(cwd, "merge", source)

def push_current(cwd: str) -> GitResult:
    return _git(cwd, "push")

def status(cwd: str) -> str:
    r = _git(cwd, "status", "-sb")
    return r.out if r.code == 0 else f"(git status error: {r.out.strip()})"
