from __future__ import annotations
from pathlib import Path
from typing import Optional, List
import os
import subprocess

def _resolve_gitdir(repo: Path) -> Optional[Path]:
    git_path = repo / ".git"
    if git_path.is_dir():
        return git_path
    if git_path.is_file():
        try:
            txt = git_path.read_text(encoding="utf-8", errors="ignore").strip()
            if txt.startswith("gitdir:"):
                p = txt.split("gitdir:",1)[1].strip()
                pth = Path(p)
                if not pth.is_absolute():
                    pth = (repo / p).resolve()
                return pth
        except Exception:
            return None
    return None

def _read_head_branch(repo: Path) -> Optional[str]:
    g = _resolve_gitdir(repo)
    if not g:
        return None
    head = g / "HEAD"
    try:
        txt = head.read_text(encoding="utf-8", errors="ignore").strip()
        if txt.startswith("ref:"):
            ref = txt.split("ref:", 1)[1].strip()
            if ref.startswith("refs/heads/"):
                return ref.split("refs/heads/", 1)[1]
            return ref
        return "(detached)"
    except Exception:
        return None


def _popen_kwargs():
    if os.name != "nt":
        return {}
    startup = subprocess.STARTUPINFO()
    startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startup.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
    return {
        "startupinfo": startup,
        "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
    }


def get_current_branch_fast(repo: Path) -> Optional[str]:
    # Intentamos primero leer HEAD directamente para evitar invocar git.exe.
    br = _read_head_branch(repo)
    if br:
        return br
    # Si falló, intentamos ejecutar git ocultando la ventana en Windows.
    popen_kwargs = _popen_kwargs()
    try:
        out = subprocess.check_output(
            ["git", "branch", "--show-current"],
            cwd=str(repo),
            stderr=subprocess.DEVNULL,
            text=True,
            **popen_kwargs,
        )
        br = out.strip()
        if br:
            return br
    except Exception:
        pass
    return None

def list_local_branches_fast(repo: Path) -> List[str]:
    g = _resolve_gitdir(repo)
    out = []
    if not g:
        return out
    rh = g / "refs" / "heads"
    if rh.exists():
        for p in rh.rglob("*"):
            if p.is_file():
                rel = p.relative_to(rh).as_posix()
                out.append(rel)
    pr = g / "packed-refs"
    if pr.exists():
        try:
            for line in pr.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("^"):
                    continue
                parts = line.split()
                if len(parts) != 2:
                    continue
                _, ref = parts
                if ref.startswith("refs/heads/"):
                    out.append(ref.split("refs/heads/",1)[1])
        except Exception:
            pass
    return sorted(set(out))

def list_remote_branches_fast(repo: Path) -> list[str]:
    return []