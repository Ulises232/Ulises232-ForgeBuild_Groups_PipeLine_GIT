from __future__ import annotations
from typing import List, Tuple, Optional
from pathlib import Path
import os

from .config import groups_for_user
from .git_fast import get_current_branch_fast


def _resolve_base_path(raw: Optional[str], env_base: Optional[Path]) -> Optional[Path]:
    if not raw:
        return env_base
    try:
        expanded = os.path.expandvars(os.path.expanduser(str(raw)))
        p = Path(expanded)
    except Exception:
        return env_base
    if p.is_absolute():
        return p.resolve(strict=False)
    if env_base:
        return (env_base / p).resolve(strict=False)
    return p.resolve(strict=False)


def _resolve_module_path(raw: Optional[str], base: Optional[Path], env_base: Optional[Path]) -> Path:
    try:
        expanded = os.path.expandvars(os.path.expanduser(str(raw or ".")))
        rel = Path(expanded)
    except Exception:
        rel = Path(str(raw or "."))
    if rel.is_absolute():
        return rel.resolve(strict=False)
    if base:
        return (base / rel).resolve(strict=False)
    if env_base:
        return (env_base / rel).resolve(strict=False)
    return (Path.cwd() / rel).resolve(strict=False)


def _iter_cfg_entries(cfg, gkey: Optional[str], pkey: Optional[str]) -> List[Tuple[str, Path]]:
    entries: List[Tuple[str, Path]] = []
    module_paths: set[Path] = set()
    env_base = None
    herr_raw = (os.environ.get("HERR_REPO", "") or "").strip()
    if herr_raw:
        try:
            env_base = Path(os.path.expandvars(os.path.expanduser(herr_raw))).resolve(strict=False)
        except Exception:
            env_base = Path(herr_raw).resolve(strict=False)

    def _push(name: str, path: Path):
        entries.append((name or "mod", path))

    groups = groups_for_user(cfg)
    if groups:
        for g in groups:
            if gkey and getattr(g, "key", None) != gkey:
                continue
            projects = getattr(g, "projects", None) or []
            for p in projects:
                if pkey and getattr(p, "key", None) != pkey:
                    continue
                base = _resolve_base_path(getattr(p, "root", None), env_base)
                if base:
                    module_paths.add(base)
                modules = getattr(p, "modules", None) or []
                if modules:
                    for m in modules:
                        name = (
                            getattr(m, "name", None)
                            or getattr(m, "key", None)
                            or str(getattr(m, "path", "") or "")
                        )
                        path = _resolve_module_path(getattr(m, "path", None), base, env_base)
                        module_paths.add(path)
                        _push(name, path)
                elif base:
                    proj_name = getattr(p, "key", None) or "root"
                    _push(proj_name, base)
            repos = getattr(g, "repos", None) or {}
            if isinstance(repos, dict):
                for name, raw in repos.items():
                    path = _resolve_module_path(raw, None, env_base)
                    if any(path == mp or path in mp.parents for mp in module_paths):
                        continue
                    _push(str(name), path)
    else:
        projects = getattr(cfg, "projects", None) or []
        for p in projects:
            if pkey and getattr(p, "key", None) != pkey:
                continue
            base = _resolve_base_path(getattr(p, "root", None), env_base)
            if base:
                module_paths.add(base)
            modules = getattr(p, "modules", None) or []
            if modules:
                for m in modules:
                    name = (
                        getattr(m, "name", None)
                        or getattr(m, "key", None)
                        or str(getattr(m, "path", "") or "")
                    )
                    path = _resolve_module_path(getattr(m, "path", None), base, env_base)
                    module_paths.add(path)
                    _push(name, path)
            elif base:
                proj_name = getattr(p, "key", None) or "root"
                _push(proj_name, base)

    seen = set()
    unique: List[Tuple[str, Path]] = []
    for name, path in entries:
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append((name, path))
    return unique


def discover_status_fast(cfg, gkey: Optional[str], pkey: Optional[str]) -> List[tuple[str, str, Path]]:
    items: List[tuple[str, str, Path]] = []
    for name, path in _iter_cfg_entries(cfg, gkey, pkey):
        branch = get_current_branch_fast(path) or "?"
        items.append((name, branch, path))
    return items

