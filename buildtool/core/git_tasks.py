
from __future__ import annotations
from typing import Dict, List, Iterable, Optional, Tuple, Set
from .config import Config, groups_for_user
from .tasks import _resolve_repo_path
from .gitwrap import (
    fetch, current_branch, checkout, create_branch, delete_branch,
    local_branch_exists, remote_branch_exists, list_local_branches, list_remote_branches,
    merge_into_current, push_current, _git
)
from .git_history import add_branch as hist_add, remove_branch as hist_remove
from pathlib import Path
import re

def _iter_modules(cfg: Config, group_key: Optional[str], project_key: Optional[str], only: Optional[Set[str]] = None):
    groups = [g for g in groups_for_user(cfg) if (group_key is None or g.key == group_key)]
    for g in groups:
        projs = [p for p in g.projects if (project_key is None or p.key == project_key)]
        for p in projs:
            repo_base = _resolve_repo_path(cfg, p.key, g.key, getattr(p, "repo", None), getattr(p, "workspace", None))
            for m in (p.modules or []):
                if only and m.name not in only:
                    continue
                module_path = repo_base / m.path
                yield g, p, m, module_path

def _scope_keys(cfg: Config, group_key: Optional[str], project_key: Optional[str]):
    return group_key, project_key

def discover_status(cfg: Config, group_key: Optional[str], project_key: Optional[str]):
    out = []
    for g, p, m, path in _iter_modules(cfg, group_key, project_key):
        br = current_branch(str(path))
        out.append((m.name, br, str(path)))
    return out

def list_presence(cfg: Config, group_key: Optional[str], project_key: Optional[str], branch: str):
    rows = []
    for g, p, m, path in _iter_modules(cfg, group_key, project_key):
        l = local_branch_exists(str(path), branch)
        r = remote_branch_exists(str(path), branch)
        rows.append((m.name, l, r))
    return rows

def switch_branch(cfg: Config, group_key: Optional[str], project_key: Optional[str], branch: str, log, only_modules: Optional[Iterable[str]] = None):
    only = set(only_modules or [])
    for g, p, m, path in _iter_modules(cfg, group_key, project_key, only):
        label = f"{p.key}/{m.name}"
        log(f"\\n=== [{label}] {path}")
        fetch(str(path))
        if local_branch_exists(str(path), branch):
            r = checkout(str(path), branch); 
            log("OK checkout" if r.code==0 else f"!! {r.out}")
        elif remote_branch_exists(str(path), branch):
            r = checkout(str(path), branch, create=True, track=f"origin/{branch}")
            log("OK checkout -b/track" if r.code==0 else f"!! {r.out}")
        else:
            log(f"-- La rama {branch} no existe (local ni remota).")

def create_branches_local(cfg: Config, group_key: Optional[str], project_key: Optional[str],
                          new_branch: str, log, only_modules: Optional[Iterable[str]] = None):
    only = set(only_modules or [])
    skg, skp = _scope_keys(cfg, group_key, project_key)
    for g, p, m, path in _iter_modules(cfg, group_key, project_key, only):
        label = f"{p.key}/{m.name}"
        log(f"\\n=== [{label}] {path}")
        if local_branch_exists(str(path), new_branch):
            log(f"-- Ya existe local {new_branch}.")
        else:
            r = checkout(str(path), new_branch, create=True)
            log("OK creada local" if r.code==0 else f"!! {r.out}")
        hist_add(skg, skp, new_branch)

def push_branch(cfg: Config, group_key: Optional[str], project_key: Optional[str],
                branch: str, log, only_modules: Optional[Iterable[str]] = None):
    only = set(only_modules or [])
    for g, p, m, path in _iter_modules(cfg, group_key, project_key, only):
        label = f"{p.key}/{m.name}"
        log(f"\\n=== [{label}] {path}")
        if not local_branch_exists(str(path), branch):
            log(f"-- No existe rama local {branch}, omitido."); continue
        r = _git(str(path), "push", "-u", "origin", f"refs/heads/{branch}:refs/heads/{branch}")
        log("OK push con upstream" if r.code==0 else f"!! {r.out}")

def create_version_branches(cfg: Config, group_key: Optional[str], project_key: Optional[str],
                            version: str, create_qa: bool,
                            version_files_override: Dict[str, List[str]] | None,
                            repos_no_change: Iterable[str],
                            log, only_modules: Optional[Iterable[str]] = None):
    branch_base = f"v{version}"
    branch_qa = f"{branch_base}_QA"
    repos_no_change = set(repos_no_change or [])
    only = set(only_modules or [])
    skg, skp = _scope_keys(cfg, group_key, project_key)

    for g, p, m, path in _iter_modules(cfg, group_key, project_key, only):
        label = f"{p.key}/{m.name}"
        log(f"\\n=== [{label}] {path}")
        if local_branch_exists(str(path), branch_base):
            log(f"-- Ya existe local {branch_base}.")
        else:
            r = checkout(str(path), branch_base, create=True)
            log("OK creada local base" if r.code==0 else f"!! {r.out}")
        changed = False
        rel_files = (version_files_override or {}).get(m.name) or list(getattr(m, "version_files", []) or [])
        if rel_files and m.name not in repos_no_change:
            for rel in rel_files:
                f = (Path(path) / rel)
                if f.exists():
                    txt = f.read_text(encoding="utf-8", errors="ignore")
                    new_txt, n = re.subn(r"(<param-value>\\s*Versi[oó]n:\\s*)(.*?)(</param-value>)",
                                         r"\\g<1>" + version + r"\\g<3>", txt, flags=re.IGNORECASE|re.DOTALL)
                    if n>0:
                        f.write_text(new_txt, encoding="utf-8"); changed = True; log(f"  * Versión {version} en {rel}")
                else:
                    log(f"  ! No existe: {rel}")
        elif m.name not in repos_no_change:
            log("  (Sin archivos de versión configurados)")
        if changed:
            _git(str(path), "add", "--all"); _git(str(path), "commit", "-m", f"cambio de versión a {version}")
        if create_qa:
            if local_branch_exists(str(path), branch_qa):
                log(f"-- Ya existe local {branch_qa}.")
            else:
                r = checkout(str(path), branch_qa, create=True)
                log("OK creada local QA" if r.code==0 else f"!! {r.out}")
        hist_add(skg, skp, branch_base); 
        if create_qa: hist_add(skg, skp, branch_qa)

def delete_local_branch_by_name(cfg: Config, group_key: Optional[str], project_key: Optional[str],
                                branch: str, confirm: bool, log, only_modules: Optional[Iterable[str]] = None):
    if not confirm:
        log("Marca Confirmar para eliminar la rama local."); return
    only = set(only_modules or [])
    skg, skp = _scope_keys(cfg, group_key, project_key)
    for g, p, m, path in _iter_modules(cfg, group_key, project_key, only):
        label = f"{p.key}/{m.name}"
        log(f"\\n=== [{label}] {path}")
        cur = current_branch(str(path))
        if cur == branch:
            log("  ! No se puede eliminar la rama actual."); continue
        if local_branch_exists(str(path), branch):
            r = delete_branch(str(path), branch, remote=False, force=True)
            log("  - eliminada local" if r.code==0 else f"  !! {r.out}")
        else:
            log("  (no existía local)")
    presence = list_presence(cfg, group_key, project_key, branch)
    if not any(l or r for _, l, r in presence):
        hist_remove(skg, skp, branch); log(f"-- '{branch}' removida de historial (no existe local/remoto en el proyecto).")
    else:
        log("-- La rama aún existe en alguna forma (local/remota), se conserva en historial.")

def delete_local_others(cfg: Config, group_key: Optional[str], project_key: Optional[str], confirm: bool, log, only_modules: Optional[Iterable[str]] = None):
    if not confirm:
        log("Usa confirm=True para eliminar TODAS las ramas locales excepto la actual."); return
    only = set(only_modules or [])
    for g, p, m, path in _iter_modules(cfg, group_key, project_key, only):
        label = f"{p.key}/{m.name}"
        log(f"\\n=== [{label}] {path}")
        cur = current_branch(str(path))
        for br in list_local_branches(str(path)):
            if br != cur:
                r = delete_branch(str(path), br, remote=False, force=True)
                log(f"  - eliminada local: {br}" if r.code==0 else f"  !! {br}: {r.out}")

def merge_into_current_branch(cfg: Config, group_key: Optional[str], project_key: Optional[str], source: str, push: bool, log, only_modules: Optional[Iterable[str]] = None):
    only = set(only_modules or [])
    conflicted: List[Tuple[str, str]] = []
    for g, p, m, path in _iter_modules(cfg, group_key, project_key, only):
        label = f"{p.key}/{m.name}"; log(f"\\n=== [{label}] {path}")
        cur = current_branch(str(path))
        fetch_result = fetch(str(path))
        if fetch_result.code != 0:
            log(f"!! fetch falló, se omite: {fetch_result.out.strip() or fetch_result.code}")
            continue
        local_exists = local_branch_exists(str(path), source)
        remote_exists = remote_branch_exists(str(path), source)
        if not (local_exists or remote_exists):
            log(f"-- La rama {source} no existe en {label}"); continue
        if (not local_exists) and remote_exists:
            checkout_new = checkout(str(path), source, create=True, track=f"origin/{source}")
            if checkout_new.code != 0:
                log(f"!! No se pudo preparar rama remota {source}: {checkout_new.out.strip()}")
                continue
            checkout_back = checkout(str(path), cur)
            if checkout_back.code != 0:
                log(f"!! No se pudo volver a {cur}: {checkout_back.out.strip()}")
                continue
        r = merge_into_current(str(path), source)
        if r.code==0:
            log(f"OK merge {source} -> {cur}")
            if push:
                rp = push_current(str(path)); log("OK push" if rp.code==0 else f"!! {rp.out}")
        else:
            log(f"!! Conflicto/merge: {r.out}"); conflicted.append((label, str(path)))
    if conflicted:
        log("\\nRepos con conflictos, requieren intervención manual:")
        for n, pth in conflicted: log(f"  → {n}: {pth}")
    else:
        log("\\nMerge completado en repos sin conflicto.")
