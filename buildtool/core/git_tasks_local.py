# buildtool/core/git_tasks_local.py
# Implementación con impresión a consola/emit SIEMPRE (sin depender de UI).
# - Escanea módulos por cfg (groups/projects/modules) y/o por filesystem (.git).
# - Ejecuta por cada repo detectado y muestra comandos + cwd + rc.
from __future__ import annotations
from pathlib import Path
from typing import Optional, Iterable, Tuple, List, Iterator
import os
import subprocess
import getpass

from buildtool.core.branch_store import (
    BranchRecord,
    load_index,
    upsert,
    remove,
    record_activity,
)
from buildtool.core.git_console_trace import clog

# --------------------- helpers de salida y ejecución ---------------------


def _out(emit, msg: str):
    """Enruta la salida hacia emit si existe; si no, a clog."""
    try:
        if emit:
            emit(msg)
        else:
            clog(msg)
    except Exception:
        try:
            clog(msg)
        except Exception:
            pass


def _run(cmd: List[str], cwd: Path, emit=None) -> Tuple[int, str]:
    """Ejecuta un comando y transmite stdout/stderr línea por línea al logger."""
    _out(emit, f"$ {' '.join(cmd)}  (cwd={cwd})")
    p = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        shell=False,
    )
    out = p.communicate()[0]
    for ln in out.splitlines():
        _out(emit, ln.rstrip())
    rc = p.wait()
    _out(emit, f"[rc={rc}] {' '.join(cmd)}")
    return rc, out


def _current_user() -> str:
    return os.environ.get("USERNAME") or os.environ.get("USER") or getpass.getuser()


def _get_record(index, gkey, pkey, branch) -> BranchRecord:
    key = f"{gkey or ''}/{pkey or ''}/{branch}"
    rec = index.get(key)
    if not rec:
        user = _current_user()
        rec = BranchRecord(branch=branch, group=gkey, project=pkey, created_by=user, last_updated_by=user)
    return rec


def _is_git_repo(path: Path, emit=None) -> bool:
    """Devuelve True si path está dentro de un repo git. Loguea de forma segura."""
    try:
        rc, _ = _run(["git", "rev-parse", "--is-inside-work-tree"], path, emit=emit)
        return rc == 0
    except Exception as e:
        _out(emit, f"[is_git_repo] EXCEPTION {e}")
        return False


# --------------------- descubrimiento de módulos/repos ---------------------


def _norm_path(p: Optional[str]) -> Path:
    """Normaliza una ruta de config: expande ~ y variables; resolve sin strict."""
    raw = (p or "").strip().strip('"').strip("'")
    if not raw:
        return Path(".").resolve()
    try:
        expanded = os.path.expandvars(os.path.expanduser(raw))
        return Path(expanded).resolve(strict=False)
    except Exception:
        # último recurso: Path directo
        return Path(raw)

def _get_herr_repo() -> Path | None:
    v = os.environ.get("HERR_REPO", "").strip()
    if not v:
        return None
    try:
        # normaliza variables de entorno y ~
        v = os.path.expanduser(os.path.expandvars(v))
        return Path(v).resolve(strict=False)  # no falla si no existe aún
    except Exception:
        return None

def _iter_cfg_modules(
    cfg, gkey: Optional[str], pkey: Optional[str], only_modules: Optional[Iterable[str]]
) -> Iterator[Tuple[str, Path]]:
    """
    Itera módulos/roots definidos en cfg, devolviendo (nombre_modulo, path).
    Soporta:
      - cfg.groups[*].projects[*].modules[*]
      - cfg.groups[*].projects[*].root (cuando no hay modules)
      - cfg.groups[*].repos (dict nombre->path)
      - cfg.projects[*].modules[*] / cfg.projects[*].root
    """
    filt = set(only_modules or [])
    base = _get_herr_repo()
    # -------- 1) Estructura con groups ----------
    if getattr(cfg, "groups", None):
        for g in cfg.groups:
            if gkey and getattr(g, "key", None) != gkey:
                continue
            projects = getattr(g, "projects", None) or []
            for p in projects:
                if pkey and getattr(p, "key", None) != pkey:
                    continue
                modules = getattr(p, "modules", None) or []
                if modules:
                    for m in modules:
                        name = (
                            getattr(m, "name", None)
                            or getattr(m, "key", None)
                            or str(getattr(m, "path", "") or "")
                        )
                        if filt and name not in filt:
                            continue
                        rel = getattr(m, "path", ".") or "."
                        relp = Path(os.path.expanduser(os.path.expandvars(rel)))
                        # si rel es absoluto, úsalo tal cual; si no, cuélgalo de HERR_REPO
                        mod_path = (relp if relp.is_absolute() else (base / relp)).resolve(strict=False)
                        yield (name or "mod", mod_path)
                else:
                    # Sin módulos: si quieres que el proyecto viva en HERR_REPO directamente:
                    name = getattr(p, "key", None) or "root"
                    yield (name, base.resolve(strict=False))
        return

def _discover_repos(cfg, gkey, pkey, only_modules, emit=None) -> List[Tuple[str, Path]]:
    """
    Devuelve una lista única de (nombre, path_repo_o_modulo),
    primero por CFG, luego por FS (a partir de HERR_REPO/cwd).
    """
    repos: List[Tuple[str, Path]] = []
    _out(emit, f"== DESCUBRIR MÓDULOS/REPOS =={os.environ['HERR_REPO']}")
    # 1) Por cfg
    for name, path in _iter_cfg_modules(cfg, gkey, pkey, only_modules):
        _out(emit, f"[cfg] posible módulo: {name} -> {path}")
        repos.append((name, path))


    # De-duplicar manteniendo orden
    seen = set()
    unique: List[Tuple[str, Path]] = []
    for name, path in repos:
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append((name, path))

    if not unique:
        _out(emit, "⚠️ No se encontraron módulos ni repos.")
    return unique


# --------------------- operaciones por cada repo ---------------------


def _create_or_switch(branch: str, path: Path, emit=None) -> bool:
    """Crea o activa una rama en el repo especificado."""
    rc, _ = _run(["git", "switch", "-c", branch], path, emit=emit)
    if rc != 0:
        rc2, _ = _run(["git", "checkout", "-b", branch], path, emit=emit)
        if rc2 != 0:
            return False
    return True


# --------------------- API consumida por la UI ---------------------


def create_branches_local(
    cfg,
    gkey,
    pkey,
    name: str,
    emit,
    only_modules: Optional[Iterable[str]] = None,
) -> bool:
    bname = (name or "").strip()
    if not bname:
        _out(emit, "❌ Nombre de rama vacío.")
        raise RuntimeError("Nombre de rama vacío.")

    repos = _discover_repos(cfg, gkey, pkey, only_modules, emit=emit)
    ok_all = True
    for mname, mpath in repos:
        if not mpath.exists():
            _out(emit, f"[{mname}] ⚠️ Ruta no existe: {mpath}")
            ok_all = False
            continue
        if not _is_git_repo(mpath, emit=emit):
            _out(emit, f"[{mname}] ⚠️ No es repo Git: {mpath}")
            ok_all = False
            continue

        _out(emit, f"[{mname}] ▶ crear/switch rama: {bname}")
        if not _create_or_switch(bname, mpath, emit=emit):
            _out(emit, f"[{mname}] ❌ No se pudo crear/switch a '{bname}'")
            ok_all = False
        else:
            _out(emit, f"[{mname}] ✅ rama lista: {bname}")
    if ok_all:
        idx = load_index()
        rec = _get_record(idx, gkey, pkey, bname)
        rec.exists_local = True
        rec.last_action = "create_local"
        rec.last_updated_by = _current_user()
        upsert(rec, idx, action="create_local")
    return ok_all


def create_version_branches(
    cfg, gkey, pkey, version: str, emit=None, only_modules=None
) -> bool:
    return create_branches_local(
        cfg, gkey, pkey, version, emit=emit, only_modules=only_modules
    )


def switch_branch(
    cfg, gkey, pkey, name: str, emit=None, only_modules=None
) -> bool:
    bname = (name or "").strip()
    if not bname:
        _out(emit, "❌ Nombre de rama vacío.")
        raise RuntimeError("Nombre de rama vacío.")

    repos = _discover_repos(cfg, gkey, pkey, only_modules, emit=emit)
    ok_all = True
    for mname, mpath in repos:
        if not _is_git_repo(mpath, emit=emit):
            _out(emit, f"[{mname}] ⚠️ No es repo Git: {mpath}")
            ok_all = False
            continue

        rc, _ = _run(["git", "switch", bname], mpath, emit=emit)
        if rc != 0:
            rc2, _ = _run(["git", "checkout", bname], mpath, emit=emit)
            if rc2 != 0:
                _out(emit, f"[{mname}] ❌ No se pudo hacer switch a '{bname}'")
                ok_all = False
            else:
                _out(emit, f"[{mname}] ✅ switch con checkout: {bname}")
        else:
            _out(emit, f"[{mname}] ✅ switch: {bname}")
    if ok_all:
        idx = load_index()
        rec = _get_record(idx, gkey, pkey, bname)
        rec.exists_local = True
        rec.last_action = "switch"
        rec.last_updated_by = _current_user()
        upsert(rec, idx, action="switch")
    return ok_all


def delete_local_branch_by_name(
    cfg, gkey, pkey, name: str, confirm: bool, emit=None, only_modules=None
) -> bool:
    bname = (name or "").strip()
    if not bname:
        _out(emit, "❌ Nombre de rama vacío.")
        raise RuntimeError("Nombre de rama vacío.")

    repos = _discover_repos(cfg, gkey, pkey, only_modules, emit=emit)
    ok_all = True
    for mname, mpath in repos:
        if not _is_git_repo(mpath, emit=emit):
            _out(emit, f"[{mname}] ⚠️ No es repo Git: {mpath}")
            ok_all = False
            continue

        args = ["git", "branch", "-D" if confirm else "-d", bname]
        rc, _ = _run(args, mpath, emit=emit)
        if rc != 0:
            _out(emit, f"[{mname}] ❌ No se pudo eliminar '{bname}'")
            ok_all = False
        else:
            _out(emit, f"[{mname}] 🗑️ rama eliminada: {bname}")
    idx = load_index()
    key_rec = f"{gkey or ''}/{pkey or ''}/{bname}"
    rec = idx.get(key_rec)
    if not ok_all:
        if rec:
            rec.last_updated_by = _current_user()
            record_activity(
                "delete_local", rec, result="error", message="git branch delete failed"
            )
        return False

    exists_local = False
    exists_origin = False
    for mname, mpath in repos:
        if _is_git_repo(mpath):
            rc, _ = _run(["git", "show-ref", "--verify", "--quiet", f"refs/heads/{bname}"], mpath)
            if rc == 0:
                exists_local = True
            rc2, _ = _run(["git", "ls-remote", "--exit-code", "--heads", "origin", bname], mpath)
            if rc2 == 0:
                exists_origin = True
    if exists_origin:
        if not rec:
            rec = BranchRecord(branch=bname, group=gkey, project=pkey, created_by=_current_user())
        rec.exists_local = exists_local
        rec.exists_origin = True
        rec.last_action = "delete_local" if not exists_local else rec.last_action
        rec.last_updated_by = _current_user()
        upsert(rec, idx, action="delete_local" if not exists_local else "update")
    else:
        if rec:
            rec.last_updated_by = _current_user()
            remove(rec, idx)
    return True


def push_branch(
    cfg, gkey, pkey, name: str, emit=None, only_modules=None
) -> bool:
    bname = (name or "").strip()
    if not bname:
        _out(emit, "❌ Nombre de rama vacío en push.")
        raise RuntimeError("Nombre de rama vacío en push.")

    repos = _discover_repos(cfg, gkey, pkey, only_modules, emit=emit)
    if repos:
        first = repos[0][1]
        if _is_git_repo(first):
            rc, _ = _run(["git", "ls-remote", "--exit-code", "--heads", "origin", bname], first)
            if rc == 0:
                _out(emit, f"La rama ya existe en origin: {bname}")
                idx = load_index()
                rec = _get_record(idx, gkey, pkey, bname)
                rec.exists_local = True
                rec.exists_origin = True
                rec.last_action = "push_skip"
                rec.last_updated_by = _current_user()
                upsert(rec, idx, action="push_skip")
                return False
    ok_all = True
    for mname, mpath in repos:
        if not _is_git_repo(mpath, emit=emit):
            _out(emit, f"[{mname}] ⚠️ No es repo Git: {mpath}")
            ok_all = False
            continue

        rc, _ = _run(["git", "push", "-u", "origin", bname], mpath, emit=emit)
        if rc != 0:
            _out(emit, f"[{mname}] ❌ push falló para '{bname}'")
            ok_all = False
        else:
            _out(emit, f"[{mname}] ☁️ push origin {bname}")

    exists_origin = False
    for _, mpath in repos:
        if _is_git_repo(mpath):
            rc, _ = _run(["git", "ls-remote", "--exit-code", "--heads", "origin", bname], mpath)
            if rc == 0:
                exists_origin = True
                break

    idx = load_index()
    rec = _get_record(idx, gkey, pkey, bname)
    rec.exists_local = True
    rec.exists_origin = exists_origin
    rec.last_action = "push_origin" if ok_all else "push_failed"
    rec.last_updated_by = _current_user()
    upsert(rec, idx, action=rec.last_action)
    return ok_all
