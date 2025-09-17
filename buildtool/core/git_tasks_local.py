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


if os.name == "nt":
    def _popen_kwargs():
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
        return {
            "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
            "startupinfo": startup,
        }
else:
    def _popen_kwargs():
        return {}


def _run(cmd: List[str], cwd: Path, emit=None) -> Tuple[int, str]:
    """Ejecuta un comando y transmite stdout/stderr línea por línea al logger."""
    _out(emit, f"$ {' '.join(cmd)}  (cwd={cwd})")
    popen_kwargs = _popen_kwargs()
    p = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        shell=False,
        **popen_kwargs,
    )
    out = p.communicate()[0]
    for ln in out.splitlines():
        _out(emit, ln.rstrip())
    rc = p.wait()
    _out(emit, f"[rc={rc}] {' '.join(cmd)}")
    return rc, out


def _run_quiet(cmd: List[str], cwd: Path) -> subprocess.CompletedProcess:
    popen_kwargs = _popen_kwargs()
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        shell=False,
        **popen_kwargs,
    )


def _last_nonempty(text: str) -> str:
    for line in reversed((text or "").splitlines()):
        line = line.strip()
        if line:
            return line
    return ""


def _current_branch_name(path: Path) -> Optional[str]:
    try:
        res = _run_quiet(["git", "rev-parse", "--abbrev-ref", "HEAD"], path)
    except Exception:
        return None
    if res.returncode != 0:
        return None
    return _last_nonempty(res.stdout)


def _branch_exists_local(path: Path, branch: str) -> bool:
    try:
        res = _run_quiet(["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"], path)
    except Exception:
        return False
    return res.returncode == 0


def _branch_exists_remote(path: Path, branch: str) -> bool:
    try:
        res = _run_quiet(["git", "ls-remote", "--exit-code", "--heads", "origin", branch], path)
    except Exception:
        return False
    return res.returncode == 0


def _switch_branch_with_fallback(path: Path, branch: str, emit=None) -> Tuple[bool, str]:
    rc, out = _run(["git", "switch", branch], path, emit=emit)
    if rc == 0:
        return True, "switch"
    rc2, out2 = _run(["git", "checkout", branch], path, emit=emit)
    if rc2 == 0:
        return True, "checkout"
    reason = _last_nonempty(out2) or _last_nonempty(out) or "Error desconocido"
    return False, reason


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
    cfg,
    gkey,
    pkey,
    version: str,
    create_qa: bool = False,
    version_files_override=None,
    repos_no_change=None,
    emit=None,
    only_modules=None,
) -> bool:
    """Shim que replica la firma de ``git_tasks.create_version_branches``.

    La versión "local" no soporta sobrescritura de archivos ni commits
    automáticos, pero sí respeta la convención de prefijar con ``v`` la
    rama base y, opcionalmente, crear la rama ``*_QA``.
    """

    _ = version_files_override, repos_no_change  # compatibilidad de firma

    ver = (version or "").strip()
    if not ver:
        _out(emit, "❌ Versión vacía.")
        raise RuntimeError("Versión vacía.")

    branch_base = f"v{ver}"
    ok_base = create_branches_local(
        cfg, gkey, pkey, branch_base, emit=emit, only_modules=only_modules
    )
    ok_qa = True
    if ok_base and create_qa:
        ok_qa = create_branches_local(
            cfg,
            gkey,
            pkey,
            f"{branch_base}_QA",
            emit=emit,
            only_modules=only_modules,
        )
    return ok_base and ok_qa


def switch_branch(
    cfg, gkey, pkey, name: str, emit=None, only_modules=None
) -> bool:
    bname = (name or "").strip()
    if not bname:
        _out(emit, "❌ Nombre de rama vacío.")
        raise RuntimeError("Nombre de rama vacío.")

    repos = _discover_repos(cfg, gkey, pkey, only_modules, emit=emit)
    ok_all = True
    abort_remaining = False
    switched: List[Tuple[str, Path, str]] = []
    failures: List[Tuple[str, str]] = []
    for mname, mpath in repos:
        if abort_remaining:
            _out(emit, f"[{mname}] ⏭️ Omitido por error previo")
            continue
        if not mpath.exists():
            _out(emit, f"[{mname}] ⚠️ Ruta no existe: {mpath}")
            ok_all = False
            failures.append((mname, "ruta inexistente"))
            continue
        if not _is_git_repo(mpath, emit=emit):
            _out(emit, f"[{mname}] ⚠️ No es repo Git: {mpath}")
            ok_all = False
            failures.append((mname, "no es un repositorio Git"))
            continue

        current = _current_branch_name(mpath)
        if not current:
            _out(emit, f"[{mname}] ❌ No se pudo determinar la rama actual.")
            ok_all = False
            failures.append((mname, "rama actual desconocida"))
            abort_remaining = True
            continue

        ok, detail = _switch_branch_with_fallback(mpath, bname, emit=emit)
        if ok:
            verb = "switch" if detail == "switch" else "switch con checkout"
            _out(emit, f"[{mname}] ✅ {verb}: {bname}")
            switched.append((mname, mpath, current))
        else:
            _out(emit, f"[{mname}] ❌ No se pudo hacer switch a '{bname}': {detail}")
            ok_all = False
            failures.append((mname, detail))
            abort_remaining = True

    if not ok_all and switched:
        _out(emit, "⚠️ Revirtiendo módulos al estado previo por errores en switch.")
        for mname, mpath, prev in reversed(switched):
            _out(emit, f"[{mname}] ↩ regresar a {prev}")
            ok_back, detail = _switch_branch_with_fallback(mpath, prev, emit=emit)
            if ok_back:
                verb = "switch" if detail == "switch" else "switch con checkout"
                _out(emit, f"[{mname}] ✅ {verb}: {prev}")
            else:
                _out(emit, f"[{mname}] ❌ No se pudo regresar a '{prev}': {detail}")

    if ok_all:
        idx = load_index()
        rec = _get_record(idx, gkey, pkey, bname)
        rec.exists_local = True
        rec.last_action = "switch"
        rec.last_updated_by = _current_user()
        upsert(rec, idx, action="switch")
        return True

    if failures:
        _out(emit, "❌ Switch global incompleto. Detalles:")
        for mname, reason in failures:
            _out(emit, f"   - {mname}: {reason}")
    return False


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


def merge_into_current_branch(
    cfg,
    gkey,
    pkey,
    source: str,
    push: bool,
    emit=None,
    only_modules: Optional[Iterable[str]] = None,
) -> bool:
    branch = (source or "").strip()
    if not branch:
        _out(emit, "❌ Nombre de rama de origen vacío en merge.")
        raise RuntimeError("Nombre de rama de origen vacío en merge.")

    repos = _discover_repos(cfg, gkey, pkey, only_modules, emit=emit)
    ok_all = True
    issues: List[Tuple[str, str]] = []
    for mname, mpath in repos:
        if not mpath.exists():
            _out(emit, f"[{mname}] ⚠️ Ruta no existe: {mpath}")
            ok_all = False
            issues.append((mname, "ruta inexistente"))
            continue
        if not _is_git_repo(mpath, emit=emit):
            _out(emit, f"[{mname}] ⚠️ No es repo Git: {mpath}")
            ok_all = False
            issues.append((mname, "no es un repositorio Git"))
            continue

        current = _current_branch_name(mpath) or "?"
        _out(emit, f"[{mname}] ▶ merge '{branch}' sobre '{current}'")

        exists_local = _branch_exists_local(mpath, branch)
        exists_remote = _branch_exists_remote(mpath, branch)
        if not exists_local and not exists_remote:
            _out(emit, f"[{mname}] ❌ La rama '{branch}' no existe (local ni origin)")
            ok_all = False
            issues.append((mname, "rama inexistente"))
            continue

        merge_target = branch
        if exists_remote:
            _run(["git", "fetch", "origin", branch], mpath, emit=emit)
            if not exists_local:
                merge_target = f"origin/{branch}"

        rc, out = _run(["git", "merge", "--no-edit", merge_target], mpath, emit=emit)
        if rc != 0:
            reason = _last_nonempty(out) or "conflictos durante el merge"
            _out(emit, f"[{mname}] ❌ Merge con conflictos: {reason}")
            ok_all = False
            issues.append((mname, reason))
            continue

        _out(emit, f"[{mname}] ✅ Merge completado")
        if push:
            rc_push, out_push = _run(["git", "push"], mpath, emit=emit)
            if rc_push != 0:
                reason = _last_nonempty(out_push) or "push falló"
                _out(emit, f"[{mname}] ⚠️ Push falló después del merge: {reason}")
                ok_all = False
                issues.append((mname, f"push falló: {reason}"))
            else:
                _out(emit, f"[{mname}] ☁️ Push origin")

    if ok_all:
        return True

    if issues:
        _out(emit, "❌ Merge global incompleto. Detalles:")
        for mname, reason in issues:
            _out(emit, f"   - {mname}: {reason}")
    return False
