# buildtool/core/git_tasks_local.py
# Implementación con impresión a consola/emit SIEMPRE (sin depender de UI).
# - Escanea módulos por cfg (groups/projects/modules) y/o por filesystem (.git).
# - Ejecuta por cada repo detectado y muestra comandos + cwd + rc.
from __future__ import annotations
from pathlib import Path
from typing import Optional, Iterable, Tuple, List, Iterator, Dict, Any, Set
import os
import subprocess
import getpass
import re

from buildtool.core.branch_store import (
    BranchRecord,
    load_index,
    upsert,
    remove,
    record_activity,
)
from buildtool.core.git_console_trace import clog
from buildtool.core.git_tasks import _iter_modules as _iter_modules_cfg

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
    """Devuelve True si path está dentro de un repo git sin ejecutar comandos."""

    try:
        current = Path(path).resolve(strict=False)
    except Exception:
        return False

    # Camina hacia arriba buscando indicadores de repositorio.
    visited: Set[Path] = set()
    while True:
        if current in visited:
            break
        visited.add(current)

        dot_git = current / ".git"
        if dot_git.is_dir():
            return True
        if dot_git.is_file():
            try:
                content = dot_git.read_text(encoding="utf-8", errors="ignore").strip()
            except Exception:
                content = ""
            if content.lower().startswith("gitdir:"):
                target = content.split(":", 1)[1].strip()
                gitdir = (current / target).resolve(strict=False)
                if gitdir.exists():
                    return True

        # Repos bare (sin carpeta .git) tienen HEAD + objects en la raíz.
        if (current / "HEAD").is_file() and (current / "objects").is_dir():
            return True

        parent = current.parent
        if parent == current:
            break
        current = parent

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

def _resolve_module_path(raw: Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve(strict=False)
    base = _get_herr_repo()
    if base:
        return (base / path).resolve(strict=False)
    return (Path.cwd() / path).resolve(strict=False)


def _iter_cfg_modules(
    cfg, gkey: Optional[str], pkey: Optional[str], only_modules: Optional[Iterable[str]]
) -> Iterator[Tuple[str, Path]]:
    """
    Itera módulos/repos declarados en la configuración y devuelve (nombre, path).
    Respeta la resolución de rutas del wizard (_resolve_repo_path) y aplica un
    fallback contra HERR_REPO o el cwd para rutas relativas.
    """

    filt: Set[str] = set(only_modules or [])
    iter_only = filt or None

    for _g, _p, module, module_path in _iter_modules_cfg(cfg, gkey, pkey, iter_only):
        name = (
            getattr(module, "name", None)
            or getattr(module, "key", None)
            or str(getattr(module, "path", "") or "")
        ) or "mod"
        yield name, _resolve_module_path(Path(module_path))

    groups = getattr(cfg, "groups", None) or []
    for g in groups:
        if gkey and getattr(g, "key", None) != gkey:
            continue
        repos = getattr(g, "repos", None) or {}
        for name, raw in repos.items():
            if filt and name not in filt:
                continue
            p = Path(os.path.expanduser(os.path.expandvars(str(raw))))
            yield name, (p if p.is_absolute() else _resolve_module_path(p))

    if not groups:
        projects = getattr(cfg, "projects", None) or []
        for proj in projects:
            if pkey and getattr(proj, "key", None) != pkey:
                continue
            modules = getattr(proj, "modules", None) or []
            if modules:
                continue
            name = getattr(proj, "key", None) or "root"
            if filt and name not in filt:
                continue
            rel = getattr(proj, "root", ".") or "."
            relp = Path(os.path.expanduser(os.path.expandvars(str(rel))))
            yield name, (relp if relp.is_absolute() else _resolve_module_path(relp))

def _discover_repos(cfg, gkey, pkey, only_modules, emit=None) -> List[Tuple[str, Path]]:
    """
    Devuelve una lista única de (nombre, path_repo_o_modulo),
    primero por CFG, luego por FS (a partir de HERR_REPO/cwd).
    """
    repos: List[Tuple[str, Path]] = []
    herr = os.environ.get("HERR_REPO", "")
    suffix = f" {herr}" if herr else ""
    _out(emit, f"== DESCUBRIR MÓDULOS/REPOS =={suffix}")
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


def _create_or_switch(branch: str, path: Path, emit=None) -> Tuple[bool, str]:
    """Crea o activa una rama en el repo especificado."""

    attempts = [
        (["git", "switch", "-c", branch], "create_switch"),
        (["git", "checkout", "-b", branch], "checkout_create"),
        (["git", "switch", branch], "switch"),
        (["git", "checkout", branch], "checkout"),
    ]
    last_reason = ""
    for cmd, label in attempts:
        rc, out = _run(cmd, path, emit=emit)
        if rc == 0:
            return True, label
        last_reason = _last_nonempty(out) or label
    return False, last_reason or "Error al crear/switch"


def _module_index(
    cfg, gkey: Optional[str], pkey: Optional[str]
) -> Dict[str, Any]:
    index: Dict[str, Any] = {}
    for _g, _p, module, _path in _iter_modules_cfg(cfg, gkey, pkey, None):
        name = (
            getattr(module, "name", None)
            or getattr(module, "key", None)
            or str(getattr(module, "path", "") or "")
        )
        if name and name not in index:
            index[name] = module
    return index


def _apply_version_to_file(
    file_path: Path, version: str, emit=None
) -> bool:
    try:
        original = file_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        _out(emit, f"    ! No se pudo leer {file_path.name}: {exc}")
        return False

    updated = original
    total_replacements = 0

    # web.xml style param-value entries
    updated, n = re.subn(
        r"(<param-value>\s*Versi[oó]n:\s*)(.*?)(</param-value>)",
        r"\g<1>" + version + r"\g<3>",
        updated,
        flags=re.IGNORECASE | re.DOTALL,
    )
    total_replacements += n

    # application.properties style key
    updated, n = re.subn(
        r"(?im)^(app\.version\s*=\s*)(.*)$",
        r"\g<1>" + version,
        updated,
    )
    total_replacements += n

    if total_replacements == 0:
        return False

    try:
        file_path.write_text(updated, encoding="utf-8")
    except Exception as exc:
        _out(emit, f"    ! No se pudo escribir {file_path.name}: {exc}")
        return False

    _out(
        emit,
        f"    * Versión {version} aplicada en {file_path.name} ({total_replacements} reemplazos)",
    )
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
        ok, detail = _create_or_switch(bname, mpath, emit=emit)
        if not ok:
            _out(emit, f"[{mname}] ❌ No se pudo crear/switch a '{bname}': {detail}")
            ok_all = False
        else:
            verb = "creada" if detail in {"create_switch", "checkout_create"} else "activada"
            _out(emit, f"[{mname}] ✅ rama {verb}: {bname}")
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
    """Crea ramas de versión locales y actualiza archivos declarados."""

    ver = (version or "").strip()
    if not ver:
        _out(emit, "❌ Versión vacía.")
        raise RuntimeError("Versión vacía.")

    repos = _discover_repos(cfg, gkey, pkey, only_modules, emit=emit)
    module_map = _module_index(cfg, gkey, pkey)
    overrides = dict(version_files_override or {})
    repos_no_change = set(repos_no_change or [])

    branch_base = f"v{ver}"
    branch_qa = f"{branch_base}_QA"

    ok_base = True
    ok_qa = True

    for mname, mpath in repos:
        if not mpath.exists():
            _out(emit, f"[{mname}] ⚠️ Ruta no existe: {mpath}")
            ok_base = False
            if create_qa:
                ok_qa = False
            continue
        if not _is_git_repo(mpath, emit=emit):
            _out(emit, f"[{mname}] ⚠️ No es repo Git: {mpath}")
            ok_base = False
            if create_qa:
                ok_qa = False
            continue

        _out(emit, f"[{mname}] ▶ preparar rama base {branch_base}")
        ok, detail = _create_or_switch(branch_base, mpath, emit=emit)
        if not ok:
            _out(emit, f"[{mname}] ❌ No se pudo preparar '{branch_base}': {detail}")
            ok_base = False
            if create_qa:
                ok_qa = False
            continue
        verb = "creada" if detail in {"create_switch", "checkout_create"} else "activada"
        _out(emit, f"[{mname}] ✅ rama base {verb}: {branch_base}")

        if mname in repos_no_change:
            _out(emit, f"[{mname}] ⏭️ Cambio de versión omitido (repos_no_change)")
        else:
            rel_files = list(overrides.get(mname) or [])
            if not rel_files:
                module = module_map.get(mname)
                rel_files = list(getattr(module, "version_files", []) or []) if module else []

            if rel_files:
                changed_any = False
                for rel in rel_files:
                    target = (mpath / rel).resolve(strict=False)
                    if not target.exists():
                        _out(emit, f"    ! No existe: {rel}")
                        continue
                    if _apply_version_to_file(target, ver, emit=emit):
                        changed_any = True
                if changed_any:
                    rc_add, out_add = _run(["git", "add", "--all"], mpath, emit=emit)
                    if rc_add != 0:
                        _out(emit, f"[{mname}] ❌ git add falló: {_last_nonempty(out_add)}")
                        ok_base = False
                        if create_qa:
                            ok_qa = False
                        continue
                    rc_commit, out_commit = _run(
                        ["git", "commit", "-m", f"cambio de versión a {ver}"],
                        mpath,
                        emit=emit,
                    )
                    if rc_commit != 0:
                        reason = _last_nonempty(out_commit) or "commit falló"
                        _out(emit, f"[{mname}] ❌ git commit falló: {reason}")
                        ok_base = False
                        if create_qa:
                            ok_qa = False
                        continue
                else:
                    _out(emit, f"[{mname}] (Sin cambios de versión que commitear)")
            else:
                _out(emit, f"[{mname}] (Sin archivos de versión configurados)")

        if create_qa:
            _out(emit, f"[{mname}] ▶ preparar rama QA {branch_qa}")
            ok_qa_repo, detail_qa = _create_or_switch(branch_qa, mpath, emit=emit)
            if not ok_qa_repo:
                _out(emit, f"[{mname}] ❌ No se pudo preparar '{branch_qa}': {detail_qa}")
                ok_qa = False
            else:
                verb = "creada" if detail_qa in {"create_switch", "checkout_create"} else "activada"
                _out(emit, f"[{mname}] ✅ rama QA {verb}: {branch_qa}")

    if ok_base:
        idx = load_index()
        rec = _get_record(idx, gkey, pkey, branch_base)
        rec.exists_local = True
        rec.last_action = "create_local"
        rec.last_updated_by = _current_user()
        upsert(rec, idx, action="create_local")
    if create_qa and ok_base and ok_qa:
        idx = load_index()
        rec = _get_record(idx, gkey, pkey, branch_qa)
        rec.exists_local = True
        rec.last_action = "create_local"
        rec.last_updated_by = _current_user()
        upsert(rec, idx, action="create_local")

    return ok_base and (ok_qa if create_qa else True)


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
