# buildtool/core/tasks.py
from __future__ import annotations
from .config import Config
from .maven import run_maven
from .copier import copy_artifacts
from .pipeline_history import PipelineHistory
from .session import current_username
import pathlib, shutil, tempfile, os, threading, getpass
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

# Carpeta de locks por proceso para módulos con run_once
_RUNONCE_DIR = pathlib.Path(tempfile.gettempdir()) / f"forgebuild_runonce_{os.getpid()}"
_RUNONCE_DIR.mkdir(parents=True, exist_ok=True)


def _locate_project(cfg: Config, project_key: str, group_key: str | None):
    if group_key:
        grp = next((g for g in cfg.groups if g.key == group_key), None)
        if grp:
            project = next((p for p in grp.projects if p.key == project_key), None)
            if project:
                return grp, project
    for grp in cfg.groups:
        project = next((p for p in grp.projects if p.key == project_key), None)
        if project:
            return grp, project
    return None, None


def _resolve_repo_path(cfg: Config, project_key: str, group_key: str | None,
                       project_repo: str | None, project_workspace: str | None) -> pathlib.Path:
    if group_key:
        grp = next((g for g in cfg.groups if g.key == group_key), None)
        if grp and project_repo and project_repo in grp.repos:
            return pathlib.Path(grp.repos[project_repo])
    if project_workspace and project_workspace in cfg.paths.workspaces:
        return pathlib.Path(cfg.paths.workspaces[project_workspace])
    if project_repo and project_repo in cfg.paths.workspaces:
        return pathlib.Path(cfg.paths.workspaces[project_repo])
    return pathlib.Path(project_workspace or project_repo or ".")

def _resolve_output_base(cfg: Config, project_key: str, profile: str, group_key: str | None) -> pathlib.Path:
    if group_key:
        grp = next((g for g in cfg.groups if g.key == group_key), None)
        if grp and grp.output_base:
            return pathlib.Path(grp.output_base) / project_key / profile
    return pathlib.Path(cfg.paths.output_base) / project_key / profile

def _ensure(dir_path: pathlib.Path) -> pathlib.Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path

def _pick_artifact(target_dir: pathlib.Path, patterns: list[str]) -> pathlib.Path | None:
    for pat in patterns:
        matches = sorted(target_dir.glob(pat))
        if matches:
            return matches[0]
    return None

def build_project_for_profile(
    cfg: Config,
    project_key: str,
    profile: str,
    include_optional: bool,
    log_cb=print,
    group_key: str | None=None,
    modules_filter: set[str] | None = None,
    cancel_event: Event | None = None,
) -> bool:
    # localizar proyecto
    grp, project = _locate_project(cfg, project_key, group_key)
    if not project:
        raise KeyError(f"Proyecto '{project_key}' no encontrado en la configuración")
    group_key = grp.key if grp else None

    repo_path = _resolve_repo_path(cfg, project_key, group_key,
                                   getattr(project, "repo", None), getattr(project, "workspace", None))
    output_base = _resolve_output_base(cfg, project_key, profile, group_key)

    cleaned_destinations: set[pathlib.Path] = set()

    def _prepare_destination(dest: pathlib.Path, *, create: bool) -> pathlib.Path:
        dest = pathlib.Path(dest)
        if create:
            dest.mkdir(parents=True, exist_ok=True)
        elif not dest.exists():
            return dest
        resolved = dest.resolve()
        if resolved in cleaned_destinations:
            return resolved
        for child in list(resolved.iterdir()):
            try:
                if child.is_dir() and not child.is_symlink():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except FileNotFoundError:
                continue
        cleaned_destinations.add(resolved)
        return resolved

    def _module_destinations(mod) -> set[pathlib.Path]:
        dests: set[pathlib.Path] = set()
        if getattr(mod, "copy_to_profile_war", False):
            if getattr(mod, "copy_to_root", False):
                dests.add(output_base)
            else:
                dests.add(output_base / "war")
        if getattr(mod, "copy_to_profile_ui", False):
            if getattr(mod, "copy_to_root", False):
                dests.add(output_base)
            else:
                dests.add(output_base / "ui-ellis")
        if getattr(mod, "copy_to_subfolder", None):
            dests.add(output_base / mod.copy_to_subfolder)
        if getattr(mod, "rename_jar_to", None):
            sub = getattr(mod, "copy_to_subfolder", None)
            dests.add(output_base / (sub or ""))
        if getattr(mod, "select_pattern", None) and getattr(mod, "rename_jar_to", None) and not dests:
            dests.add(output_base)
        return dests

    def _clean_if_skipped(dests: set[pathlib.Path]):
        for dest in dests:
            _prepare_destination(dest, create=False)

    # --- ORDEN: primero los módulos "commons": run_once + no_profile (prioridad)
    def _priority(m):
        return 0 if getattr(m, "run_once", False) and getattr(m, "no_profile", False) else 1
    modules_in_order = sorted(project.modules, key=_priority)

    for mod in modules_in_order:
        module_dests = _module_destinations(mod)

        # Filtro por módulos seleccionados
        if modules_filter and mod.name not in modules_filter:
            _clean_if_skipped(module_dests)
            continue

        if cancel_event and cancel_event.is_set():
            log_cb(f"[{profile}] Cancelado por el usuario antes de ejecutar {mod.name}.")
            return False

        if getattr(mod, "optional", False) and not include_optional:
            _clean_if_skipped(module_dests)
            log_cb(f"[{profile}] Saltando módulo opcional: {mod.name}")
            continue
        if getattr(mod, "only_if_profile_equals", None) and mod.only_if_profile_equals != profile:
            _clean_if_skipped(module_dests)
            log_cb(f"[{profile}] Saltando {mod.name} (solo para perfil {mod.only_if_profile_equals})")
            continue

        module_path = repo_path / mod.path
        if not module_path.exists():
            log_cb(f"[{profile}] ADVERTENCIA: ruta de módulo no existe: {module_path}")

        effective_profile = getattr(mod, "profile_override", None) or profile
        profile_to_pass = None if getattr(mod, "no_profile", False) else effective_profile

        separate = (getattr(project, "execution_mode", None) or getattr(cfg, "default_execution_mode", "integrated")) == "separate_windows"

        # run_once: compilar solo una vez por sesión
        lock_file = _RUNONCE_DIR / f"{project_key}__{mod.name}.lock"
        if getattr(mod, "run_once", False) and lock_file.exists():
            log_cb(f"[{profile}] {mod.name}: run_once, reutilizando artefactos de esta sesión.")
        else:
            ret = run_maven(
                str(module_path),
                mod.goals,
                profile=profile_to_pass,
                log_cb=lambda s: log_cb(f"[{profile}] {s}"),
                separate_window=separate,
                cancel_event=cancel_event,
            )
            if cancel_event and cancel_event.is_set():
                log_cb(f"[{profile}] {mod.name}: cancelado por el usuario.")
                return False
            if ret != 0:
                log_cb(f"[{profile}] ERROR: Maven falló en {mod.name} (código {ret}). Abortando perfil.")
                return False
            if getattr(mod, "run_once", False):
                try:
                    lock_file.write_text("ok", encoding="utf-8")
                except Exception:
                    pass

        # --- Copia de artefactos ---
        target_dir = module_path / "target"

        # Caso selectivo: tomar UN archivo exacto y renombrarlo
        if getattr(mod, "select_pattern", None) and getattr(mod, "rename_jar_to", None):
            # --- Destino preferente para selectivo ---
            if getattr(mod, "copy_to_subfolder", None):
                dest_dir = _prepare_destination(output_base / mod.copy_to_subfolder, create=True)
            elif getattr(mod, "copy_to_profile_ui", False):
                dest_dir = (_prepare_destination(output_base, create=True)
                            if getattr(mod, "copy_to_root", False)
                            else _prepare_destination(output_base / "ui-ellis", create=True))
            elif getattr(mod, "copy_to_profile_war", False):
                dest_dir = (_prepare_destination(output_base, create=True)
                            if getattr(mod, "copy_to_root", False)
                            else _prepare_destination(output_base / "war", create=True))
            else:
                dest_dir = _prepare_destination(output_base, create=True)


            src = _pick_artifact(target_dir, [mod.select_pattern])
            if not src:
                log_cb(f"[{profile}] ADVERTENCIA: no se encontró patrón {mod.select_pattern} en {target_dir}")
            else:
                shutil.copy2(src, dest_dir / mod.rename_jar_to)
                log_cb(f"[{profile}] Copiado único: {src.name} -> {dest_dir/mod.rename_jar_to}")
            continue  # no copiar nada más

        # Flujo clásico
        
        if getattr(mod, "copy_to_profile_war", False):
            dest = (_prepare_destination(output_base, create=True)
                    if getattr(mod, "copy_to_root", False)
                    else _prepare_destination(output_base / "war", create=True))
            copy_artifacts(
                target_dir,
                ["*.war"],
                dest,
                log_cb=lambda s: log_cb(f"[{profile}] {s}"),
                recursive=False,
                cancel_event=cancel_event,
            )
        if getattr(mod, "copy_to_profile_ui", False):
            dest = (_prepare_destination(output_base, create=True)
                    if getattr(mod, "copy_to_root", False)
                    else _prepare_destination(output_base / "ui-ellis", create=True))
            copy_artifacts(
                target_dir,
                ["*.jar"],
                dest,
                log_cb=lambda s: log_cb(f"[{profile}] {s}"),
                recursive=False,
                cancel_event=cancel_event,
            )
        if getattr(mod, "copy_to_subfolder", None):
            dest = _prepare_destination(output_base / mod.copy_to_subfolder, create=True)
            copy_artifacts(
                target_dir,
                ["*.jar","*.war"],
                dest,
                log_cb=lambda s: log_cb(f"[{profile}] {s}"),
                recursive=False,
                cancel_event=cancel_event,
            )

        if getattr(mod, "rename_jar_to", None):
            src = _pick_artifact(target_dir, ["*-jar-with-dependencies.jar", "*.jar", "*.war"])
            if src:
                dest_dir = _prepare_destination(output_base / (mod.copy_to_subfolder or ""), create=True)
                shutil.copy2(src, dest_dir / mod.rename_jar_to)
                log_cb(f"[{profile}] Renombrado {src.name} -> {mod.rename_jar_to} en {dest_dir}")

    return True

# ------------------ NUEVO: Scheduler perfiles en serie, módulos en paralelo ------------------

def build_project_scheduled(
    cfg: Config,
    project_key: str,
    profiles: list[str],
    modules_filter: set[str] | None,
    log_cb=print,
    group_key: str | None=None,
    max_workers: int | None = None,
    cancel_event: Event | None = None,
) -> bool:
    # localizar proyecto
    grp, project = _locate_project(cfg, project_key, group_key)
    if not project:
        raise KeyError(f"Proyecto '{project_key}' no encontrado en la configuración")
    group_key = grp.key if grp else None

    all_mods = [m for m in project.modules if (not modules_filter or m.name in modules_filter)]
    cancel_event = cancel_event or Event()

    history = PipelineHistory()
    try:
        history_run_id = history.start_run(
            "build",
            user=current_username(getpass.getuser()),
            group_key=group_key,
            project_key=project_key,
            profiles=profiles,
            modules=[m.name for m in all_mods],
        )
    except Exception:
        history_run_id = None

    def _log(message: str) -> None:
        log_cb(message)
        if history_run_id:
            try:
                history.log_message(history_run_id, message)
            except Exception:
                pass

    def _finalize(result: bool, message: str | None = None) -> bool:
        if history_run_id:
            status = "success" if result else ("cancelled" if cancel_event.is_set() else "error")
            try:
                history.finish_run(history_run_id, status, message)
            except Exception:
                pass
        return result

    # commons (run_once + no_profile) una sola vez
    commons = [m.name for m in all_mods if getattr(m, "run_once", False) and getattr(m, "no_profile", False)]

    success = True
    error_reported = False

    if commons:
        first = profiles[0]
        commons_ok = build_project_for_profile(
            cfg,
            project_key,
            first,
            True,
            log_cb=_log,
            group_key=group_key,
            modules_filter=set(commons),
            cancel_event=cancel_event,
        )
        if not commons_ok:
            success = False
            if not cancel_event.is_set():
                cancel_event.set()
            if not error_reported:
                _log("<< ERROR: Falló la fase común. Deteniendo el pipeline.")
                error_reported = True
            return _finalize(False, "Falló la fase común.")

    # módulos que se bloquean entre perfiles
    import threading
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed
    mod_locks = defaultdict(threading.Lock)
    serial_mods = {m.name for m in all_mods if getattr(m, "serial_across_profiles", False)}

    # tareas por perfil/módulo (excluye commons)
    tasks = []
    profile_pending: dict[str, set[str]] = {}

    for prof in profiles:
        _log(f"== Perfil: {prof} ==")
        pending: set[str] = set()
        for mod in all_mods:
            if mod.name in commons:
                continue
            tasks.append((prof, mod.name))
            pending.add(mod.name)
        if pending:
            profile_pending[prof] = pending

    if not tasks:
        return _finalize(True, "Sin tareas pendientes.")

    workers = max_workers or max(2, min(4, len(tasks)))

    def _run_one(profile: str, mod_name: str) -> str:
        if cancel_event.is_set():
            return "cancelled"

        if mod_name in serial_mods:
            with mod_locks[mod_name]:
                ok = build_project_for_profile(
                    cfg,
                    project_key,
                    profile,
                    True,
                    log_cb=_log,
                    group_key=group_key,
                    modules_filter={mod_name},
                    cancel_event=cancel_event,
                )
        else:
            ok = build_project_for_profile(
                cfg,
                project_key,
                profile,
                True,
                log_cb=_log,
                group_key=group_key,
                modules_filter={mod_name},
                cancel_event=cancel_event,
            )

        if ok:
            return "ok"

        if not cancel_event.is_set():
            cancel_event.set()
        return "error"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(_run_one, prof, mod): (prof, mod) for prof, mod in tasks}
        for f in as_completed(future_map):
            prof, mod = future_map[f]

            if cancel_event.is_set() and f.cancelled():
                continue

            try:
                result = f.result()
            except Exception as err:
                success = False
                if not cancel_event.is_set():
                    cancel_event.set()
                if not error_reported:
                    _log(f"[{prof}] << ERROR en {mod}: {err}")
                    _log("<< ERROR: Pipeline detenido por fallas.")
                    error_reported = True
                continue

            if result == "ok":
                pending = profile_pending.get(prof)
                if pending:
                    pending.discard(mod)
                    if not pending:
                        _log(f"[{prof}] >> Perfil completado.")
                continue

            if result == "cancelled":
                success = False
                continue

            if result == "error":
                success = False
                if not error_reported:
                    _log("<< ERROR: Pipeline detenido por fallas.")
                    error_reported = True

        if cancel_event.is_set():
            for fut in future_map:
                fut.cancel()

    if cancel_event.is_set() and not error_reported and not success:
        _log("<< Pipeline cancelado por el usuario.")

    result = success and not cancel_event.is_set()
    if result:
        summary = "Pipeline completado."
    elif cancel_event.is_set():
        summary = "Pipeline cancelado por el usuario."
    else:
        summary = "Pipeline con errores."
    return _finalize(result, summary)


# ------------------ Deploy ------------------

def deploy_profiles_scheduled(
    cfg: Config,
    project_key: str,
    profiles: list[str],
    profile_targets: dict[str, str],
    version: str,
    *,
    log_cb=print,
    group_key: str | None = None,
    hotfix: bool = False,
    cancel_event: Event | None = None,
) -> bool:
    cancel_event = cancel_event or Event()

    grp, project = _locate_project(cfg, project_key, group_key)
    if not project:
        raise KeyError(f"Proyecto '{project_key}' no encontrado en la configuración")
    group_key = grp.key if grp else group_key

    history = PipelineHistory()
    try:
        history_run_id = history.start_run(
            "deploy",
            user=current_username(getpass.getuser()),
            group_key=group_key,
            project_key=project_key,
            profiles=profiles,
            modules=[],
            version=version,
            hotfix=hotfix,
        )
    except Exception:
        history_run_id = None

    def _log(message: str) -> None:
        log_cb(message)
        if history_run_id:
            try:
                history.log_message(history_run_id, message)
            except Exception:
                pass

    def _finalize(result: bool, message: str | None = None) -> bool:
        if history_run_id:
            status = "success" if result else ("cancelled" if cancel_event.is_set() else "error")
            try:
                history.finish_run(history_run_id, status, message)
            except Exception:
                pass
        return result

    success = True
    error_reported = False

    for prof in profiles:
        if cancel_event.is_set():
            success = False
            break

        _log(f"== Perfil: {prof} ==")

        target_name = profile_targets.get(prof)
        if not target_name:
            success = False
            if not error_reported:
                _log(f"[{prof}] << ERROR: No hay destino configurado.")
                _log("<< ERROR: Pipeline detenido por fallas.")
                error_reported = True
            if not cancel_event.is_set():
                cancel_event.set()
            break

        try:
            ok = deploy_version(
                cfg,
                project_key,
                prof,
                version,
                target_name,
                log_cb=_log,
                group_key=group_key,
                hotfix=hotfix,
                cancel_event=cancel_event,
            )
        except Exception as err:
            success = False
            if not cancel_event.is_set():
                cancel_event.set()
            if not error_reported:
                _log(f"[{prof}] << ERROR: {err}")
                _log("<< ERROR: Pipeline detenido por fallas.")
                error_reported = True
            continue

        if cancel_event.is_set():
            success = False
            break

        if ok:
            _log(f"[{prof}] >> Perfil completado.")
            continue

        success = False
        if not cancel_event.is_set():
            cancel_event.set()
        if not error_reported:
            _log("<< ERROR: Pipeline detenido por fallas.")
            error_reported = True
        break

    if cancel_event.is_set() and not error_reported and not success:
        _log("<< Pipeline cancelado por el usuario.")

    result = success and not cancel_event.is_set()
    if result:
        summary = "Deploy completado."
    elif cancel_event.is_set():
        summary = "Deploy cancelado por el usuario."
    else:
        summary = "Deploy con errores."
    return _finalize(result, summary)

def build_project(
    cfg: Config,
    project_key: str,
    profile: str,
    include_optional: bool,
    log_cb=print,
    group_key: str | None=None,
    modules_filter: set[str] | None = None,
) -> bool:
    return build_project_for_profile(
        cfg, project_key, profile, include_optional, log_cb=log_cb,
        group_key=group_key, modules_filter=modules_filter
    )

def deploy_version(
    cfg: Config,
    project_key: str,
    profile: str,
    version: str,
    target_name: str,
    log_cb=print,
    group_key: str | None=None,
    hotfix: bool = False,
    cancel_event: Event | None = None,
) -> bool:
    """Copia los artefactos del build al target (normal u hotfix)."""
    # --- resolver target ---
    grp, project = _locate_project(cfg, project_key, group_key)
    if not project:
        raise KeyError(f"Proyecto '{project_key}' no encontrado en la configuración")
    group_key = grp.key if grp else group_key
    tgt = None
    if grp:
        tgt = next((t for t in (grp.deploy_targets or []) if t.name == target_name), None)
    if tgt is None:
        raise ValueError(f"Target '{target_name}' no existe.")

    if tgt.project_key != project_key:
        raise ValueError(f"Target '{target_name}' es para proyecto '{tgt.project_key}', no '{project_key}'.")
    if profile not in (tgt.profiles or []):
        raise ValueError(f"Target '{target_name}' no acepta el perfil '{profile}'.")

    # --- escoger plantilla ---
    template = (tgt.hotfix_path_template if (hotfix and getattr(tgt, "hotfix_path_template", None))
                else tgt.path_template)

    # Normaliza y asegura que la versión quede incluida aunque el template no tenga {version}
    if "{version}" in template:
        formatted = template.format(version=version)
        dst = pathlib.Path(formatted)
    else:
        # si el template ya acaba con separador, ignorarlo y añadir versión como subcarpeta
        dst = pathlib.Path(template) / version

    # --- origen ---
    src_base = _resolve_output_base(cfg, project_key, profile, group_key)
    if not src_base.exists():
        src_base = pathlib.Path(cfg.paths.output_base) / project_key / profile
    if not src_base.exists():
        raise FileNotFoundError(f"No existe la carpeta de build: {src_base}")

    # --- crear destino y copiar ---
    dst.mkdir(parents=True, exist_ok=True)
    if cancel_event and cancel_event.is_set():
        log_cb(f"[{profile}] Cancelado por el usuario antes de iniciar el deploy.")
        return False

    log_cb(f"[{profile}] Deploy -> {dst}")

    copy_artifacts(
        src_base,
        ["*"],
        dst,
        log_cb=lambda s: log_cb(f"[{profile}] {s}"),
        cancel_event=cancel_event,
    )
    if cancel_event and cancel_event.is_set():
        log_cb(f"[{profile}] Deploy cancelado durante la copia de artefactos.")
        return False

    for sub in src_base.iterdir():
        if cancel_event and cancel_event.is_set():
            log_cb(f"[{profile}] Deploy cancelado por el usuario.")
            return False
        if sub.is_dir():
            sub_dst = dst / sub.name
            sub_dst.mkdir(parents=True, exist_ok=True)
            copy_artifacts(
                sub,
                ["*"],
                sub_dst,
                log_cb=lambda s: log_cb(f"[{profile}] {s}"),
                recursive=True,
                cancel_event=cancel_event,
            )
            if cancel_event and cancel_event.is_set():
                log_cb(f"[{profile}] Deploy cancelado durante la copia de {sub.name}.")
                return False

    return True

