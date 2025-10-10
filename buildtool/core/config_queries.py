"""Utilidades de consulta sobre la configuración actual."""
from __future__ import annotations

from collections.abc import Iterator
from typing import Optional, Tuple, Dict

from .config import Config, Group, Project, Module, DeployTarget, groups_for_user


def iter_groups(cfg: Config) -> Iterator[Group]:
    """Itera los grupos configurados, tolerando listas vacías o nulas."""

    for group in groups_for_user(cfg):
        if group is not None:
            yield group


def get_group(cfg: Config, group_key: str | None) -> Optional[Group]:
    """Obtiene un grupo por clave si existe."""

    if not group_key:
        return None
    return next((grp for grp in iter_groups(cfg) if grp.key == group_key), None)


def first_group_key(cfg: Config) -> Optional[str]:
    """Retorna la primera clave de grupo disponible."""

    first = next(iter_groups(cfg), None)
    return first.key if first else None


def iter_group_projects(
    cfg: Config, group_key: str | None = None
) -> Iterator[Tuple[Group, Project]]:
    """Itera parejas (grupo, proyecto) opcionalmente filtradas por grupo."""

    for group in iter_groups(cfg):
        if group_key and group.key != group_key:
            continue
        for project in getattr(group, "projects", None) or []:
            if project is not None:
                yield group, project


def find_project(
    cfg: Config, project_key: str | None, group_key: str | None = None
) -> Tuple[Optional[Group], Optional[Project]]:
    """Localiza un proyecto por clave, priorizando el grupo indicado."""

    if not project_key:
        return None, None

    group = get_group(cfg, group_key)
    if group:
        project = next(
            (
                proj
                for proj in getattr(group, "projects", None) or []
                if proj.key == project_key
            ),
            None,
        )
        if project:
            return group, project

    for grp, project in iter_group_projects(cfg):
        if project.key == project_key:
            return grp, project

    return None, None


def default_project_key(cfg: Config, group_key: str | None) -> Optional[str]:
    """Obtiene la clave del primer proyecto disponible para un grupo."""

    group = get_group(cfg, group_key)
    if not group:
        return None
    first = next((proj for proj in getattr(group, "projects", None) or []), None)
    return first.key if first else None


def iter_project_modules(
    cfg: Config, group_key: str | None, project_key: str | None
) -> Iterator[Module]:
    """Itera los módulos declarados para un proyecto concreto."""

    _, project = find_project(cfg, project_key, group_key)
    for module in getattr(project, "modules", None) or []:
        if module is not None:
            yield module


def project_module_names(
    cfg: Config, group_key: str | None, project_key: str | None
) -> list[str]:
    """Devuelve los nombres de módulos configurados para un proyecto."""

    return [module.name for module in iter_project_modules(cfg, group_key, project_key)]


def project_profiles(
    cfg: Config, group_key: str | None, project_key: str | None
) -> list[str]:
    """Obtiene los perfiles vigentes para un proyecto, con fallback al grupo."""

    group, project = find_project(cfg, project_key, group_key)
    if project and getattr(project, "profiles", None):
        return list(project.profiles)
    if group and getattr(group, "profiles", None):
        return list(group.profiles)
    return []


def iter_deploy_targets(
    cfg: Config, group_key: str | None = None, project_key: str | None = None
) -> Iterator[Tuple[Group, DeployTarget]]:
    """Itera los targets de despliegue aplicando filtros opcionales."""

    for group in iter_groups(cfg):
        if group_key and group.key != group_key:
            continue
        for target in getattr(group, "deploy_targets", None) or []:
            if target is None:
                continue
            if project_key and target.project_key != project_key:
                continue
            yield group, target


def profile_target_map(
    cfg: Config, group_key: str | None, project_key: str | None
) -> Dict[str, str]:
    """Construye un mapa perfil -> nombre de target para el proyecto indicado."""

    mapping: dict[str, str] = {}
    for _, target in iter_deploy_targets(cfg, group_key, project_key):
        for profile in getattr(target, "profiles", None) or []:
            mapping.setdefault(profile, target.name)
    return mapping
