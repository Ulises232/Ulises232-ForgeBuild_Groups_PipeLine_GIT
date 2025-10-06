
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Literal, Callable, Tuple
import yaml, pathlib, os, sys
from datetime import datetime

from .branch_history_db import BranchHistoryDB
from .config_store import ConfigStore
class PipelinePreset(BaseModel):
    name: str
    pipeline: Literal["build", "deploy"]
    group_key: Optional[str] = None
    project_key: Optional[str] = None
    profiles: List[str] = Field(default_factory=list)
    modules: List[str] = Field(default_factory=list)
    version: Optional[str] = None
    hotfix: Optional[bool] = None


class Paths(BaseModel):
    workspaces: Dict[str, str] = Field(default_factory=dict)
    output_base: str = ""
    nas_dir: str = ""

class Module(BaseModel):
    version_files: List[str] = Field(default_factory=list)  # archivos relativos al módulo para actualizar versión
    name: str
    path: str
    goals: List[str] = Field(default_factory=lambda: ["clean","package"])
    optional: bool = False
    profile_override: Optional[str] = None
    only_if_profile_equals: Optional[str] = None
    copy_to_profile_war: bool = False
    copy_to_profile_ui: bool = False
    copy_to_subfolder: Optional[str] = None
    rename_jar_to: Optional[str] = None
    no_profile: bool = False
    run_once: bool = False
    select_pattern: Optional[str] = None
    serial_across_profiles: bool = False
    copy_to_root: bool = False          # << NUEVO: copia a la raíz del perfil


class Project(BaseModel):
    key: str
    modules: List[Module]
    profiles: Optional[List[str]] = None
    execution_mode: Optional[str] = None  # integrated | separate_windows
    workspace: Optional[str] = None  # legacy
    repo: Optional[str] = None       # new

class DeployTarget(BaseModel):
    name: str
    project_key: str
    profiles: List[str]
    path_template: str
    hotfix_path_template: Optional[str] = None  # << NUEVO: ruta alternativa para hotfix

class Group(BaseModel):
    key: str
    repos: Dict[str, str]
    output_base: str
    profiles: List[str] = Field(default_factory=list)
    projects: List[Project] = Field(default_factory=list)
    deploy_targets: List[DeployTarget] = Field(default_factory=list)

class Config(BaseModel):
    paths: Paths
    artifact_patterns: List[str] = Field(default_factory=lambda: ["*.war","*.jar"])
    default_execution_mode: str = "integrated"
    groups: List[Group] = Field(default_factory=list)
    environment: Dict[str, str] = Field(default_factory=dict)
    pipeline_presets: List[PipelinePreset] = Field(default_factory=list)
    max_build_workers: Optional[int] = None

_APPLIED_ENV_KEYS: set[str] = set()
_GROUPS_CACHE: Dict[Tuple[int, Optional[str]], List[Group]] = {}


def _package_data_dir() -> pathlib.Path:
    """Return the folder that contains bundled data, compatible with PyInstaller."""
    base = pathlib.Path(__file__).resolve().parent.parent
    if hasattr(sys, "_MEIPASS"):
        candidate = pathlib.Path(getattr(sys, "_MEIPASS")) / "buildtool" / "data"
        if candidate.exists():
            return candidate
    return base / "data"


_PACKAGE_CFG_FILE = _package_data_dir() / "config.yaml"


def _state_dir() -> pathlib.Path:
    base = os.environ.get("APPDATA")
    if base:
        return pathlib.Path(base) / "ForgeBuild"
    return pathlib.Path.home() / ".forgebuild"


def _cfg_file() -> pathlib.Path:
    return _state_dir() / "config.yaml"


def _model_to_dict(model) -> Dict:
    if hasattr(model, "dict"):
        return model.dict()
    return model.model_dump()


_CONFIG_REPO_FACTORY: Optional[Callable[[], BranchHistoryDB]] = None


def set_config_repo_factory(factory: Optional[Callable[[], BranchHistoryDB]]) -> None:
    """Permite inyectar un factory de repositorio para pruebas."""

    global _CONFIG_REPO_FACTORY
    _CONFIG_REPO_FACTORY = factory
    _GROUPS_CACHE.clear()


def _create_config_store() -> ConfigStore:
    repo: Optional[BranchHistoryDB] = None
    if _CONFIG_REPO_FACTORY is not None:
        repo = _CONFIG_REPO_FACTORY()
    else:
        try:
            repo = BranchHistoryDB()
        except Exception:
            repo = None
    if repo is not None:
        return ConfigStore(repo=repo)
    return ConfigStore()


def groups_for_user(cfg: Config, username: Optional[str] = None) -> List[Group]:
    """Return groups with overrides applied for the requested username."""

    if username:
        user = username
    else:
        try:
            from .session import current_username as _current_username

            user = _current_username("")
        except Exception:
            user = ""
    key = (id(cfg), user or None)
    cached = _GROUPS_CACHE.get(key)
    if cached is not None:
        return cached

    if not user:
        groups = list(cfg.groups or [])
    else:
        store = _create_config_store()
        try:
            groups = store.list_groups(username=user)
        except Exception:
            groups = list(cfg.groups or [])

    _GROUPS_CACHE[key] = groups
    return groups

def apply_environment(cfg: Config) -> None:
    """Apply configured environment variables to the current process."""
    global _APPLIED_ENV_KEYS
    env_map = dict(getattr(cfg, "environment", {}) or {})

    # Remove variables that were previously applied but no longer exist
    for key in list(_APPLIED_ENV_KEYS - set(env_map.keys())):
        os.environ.pop(key, None)

    for key, value in env_map.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)

    _APPLIED_ENV_KEYS = set(env_map.keys())



def load_config() -> Config:
    _GROUPS_CACHE.clear()
    cfg_path = _cfg_file()
    store = _create_config_store()
    legacy_groups_data = []
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        legacy_groups_data = data.get("groups") or []
        data.pop("groups", None)
        cfg = Config(**data)
        migrated = False
        if store.is_empty() and legacy_groups_data:
            try:
                cfg.groups = [Group(**item) for item in legacy_groups_data]
            except Exception:
                cfg.groups = []
            store.replace_groups(cfg.groups)
            store.save_metadata("yaml_migrated_at", datetime.utcnow().isoformat())
            migrated = True
        cfg.groups = store.list_groups()
        if migrated:
            save_config(cfg)
        apply_environment(cfg)
        return cfg

    if _PACKAGE_CFG_FILE.exists():
        with open(_PACKAGE_CFG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        legacy_groups_data = data.get("groups") or []
        data.pop("groups", None)
        cfg = Config(**data)
        migrated = False
        if store.is_empty() and legacy_groups_data:
            try:
                cfg.groups = [Group(**item) for item in legacy_groups_data]
            except Exception:
                cfg.groups = []
            store.replace_groups(cfg.groups)
            store.save_metadata("yaml_migrated_at", datetime.utcnow().isoformat())
            migrated = True
        cfg.groups = store.list_groups()
        try:
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            data_to_save = _model_to_dict(cfg)
            data_to_save.pop("groups", None)
            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(data_to_save, f, sort_keys=False, allow_unicode=True)
        except Exception:
            pass
        if migrated:
            save_config(cfg)
        apply_environment(cfg)
        return cfg

    # default
    cfg = Config(paths=Paths(workspaces={}, output_base="", nas_dir=""))
    cfg.groups = store.list_groups()
    apply_environment(cfg)
    return cfg

def save_config(cfg: Config) -> str:
    # v1 usa .dict(), v2 usa .model_dump()
    store = _create_config_store()
    store.replace_groups(cfg.groups or [])
    _GROUPS_CACHE.clear()
    data = _model_to_dict(cfg)
    data.pop("groups", None)
    cfg_path = _cfg_file()
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
    apply_environment(cfg)
    return str(cfg_path)

