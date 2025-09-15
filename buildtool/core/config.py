
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
import yaml, pathlib, os

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
    projects: List[Project] = Field(default_factory=list)  # legacy
    profiles: List[str] = Field(default_factory=list)      # legacy
    default_execution_mode: str = "integrated"
    deploy_targets: List[DeployTarget] = Field(default_factory=list)  # legacy
    groups: List[Group] = Field(default_factory=list)

_PACKAGE_CFG_FILE = pathlib.Path(__file__).parent.parent / "data" / "config.yaml"


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

def load_config() -> Config:
    cfg_path = _cfg_file()
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return Config(**data)

    if _PACKAGE_CFG_FILE.exists():
        with open(_PACKAGE_CFG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        cfg = Config(**data)
        try:
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(_model_to_dict(cfg), f, sort_keys=False, allow_unicode=True)
        except Exception:
            pass
        return cfg

    # default
    return Config(paths=Paths(workspaces={}, output_base="", nas_dir=""))

def save_config(cfg: Config) -> str:
    # v1 usa .dict(), v2 usa .model_dump()
    data = _model_to_dict(cfg)
    cfg_path = _cfg_file()
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
    return str(cfg_path)

