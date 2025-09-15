"""Core helpers for the ForgeBuild application."""

from importlib import import_module as _import_module
from types import ModuleType as _ModuleType

__all__ = [
    "errguard",
]

# Lazy re-export so ``from buildtool.core import errguard`` keeps working
# without importing every submodule eagerly (which helps PyInstaller's
# analysis and keeps runtime startup minimal).

def __getattr__(name: str) -> _ModuleType:
    if name in __all__:
        module = _import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
