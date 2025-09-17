"""Helper utilities to load SVG icons bundled with the UI."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from PySide6.QtGui import QIcon

_ICON_DIR = Path(__file__).resolve().parent / "icons"


@lru_cache(maxsize=64)
def get_icon(name: str) -> QIcon:
    """Return a :class:`QIcon` for the given SVG name.

    Parameters
    ----------
    name:
        Base filename (without extension) located under ``buildtool/ui/icons``.
    """
    path = _ICON_DIR / f"{name}.svg"
    if not path.exists():
        return QIcon()
    return QIcon(str(path))


__all__ = ["get_icon"]
