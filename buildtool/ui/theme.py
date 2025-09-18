"""Theme helpers for applying ForgeBuild styles."""
from __future__ import annotations

from pathlib import Path
from typing import Literal

from PySide6.QtCore import Qt
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import QApplication

ThemeMode = Literal["dark", "light", "auto"]

_THEME_FILES = {
    "dark": "theme.qss",
    "light": "theme_light.qss",
}


def _detect_system_mode(app: QApplication) -> str:
    hints = app.styleHints()
    mode = None
    if hasattr(hints, "colorScheme"):
        try:
            scheme = hints.colorScheme()
        except Exception:
            scheme = None
        if scheme == Qt.ColorScheme.Dark:
            mode = "dark"
        elif scheme == Qt.ColorScheme.Light:
            mode = "light"
    if mode:
        return mode

    palette = app.palette()
    window = palette.color(QPalette.Window)
    return "dark" if window.value() < 128 else "light"


def apply_theme(mode: ThemeMode = "auto") -> str:
    """Apply the requested theme to the current application."""

    app = QApplication.instance()
    if app is None:
        return "none"

    resolved = mode if mode != "auto" else _detect_system_mode(app)
    current = getattr(app, "_forgebuild_theme_mode", None)
    if current == resolved:
        return resolved

    theme_dir = Path(__file__).resolve().parent
    filename = _THEME_FILES.get(resolved, _THEME_FILES["dark"])
    path = theme_dir / filename
    if not path.exists():
        path = theme_dir / _THEME_FILES["dark"]
        resolved = "dark"

    try:
        data = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return resolved

    app.setStyleSheet(data)
    setattr(app, "_forgebuild_theme_mode", resolved)
    return resolved


__all__ = ["apply_theme", "ThemeMode"]
