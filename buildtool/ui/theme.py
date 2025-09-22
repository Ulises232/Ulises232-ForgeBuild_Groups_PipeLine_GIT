"""Theme helpers for applying ForgeBuild styles."""
from __future__ import annotations

from importlib import import_module
from typing import Literal, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import QApplication

ThemeMode = Literal["dark", "light", "auto"]

_ACCENT_COLOR = "#4CA3FF"


def _detect_system_mode(app: QApplication) -> str:
    hints = app.styleHints()
    mode: Optional[str] = None
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


def _import_qfluentwidgets():
    try:
        style_sheet = import_module("qfluentwidgets.common.style_sheet")
        config = import_module("qfluentwidgets.common.config")
    except Exception:
        return None

    theme_enum = getattr(config, "Theme", None)
    set_theme = getattr(style_sheet, "setTheme", None)
    set_theme_color = getattr(style_sheet, "setThemeColor", None)
    return theme_enum, set_theme, set_theme_color


def apply_theme(mode: ThemeMode = "auto", app: QApplication | None = None) -> str:
    """Apply the requested theme to the current application."""

    app = app or QApplication.instance()
    if app is None:
        return "none"

    resolved = mode if mode != "auto" else _detect_system_mode(app)
    current = getattr(app, "_forgebuild_theme_mode", None)
    if current == resolved:
        return resolved

    payload = _import_qfluentwidgets()
    if payload is None:
        setattr(app, "_forgebuild_theme_mode", resolved)
        return resolved

    theme_enum, set_theme, set_theme_color = payload
    if not (theme_enum and callable(set_theme) and callable(set_theme_color)):
        setattr(app, "_forgebuild_theme_mode", resolved)
        return resolved

    qtheme = theme_enum.DARK if resolved == "dark" else theme_enum.LIGHT

    try:
        set_theme(qtheme, save=False, lazy=False)
    except Exception:
        pass

    try:
        set_theme_color(_ACCENT_COLOR, save=False, lazy=False)
    except Exception:
        pass

    setattr(app, "_forgebuild_theme_mode", resolved)
    return resolved


def initialize_fluent_widgets(app: QApplication, mode: ThemeMode = "auto") -> str:
    """Install QFluentWidgets helpers on the application and apply a theme."""

    try:
        translator_mod = import_module("qfluentwidgets.common.translator")
        FluentTranslator = getattr(translator_mod, "FluentTranslator", None)
    except Exception:
        FluentTranslator = None

    translator = getattr(app, "_forgebuild_fluent_translator", None)
    if FluentTranslator and translator is None:
        try:
            translator = FluentTranslator()
            app.installTranslator(translator)
        except Exception:
            translator = None
        if translator is not None:
            setattr(app, "_forgebuild_fluent_translator", translator)

    try:
        icon_mod = import_module("qfluentwidgets.common.icon")
        FluentIcon = getattr(icon_mod, "FluentIcon", None)
    except Exception:
        FluentIcon = None

    if FluentIcon and not getattr(app, "_forgebuild_fluent_icons_ready", False):
        try:
            FluentIcon.ADD.qicon()
        except Exception:
            pass
        else:
            setattr(app, "_forgebuild_fluent_icons_ready", True)

    return apply_theme(mode, app=app)


__all__ = ["apply_theme", "initialize_fluent_widgets", "ThemeMode"]
