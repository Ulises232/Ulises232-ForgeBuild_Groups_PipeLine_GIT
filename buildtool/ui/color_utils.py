"""Utilities to work with color values in the UI."""

from __future__ import annotations

from typing import Optional, Tuple

from PySide6.QtGui import QBrush, QColor, QPalette
from PySide6.QtWidgets import QApplication


def _srgb_to_linear(value: float) -> float:
    if value <= 0.04045:
        return value / 12.92
    return ((value + 0.055) / 1.055) ** 2.4


def _relative_luminance(color: QColor) -> float:
    red = _srgb_to_linear(color.redF())
    green = _srgb_to_linear(color.greenF())
    blue = _srgb_to_linear(color.blueF())
    return 0.2126 * red + 0.7152 * green + 0.0722 * blue


def _blend_over(color: QColor, base: QColor) -> QColor:
    alpha = color.alphaF()
    if alpha >= 0.999:
        return QColor(color)
    blended = QColor()
    blended.setRedF((color.redF() * alpha) + (base.redF() * (1 - alpha)))
    blended.setGreenF((color.greenF() * alpha) + (base.greenF() * (1 - alpha)))
    blended.setBlueF((color.blueF() * alpha) + (base.blueF() * (1 - alpha)))
    blended.setAlpha(255)
    return blended


_STATUS_COLORS = {
    "pending": "#28F6E08D",
    "pendiente": "#28F6E08D",
    "unit": "#2864B9F4",
    "qa": "#28A7E8A1",
    "bloqueado": "#28F28B82",
    "merge_en_progreso": "#28378AD7",
    "merge_ok": "#2821A179",
    "merge_error": "#28D94F4F",
    "terminated": "#28F7B0C6",
    "terminado": "#28F7B0C6",
}

_DEFAULT_STATUS_COLOR = "#12000000"


def _system_base_color() -> QColor:
    app = QApplication.instance()
    if app:
        palette = app.palette()
        base = palette.color(QPalette.ColorRole.Base)
        if base.isValid():
            return base
        window = palette.color(QPalette.ColorRole.Window)
        if window.isValid():
            return window
    return QColor("#ffffff")


def status_brushes(value: Optional[str]) -> Tuple[Optional[QBrush], Optional[QBrush]]:
    """Return background/foreground brushes for the given status value."""

    status = (value or "").strip().lower()
    if not status:
        return (None, None)

    color_name = _STATUS_COLORS.get(status, _DEFAULT_STATUS_COLOR)
    color = QColor(color_name)
    if not color.isValid():
        return (None, None)

    base = _system_base_color()
    effective_color = QColor(color)
    base_luminance = _relative_luminance(base)
    if base_luminance < 0.35:
        alpha = max(0.05, effective_color.alphaF() * 0.7)
        effective_color.setAlphaF(alpha)
    blended = _blend_over(effective_color, base)

    background = QBrush(blended)
    luminance = _relative_luminance(blended)
    if luminance >= 0.65:
        foreground_color = QColor("#202020")
    elif luminance <= 0.4:
        foreground_color = QColor("#f5f5f5")
    else:
        foreground_color = QColor("#1f1f1f") if base_luminance > 0.5 else QColor("#f0f0f0")
    foreground = QBrush(foreground_color)
    return (background, foreground)

