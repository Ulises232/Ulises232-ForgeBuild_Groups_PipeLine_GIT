"""Utilities to work with color values in the UI."""

from __future__ import annotations

from typing import Optional, Tuple

from PySide6.QtGui import QBrush, QColor


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
    "pending": "#33F2C94C",
    "pendiente": "#33F2C94C",
    "unit": "#332B6BE4",
    "qa": "#33E07A5F",
    "bloqueado": "#33D66B6B",
    "merge_en_progreso": "#33378AD7",
    "merge_ok": "#3321A179",
    "merge_error": "#33D94F4F",
    "terminated": "#3321A179",
    "terminado": "#3321A179",
}

_DEFAULT_STATUS_COLOR = "#19000000"


def status_brushes(value: Optional[str]) -> Tuple[Optional[QBrush], Optional[QBrush]]:
    """Return background/foreground brushes for the given status value."""

    status = (value or "").strip().lower()
    if not status:
        return (None, None)

    color_name = _STATUS_COLORS.get(status, _DEFAULT_STATUS_COLOR)
    color = QColor(color_name)
    if not color.isValid():
        return (None, None)

    background = QBrush(color)
    effective = _blend_over(color, QColor("#ffffff"))
    luminance = _relative_luminance(effective)
    foreground_color = QColor("#202020") if luminance > 0.6 else QColor("#f5f5f5")
    foreground = QBrush(foreground_color)
    return (background, foreground)

