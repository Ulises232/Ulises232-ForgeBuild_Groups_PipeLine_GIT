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


def parse_color(value: Optional[str]) -> Optional[QColor]:
    """Return a QColor for *value* or ``None`` when it is invalid."""

    if not value:
        return None
    color = QColor(value.strip())
    if not color.isValid():
        return None
    return color


def incidence_brushes(value: Optional[str]) -> Tuple[Optional[QBrush], Optional[QBrush]]:
    """Return background/foreground brushes suited for incidence colors."""

    color = parse_color(value)
    if color is None:
        return (None, None)

    background = QBrush(color)

    # Determine the most legible foreground by blending the color over a
    # light background and checking relative luminance.
    effective = _blend_over(color, QColor("#ffffff"))
    luminance = _relative_luminance(effective)
    foreground_color = QColor("#202020") if luminance > 0.55 else QColor("#f0f0f0")
    foreground = QBrush(foreground_color)
    return (background, foreground)

