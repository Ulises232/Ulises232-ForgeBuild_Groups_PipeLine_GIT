"""Reusable Fluent-friendly widget helpers."""
from __future__ import annotations

from typing import Optional

from PySide6.QtGui import QFont, QFontDatabase
from qfluentwidgets import TextEdit

__all__ = ["ForgeLogTextEdit", "apply_monospace_font"]


def _monospace_font(point_size: int = 12) -> QFont:
    font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
    if font.pointSize() <= 0:
        font.setPointSize(point_size)
    else:
        font.setPointSize(point_size)
    font.setStyleStrategy(QFont.PreferDefault)
    return font


class ForgeLogTextEdit(TextEdit):
    """Text edit preconfigured for ForgeBuild log viewers."""

    def __init__(
        self,
        object_name: Optional[str] = None,
        parent=None,
        *,
        wrap_mode: Optional[TextEdit.LineWrapMode] = TextEdit.NoWrap,
        minimum_height: Optional[int] = None,
        point_size: int = 12,
    ) -> None:
        super().__init__(parent)
        self.setReadOnly(True)
        self.setProperty("isAcrylic", False)
        self.setProperty("useThemePalette", True)
        if object_name:
            self.setObjectName(object_name)
        if wrap_mode is not None:
            try:
                self.setLineWrapMode(wrap_mode)
            except Exception:
                pass
        self.setFont(_monospace_font(point_size))
        if minimum_height:
            self.setMinimumHeight(minimum_height)


def apply_monospace_font(widget, point_size: int = 12) -> None:
    """Apply a monospace font to any QTextEdit/TextEdit."""

    try:
        widget.setFont(_monospace_font(point_size))
    except Exception:
        pass
