"""Shared UI widgets and helpers for ForgeBuild."""
from __future__ import annotations

from typing import Optional

from PySide6.QtCore import Qt, QSize, QObject, QEvent, Slot
from PySide6.QtWidgets import (
    QComboBox,
    QCompleter,
    QHBoxLayout,
    QSizePolicy,
    QToolButton,
    QWidget,
)

from .icons import get_icon


class _ComboSync(QObject):
    """Synchronize combo enabled state with its attached arrow button."""

    def __init__(self, combo: QComboBox, arrow: QToolButton) -> None:
        super().__init__(combo)
        self._combo = combo
        self._arrow = arrow
        combo.installEventFilter(self)

    def eventFilter(self, obj: QObject, event: QEvent) -> bool:  # noqa: N802 - Qt signature
        if obj is self._combo and event.type() == QEvent.EnabledChange:
            self._arrow.setEnabled(self._combo.isEnabled())
        return super().eventFilter(obj, event)


def combo_with_arrow(combo: QComboBox, *, arrow_tooltip: Optional[str] = None) -> QWidget:
    """Wrap a :class:`QComboBox` with a clickable arrow button."""

    container = QWidget()
    layout = QHBoxLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(6)
    layout.addWidget(combo, 1)

    arrow = QToolButton(container)
    arrow.setIcon(get_icon("chevron-down"))
    arrow.setAutoRaise(True)
    arrow.setCursor(Qt.PointingHandCursor)
    arrow.setFixedSize(26, 24)
    arrow.setIconSize(QSize(16, 16))
    arrow.setToolButtonStyle(Qt.ToolButtonIconOnly)
    if arrow_tooltip:
        arrow.setToolTip(arrow_tooltip)
    arrow.setEnabled(combo.isEnabled())

    @Slot()
    def _show_popup() -> None:
        combo.showPopup()

    arrow.clicked.connect(_show_popup)
    layout.addWidget(arrow)

    container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
    setattr(combo, "_arrow_button", arrow)
    _ComboSync(combo, arrow)
    return container


def set_combo_enabled(combo: QComboBox, enabled: bool) -> None:
    """Enable/disable a combo and its extra arrow button."""

    try:
        combo.setEnabled(enabled)
    finally:
        arrow = getattr(combo, "_arrow_button", None)
        if isinstance(arrow, QToolButton):
            arrow.setEnabled(enabled)


def setup_quick_filter(combo: QComboBox) -> None:
    """Enable case-insensitive quick filtering for editable combos."""

    combo.setEditable(True)
    combo.setInsertPolicy(QComboBox.NoInsert)
    completer: QCompleter = combo.completer()
    completer.setCompletionMode(QCompleter.PopupCompletion)
    completer.setFilterMode(Qt.MatchContains)
    line_edit = combo.lineEdit()
    if line_edit is not None:
        line_edit.setReadOnly(False)


class SignalBlocker:
    """Context helper similar to :class:`QSignalBlocker` but usable with ``with``."""

    def __init__(self, widget: QObject) -> None:
        self.widget = widget
        self._blocked = False

    def __enter__(self) -> "SignalBlocker":
        try:
            self.widget.blockSignals(True)
            self._blocked = True
        except Exception:  # noqa: BLE001 - no interrumpir flujo de UI
            self._blocked = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[override]
        if self._blocked:
            try:
                self.widget.blockSignals(False)
            except Exception:  # noqa: BLE001 - silencioso, solo UI
                pass


__all__ = [
    "SignalBlocker",
    "combo_with_arrow",
    "set_combo_enabled",
    "setup_quick_filter",
]
