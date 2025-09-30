"""Panel general de administración para usuarios y catálogos."""
from __future__ import annotations

from typing import Optional

from PySide6.QtWidgets import QHBoxLayout, QLabel, QTabWidget, QVBoxLayout, QWidget

from ..ui.icons import get_icon
from .catalogs_view import CatalogsView
from .user_admin import UserAdminView


class AdminPanelView(QWidget):
    """Panel de administración visible para roles de liderazgo."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)
        icon = QLabel()
        icon.setPixmap(get_icon("config").pixmap(28, 28))
        header.addWidget(icon)
        title = QLabel("Administración")
        title.setProperty("role", "title")
        header.addWidget(title)
        header.addStretch(1)
        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        self.user_admin = UserAdminView(self)
        self.catalogs = CatalogsView(self)
        self.tabs.addTab(self.user_admin, get_icon("config"), "Usuarios")
        self.tabs.addTab(self.catalogs, get_icon("build"), "Catálogos")
        layout.addWidget(self.tabs, 1)

    # ------------------------------------------------------------------
    def reload(self) -> None:
        self.user_admin.reload()
        self.catalogs.reload()


__all__ = ["AdminPanelView"]
