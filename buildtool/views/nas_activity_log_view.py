from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from operator import itemgetter

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..core.branch_store import load_nas_activity_log
from ..ui.widgets import combo_with_arrow
from .shared_filters import (
    iter_filtered_records,
    sync_group_project_filters,
    update_project_filter,
)


class NasActivityLogView(QWidget):
    """Visualiza el registro de actividad NAS persistido en SQLite."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._entries: List[dict] = []
        self._group_getter = itemgetter("group")
        self._project_getter = itemgetter("project")
        self._setup_ui()
        self._load_entries()

    # ----- setup -----
    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        filters = QHBoxLayout()
        filters.setSpacing(6)

        filters.addWidget(QLabel("Grupo:"))
        self.cboGroup = QComboBox()
        self.cboGroup.addItem("Todos", userData=None)
        filters.addWidget(combo_with_arrow(self.cboGroup))

        filters.addWidget(QLabel("Proyecto:"))
        self.cboProject = QComboBox()
        self.cboProject.addItem("Todos", userData=None)
        filters.addWidget(combo_with_arrow(self.cboProject))

        filters.addWidget(QLabel("Buscar:"))
        self.txtSearch = QLineEdit()
        self.txtSearch.setPlaceholderText("Usuario, rama, acción o mensaje")
        filters.addWidget(self.txtSearch, 1)

        self.btnRefresh = QPushButton("Refrescar")
        filters.addWidget(self.btnRefresh)
        self.lblCount = QLabel("0 registros")
        filters.addWidget(self.lblCount)

        root.addLayout(filters)

        self.tree = QTreeWidget()
        self.tree.setAlternatingRowColors(True)
        self.tree.setRootIsDecorated(False)
        self.tree.setHeaderLabels([
            "Fecha",
            "Usuario",
            "Grupo",
            "Proyecto",
            "Rama",
            "Acción",
            "Resultado",
            "Mensaje",
        ])
        root.addWidget(self.tree, 1)

        self.btnRefresh.clicked.connect(self._load_entries)
        self.cboGroup.currentIndexChanged.connect(self._on_group_filter_changed)
        self.cboProject.currentIndexChanged.connect(self._refresh_tree)
        self.txtSearch.textChanged.connect(self._refresh_tree)

    # ----- data -----
    @Slot()
    def _load_entries(self) -> None:
        self._entries = load_nas_activity_log()
        self._entries.sort(key=lambda e: e.get("ts") or 0, reverse=True)
        sync_group_project_filters(
            self.cboGroup,
            self.cboProject,
            self._entries,
            group_getter=self._group_getter,
            project_getter=self._project_getter,
        )
        self._refresh_tree()

    @Slot()
    def _on_group_filter_changed(self) -> None:
        update_project_filter(
            self.cboGroup,
            self.cboProject,
            self._entries,
            group_getter=self._group_getter,
            project_getter=self._project_getter,
        )
        self._refresh_tree()

    # ----- rendering -----
    @Slot()
    def _refresh_tree(self) -> None:
        self.tree.setUpdatesEnabled(False)
        self.tree.clear()
        count = 0
        filtered = iter_filtered_records(
            self._entries,
            group_combo=self.cboGroup,
            project_combo=self.cboProject,
            search_text=self.txtSearch.text(),
            group_getter=self._group_getter,
            project_getter=self._project_getter,
            haystack_builder=lambda entry: (
                str(entry.get("user", "")),
                str(entry.get("branch", "")),
                str(entry.get("action", "")),
                str(entry.get("result", "")),
                str(entry.get("message", "")),
            ),
        )
        for entry in filtered:
            item = QTreeWidgetItem([
                self._fmt_ts(entry.get("ts")),
                entry.get("user", ""),
                entry.get("group", ""),
                entry.get("project", ""),
                entry.get("branch", ""),
                entry.get("action", ""),
                entry.get("result", ""),
                entry.get("message", ""),
            ])
            self.tree.addTopLevelItem(item)
            count += 1
        self.tree.setUpdatesEnabled(True)
        self.tree.resizeColumnToContents(0)
        self.lblCount.setText(f"{count} registros")

    @staticmethod
    def _fmt_ts(ts: Optional[int]) -> str:
        if not ts:
            return "—"
        try:
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return "—"
