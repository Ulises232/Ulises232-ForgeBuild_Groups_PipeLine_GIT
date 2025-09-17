from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from PySide6.QtCore import Qt
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


class NasActivityLogView(QWidget):
    """Visualiza el archivo activity_log.jsonl almacenado en la NAS."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._entries: List[dict] = []
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
        filters.addWidget(self.cboGroup)

        filters.addWidget(QLabel("Proyecto:"))
        self.cboProject = QComboBox()
        self.cboProject.addItem("Todos", userData=None)
        filters.addWidget(self.cboProject)

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
        self.cboGroup.currentIndexChanged.connect(self._on_filters_changed)
        self.cboProject.currentIndexChanged.connect(self._refresh_tree)
        self.txtSearch.textChanged.connect(self._refresh_tree)

    # ----- data -----
    def _load_entries(self) -> None:
        self._entries = load_nas_activity_log()
        self._entries.sort(key=lambda e: e.get("ts") or 0, reverse=True)
        self._populate_filters()
        self._refresh_tree()

    def _populate_filters(self) -> None:
        groups = sorted({e.get("group") or "" for e in self._entries})
        with SignalBlocker(self.cboGroup):
            self.cboGroup.clear()
            self.cboGroup.addItem("Todos", userData=None)
            for g in groups:
                if g:
                    self.cboGroup.addItem(g, userData=g)
        self._update_projects_filter()

    def _update_projects_filter(self) -> None:
        group = self._current_group_filter()
        projects = set()
        for e in self._entries:
            if group and e.get("group") != group:
                continue
            proj = e.get("project")
            if proj:
                projects.add(proj)
        with SignalBlocker(self.cboProject):
            self.cboProject.clear()
            self.cboProject.addItem("Todos", userData=None)
            for proj in sorted(projects):
                self.cboProject.addItem(proj, userData=proj)

    def _current_group_filter(self) -> Optional[str]:
        idx = self.cboGroup.currentIndex()
        return self.cboGroup.itemData(idx)

    def _current_project_filter(self) -> Optional[str]:
        idx = self.cboProject.currentIndex()
        return self.cboProject.itemData(idx)

    def _on_filters_changed(self) -> None:
        self._update_projects_filter()
        self._refresh_tree()

    # ----- rendering -----
    def _refresh_tree(self) -> None:
        self.tree.setUpdatesEnabled(False)
        self.tree.clear()
        group = self._current_group_filter()
        project = self._current_project_filter()
        search = (self.txtSearch.text() or "").strip().lower()
        count = 0
        for entry in self._entries:
            if group and entry.get("group") != group:
                continue
            if project and entry.get("project") != project:
                continue
            if search:
                haystack = " ".join(
                    str(entry.get(key, ""))
                    for key in ("user", "branch", "action", "result", "message")
                ).lower()
                if search not in haystack:
                    continue
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


class SignalBlocker:
    def __init__(self, widget):
        self.widget = widget
        self._blocked = False

    def __enter__(self):
        try:
            self.widget.blockSignals(True)
            self._blocked = True
        except Exception:
            self._blocked = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._blocked:
            try:
                self.widget.blockSignals(False)
            except Exception:
                pass
