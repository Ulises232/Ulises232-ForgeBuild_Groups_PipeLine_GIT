from __future__ import annotations

import getpass
import os
import time
from dataclasses import replace
from datetime import datetime
from typing import Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..core.branch_store import (
    BranchRecord,
    Index,
    load_nas_index,
    record_activity,
    save_nas_index,
)


class NasBranchesView(QWidget):
    """Permite consultar y editar el historial de ramas almacenado en la NAS."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._index: Index = {}
        self._current_key: Optional[str] = None
        self._user_default = os.environ.get("USERNAME") or os.environ.get("USER") or getpass.getuser()
        self._setup_ui()
        self._load_index()

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
        self.txtSearch.setPlaceholderText("Nombre de rama o usuario")
        filters.addWidget(self.txtSearch, 1)

        self.btnRefresh = QPushButton("Refrescar")
        filters.addWidget(self.btnRefresh)
        self.btnNew = QPushButton("Nuevo registro")
        filters.addWidget(self.btnNew)

        root.addLayout(filters)

        splitter = QSplitter()
        splitter.setOrientation(Qt.Horizontal)
        root.addWidget(splitter, 1)

        self.tree = QTreeWidget()
        self.tree.setAlternatingRowColors(True)
        self.tree.setRootIsDecorated(False)
        self.tree.setHeaderLabels([
            "Rama",
            "Grupo",
            "Proyecto",
            "Local",
            "Origin",
            "Merge",
            "Actualizado",
            "Usuario",
        ])
        splitter.addWidget(self.tree)

        form_box = QGroupBox("Detalle")
        form_layout = QFormLayout(form_box)
        form_layout.setLabelAlignment(Qt.AlignLeft)
        form_layout.setFormAlignment(Qt.AlignTop)

        self.txtGroup = QLineEdit()
        form_layout.addRow("Grupo", self.txtGroup)
        self.txtProject = QLineEdit()
        form_layout.addRow("Proyecto", self.txtProject)
        self.txtBranch = QLineEdit()
        form_layout.addRow("Rama", self.txtBranch)

        self.chkLocal = QCheckBox("Existe local")
        form_layout.addRow("Local", self.chkLocal)
        self.chkOrigin = QCheckBox("Existe origin")
        form_layout.addRow("Origin", self.chkOrigin)
        self.txtMerge = QLineEdit()
        form_layout.addRow("Estado merge", self.txtMerge)
        self.txtUser = QLineEdit()
        self.txtUser.setPlaceholderText(self._user_default)
        form_layout.addRow("Usuario", self.txtUser)

        self.lblCreated = QLabel("—")
        form_layout.addRow("Creada", self.lblCreated)
        self.lblUpdated = QLabel("—")
        form_layout.addRow("Última actualización", self.lblUpdated)

        buttons = QHBoxLayout()
        buttons.setSpacing(6)
        self.btnSave = QPushButton("Guardar cambios")
        self.btnDelete = QPushButton("Eliminar")
        self.btnReset = QPushButton("Descartar")
        buttons.addWidget(self.btnSave)
        buttons.addWidget(self.btnDelete)
        buttons.addWidget(self.btnReset)
        form_layout.addRow(buttons)

        splitter.addWidget(form_box)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)

        self.cboGroup.currentIndexChanged.connect(self._update_projects_filter)
        self.cboGroup.currentIndexChanged.connect(self._refresh_tree)
        self.cboProject.currentIndexChanged.connect(self._refresh_tree)
        self.txtSearch.textChanged.connect(self._refresh_tree)
        self.btnRefresh.clicked.connect(self._load_index)
        self.btnNew.clicked.connect(self._on_new)
        self.tree.itemSelectionChanged.connect(self._on_select)
        self.btnSave.clicked.connect(self._on_save)
        self.btnDelete.clicked.connect(self._on_delete)
        self.btnReset.clicked.connect(self._on_reset)

    # ----- data helpers -----
    def _load_index(self) -> None:
        self._index = load_nas_index()
        self._current_key = None
        self._populate_filters()
        self._refresh_tree()
        self._clear_form()

    def _populate_filters(self) -> None:
        groups = sorted({rec.group or "" for rec in self._index.values()})
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
        for rec in self._index.values():
            if group and rec.group != group:
                continue
            if rec.project:
                projects.add(rec.project)
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

    def _refresh_tree(self) -> None:
        if not self.tree:
            return
        self.tree.setUpdatesEnabled(False)
        self.tree.clear()
        group = self._current_group_filter()
        project = self._current_project_filter()
        search = (self.txtSearch.text() or "").strip().lower()
        records = sorted(self._index.values(), key=lambda r: (r.group or "", r.project or "", r.branch))
        for rec in records:
            if group and rec.group != group:
                continue
            if project and rec.project != project:
                continue
            if search:
                haystack = " ".join(
                    filter(
                        None,
                        [rec.branch, rec.group or "", rec.project or "", rec.last_updated_by or rec.created_by or ""],
                    )
                ).lower()
                if search not in haystack:
                    continue
            item = QTreeWidgetItem([
                rec.branch,
                rec.group or "",
                rec.project or "",
                "Sí" if rec.exists_local else "No",
                "Sí" if rec.exists_origin else "No",
                rec.merge_status or "",
                self._fmt_ts(rec.last_updated_at or rec.created_at),
                rec.last_updated_by or rec.created_by or "",
            ])
            item.setData(0, Qt.UserRole, rec.key())
            self.tree.addTopLevelItem(item)
        self.tree.setUpdatesEnabled(True)
        self.tree.resizeColumnToContents(0)
        self.tree.resizeColumnToContents(1)

    # ----- selection handling -----
    def _on_select(self) -> None:
        items = self.tree.selectedItems()
        if not items:
            self._current_key = None
            self._clear_form()
            return
        key = items[0].data(0, Qt.UserRole)
        self._current_key = key
        rec = self._index.get(key)
        if rec:
            self._load_record(rec)

    def _clear_form(self) -> None:
        self.txtGroup.clear()
        self.txtProject.clear()
        self.txtBranch.clear()
        self.chkLocal.setChecked(False)
        self.chkOrigin.setChecked(False)
        self.txtMerge.clear()
        self.txtUser.clear()
        self.lblCreated.setText("—")
        self.lblUpdated.setText("—")

    def _load_record(self, rec: BranchRecord) -> None:
        self.txtGroup.setText(rec.group or "")
        self.txtProject.setText(rec.project or "")
        self.txtBranch.setText(rec.branch)
        self.chkLocal.setChecked(bool(rec.exists_local))
        self.chkOrigin.setChecked(bool(rec.exists_origin))
        self.txtMerge.setText(rec.merge_status or "")
        self.txtUser.setText(rec.last_updated_by or rec.created_by or "")
        created = self._fmt_ts(rec.created_at)
        updated = self._fmt_ts(rec.last_updated_at or rec.created_at)
        self.lblCreated.setText(created)
        self.lblUpdated.setText(updated)

    # ----- commands -----
    def _on_new(self) -> None:
        self._current_key = None
        self._clear_form()
        self.txtGroup.setFocus()

    def _on_reset(self) -> None:
        if self._current_key and self._current_key in self._index:
            self._load_record(self._index[self._current_key])
        else:
            self._clear_form()

    def _on_save(self) -> None:
        group = (self.txtGroup.text() or "").strip() or None
        project = (self.txtProject.text() or "").strip() or None
        branch = (self.txtBranch.text() or "").strip()
        if not branch:
            QMessageBox.warning(self, "NAS", "El nombre de la rama es obligatorio.")
            return
        user = (self.txtUser.text() or "").strip() or self._user_default

        now = int(time.time())
        action = "manual_update"
        if self._current_key and self._current_key in self._index:
            base = self._index[self._current_key]
        else:
            action = "manual_create"
            base = BranchRecord(branch=branch)
            base.created_at = now
            base.created_by = user

        rec = replace(
            base,
            branch=branch,
            group=group,
            project=project,
            exists_local=bool(self.chkLocal.isChecked()),
            exists_origin=bool(self.chkOrigin.isChecked()),
            merge_status=(self.txtMerge.text() or "").strip(),
            last_action=action,
            last_updated_at=now,
            last_updated_by=user,
        )
        if not rec.created_at:
            rec.created_at = now
        if not rec.created_by:
            rec.created_by = user

        new_key = rec.key()
        if self._current_key and self._current_key != new_key:
            self._index.pop(self._current_key, None)
        self._index[new_key] = rec
        try:
            save_nas_index(self._index)
        except Exception as exc:
            QMessageBox.critical(self, "NAS", f"No se pudo guardar el índice: {exc}")
            return
        record_activity(action, rec, targets=("local", "nas"), message="NAS manual")
        self._current_key = new_key
        self._populate_filters()
        self._refresh_tree()
        self._select_current()

    def _select_current(self) -> None:
        if not self._current_key:
            return
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if item.data(0, Qt.UserRole) == self._current_key:
                self.tree.setCurrentItem(item)
                break

    def _on_delete(self) -> None:
        if not self._current_key or self._current_key not in self._index:
            return
        rec = self._index[self._current_key]
        confirm = QMessageBox.question(
            self,
            "NAS",
            f"¿Eliminar la rama '{rec.branch}' del proyecto '{rec.project or ''}'?",
        )
        if confirm != QMessageBox.Yes:
            return
        self._index.pop(self._current_key, None)
        try:
            save_nas_index(self._index)
        except Exception as exc:
            QMessageBox.critical(self, "NAS", f"No se pudo eliminar el registro: {exc}")
            return
        record_activity("manual_delete", rec, targets=("local", "nas"), message="NAS manual")
        self._current_key = None
        self._populate_filters()
        self._refresh_tree()
        self._clear_form()

    # ----- utils -----
    @staticmethod
    def _fmt_ts(ts: int) -> str:
        if not ts:
            return "—"
        try:
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return "—"


class SignalBlocker:
    """Context helper similar a QSignalBlocker pero apto para `with`."""

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
