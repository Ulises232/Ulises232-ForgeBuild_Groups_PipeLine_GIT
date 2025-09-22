from __future__ import annotations

import getpass
import os
import time
from dataclasses import replace
from datetime import datetime
from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from qfluentwidgets import (
    ComboBox,
    CheckBox,
    Dialog,
    InfoBar,
    InfoBarPosition,
    PrimaryPushButton,
    PushButton,
    ScrollArea,
    SettingCardGroup,
)

from ..core.branch_store import (
    BranchRecord,
    Index,
    NasUnavailableError,
    load_nas_index,
    record_activity,
    save_nas_index,
)


class NasBranchesView(QWidget):
    """Permite consultar y editar el historial de ramas almacenado en la NAS (SQLite)."""

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
        self.cboGroup = ComboBox()
        self.cboGroup.addItem("Todos", userData=None)
        filters.addWidget(self.cboGroup)

        filters.addWidget(QLabel("Proyecto:"))
        self.cboProject = ComboBox()
        self.cboProject.addItem("Todos", userData=None)
        filters.addWidget(self.cboProject)

        filters.addWidget(QLabel("Buscar:"))
        self.txtSearch = QLineEdit()
        self.txtSearch.setPlaceholderText("Nombre de rama o usuario")
        filters.addWidget(self.txtSearch, 1)

        self.btnRefresh = PushButton("Refrescar")
        filters.addWidget(self.btnRefresh)
        self.btnNew = PrimaryPushButton("Nuevo registro")
        filters.addWidget(self.btnNew)

        root.addLayout(filters)

        scroll = ScrollArea(self)
        scroll.setWidgetResizable(True)
        container = QWidget()
        content_layout = QVBoxLayout(container)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(12)

        tree_group = SettingCardGroup("Registros de ramas", container)
        tree_card = QWidget(tree_group)
        tree_layout = QVBoxLayout(tree_card)
        tree_layout.setContentsMargins(16, 16, 16, 16)
        tree_layout.setSpacing(8)
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
        tree_layout.addWidget(self.tree)
        tree_group.addSettingCard(tree_card)
        content_layout.addWidget(tree_group)

        detail_group = SettingCardGroup("Detalle", container)
        detail_card = QWidget(detail_group)
        form_layout = QFormLayout(detail_card)
        form_layout.setLabelAlignment(Qt.AlignLeft)
        form_layout.setFormAlignment(Qt.AlignTop)
        form_layout.setContentsMargins(16, 16, 16, 16)

        self.txtGroup = QLineEdit()
        form_layout.addRow("Grupo", self.txtGroup)
        self.txtProject = QLineEdit()
        form_layout.addRow("Proyecto", self.txtProject)
        self.txtBranch = QLineEdit()
        form_layout.addRow("Rama", self.txtBranch)

        self.chkLocal = CheckBox("Existe local")
        form_layout.addRow("Local", self.chkLocal)
        self.chkOrigin = CheckBox("Existe origin")
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
        self.btnSave = PrimaryPushButton("Guardar cambios")
        self.btnDelete = PushButton("Eliminar")
        self.btnReset = PushButton("Descartar")
        buttons.addWidget(self.btnSave)
        buttons.addWidget(self.btnDelete)
        buttons.addWidget(self.btnReset)
        form_layout.addRow(buttons)

        detail_group.addSettingCard(detail_card)
        content_layout.addWidget(detail_group)
        content_layout.addStretch(1)

        scroll.setWidget(container)
        root.addWidget(scroll, 1)

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
    @Slot()
    @Slot(bool)
    def _load_index(self, *_args: object) -> None:
        try:
            self._index = load_nas_index()
        except NasUnavailableError as exc:
            self._index = {}
            self._current_key = None
            self.tree.clear()
            self.tree.setEnabled(False)
            for btn in (self.btnNew, self.btnSave, self.btnDelete, self.btnReset):
                btn.setEnabled(False)
            self._notify("NAS no disponible", str(exc), "warning")
            self._populate_filters()
            self._refresh_tree()
            self._clear_form()
            return
        self.tree.setEnabled(True)
        for btn in (self.btnNew, self.btnSave, self.btnDelete, self.btnReset):
            btn.setEnabled(True)
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

    @Slot()
    @Slot(int)
    def _update_projects_filter(self, *_args: object) -> None:
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

    @Slot()
    @Slot(int)
    @Slot(str)
    def _refresh_tree(self, *_args: object) -> None:
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
    @Slot()
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
    @Slot()
    @Slot(bool)
    def _on_new(self, *_args: object) -> None:
        self._current_key = None
        self._clear_form()
        self.txtGroup.setFocus()

    @Slot()
    @Slot(bool)
    def _on_reset(self, *_args: object) -> None:
        if self._current_key and self._current_key in self._index:
            self._load_record(self._index[self._current_key])
        else:
            self._clear_form()

    @Slot()
    @Slot(bool)
    def _on_save(self, *_args: object) -> None:
        group = (self.txtGroup.text() or "").strip() or None
        project = (self.txtProject.text() or "").strip() or None
        branch = (self.txtBranch.text() or "").strip()
        if not branch:
            self._notify("NAS", "El nombre de la rama es obligatorio.", "warning")
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
            record_activity(action, rec, targets=("local", "nas"), message="NAS manual")
        except NasUnavailableError as exc:
            self._notify("NAS", str(exc), "warning")
            return
        except Exception as exc:
            self._notify("NAS", f"No se pudo guardar el índice: {exc}", "error")
            return
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

    @Slot()
    @Slot(bool)
    def _on_delete(self, *_args: object) -> None:
        if not self._current_key or self._current_key not in self._index:
            return
        rec = self._index[self._current_key]
        if not self._confirm(
            "NAS",
            f"¿Eliminar la rama '{rec.branch}' del proyecto '{rec.project or ''}'?",
        ):
            return
        self._index.pop(self._current_key, None)
        try:
            save_nas_index(self._index)
            record_activity("manual_delete", rec, targets=("local", "nas"), message="NAS manual")
        except NasUnavailableError as exc:
            self._notify("NAS", str(exc), "warning")
            return
        except Exception as exc:
            self._notify("NAS", f"No se pudo eliminar el registro: {exc}", "error")
            return
        self._current_key = None
        self._populate_filters()
        self._refresh_tree()
        self._clear_form()

    # ----- utils -----
    def _notify(self, title: str, message: str, level: str = "info") -> None:
        mapping = {
            "info": InfoBar.info,
            "warning": InfoBar.warning,
            "error": InfoBar.error,
            "success": InfoBar.success,
        }
        method = mapping.get(level, InfoBar.info)
        try:
            method(
                title=title,
                content=message,
                isClosable=True,
                duration=5000,
                position=InfoBarPosition.TOP_RIGHT,
                parent=self,
            )
        except Exception:
            pass

    def _confirm(self, title: str, message: str) -> bool:
        dialog = Dialog(title, message, self)
        dialog.yesButton.setText("Aceptar")
        dialog.cancelButton.setText("Cancelar")
        return dialog.exec() == QDialog.DialogCode.Accepted

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
