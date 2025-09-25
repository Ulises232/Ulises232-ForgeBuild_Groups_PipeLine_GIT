import getpass
import os
import time
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Callable, Literal, Optional, Sequence, Tuple

from operator import attrgetter

from PySide6.QtCore import Qt, Slot
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
from ..core.session import current_username

from ..core.branch_store import (
    BranchRecord,
    Index,
    NasUnavailableError,
    load_index,
    load_nas_index,
    record_activity,
    save_index,
    save_nas_index,
)
from ..ui.widgets import combo_with_arrow
from .shared_filters import (
    iter_filtered_records,
    sync_group_project_filters,
    update_project_filter,
)


@dataclass(frozen=True)
class BranchHistoryBackend:
    """Configura el origen de datos para la vista de historial de ramas."""

    storage: Literal["local", "nas"]
    title: str
    load: Callable[[], Index]
    save: Callable[[Index], None]
    activity_targets: Sequence[str]
    activity_message: str
    unavailable_exceptions: Tuple[type[BaseException], ...] = ()

    def is_unavailable_error(self, exc: BaseException) -> bool:
        return bool(self.unavailable_exceptions) and isinstance(exc, self.unavailable_exceptions)


class BranchHistoryView(QWidget):
    """Permite consultar y editar el historial de ramas tanto local como en NAS."""

    def __init__(self, storage: Literal["local", "nas"], parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.backend = self._build_backend(storage)
        self._index: Index = {}
        self._current_key: Optional[str] = None
        self._user_default = current_username(
            os.environ.get("USERNAME") or os.environ.get("USER") or getpass.getuser()
        )
        self._group_getter = attrgetter("group")
        self._project_getter = attrgetter("project")
        self._setup_ui()
        self._load_index()

    def _build_backend(self, storage: Literal["local", "nas"]) -> BranchHistoryBackend:
        if storage == "local":
            return BranchHistoryBackend(
                storage="local",
                title="Historial local",
                load=load_index,
                save=save_index,
                activity_targets=("local",),
                activity_message="Local manual",
            )
        if storage == "nas":
            return BranchHistoryBackend(
                storage="nas",
                title="NAS",
                load=load_nas_index,
                save=save_nas_index,
                activity_targets=("local", "nas"),
                activity_message="NAS manual",
                unavailable_exceptions=(NasUnavailableError,),
            )
        raise ValueError(f"Storage '{storage}' is not supported")

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
        self.tree.setHeaderLabels(
            [
                "Rama",
                "Grupo",
                "Proyecto",
                "Local",
                "Origin",
                "Merge",
                "Actualizado",
                "Usuario",
            ]
        )
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

        self._togglable_controls = (self.tree, self.btnNew, self.btnSave, self.btnDelete, self.btnReset)

        self.cboGroup.currentIndexChanged.connect(self._on_group_filter_changed)
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
            self._index = self.backend.load()
        except Exception as exc:  # noqa: BLE001 - Queremos distinguir indisponibilidad NAS
            if self.backend.is_unavailable_error(exc):
                self._handle_unavailable(exc)
                return
            QMessageBox.critical(self, self.backend.title, f"No se pudo cargar el índice: {exc}")
            self._index = {}
        self._set_controls_enabled(True)
        self._current_key = None
        sync_group_project_filters(
            self.cboGroup,
            self.cboProject,
            self._index.values(),
            group_getter=self._group_getter,
            project_getter=self._project_getter,
        )
        self._refresh_tree()
        self._clear_form()

    def _handle_unavailable(self, exc: BaseException) -> None:
        self._index = {}
        self._current_key = None
        self._set_controls_enabled(False)
        QMessageBox.warning(self, f"{self.backend.title} no disponible", str(exc))
        sync_group_project_filters(
            self.cboGroup,
            self.cboProject,
            self._index.values(),
            group_getter=self._group_getter,
            project_getter=self._project_getter,
        )
        self._refresh_tree()
        self._clear_form()

    def _set_controls_enabled(self, enabled: bool) -> None:
        for widget in self._togglable_controls:
            widget.setEnabled(enabled)

    @Slot()
    @Slot(int)
    def _on_group_filter_changed(self, *_args: object) -> None:
        update_project_filter(
            self.cboGroup,
            self.cboProject,
            self._index.values(),
            group_getter=self._group_getter,
            project_getter=self._project_getter,
        )
        self._refresh_tree()

    @Slot()
    @Slot(int)
    @Slot(str)
    def _refresh_tree(self, *_args: object) -> None:
        if not self.tree:
            return
        self.tree.setUpdatesEnabled(False)
        self.tree.clear()
        records = sorted(self._index.values(), key=lambda r: (r.group or "", r.project or "", r.branch))
        filtered = iter_filtered_records(
            records,
            group_combo=self.cboGroup,
            project_combo=self.cboProject,
            search_text=self.txtSearch.text(),
            group_getter=self._group_getter,
            project_getter=self._project_getter,
            haystack_builder=lambda rec: (
                rec.branch,
                rec.group or "",
                rec.project or "",
                rec.created_by or "",
                rec.last_updated_by or "",
            ),
        )
        for rec in filtered:
            item = QTreeWidgetItem(
                [
                    rec.branch,
                    rec.group or "",
                    rec.project or "",
                    "Sí" if rec.exists_local else "No",
                    "Sí" if rec.exists_origin else "No",
                    rec.merge_status or "",
                    self._fmt_ts(rec.last_updated_at or rec.created_at),
                    self._format_user(rec),
                ]
            )
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
        self.txtUser.setText(rec.created_by or rec.last_updated_by or "")
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
            QMessageBox.warning(self, self.backend.title, "El nombre de la rama es obligatorio.")
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
        if not self._persist_index():
            return
        self._record_activity(action, rec)
        self._current_key = new_key
        sync_group_project_filters(
            self.cboGroup,
            self.cboProject,
            self._index.values(),
            group_getter=self._group_getter,
            project_getter=self._project_getter,
        )
        self._refresh_tree()
        self._select_current()

    def _persist_index(self) -> bool:
        try:
            self.backend.save(self._index)
        except Exception as exc:  # noqa: BLE001 - diferenciamos indisponibilidad NAS
            if self.backend.is_unavailable_error(exc):
                QMessageBox.warning(self, self.backend.title, str(exc))
                return False
            QMessageBox.critical(self, self.backend.title, f"No se pudo guardar el índice: {exc}")
            return False
        return True

    def _record_activity(self, action: str, rec: BranchRecord) -> None:
        if not self.backend.activity_targets:
            return
        record_activity(action, rec, targets=tuple(self.backend.activity_targets), message=self.backend.activity_message)

    def _select_current(self) -> None:
        if not self._current_key:
            return
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if item.data(0, Qt.UserRole) == self._current_key:
                self.tree.setCurrentItem(item)
                break

    def _format_user(self, rec: BranchRecord) -> str:
        creator = (rec.created_by or "").strip()
        editor = (rec.last_updated_by or "").strip()
        parts = []
        if creator:
            parts.append(creator)
        if editor and editor != creator:
            parts.append(editor)
        if not parts and editor:
            parts.append(editor)
        return " / ".join(parts)

    @Slot()
    @Slot(bool)
    def _on_delete(self, *_args: object) -> None:
        if not self._current_key or self._current_key not in self._index:
            return
        rec = self._index[self._current_key]
        confirm = QMessageBox.question(
            self,
            self.backend.title,
            f"¿Eliminar la rama '{rec.branch}' del proyecto '{rec.project or ''}'?",
        )
        if confirm != QMessageBox.Yes:
            return
        backup = rec
        self._index.pop(self._current_key, None)
        if not self._persist_index():
            self._index[self._current_key] = backup
            return
        self._record_activity("manual_delete", rec)
        self._current_key = None
        sync_group_project_filters(
            self.cboGroup,
            self.cboProject,
            self._index.values(),
            group_getter=self._group_getter,
            project_getter=self._project_getter,
        )
        self._refresh_tree()
        self._clear_form()

    # ----- utils -----
    @staticmethod
    def _fmt_ts(ts: int) -> str:
        if not ts:
            return "—"
        try:
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        except Exception:  # noqa: BLE001 - cualquier error produce guion largo
            return "—"
