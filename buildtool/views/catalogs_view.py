"""Vistas para la administración de catálogos."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPixmap
from PySide6.QtWidgets import (
    QComboBox,
    QColorDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
    QFileDialog,
)

from ..core.catalog_queries import (
    Company,
    IncidenceType,
    list_companies,
    list_incidence_types,
    remove_incidence_type,
    save_company,
    save_incidence_type,
)
from ..core.config import load_config
from ..core.session import current_username
from ..ui.icons import get_icon


class CompanyCatalogView(QWidget):
    """Gestión del catálogo de empresas."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._companies: Dict[int, Company] = {}
        self._current_id: Optional[int] = None
        self._setup_ui()
        self.reload()

    # ------------------------------------------------------------------
    def _setup_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        left = QVBoxLayout()
        left.setContentsMargins(0, 0, 0, 0)
        left.setSpacing(8)

        self.lstCompanies = QListWidget()
        self.lstCompanies.setSelectionMode(QListWidget.SingleSelection)
        self.lstCompanies.currentItemChanged.connect(self._on_company_selected)
        left.addWidget(self.lstCompanies, 1)

        buttons = QHBoxLayout()
        buttons.setContentsMargins(0, 0, 0, 0)
        buttons.setSpacing(8)
        self.btnNew = QPushButton("Nueva empresa")
        self.btnNew.clicked.connect(self._start_new)
        self.btnRefresh = QPushButton("Recargar")
        self.btnRefresh.clicked.connect(self.reload)
        buttons.addWidget(self.btnNew)
        buttons.addWidget(self.btnRefresh)
        left.addLayout(buttons)

        layout.addLayout(left, 1)

        self.grpForm = QGroupBox("Detalle de empresa")
        form = QFormLayout(self.grpForm)
        form.setLabelAlignment(Qt.AlignRight)
        form.setSpacing(8)

        self.txtName = QLineEdit()
        form.addRow("Nombre", self.txtName)

        self.cboGroup = QComboBox()
        form.addRow("Grupo", self.cboGroup)

        self.lblCreated = QLabel("-")
        self.lblUpdated = QLabel("-")
        self.spnNextSprint = QSpinBox()
        self.spnNextSprint.setMinimum(1)
        self.spnNextSprint.setMaximum(999999)
        self.spnNextSprint.setValue(1)
        form.addRow("Creada por", self.lblCreated)
        form.addRow("Última actualización", self.lblUpdated)
        form.addRow("Próximo sprint", self.spnNextSprint)

        action_row = QHBoxLayout()
        action_row.addStretch(1)
        self.btnCancel = QPushButton("Cancelar")
        self.btnCancel.clicked.connect(self._cancel)
        self.btnSave = QPushButton("Guardar")
        self.btnSave.setIcon(get_icon("save"))
        self.btnSave.clicked.connect(self._save)
        action_row.addWidget(self.btnCancel)
        action_row.addWidget(self.btnSave)
        form.addRow("", action_row)

        layout.addWidget(self.grpForm, 2)

    # ------------------------------------------------------------------
    def reload(self) -> None:
        selected_id = self._current_id
        try:
            companies = list_companies()
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.critical(self, "Empresas", f"No fue posible cargar las empresas: {exc}")
            companies = []
        self._companies = {
            company.id: company for company in companies if company.id is not None
        }
        self._populate_groups()
        self._populate_list(selected_id)

    # ------------------------------------------------------------------
    def _populate_groups(self) -> None:
        config = load_config()
        current_group = self.cboGroup.currentData()
        self.cboGroup.blockSignals(True)
        self.cboGroup.clear()
        self.cboGroup.addItem("Sin grupo", None)
        group_keys = sorted({group.key for group in config.groups})
        for key in group_keys:
            self.cboGroup.addItem(key, key)
        self._set_group(current_group)
        self.cboGroup.blockSignals(False)

    # ------------------------------------------------------------------
    def _set_group(self, group_key: Optional[str]) -> None:
        if group_key is None:
            self.cboGroup.setCurrentIndex(0)
            return
        for idx in range(self.cboGroup.count()):
            if self.cboGroup.itemData(idx) == group_key:
                self.cboGroup.setCurrentIndex(idx)
                return
        self.cboGroup.setCurrentIndex(0)

    # ------------------------------------------------------------------
    def _populate_list(self, selected_id: Optional[int]) -> None:
        self.lstCompanies.blockSignals(True)
        self.lstCompanies.clear()
        sorted_companies = sorted(
            self._companies.values(),
            key=lambda company: ((company.name or "").lower(), company.id or 0),
        )
        for company in sorted_companies:
            item = QListWidgetItem(company.name or "(sin nombre)")
            item.setData(Qt.UserRole, company.id)
            subtitle = company.group_name or "Sin grupo"
            item.setToolTip(f"Grupo: {subtitle}")
            self.lstCompanies.addItem(item)
            if company.id and selected_id and company.id == selected_id:
                self.lstCompanies.setCurrentItem(item)
        self.lstCompanies.blockSignals(False)
        if self.lstCompanies.currentItem() is None and self.lstCompanies.count() > 0:
            self.lstCompanies.setCurrentRow(0)
        if self.lstCompanies.count() == 0:
            self._start_new()
        else:
            self._on_company_selected(self.lstCompanies.currentItem(), None)

    # ------------------------------------------------------------------
    def _start_new(self) -> None:
        self.lstCompanies.clearSelection()
        self._current_id = None
        self.txtName.clear()
        self._set_group(None)
        self.lblCreated.setText("-")
        self.lblUpdated.setText("-")
        self.spnNextSprint.setValue(1)
        self.txtName.setFocus()

    # ------------------------------------------------------------------
    def _on_company_selected(self, current: Optional[QListWidgetItem], previous: Optional[QListWidgetItem]) -> None:
        if current is None:
            return
        company_id = current.data(Qt.UserRole)
        company = self._companies.get(company_id)
        if not company:
            return
        self._current_id = company.id
        self.txtName.setText(company.name)
        self._set_group(company.group_name)
        self.lblCreated.setText(self._format_meta(company.created_by, company.created_at))
        self.lblUpdated.setText(self._format_meta(company.updated_by, company.updated_at))
        self.spnNextSprint.setValue(int(company.next_sprint_number or 1))

    # ------------------------------------------------------------------
    def _cancel(self) -> None:
        if self._current_id is None:
            self._start_new()
        else:
            company = self._companies.get(self._current_id)
            if company:
                self.txtName.setText(company.name)
                self._set_group(company.group_name)
                self.lblCreated.setText(self._format_meta(company.created_by, company.created_at))
                self.lblUpdated.setText(self._format_meta(company.updated_by, company.updated_at))
                self.spnNextSprint.setValue(int(company.next_sprint_number or 1))

    # ------------------------------------------------------------------
    def _save(self) -> None:
        name = self.txtName.text().strip()
        if not name:
            QMessageBox.warning(self, "Empresas", "El nombre es obligatorio.")
            return
        group_key = self.cboGroup.currentData()
        next_sprint = max(1, self.spnNextSprint.value())
        if self._current_id is not None:
            base_company = self._companies.get(self._current_id)
            company = Company(
                id=base_company.id if base_company else self._current_id,
                name=name,
                group_name=group_key,
                next_sprint_number=next_sprint,
                created_at=base_company.created_at if base_company else 0,
                created_by=base_company.created_by if base_company else current_username(""),
                updated_at=base_company.updated_at if base_company else 0,
                updated_by=base_company.updated_by if base_company else current_username(""),
            )
        else:
            company = Company(
                id=None,
                name=name,
                group_name=group_key,
                next_sprint_number=next_sprint,
            )
        try:
            saved = save_company(company)
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.critical(self, "Empresas", f"No fue posible guardar la empresa: {exc}")
            return
        self._current_id = saved.id
        if saved.id is not None:
            self._companies[saved.id] = saved
        self.reload()
        self._select_company(saved.id)

    # ------------------------------------------------------------------
    def _select_company(self, company_id: Optional[int]) -> None:
        if company_id is None:
            return
        for idx in range(self.lstCompanies.count()):
            item = self.lstCompanies.item(idx)
            if item.data(Qt.UserRole) == company_id:
                self.lstCompanies.setCurrentItem(item)
                return

    # ------------------------------------------------------------------
    def _format_meta(self, user: Optional[str], ts: int) -> str:
        if not ts:
            return user or "-"
        try:
            text = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M")
        except (TypeError, ValueError):
            text = "-"
        if user:
            return f"{user} — {text}"
        return text


class IncidenceCatalogView(QWidget):
    """Gestión del catálogo de tipos de incidencia."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._types: Dict[int, IncidenceType] = {}
        self._current_id: Optional[int] = None
        self._current_icon: Optional[bytes] = None
        self._setup_ui()
        self.reload()

    # ------------------------------------------------------------------
    def _setup_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        left = QVBoxLayout()
        left.setContentsMargins(0, 0, 0, 0)
        left.setSpacing(8)

        self.lstTypes = QListWidget()
        self.lstTypes.setSelectionMode(QListWidget.SingleSelection)
        self.lstTypes.currentItemChanged.connect(self._on_type_selected)
        left.addWidget(self.lstTypes, 1)

        buttons = QHBoxLayout()
        buttons.setContentsMargins(0, 0, 0, 0)
        buttons.setSpacing(8)
        self.btnNew = QPushButton("Nuevo tipo")
        self.btnNew.clicked.connect(self._start_new)
        self.btnDelete = QPushButton("Eliminar")
        self.btnDelete.clicked.connect(self._delete)
        self.btnDelete.setEnabled(False)
        self.btnRefresh = QPushButton("Recargar")
        self.btnRefresh.clicked.connect(self.reload)
        buttons.addWidget(self.btnNew)
        buttons.addWidget(self.btnDelete)
        buttons.addWidget(self.btnRefresh)
        left.addLayout(buttons)

        layout.addLayout(left, 1)

        self.grpForm = QGroupBox("Detalle del tipo")
        form = QFormLayout(self.grpForm)
        form.setLabelAlignment(Qt.AlignRight)
        form.setSpacing(8)

        self.txtName = QLineEdit()
        form.addRow("Nombre", self.txtName)

        color_row = QHBoxLayout()
        color_row.setContentsMargins(0, 0, 0, 0)
        color_row.setSpacing(6)
        self.txtColor = QLineEdit()
        self.txtColor.setPlaceholderText("#RRGGBB")
        color_row.addWidget(self.txtColor, 1)
        self.btnPickColor = QPushButton("Seleccionar color")
        self.btnPickColor.clicked.connect(self._pick_color)
        color_row.addWidget(self.btnPickColor)
        self.lblColorPreview = QLabel("    ")
        self.lblColorPreview.setMinimumWidth(36)
        self.lblColorPreview.setFixedHeight(24)
        self.lblColorPreview.setFrameShape(QLabel.Box)
        color_row.addWidget(self.lblColorPreview)
        form.addRow("Color", color_row)

        icon_row = QVBoxLayout()
        icon_row.setContentsMargins(0, 0, 0, 0)
        icon_row.setSpacing(6)
        self.lblIconPreview = QLabel("Sin icono")
        self.lblIconPreview.setAlignment(Qt.AlignCenter)
        self.lblIconPreview.setFixedSize(96, 96)
        self.lblIconPreview.setStyleSheet("border: 1px solid palette(mid);")
        icon_row.addWidget(self.lblIconPreview, alignment=Qt.AlignCenter)

        icon_buttons = QHBoxLayout()
        icon_buttons.setContentsMargins(0, 0, 0, 0)
        icon_buttons.setSpacing(6)
        self.btnPickIcon = QPushButton("Seleccionar icono")
        self.btnPickIcon.clicked.connect(self._pick_icon)
        self.btnClearIcon = QPushButton("Quitar icono")
        self.btnClearIcon.clicked.connect(self._clear_icon)
        icon_buttons.addWidget(self.btnPickIcon)
        icon_buttons.addWidget(self.btnClearIcon)
        icon_row.addLayout(icon_buttons)
        form.addRow("Icono", icon_row)

        action_row = QHBoxLayout()
        action_row.addStretch(1)
        self.btnCancel = QPushButton("Cancelar")
        self.btnCancel.clicked.connect(self._cancel)
        self.btnSave = QPushButton("Guardar")
        self.btnSave.setIcon(get_icon("save"))
        self.btnSave.clicked.connect(self._save)
        action_row.addWidget(self.btnCancel)
        action_row.addWidget(self.btnSave)
        form.addRow("", action_row)

        layout.addWidget(self.grpForm, 2)

    # ------------------------------------------------------------------
    def reload(self) -> None:
        selected = self._current_id
        try:
            types = list_incidence_types()
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.critical(
                self,
                "Tipos de incidencia",
                f"No fue posible cargar los tipos de incidencia: {exc}",
            )
            types = []
        self._types = {entry.id: entry for entry in types if entry.id is not None}
        self._populate_list(selected)

    # ------------------------------------------------------------------
    def _populate_list(self, selected: Optional[int]) -> None:
        self.lstTypes.blockSignals(True)
        self.lstTypes.clear()
        for entry in sorted(self._types.values(), key=lambda inc: (inc.name or "").lower()):
            if entry.id is None:
                continue
            item = QListWidgetItem(entry.name or "(sin nombre)")
            item.setData(Qt.UserRole, entry.id)
            self.lstTypes.addItem(item)
            if selected is not None and entry.id == selected:
                self.lstTypes.setCurrentItem(item)
        self.lstTypes.blockSignals(False)
        if self.lstTypes.currentItem() is None and self.lstTypes.count():
            self.lstTypes.setCurrentRow(0)
        if self.lstTypes.count() == 0:
            self._start_new()

    # ------------------------------------------------------------------
    def _start_new(self) -> None:
        self.lstTypes.clearSelection()
        self._current_id = None
        self._current_icon = None
        self.txtName.clear()
        self.txtColor.clear()
        self._update_color_preview(None)
        self._update_icon_preview(None)
        self.txtName.setFocus()
        self.btnDelete.setEnabled(False)

    # ------------------------------------------------------------------
    def _on_type_selected(
        self, current: Optional[QListWidgetItem], previous: Optional[QListWidgetItem]
    ) -> None:
        if current is None:
            self.btnDelete.setEnabled(False)
            return
        type_id = current.data(Qt.UserRole)
        entry = self._types.get(type_id)
        if not entry:
            self.btnDelete.setEnabled(False)
            return
        self._current_id = entry.id
        self.txtName.setText(entry.name or "")
        self.txtColor.setText(entry.color or "")
        self._update_color_preview(entry.color)
        icon_value = getattr(entry, "icon", None)
        if isinstance(icon_value, memoryview):
            icon_value = icon_value.tobytes()
        elif isinstance(icon_value, bytearray):
            icon_value = bytes(icon_value)
        self._current_icon = icon_value if isinstance(icon_value, (bytes, bytearray)) else None
        self._update_icon_preview(self._current_icon)
        self.btnDelete.setEnabled(self._current_id is not None)

    # ------------------------------------------------------------------
    def _pick_color(self) -> None:
        initial_text = self.txtColor.text().strip() or "#ffffff"
        initial_color = QColor(initial_text)
        if not initial_color.isValid():
            initial_color = QColor("#ffffff")
        color = QColorDialog.getColor(initial_color, self, "Selecciona un color")
        if not color.isValid():
            return
        self.txtColor.setText(color.name())
        self._update_color_preview(color.name())

    # ------------------------------------------------------------------
    def _pick_icon(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Selecciona un icono",
            "",
            "Imágenes (*.png *.jpg *.jpeg *.bmp *.ico);;Todos los archivos (*)",
        )
        if not path:
            return
        try:
            data = Path(path).read_bytes()
        except Exception as exc:  # pragma: no cover - dependencias externas
            QMessageBox.warning(
                self,
                "Icono",
                f"No fue posible leer el archivo seleccionado: {exc}",
            )
            return
        self._current_icon = data
        self._update_icon_preview(self._current_icon)

    # ------------------------------------------------------------------
    def _clear_icon(self) -> None:
        self._current_icon = None
        self._update_icon_preview(None)

    # ------------------------------------------------------------------
    def _update_color_preview(self, value: Optional[str]) -> None:
        color = (value or "").strip()
        if not color:
            self.lblColorPreview.setStyleSheet("border: 1px solid palette(mid);")
            return
        self.lblColorPreview.setStyleSheet(
            f"background-color: {color}; border: 1px solid palette(mid);"
        )

    # ------------------------------------------------------------------
    def _update_icon_preview(self, data: Optional[bytes]) -> None:
        if not data:
            self.lblIconPreview.setPixmap(QPixmap())
            self.lblIconPreview.setText("Sin icono")
            return
        pixmap = QPixmap()
        if pixmap.loadFromData(data):
            scaled = pixmap.scaled(
                self.lblIconPreview.size(),
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation,
            )
            self.lblIconPreview.setPixmap(scaled)
            self.lblIconPreview.setText("")
        else:
            self.lblIconPreview.setPixmap(QPixmap())
            self.lblIconPreview.setText("Sin icono")

    # ------------------------------------------------------------------
    def _cancel(self) -> None:
        if self._current_id is None:
            self._start_new()
            return
        entry = self._types.get(self._current_id)
        if entry:
            self.txtName.setText(entry.name or "")
            self.txtColor.setText(entry.color or "")
            self._update_color_preview(entry.color)
            icon_value = getattr(entry, "icon", None)
            if isinstance(icon_value, memoryview):
                icon_value = icon_value.tobytes()
            elif isinstance(icon_value, bytearray):
                icon_value = bytes(icon_value)
            self._current_icon = icon_value if isinstance(icon_value, (bytes, bytearray)) else None
            self._update_icon_preview(self._current_icon)
            self.btnDelete.setEnabled(True)
            return
        self._start_new()

    # ------------------------------------------------------------------
    def _save(self) -> None:
        name = self.txtName.text().strip()
        if not name:
            QMessageBox.warning(self, "Tipos de incidencia", "El nombre es obligatorio.")
            return
        color = (self.txtColor.text().strip() or None)
        icon_data = self._current_icon

        if self._current_id is not None:
            base = self._types.get(self._current_id)
            entry = IncidenceType(
                id=self._current_id,
                name=name,
                color=color,
                icon=icon_data,
                created_at=base.created_at if base else 0,
                created_by=base.created_by if base else current_username(""),
                updated_at=base.updated_at if base else 0,
                updated_by=base.updated_by if base else current_username(""),
            )
        else:
            entry = IncidenceType(id=None, name=name, color=color, icon=icon_data)

        try:
            saved = save_incidence_type(entry)
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.critical(self, "Tipos de incidencia", f"No se pudo guardar el tipo: {exc}")
            return

        self._current_id = saved.id
        if saved.id is not None:
            self._types[saved.id] = saved
        self.reload()
        self._select_type(saved.id)

    # ------------------------------------------------------------------
    def _delete(self) -> None:
        if self._current_id is None:
            return
        entry = self._types.get(self._current_id)
        name = entry.name if entry else "este tipo"
        confirm = QMessageBox.question(
            self,
            "Eliminar tipo",
            f"¿Eliminar {name}?",
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            remove_incidence_type(self._current_id)
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.critical(self, "Tipos de incidencia", f"No se pudo eliminar: {exc}")
            return
        self._current_id = None
        self._current_icon = None
        self.reload()

    # ------------------------------------------------------------------
    def _select_type(self, type_id: Optional[int]) -> None:
        if type_id is None:
            return
        for idx in range(self.lstTypes.count()):
            item = self.lstTypes.item(idx)
            if item.data(Qt.UserRole) == type_id:
                self.lstTypes.setCurrentItem(item)
                return

class CatalogsView(QWidget):
    """Panel con los catálogos disponibles."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)
        icon = QLabel()
        icon.setPixmap(get_icon("config").pixmap(24, 24))
        header.addWidget(icon)
        title = QLabel("Catálogos")
        title.setProperty("role", "title")
        header.addWidget(title)
        header.addStretch(1)
        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        self.company_view = CompanyCatalogView(self)
        self.incidence_view = IncidenceCatalogView(self)
        self.tabs.addTab(self.company_view, "Empresas")
        self.tabs.addTab(self.incidence_view, "Tipos de incidencia")
        layout.addWidget(self.tabs, 1)

    # ------------------------------------------------------------------
    def reload(self) -> None:
        self.company_view.reload()
        self.incidence_view.reload()


__all__ = ["CatalogsView", "CompanyCatalogView", "IncidenceCatalogView"]
