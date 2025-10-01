"""Vistas para la administración de catálogos."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
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
)

from ..core.catalog_queries import Company, list_companies, save_company
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
        self.tabs.addTab(self.company_view, "Empresas")
        layout.addWidget(self.tabs, 1)

    # ------------------------------------------------------------------
    def reload(self) -> None:
        self.company_view.reload()


__all__ = ["CatalogsView", "CompanyCatalogView"]
