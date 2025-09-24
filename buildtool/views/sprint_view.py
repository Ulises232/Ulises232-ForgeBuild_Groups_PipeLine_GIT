from __future__ import annotations

from typing import Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
    QLineEdit,
)

from ..core.config import Config, Sprint, Card
from ..core.config_queries import (
    iter_groups,
    iter_group_projects,
    iter_group_sprints,
    find_sprint,
)
from ..core.config_store import ConfigStore
from ..ui.icons import get_icon
from ..ui.widgets import combo_with_arrow


class SprintView(QWidget):
    """Gestión de sprints y tarjetas vinculadas a proyectos."""

    def __init__(self, cfg: Config, on_request_reload_config):
        super().__init__()
        self.cfg = cfg
        self.on_request_reload_config = on_request_reload_config
        self.store = ConfigStore()
        self._current_group_key: Optional[str] = None
        self._current_sprint_key: Optional[str] = None
        self._current_card_key: Optional[str] = None
        self._setup_ui()
        self._refresh_config_cache()
        self._populate_groups()

    # ------------------------------------------------------------------
    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(12)
        icon = QLabel()
        icon.setPixmap(get_icon("history").pixmap(32, 32))
        header.addWidget(icon)
        title = QLabel("Planeación de sprints")
        title.setProperty("role", "title")
        header.addWidget(title)
        header.addStretch(1)
        root.addLayout(header)

        splitter = QSplitter(Qt.Horizontal)
        root.addWidget(splitter, 1)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)

        group_row = QHBoxLayout()
        group_row.setContentsMargins(0, 0, 0, 0)
        group_row.setSpacing(6)
        group_row.addWidget(QLabel("Grupo:"))
        self.cboGroup = QComboBox()
        group_row.addWidget(combo_with_arrow(self.cboGroup))
        left_layout.addLayout(group_row)

        sprint_buttons = QHBoxLayout()
        sprint_buttons.setContentsMargins(0, 0, 0, 0)
        sprint_buttons.setSpacing(6)
        self.btnNewSprint = self._make_tool_button("Nuevo sprint", "build")
        self.btnDeleteSprint = self._make_tool_button("Eliminar", "delete")
        sprint_buttons.addWidget(self.btnNewSprint)
        sprint_buttons.addWidget(self.btnDeleteSprint)
        sprint_buttons.addStretch(1)
        left_layout.addLayout(sprint_buttons)

        self.lstSprints = QListWidget()
        self.lstSprints.setSelectionMode(QListWidget.SingleSelection)
        left_layout.addWidget(self.lstSprints, 1)

        sprint_box = QGroupBox("Detalle del sprint")
        sprint_form = QFormLayout(sprint_box)
        sprint_form.setLabelAlignment(Qt.AlignLeft)
        sprint_form.setFormAlignment(Qt.AlignLeft)
        self.txtSprintKey = QLineEdit()
        sprint_form.addRow("Clave:", self.txtSprintKey)
        self.txtSprintName = QLineEdit()
        sprint_form.addRow("Nombre:", self.txtSprintName)
        self.txtSprintGoal = QLineEdit()
        sprint_form.addRow("Objetivo:", self.txtSprintGoal)
        self.txtSprintStart = QLineEdit()
        sprint_form.addRow("Inicio:", self.txtSprintStart)
        self.txtSprintEnd = QLineEdit()
        sprint_form.addRow("Fin:", self.txtSprintEnd)
        self.btnSaveSprint = QPushButton("Guardar sprint")
        sprint_form.addRow(self.btnSaveSprint)
        left_layout.addWidget(sprint_box)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        card_buttons = QHBoxLayout()
        card_buttons.setContentsMargins(0, 0, 0, 0)
        card_buttons.setSpacing(6)
        self.btnNewCard = self._make_tool_button("Nueva tarjeta", "build")
        self.btnDeleteCard = self._make_tool_button("Eliminar", "delete")
        card_buttons.addWidget(self.btnNewCard)
        card_buttons.addWidget(self.btnDeleteCard)
        card_buttons.addStretch(1)
        right_layout.addLayout(card_buttons)

        self.lstCards = QListWidget()
        self.lstCards.setSelectionMode(QListWidget.SingleSelection)
        right_layout.addWidget(self.lstCards, 1)

        card_box = QGroupBox("Detalle de la tarjeta")
        card_form = QFormLayout(card_box)
        card_form.setLabelAlignment(Qt.AlignLeft)
        card_form.setFormAlignment(Qt.AlignLeft)
        self.txtCardKey = QLineEdit()
        card_form.addRow("Clave:", self.txtCardKey)
        self.txtCardTitle = QLineEdit()
        card_form.addRow("Título:", self.txtCardTitle)
        self.cboCardProject = QComboBox()
        card_form.addRow("Proyecto:", combo_with_arrow(self.cboCardProject))
        self.txtCardVersion = QLineEdit()
        card_form.addRow("Versión:", self.txtCardVersion)
        self.txtCardOwners = QLineEdit()
        self.txtCardOwners.setPlaceholderText("Responsables separados por coma")
        card_form.addRow("Responsables:", self.txtCardOwners)
        self.chkTests = QCheckBox("Pruebas ejecutadas")
        self.chkQA = QCheckBox("QA aprobado")
        checks = QHBoxLayout()
        checks.setSpacing(12)
        checks.addWidget(self.chkTests)
        checks.addWidget(self.chkQA)
        checks.addStretch(1)
        card_form.addRow(checks)
        self.txtCardNotes = QTextEdit()
        self.txtCardNotes.setPlaceholderText("Notas o pendientes")
        self.txtCardNotes.setFixedHeight(80)
        card_form.addRow("Notas:", self.txtCardNotes)
        self.btnSaveCard = QPushButton("Guardar tarjeta")
        card_form.addRow(self.btnSaveCard)
        right_layout.addWidget(card_box)

        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 1)

        self.cboGroup.currentIndexChanged.connect(self._on_group_changed)
        self.lstSprints.currentItemChanged.connect(self._on_sprint_selected)
        self.lstCards.currentItemChanged.connect(self._on_card_selected)
        self.btnNewSprint.clicked.connect(self._prepare_new_sprint)
        self.btnDeleteSprint.clicked.connect(self._delete_current_sprint)
        self.btnSaveSprint.clicked.connect(self._save_sprint)
        self.btnNewCard.clicked.connect(self._prepare_new_card)
        self.btnDeleteCard.clicked.connect(self._delete_current_card)
        self.btnSaveCard.clicked.connect(self._save_card)

    # ------------------------------------------------------------------
    def _make_tool_button(self, text: str, icon_name: str) -> QToolButton:
        btn = QToolButton()
        btn.setText(text)
        btn.setIcon(get_icon(icon_name))
        btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        btn.setAutoRaise(True)
        return btn

    # ------------------------------------------------------------------
    def _refresh_config_cache(self) -> None:
        try:
            self.cfg.sprints = self.store.list_sprints()
        except Exception as exc:
            QMessageBox.warning(self, "Sprints", f"No fue posible cargar los sprints: {exc}")

    # ------------------------------------------------------------------
    def _populate_groups(self) -> None:
        keys = [group.key for group in iter_groups(self.cfg)]
        block = self.cboGroup.blockSignals(True)
        self.cboGroup.clear()
        for key in keys:
            self.cboGroup.addItem(key, key)
        self.cboGroup.blockSignals(block)
        if keys:
            self.cboGroup.setCurrentIndex(0)
            self._on_group_changed(0)
        else:
            self._current_group_key = None
            self._refresh_sprint_list()

    # ------------------------------------------------------------------
    @Slot(int)
    def _on_group_changed(self, index: int) -> None:
        self._current_group_key = self.cboGroup.itemData(index)
        self._current_sprint_key = None
        self._current_card_key = None
        self._update_project_combo()
        self._refresh_sprint_list()
        self._clear_sprint_form()
        self._clear_card_form()

    # ------------------------------------------------------------------
    def _update_project_combo(self) -> None:
        block = self.cboCardProject.blockSignals(True)
        self.cboCardProject.clear()
        self.cboCardProject.addItem("", "")
        if self._current_group_key:
            for _, project in iter_group_projects(self.cfg, self._current_group_key):
                self.cboCardProject.addItem(project.key, project.key)
        self.cboCardProject.blockSignals(block)

    # ------------------------------------------------------------------
    def _refresh_sprint_list(self) -> None:
        block = self.lstSprints.blockSignals(True)
        self.lstSprints.clear()
        if not self._current_group_key:
            self.lstSprints.blockSignals(block)
            return
        for sprint in iter_group_sprints(self.cfg, self._current_group_key):
            item = QListWidgetItem(sprint.name or sprint.key)
            item.setData(Qt.UserRole, sprint.key)
            self.lstSprints.addItem(item)
        self.lstSprints.blockSignals(block)
        if self.lstSprints.count():
            target_key = self._current_sprint_key or self.lstSprints.item(0).data(Qt.UserRole)
            self._select_sprint_by_key(str(target_key))

    # ------------------------------------------------------------------
    def _select_sprint_by_key(self, sprint_key: str) -> None:
        for row in range(self.lstSprints.count()):
            if self.lstSprints.item(row).data(Qt.UserRole) == sprint_key:
                self.lstSprints.setCurrentRow(row)
                return
        self.lstSprints.clearSelection()
        self._current_sprint_key = None
        self._clear_sprint_form()
        self._refresh_card_list()

    # ------------------------------------------------------------------
    @Slot(QListWidgetItem, QListWidgetItem)
    def _on_sprint_selected(
        self, current: QListWidgetItem, _previous: QListWidgetItem
    ) -> None:
        if not current:
            self._current_sprint_key = None
            self._clear_sprint_form()
            self._refresh_card_list()
            return
        self._current_sprint_key = current.data(Qt.UserRole)
        sprint = find_sprint(self.cfg, self._current_sprint_key, self._current_group_key)
        self._load_sprint_into_form(sprint)
        self._refresh_card_list()

    # ------------------------------------------------------------------
    def _load_sprint_into_form(self, sprint: Optional[Sprint]) -> None:
        if not sprint:
            self._clear_sprint_form()
            return
        self.txtSprintKey.setText(sprint.key)
        self.txtSprintName.setText(sprint.name)
        self.txtSprintGoal.setText(sprint.goal or "")
        self.txtSprintStart.setText(sprint.start_date or "")
        self.txtSprintEnd.setText(sprint.end_date or "")

    # ------------------------------------------------------------------
    def _clear_sprint_form(self) -> None:
        self.txtSprintKey.clear()
        self.txtSprintName.clear()
        self.txtSprintGoal.clear()
        self.txtSprintStart.clear()
        self.txtSprintEnd.clear()

    # ------------------------------------------------------------------
    def _refresh_card_list(self) -> None:
        block = self.lstCards.blockSignals(True)
        self.lstCards.clear()
        if not (self._current_group_key and self._current_sprint_key):
            self.lstCards.blockSignals(block)
            return
        sprint = find_sprint(self.cfg, self._current_sprint_key, self._current_group_key)
        if sprint:
            for card in sprint.cards:
                item = QListWidgetItem(card.title or card.key)
                item.setData(Qt.UserRole, card.key)
                self.lstCards.addItem(item)
        self.lstCards.blockSignals(block)
        if self.lstCards.count():
            target = self._current_card_key or self.lstCards.item(0).data(Qt.UserRole)
            self._select_card_by_key(str(target))
        else:
            self._current_card_key = None
            self._clear_card_form()

    # ------------------------------------------------------------------
    def _select_card_by_key(self, card_key: str) -> None:
        for row in range(self.lstCards.count()):
            if self.lstCards.item(row).data(Qt.UserRole) == card_key:
                self.lstCards.setCurrentRow(row)
                return
        self.lstCards.clearSelection()
        self._current_card_key = None
        self._clear_card_form()

    # ------------------------------------------------------------------
    @Slot(QListWidgetItem, QListWidgetItem)
    def _on_card_selected(self, current: QListWidgetItem, _previous: QListWidgetItem) -> None:
        if not current:
            self._current_card_key = None
            self._clear_card_form()
            return
        self._current_card_key = current.data(Qt.UserRole)
        sprint = find_sprint(self.cfg, self._current_sprint_key, self._current_group_key)
        card = None
        if sprint:
            card = next((c for c in sprint.cards if c.key == self._current_card_key), None)
        self._load_card_into_form(card)

    # ------------------------------------------------------------------
    def _load_card_into_form(self, card: Optional[Card]) -> None:
        if not card:
            self._clear_card_form()
            return
        self.txtCardKey.setText(card.key)
        self.txtCardTitle.setText(card.title)
        self._set_project_selection(card.project_key)
        self.txtCardVersion.setText(card.version or "")
        self.txtCardOwners.setText(", ".join(card.owners) if card.owners else "")
        self.chkTests.setChecked(bool(card.tests_ready))
        self.chkQA.setChecked(bool(card.qa_ready))
        self.txtCardNotes.setPlainText(card.notes or "")

    # ------------------------------------------------------------------
    def _set_project_selection(self, project_key: Optional[str]) -> None:
        if project_key is None:
            self.cboCardProject.setCurrentIndex(0)
            return
        for idx in range(self.cboCardProject.count()):
            if self.cboCardProject.itemData(idx) == project_key:
                self.cboCardProject.setCurrentIndex(idx)
                return
        self.cboCardProject.setCurrentIndex(0)

    # ------------------------------------------------------------------
    def _clear_card_form(self) -> None:
        self.txtCardKey.clear()
        self.txtCardTitle.clear()
        self.cboCardProject.setCurrentIndex(0)
        self.txtCardVersion.clear()
        self.txtCardOwners.clear()
        self.chkTests.setChecked(False)
        self.chkQA.setChecked(False)
        self.txtCardNotes.clear()

    # ------------------------------------------------------------------
    @Slot()
    def _prepare_new_sprint(self) -> None:
        self.lstSprints.clearSelection()
        self._current_sprint_key = None
        self._clear_sprint_form()
        self._clear_card_form()
        self.lstCards.clear()

    # ------------------------------------------------------------------
    @Slot()
    def _delete_current_sprint(self) -> None:
        if not (self._current_group_key and self._current_sprint_key):
            return
        confirm = QMessageBox.question(
            self,
            "Sprints",
            "¿Eliminar el sprint seleccionado y todas sus tarjetas?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            self.store.delete_sprint(self._current_group_key, self._current_sprint_key)
        except Exception as exc:
            QMessageBox.critical(self, "Sprints", f"No se pudo eliminar el sprint: {exc}")
            return
        self._refresh_config_cache()
        self._current_sprint_key = None
        self._refresh_sprint_list()
        self._refresh_card_list()

    # ------------------------------------------------------------------
    @Slot()
    def _save_sprint(self) -> None:
        if not self._current_group_key:
            QMessageBox.information(self, "Sprints", "Seleccione un grupo antes de guardar.")
            return
        key = self.txtSprintKey.text().strip()
        name = self.txtSprintName.text().strip()
        if not key or not name:
            QMessageBox.warning(self, "Sprints", "La clave y el nombre del sprint son obligatorios.")
            return
        existing = find_sprint(self.cfg, key, self._current_group_key)
        cards = list(existing.cards) if existing else []
        sprint = Sprint(
            key=key,
            name=name,
            group_key=self._current_group_key,
            goal=self.txtSprintGoal.text().strip() or None,
            start_date=self.txtSprintStart.text().strip() or None,
            end_date=self.txtSprintEnd.text().strip() or None,
            cards=cards,
        )
        try:
            self.store.upsert_sprint(sprint)
        except Exception as exc:
            QMessageBox.critical(self, "Sprints", f"No se pudo guardar el sprint: {exc}")
            return
        self._refresh_config_cache()
        self._current_sprint_key = key
        self._refresh_sprint_list()
        self._select_sprint_by_key(key)

    # ------------------------------------------------------------------
    @Slot()
    def _prepare_new_card(self) -> None:
        if not (self._current_group_key and self._current_sprint_key):
            QMessageBox.information(self, "Tarjetas", "Seleccione un sprint antes de añadir tarjetas.")
            return
        self.lstCards.clearSelection()
        self._current_card_key = None
        self._clear_card_form()

    # ------------------------------------------------------------------
    @Slot()
    def _delete_current_card(self) -> None:
        if not (self._current_group_key and self._current_sprint_key and self._current_card_key):
            return
        confirm = QMessageBox.question(
            self,
            "Tarjetas",
            "¿Eliminar la tarjeta seleccionada?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        try:
            self.store.delete_card(self._current_group_key, self._current_sprint_key, self._current_card_key)
        except Exception as exc:
            QMessageBox.critical(self, "Tarjetas", f"No se pudo eliminar la tarjeta: {exc}")
            return
        self._refresh_config_cache()
        self._current_card_key = None
        self._refresh_card_list()

    # ------------------------------------------------------------------
    @Slot()
    def _save_card(self) -> None:
        if not (self._current_group_key and self._current_sprint_key):
            QMessageBox.information(self, "Tarjetas", "Seleccione un sprint antes de guardar.")
            return
        key = self.txtCardKey.text().strip()
        title = self.txtCardTitle.text().strip()
        if not key or not title:
            QMessageBox.warning(self, "Tarjetas", "La clave y el título de la tarjeta son obligatorios.")
            return
        project_key = self.cboCardProject.currentData()
        project_key = project_key or None
        owners_raw = self.txtCardOwners.text().split(",")
        owners = [owner.strip() for owner in owners_raw if owner.strip()]
        card = Card(
            key=key,
            title=title,
            project_key=project_key,
            version=self.txtCardVersion.text().strip() or None,
            owners=owners,
            tests_ready=self.chkTests.isChecked(),
            qa_ready=self.chkQA.isChecked(),
            notes=self.txtCardNotes.toPlainText().strip() or None,
        )
        try:
            self.store.upsert_card(self._current_group_key, self._current_sprint_key, card)
        except Exception as exc:
            QMessageBox.critical(self, "Tarjetas", f"No se pudo guardar la tarjeta: {exc}")
            return
        self._refresh_config_cache()
        self._current_card_key = key
        self._refresh_card_list()
        self._select_card_by_key(key)


__all__ = ["SprintView"]
