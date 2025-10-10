from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QBrush, QIcon, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QListWidgetItem,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTabWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..core.branch_store import (
    BranchRecord,
    Card,
    CardScript,
    Sprint,
    assign_cards_to_sprint,
    collect_sprint_scripts,
    delete_card,
    delete_card_script,
    delete_sprint,
    list_cards,
    list_sprints,
    list_users,
    list_user_roles,
    load_card_script,
    load_index,
    save_card_script,
    upsert_card,
    upsert_sprint,
)
from ..core.catalog_queries import (
    Company,
    IncidenceType,
    list_companies as list_company_catalog,
    list_incidence_types as list_incidence_catalog,
)
from ..core.card_importer import (
    CardImportError,
    CardImportSummary,
    import_cards_from_file,
    write_cards_template,
)
from ..core.config import load_config
from ..core.git_tasks_local import create_branches_local
from ..core.pipeline_history import PipelineHistory
from ..core.session import current_username, get_active_user, require_roles
from ..core.sprint_queries import branches_by_group, is_card_ready_for_merge
from .sprint_helpers import filter_users_by_role
from ..ui.color_utils import status_brushes
from ..ui.icons import get_icon
from .editor_forms import CardFormWidget, SprintFormWidget
from .form_dialogs import FormDialog


def _card_has_assigned_sprint(card: Card) -> bool:
    sprint_id = getattr(card, "sprint_id", None)
    if sprint_id in (None, "", 0):
        return False
    try:
        return int(sprint_id) != 0
    except (TypeError, ValueError):
        return bool(sprint_id)

class SprintView(QWidget):
    """Single window to manage sprints and cards."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._sprints: Dict[int, Sprint] = {}
        self._cards: Dict[int, Card] = {}
        self._branch_index: Dict[str, BranchRecord] = {}
        self._companies: Dict[int, Company] = {}
        self._companies_by_group: Dict[Optional[str], List[Company]] = {}
        self._incidence_types: Dict[int, IncidenceType] = {}
        self._incidence_icons: Dict[int, QIcon] = {}
        self._users: List[str] = []
        self._user_roles: Dict[str, List[str]] = {}
        self._cfg = load_config()

        self._selected_sprint_id: Optional[int] = None
        self._selected_card_id: Optional[int] = None
        self._card_parent_id: Optional[int] = None
        self._current_sprint_branch_key: Optional[str] = None
        self._current_sprint_qa_branch_key: Optional[str] = None
        self._current_card_base: str = ""
        self._branch_override: bool = False
        self._sprint_filter_group: Optional[str] = None
        self._sprint_filter_status: Optional[str] = None
        self._card_form_card: Optional[Card] = None
        self._card_form_sprint: Optional[Sprint] = None
        self._card_form_script: Optional[CardScript] = None
        self._card_script_dirty: bool = False
        self._card_script_deleted: bool = False
        self._card_script_filename: Optional[str] = None
        self._card_script_original_text: str = ""
        self._updating_script_text: bool = False
        self._sprint_form_sprint: Optional[Sprint] = None
        self._unassigned_cards: Dict[int, Card] = {}
        self._selected_card_ids: List[int] = []
        self._sprint_dialog: Optional[FormDialog] = None
        self._card_dialog: Optional[FormDialog] = None
        self._active_form: Optional[str] = None

        self._setup_ui()
        self.refresh()

    # ------------------------------------------------------------------
    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)

        icon = QLabel()
        icon.setPixmap(get_icon("history").pixmap(32, 32))
        header.addWidget(icon)

        title = QLabel("Planeación de Sprints")
        title.setProperty("role", "title")
        header.addWidget(title)
        header.addStretch(1)

        self.btnRefresh = QPushButton("Refrescar")
        self.btnRefresh.setIcon(get_icon("refresh"))
        header.addWidget(self.btnRefresh)

        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        layout.addWidget(self.tabs, 1)

        self.planning_page = self._build_planning_tab()
        self.tabs.addTab(self.planning_page, "Planeación")

        self.card_browser = CardBrowser(self)
        self.card_browser.cardActivated.connect(self._open_card_from_browser)
        self.card_browser.newCardRequested.connect(self._start_new_card_from_browser)
        self.card_browser.cardsImported.connect(self.refresh)
        self.tabs.addTab(self.card_browser, "Tarjetas")

        self.btnRefresh.clicked.connect(self.refresh)

        self.update_permissions()

    # ------------------------------------------------------------------
    def _clear_sprint_form_refs(self) -> None:
        """Remove sprint-form widgets so deleted Qt objects aren't reused."""
        self._sprint_form_sprint = None
        names = [
            "pageSprint",
            "cboSprintGroup",
            "txtSprintBranch",
            "btnPickBranch",
            "txtSprintQABranch",
            "btnPickQABranch",
            "txtSprintName",
            "txtSprintVersion",
            "cboCompany",
            "lblSprintSequence",
            "cboSprintLead",
            "cboSprintQA",
            "chkSprintClosed",
            "lblSprintMeta",
            "btnSprintDelete",
            "btnSprintCancel",
            "btnSprintSave",
            "pending_box",
            "lstUnassignedCards",
            "btnAssignCards",
        ]
        for name in names:
            if hasattr(self, name):
                delattr(self, name)

    # ------------------------------------------------------------------
    def _clear_card_form_refs(self) -> None:
        """Remove card-form widgets so deleted Qt objects aren't reused."""
        self._card_form_card = None
        self._card_form_sprint = None
        self._card_form_script = None
        self._card_script_dirty = False
        self._card_script_deleted = False
        self._card_script_filename = None
        self._card_script_original_text = ""
        self._updating_script_text = False
        names = [
            "pageCard",
            "lblCardSprint",
            "cboCardSprint",
            "cboCardGroup",
            "cboCardCompany",
            "cboCardIncidence",
            "lblCardStatus",
            "txtCardTicket",
            "txtCardTitle",
            "lblCardPrefix",
            "txtCardBranch",
            "lblCardBranchPreview",
            "cboCardAssignee",
            "cboCardQA",
            "txtCardUnitUrl",
            "txtCardQAUrl",
            "lblCardChecks",
            "lblCardLocal",
            "lblCardOrigin",
            "lblCardCreator",
            "btnCardDelete",
            "btnCardMarkUnit",
            "btnCardMarkQA",
            "btnCardCreateBranch",
            "btnCardCancel",
            "btnCardSave",
            "script_box",
            "lblCardScriptInfo",
            "btnCardLoadScript",
            "btnCardDeleteScript",
            "txtCardScript",
        ]
        for name in names:
            if hasattr(self, name):
                delattr(self, name)

    # ------------------------------------------------------------------
    def _build_planning_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        filter_row = QHBoxLayout()
        filter_row.setContentsMargins(0, 0, 0, 0)
        filter_row.setSpacing(6)

        lbl_group = QLabel("Grupo:")
        filter_row.addWidget(lbl_group)
        self.cboSprintFilterGroup = QComboBox()
        self.cboSprintFilterGroup.addItem("Todos los grupos", None)
        filter_row.addWidget(self.cboSprintFilterGroup, 1)

        lbl_status = QLabel("Estado:")
        filter_row.addWidget(lbl_status)
        self.cboSprintFilterStatus = QComboBox()
        self.cboSprintFilterStatus.addItem("Todos los estados", None)
        self.cboSprintFilterStatus.addItem("Abiertos", "open")
        self.cboSprintFilterStatus.addItem("Cerrados", "closed")
        filter_row.addWidget(self.cboSprintFilterStatus, 1)

        filter_row.addStretch(1)
        layout.addLayout(filter_row)

        action_row = QHBoxLayout()
        action_row.setContentsMargins(0, 0, 0, 0)
        action_row.setSpacing(6)

        self.btnNewSprint = QPushButton("Nuevo sprint")
        self.btnNewSprint.setIcon(get_icon("branch"))
        action_row.addWidget(self.btnNewSprint)

        self.btnNewCard = QPushButton("Nueva tarjeta")
        self.btnNewCard.setIcon(get_icon("build"))
        action_row.addWidget(self.btnNewCard)

        self.btnExportScripts = QPushButton("Exportar scripts del sprint")
        self.btnExportScripts.setIcon(get_icon("cloud-download"))
        action_row.addWidget(self.btnExportScripts)

        self.btnBulkAssign = QPushButton("Asignar responsables")
        self.btnBulkAssign.setIcon(get_icon("sync"))
        self.btnBulkAssign.setEnabled(False)
        action_row.addWidget(self.btnBulkAssign)

        action_row.addStretch(1)
        layout.addLayout(action_row)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(
            [
                "Sprint/Tarjeta",
                "Tipo incidencia",
                "Asignado",
                "QA",
                "Empresa",
                "Script",
                "Estado / Checks",
                "Rama",
                "Rama QA",
                "Local",
                "Origen",
                "Creada por",
            ]
        )
        self.tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.tree.setUniformRowHeights(True)
        layout.addWidget(self.tree, 1)

        self.cboSprintFilterGroup.currentIndexChanged.connect(self._apply_sprint_filters)
        self.cboSprintFilterStatus.currentIndexChanged.connect(self._apply_sprint_filters)
        self.btnNewSprint.clicked.connect(self._start_new_sprint)
        self.btnNewCard.clicked.connect(self._start_new_card)
        self.btnExportScripts.clicked.connect(self._on_export_sprint_scripts)
        self.btnBulkAssign.clicked.connect(self._on_bulk_assign_clicked)
        self.tree.itemSelectionChanged.connect(self._on_selection_changed)
        self.tree.itemActivated.connect(self._on_planning_item_activated)
        self.tree.itemDoubleClicked.connect(self._on_planning_item_activated)

        return page

    # ------------------------------------------------------------------
    def _create_sprint_form(self) -> SprintFormWidget:
        form = SprintFormWidget()
        self.pageSprint = form
        self.cboSprintGroup = form.cboSprintGroup
        self.cboSprintGroup.currentIndexChanged.connect(self._on_sprint_group_changed)
        self.txtSprintBranch = form.txtSprintBranch
        self.btnPickBranch = form.btnPickBranch
        self.btnPickBranch.clicked.connect(self._on_pick_branch)
        self.txtSprintQABranch = form.txtSprintQABranch
        self.btnPickQABranch = form.btnPickQABranch
        self.btnPickQABranch.clicked.connect(self._on_pick_qa_branch)
        self.txtSprintName = form.txtSprintName
        self.txtSprintVersion = form.txtSprintVersion
        self.cboCompany = form.cboCompany
        self.cboCompany.currentIndexChanged.connect(self._on_company_changed)
        self.lblSprintSequence = form.lblSprintSequence
        self.cboSprintLead = form.cboSprintLead
        self.cboSprintQA = form.cboSprintQA
        self.chkSprintClosed = form.chkSprintClosed
        self.lblSprintMeta = form.lblSprintMeta
        self.btnSprintDelete = form.btnSprintDelete
        self.btnSprintCancel = form.btnSprintCancel
        self.btnSprintSave = form.btnSprintSave
        self.pending_box = form.pending_box
        self.lstUnassignedCards = form.lstUnassignedCards
        self.btnAssignCards = form.btnAssignCards
        self.btnSprintSave.clicked.connect(self._on_save_sprint)
        self.btnSprintCancel.clicked.connect(self._on_cancel)
        self.btnSprintDelete.clicked.connect(self._on_delete_sprint)
        self.btnAssignCards.clicked.connect(self._on_assign_pending_cards)
        return form

    # ------------------------------------------------------------------
    def _create_card_form(self) -> CardFormWidget:
        form = CardFormWidget()
        self.pageCard = form
        self.lblCardSprint = form.lblCardSprint
        self.cboCardSprint = form.cboCardSprint
        self.cboCardSprint.currentIndexChanged.connect(self._on_card_sprint_changed)
        self.cboCardGroup = form.cboCardGroup
        self.cboCardGroup.currentIndexChanged.connect(self._on_card_group_changed)
        self.cboCardCompany = form.cboCardCompany
        self.cboCardCompany.currentIndexChanged.connect(self._on_card_company_changed)
        self.cboCardIncidence = form.cboCardIncidence
        self.lblCardStatus = form.lblCardStatus
        self.txtCardTicket = form.txtCardTicket
        self.txtCardTicket.textChanged.connect(self._on_ticket_changed)
        self.txtCardTitle = form.txtCardTitle
        self.lblCardPrefix = form.lblCardPrefix
        self.txtCardBranch = form.txtCardBranch
        self.txtCardBranch.textChanged.connect(self._on_branch_text_changed)
        self.lblCardBranchPreview = form.lblCardBranchPreview
        self.cboCardAssignee = form.cboCardAssignee
        self.cboCardQA = form.cboCardQA
        self.txtCardUnitUrl = form.txtCardUnitUrl
        self.txtCardQAUrl = form.txtCardQAUrl
        self.lblCardChecks = form.lblCardChecks
        self.lblCardLocal = form.lblCardLocal
        self.lblCardOrigin = form.lblCardOrigin
        self.lblCardCreator = form.lblCardCreator
        self.btnCardDelete = form.btnCardDelete
        self.btnCardMarkUnit = form.btnCardMarkUnit
        self.btnCardMarkQA = form.btnCardMarkQA
        self.btnCardCreateBranch = form.btnCardCreateBranch
        self.btnCardCancel = form.btnCardCancel
        self.btnCardSave = form.btnCardSave
        self.script_box = form.script_box
        self.lblCardScriptInfo = form.lblCardScriptInfo
        self.btnCardLoadScript = form.btnCardLoadScript
        self.btnCardDeleteScript = form.btnCardDeleteScript
        self.txtCardScript = form.txtCardScript
        self.btnCardSave.clicked.connect(self._on_save_card)
        self.btnCardCancel.clicked.connect(self._on_cancel)
        self.btnCardDelete.clicked.connect(self._on_delete_card)
        self.btnCardCreateBranch.clicked.connect(self._on_create_branch)
        self.btnCardMarkUnit.clicked.connect(lambda: self._mark_card("unit"))
        self.btnCardMarkQA.clicked.connect(lambda: self._mark_card("qa"))
        self.btnCardLoadScript.clicked.connect(self._on_load_card_script_from_file)
        self.btnCardDeleteScript.clicked.connect(self._on_delete_card_script)
        self.txtCardScript.textChanged.connect(self._on_card_script_changed)
        return form

    # ------------------------------------------------------------------
    def _close_sprint_dialog(self) -> None:
        dialog = self._sprint_dialog
        if dialog is None:
            return
        self._sprint_dialog = None
        self._unassigned_cards = {}
        if hasattr(self, "lstUnassignedCards"):
            try:
                self.lstUnassignedCards.clear()
                self.lstUnassignedCards.setEnabled(False)
            except RuntimeError:
                pass
        if hasattr(self, "btnAssignCards"):
            try:
                self.btnAssignCards.setEnabled(False)
            except RuntimeError:
                pass
        if hasattr(self, "pending_box"):
            try:
                self.pending_box.setVisible(False)
            except RuntimeError:
                pass
        if dialog.isVisible():
            dialog.close()
        else:
            self._on_dialog_closed("sprint")

    # ------------------------------------------------------------------
    def _close_card_dialog(self) -> None:
        dialog = self._card_dialog
        if dialog is None:
            return
        self._card_dialog = None
        was_visible = dialog.isVisible()
        if was_visible:
            try:
                dialog.hide()
            except RuntimeError:
                pass
        handled = False
        if was_visible:
            try:
                dialog.done(QDialog.Rejected)
                handled = True
            except RuntimeError:
                try:
                    dialog.reject()
                    handled = True
                except RuntimeError:
                    pass
        if dialog.isVisible():
            try:
                dialog.close()
                handled = True
            except RuntimeError:
                pass
        if not handled:
            self._on_dialog_closed("card")

    # ------------------------------------------------------------------
    def _on_dialog_closed(self, kind: str) -> None:
        if kind == "sprint":
            self._clear_sprint_form_refs()
            self._sprint_dialog = None
        elif kind == "card":
            self._clear_card_form_refs()
            self._card_dialog = None
        if self._active_form == kind:
            self._active_form = None
        self.update_permissions()

    # ------------------------------------------------------------------
    def update_permissions(self) -> None:
        username = self._current_user()
        can_lead = require_roles("leader")
        sprint_mode = self._active_form == "sprint"
        card_mode = self._active_form == "card"
        card = self._cards.get(self._selected_card_id or -1)
        sprint = None
        if card and card.sprint_id:
            sprint = self._sprints.get(card.sprint_id)
        elif self._card_parent_id:
            sprint = self._sprints.get(self._card_parent_id)
        elif self._selected_sprint_id is not None:
            sprint = self._sprints.get(self._selected_sprint_id)

        self.btnNewSprint.setEnabled(can_lead)

        if hasattr(self, 'btnPickBranch'):
            self.btnPickBranch.setEnabled(can_lead and sprint_mode)
        if hasattr(self, 'btnPickQABranch'):
            self.btnPickQABranch.setEnabled(can_lead and sprint_mode)
        if hasattr(self, 'btnSprintSave'):
            self.btnSprintSave.setEnabled(can_lead and sprint_mode)
        if hasattr(self, 'btnSprintDelete'):
            self.btnSprintDelete.setEnabled(can_lead and sprint_mode and self._selected_sprint_id is not None)
        if hasattr(self, 'chkSprintClosed'):
            self.chkSprintClosed.setEnabled(can_lead and sprint_mode)
        if hasattr(self, 'btnAssignCards'):
            can_assign = can_lead and sprint_mode and bool(self._unassigned_cards)
            self.btnAssignCards.setEnabled(can_assign)
            if hasattr(self, 'lstUnassignedCards'):
                self.lstUnassignedCards.setEnabled(can_assign)

        has_card = card is not None and card.id is not None
        is_card_assignee = bool(card and card.assignee and card.assignee == username)
        is_card_qa = bool(card and card.qa_assignee and card.qa_assignee == username)
        allow_unit_toggle = card_mode and has_card and (can_lead or is_card_assignee)
        allow_qa_toggle = card_mode and has_card and (can_lead or is_card_qa)
        can_edit_card = card_mode and (can_lead or is_card_assignee or is_card_qa)

        if hasattr(self, 'btnCardSave'):
            self.btnCardSave.setEnabled(card_mode and (can_lead or is_card_assignee or is_card_qa))
        if hasattr(self, 'btnCardDelete'):
            self.btnCardDelete.setEnabled(can_lead and card_mode and self._selected_card_id is not None)
        if hasattr(self, 'btnCardCreateBranch'):
            self.btnCardCreateBranch.setEnabled(card_mode and (can_lead or is_card_assignee))

        if hasattr(self, 'btnCardMarkUnit'):
            if card and card.unit_tests_done:
                self.btnCardMarkUnit.setText('Desmarcar pruebas unitarias')
            else:
                self.btnCardMarkUnit.setText('Marcar pruebas unitarias')
            self.btnCardMarkUnit.setEnabled(allow_unit_toggle)
            if not allow_unit_toggle:
                if card_mode and not has_card:
                    tooltip = 'Guarda la tarjeta antes de actualizar las pruebas unitarias'
                elif not (can_lead or is_card_assignee):
                    if card and card.assignee:
                        tooltip = 'Solo el desarrollador asignado o un líder pueden actualizar las pruebas unitarias'
                    else:
                        tooltip = 'Asigna un desarrollador antes de marcar las pruebas unitarias'
                else:
                    tooltip = ''
                self.btnCardMarkUnit.setToolTip(tooltip)
            else:
                self.btnCardMarkUnit.setToolTip('')

        if hasattr(self, 'btnCardMarkQA'):
            if card and card.qa_done:
                self.btnCardMarkQA.setText('Desmarcar QA')
            else:
                self.btnCardMarkQA.setText('Marcar QA')
            self.btnCardMarkQA.setEnabled(allow_qa_toggle)
            if not allow_qa_toggle:
                if card_mode and not has_card:
                    tooltip = 'Guarda la tarjeta antes de actualizar las pruebas QA'
                elif not (can_lead or is_card_qa):
                    if card and card.qa_assignee:
                        tooltip = 'Solo la persona asignada en QA o un líder pueden aprobar QA'
                    else:
                        tooltip = 'Asigna un responsable de QA antes de marcar la revisión'
                else:
                    tooltip = ''
                self.btnCardMarkQA.setToolTip(tooltip)
            else:
                self.btnCardMarkQA.setToolTip('')

        if hasattr(self, 'cboCardSprint'):
            enabled = card_mode and (card is None or (card.status or '').lower() != 'terminated')
            self.cboCardSprint.setEnabled(enabled)

        if hasattr(self, 'txtCardScript'):
            self.txtCardScript.setEnabled(can_edit_card)
        if hasattr(self, 'btnCardLoadScript'):
            self.btnCardLoadScript.setEnabled(can_edit_card)
        if hasattr(self, 'btnCardDeleteScript'):
            allow_delete_script = can_edit_card and (
                bool(self._card_form_script and (
                    (self._card_form_script.content or '').strip()
                    or (self._card_form_script.file_name or '')
                ))
                or bool(self.txtCardScript.toPlainText())
            )
            self.btnCardDeleteScript.setEnabled(allow_delete_script)
            if allow_delete_script:
                self.btnCardDeleteScript.setToolTip('')
            else:
                tooltip = (
                    'Guarda la tarjeta o solicita permisos para editar el script'
                    if not can_edit_card
                    else 'No hay script para eliminar'
                )
                self.btnCardDeleteScript.setToolTip(tooltip)

        if hasattr(self, "card_browser"):
            self.card_browser.set_new_card_enabled(can_lead)
            self.card_browser.set_import_enabled(can_lead)
            self.card_browser.set_template_enabled(True)

        if hasattr(self, 'btnExportScripts'):
            sprint_id = self._selected_sprint_id
            allow_export = sprint_id is not None and sprint_id in self._sprints
            self.btnExportScripts.setEnabled(bool(allow_export))

        if hasattr(self, "btnBulkAssign"):
            allow_bulk = can_lead and bool(self._selected_card_ids)
            self.btnBulkAssign.setEnabled(allow_bulk)
            if allow_bulk:
                self.btnBulkAssign.setToolTip("")
            elif not can_lead:
                self.btnBulkAssign.setToolTip("Solo líderes pueden asignar tarjetas masivamente.")
            else:
                self.btnBulkAssign.setToolTip("Selecciona al menos una tarjeta para asignar responsables.")

    # ------------------------------------------------------------------
    def refresh(self) -> None:
        self._sprints.clear()
        self._cards.clear()
        self._branch_index = load_index()
        self._load_companies()
        self._load_incidence_types()
        self._populate_group_combo(self._current_sprint_group())

        for sprint in list_sprints():
            if sprint.id is None:
                continue
            self._sprints[sprint.id] = sprint

        for card in list_cards():
            if card.id is None:
                continue
            self._cards[card.id] = card

        self._auto_finalize_cards()

        users = list_users(include_inactive=False)
        self._users = sorted({user.username for user in users})
        self._user_roles = list_user_roles()

        if hasattr(self, "cboSprintLead"):
            self._populate_user_combo(self.cboSprintLead, None)
        if hasattr(self, "cboSprintQA"):
            self._populate_user_combo(
                self.cboSprintQA, None, allow_empty=True, required_role="qa"
            )
        if hasattr(self, "cboCardAssignee"):
            self._populate_user_combo(
                self.cboCardAssignee,
                None,
                allow_empty=True,
                required_role="developer",
            )
        if hasattr(self, "cboCardQA"):
            self._populate_user_combo(
                self.cboCardQA, None, allow_empty=True, required_role="qa"
            )

        self._populate_card_sprint_combo(None, None)
        self._populate_card_group_combo(None)
        self._populate_card_company_combo(None, None)

        self._populate_tree()
        self._restore_selection()
        self._update_new_card_button()
        if hasattr(self, "card_browser"):
            self.card_browser.update_sources(
                self._cards,
                self._sprints,
                self._companies,
                self._incidence_types,
                self._incidence_icons,
            )
        self.update_permissions()

    # ------------------------------------------------------------------
    def _load_companies(self) -> None:
        try:
            companies = list_company_catalog()
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.warning(
                self,
                "Empresas",
                f"No fue posible cargar el catálogo de empresas: {exc}",
            )
            companies = []
        self._companies = {
            company.id: company for company in companies if company.id is not None
        }
        grouped: Dict[Optional[str], List[Company]] = {}
        for company in companies:
            key = company.group_name or None
            grouped.setdefault(key, []).append(company)
        for values in grouped.values():
            values.sort(key=lambda comp: (comp.name or "").lower())
        self._companies_by_group = grouped
        self._populate_company_combo(None)

    # ------------------------------------------------------------------
    def _load_incidence_types(self) -> None:
        try:
            types = list_incidence_catalog()
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.warning(
                self,
                "Tipos de incidencia",
                f"No fue posible cargar el catálogo de tipos de incidencia: {exc}",
            )
            types = []
        mapping: Dict[int, IncidenceType] = {}
        icons: Dict[int, QIcon] = {}
        for entry in types:
            if entry.id is None:
                continue
            mapping[entry.id] = entry
            icon = self._build_incidence_icon(entry)
            if icon and not icon.isNull():
                icons[entry.id] = icon
        self._incidence_types = mapping
        self._incidence_icons = icons
        self._populate_card_incidence_combo(None)

    # ------------------------------------------------------------------
    def _build_incidence_icon(self, incidence: IncidenceType) -> QIcon:
        data = getattr(incidence, "icon", None)
        if data is None:
            return QIcon()
        if isinstance(data, memoryview):
            data = data.tobytes()
        elif isinstance(data, bytearray):
            data = bytes(data)
        if not isinstance(data, (bytes, bytearray)):
            return QIcon()
        pixmap = QPixmap()
        if data and pixmap.loadFromData(data):
            return QIcon(pixmap)
        return QIcon()

    # ------------------------------------------------------------------
    def _populate_group_combo(self, selected: Optional[str]) -> None:
        group_keys = sorted({group.key for group in self._cfg.groups if getattr(group, "key", None)})
        if hasattr(self, "cboSprintGroup"):
            self.cboSprintGroup.blockSignals(True)
            current = selected or self._current_sprint_group()
            self.cboSprintGroup.clear()
            self.cboSprintGroup.addItem("Sin grupo", None)
            for key in group_keys:
                self.cboSprintGroup.addItem(key, key)
            if current and current in group_keys:
                index = self.cboSprintGroup.findData(current)
                if index >= 0:
                    self.cboSprintGroup.setCurrentIndex(index)
                else:
                    self.cboSprintGroup.setCurrentIndex(0)
            else:
                self.cboSprintGroup.setCurrentIndex(0)
            self.cboSprintGroup.blockSignals(False)
        if hasattr(self, "cboSprintFilterGroup"):
            self.cboSprintFilterGroup.blockSignals(True)
            current_filter = self.cboSprintFilterGroup.currentData()
            self.cboSprintFilterGroup.clear()
            self.cboSprintFilterGroup.addItem("Todos los grupos", None)
            for key in group_keys:
                self.cboSprintFilterGroup.addItem(key, key)
            if current_filter and current_filter in group_keys:
                idx = self.cboSprintFilterGroup.findData(current_filter)
                if idx >= 0:
                    self.cboSprintFilterGroup.setCurrentIndex(idx)
                else:
                    self.cboSprintFilterGroup.setCurrentIndex(0)
            else:
                self.cboSprintFilterGroup.setCurrentIndex(0)
            self.cboSprintFilterGroup.blockSignals(False)

    # ------------------------------------------------------------------
    def _company_name(self, company_id: Optional[int]) -> str:
        if company_id is None:
            return ""
        company = self._companies.get(company_id)
        return company.name if company else ""

    # ------------------------------------------------------------------
    def _populate_tree(self) -> None:
        self.tree.clear()
        group_filter = self._sprint_filter_group or None
        status_filter = (self._sprint_filter_status or "").lower() if self._sprint_filter_status else None
        for sprint in sorted(
            self._sprints.values(), key=lambda s: ((s.version or "").lower(), (s.name or "").lower())
        ):
            if group_filter and (sprint.group_name or None) != group_filter:
                continue
            if status_filter and (sprint.status or "").lower() != status_filter:
                continue
            sprint_item = QTreeWidgetItem()
            sprint_label = f"{sprint.version} — {sprint.name}"
            details: List[str] = []
            if sprint.group_name:
                details.append(sprint.group_name)
            if sprint.company_sequence:
                details.append(f"#{sprint.company_sequence}")
            if sprint.status == "closed":
                details.append("finalizado")
            if details:
                sprint_label += f" ({', '.join(details)})"
            sprint_item.setText(0, sprint_label)
            sprint_item.setText(1, "")
            sprint_item.setText(2, sprint.lead_user or "")
            sprint_item.setText(3, sprint.qa_user or "")
            sprint_item.setText(4, self._company_name(sprint.company_id))
            sprint_item.setText(5, "-")
            sprint_item.setText(6, "Cerrado" if sprint.status == "closed" else "Abierto")
            sprint_item.setText(7, sprint.branch_key)
            sprint_item.setText(8, sprint.qa_branch_key or "")
            sprint_item.setText(9, "-")
            sprint_item.setText(10, "-")
            sprint_item.setText(11, sprint.created_by or "")
            sprint_item.setData(0, Qt.UserRole, ("sprint", sprint.id))
            self.tree.addTopLevelItem(sprint_item)

            cards = [card for card in self._cards.values() if card.sprint_id == sprint.id]
            cards.sort(
                key=lambda c: (
                    (c.ticket_id or "").lower(),
                    (c.title or "").lower(),
                )
            )
            for card in cards:
                self._populate_card_item(sprint_item, sprint, card)
            sprint_item.setExpanded(True)

    # ------------------------------------------------------------------
    def _populate_card_item(self, parent: QTreeWidgetItem, sprint: Sprint, card: Card) -> None:
        display = card.title or card.ticket_id or "(sin título)"
        if card.ticket_id:
            display = f"{card.ticket_id} — {card.title}"
        status_display = (card.status or "pendiente").capitalize()
        if card.group_name:
            display += f" [{card.group_name}]"
        if status_display:
            display += f" ({status_display})"

        incidence = None
        if card.incidence_type_id is not None:
            incidence = self._incidence_types.get(int(card.incidence_type_id))

        item = QTreeWidgetItem()
        item.setText(0, display)
        item.setText(1, incidence.name if incidence else "")
        item.setText(2, card.assignee or "")
        item.setText(3, card.qa_assignee or "")
        checks = []
        checks.append("Unit ✔" if card.unit_tests_done else "Unit ✖")
        checks.append("QA ✔" if card.qa_done else "QA ✖")
        checks.append("Merge ✔" if is_card_ready_for_merge(card) else "Merge ✖")
        company_name = self._company_name(card.company_id) or self._company_name(sprint.company_id)
        item.setText(4, company_name or "")
        item.setText(5, "Sí" if card.script_id else "No")
        item.setText(6, " / ".join(checks))
        item.setText(7, card.branch)
        item.setText(8, sprint.qa_branch_key or "")

        record = self._branch_record_for_card(card, sprint)
        has_branch = bool((card.branch or "").strip())
        if record:
            local_text = "Sí" if record.has_local_copy() else "No"
            origin_text = "Sí" if record.exists_origin else "No"
            creator = card.branch_created_by or record.last_updated_by or record.created_by or ""
        else:
            local_text = "No" if has_branch else "-"
            origin_text = "No" if has_branch else "-"
            creator = card.branch_created_by or ""

        item.setText(9, local_text)
        item.setText(10, origin_text)
        item.setText(11, creator or "")
        item.setData(0, Qt.UserRole, ("card", card.id))
        self._apply_card_style(item, card, incidence)
        parent.addChild(item)

    # ------------------------------------------------------------------
    def _apply_card_style(
        self,
        item: QTreeWidgetItem,
        card: Card,
        incidence: Optional[IncidenceType],
    ) -> None:
        icon = QIcon()
        if incidence and incidence.id is not None:
            icon = self._incidence_icons.get(int(incidence.id), QIcon())
        if icon and not icon.isNull():
            item.setIcon(1, icon)
        else:
            item.setIcon(1, QIcon())

        background = None
        foreground = None
        status_value = getattr(card, "status", None)
        if isinstance(status_value, str) and _card_has_assigned_sprint(card):
            background, foreground = status_brushes(status_value)

        for column in range(item.columnCount()):
            if background:
                item.setBackground(column, background)
            else:
                item.setBackground(column, QBrush())
            if foreground:
                item.setForeground(column, foreground)
            else:
                item.setForeground(column, QBrush())

    # ------------------------------------------------------------------
    def _branch_record_for_card(
        self, card: Card, sprint: Optional[Sprint]
    ) -> Optional[BranchRecord]:
        branch_key = card.branch_key
        if not branch_key and sprint:
            branch_key = self._build_card_branch_key(card, sprint)
        if not branch_key:
            return None
        return self._branch_index.get(branch_key)

    # ------------------------------------------------------------------
    def _auto_finalize_cards(self) -> None:
        updates: List[Card] = []
        now = int(time.time())
        for card in list(self._cards.values()):
            sprint = self._sprints.get(card.sprint_id)
            if not sprint:
                continue
            if (card.status or "").lower() == "terminated":
                continue
            if sprint.status == "closed" and card.unit_tests_done and card.qa_done:
                updated = replace(
                    card,
                    status="terminated",
                    closed_at=card.closed_at or now,
                    closed_by=card.closed_by or sprint.closed_by or self._current_user(),
                )
                try:
                    saved = upsert_card(updated)
                except Exception as exc:  # pragma: no cover - registro y continúa
                    logging.debug("No se pudo finalizar tarjeta %s: %s", card.id, exc)
                    continue
                updates.append(saved)
        for card in updates:
            if card.id is not None:
                self._cards[card.id] = card

    # ------------------------------------------------------------------
    def _branch_record_for_name(self, sprint: Sprint, branch: str) -> Optional[BranchRecord]:
        if not sprint or not branch:
            return None
        temp = Card(id=None, sprint_id=sprint.id, branch=branch)
        key = self._build_card_branch_key(temp, sprint)
        if not key:
            return None
        return self._branch_index.get(key)

    # ------------------------------------------------------------------
    def _split_branch_key(
        self, branch_key: Optional[str]
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        if not branch_key:
            return None, None, None
        parts = branch_key.split("/", 2)
        if len(parts) == 1:
            return None, None, parts[0] or None
        if len(parts) == 2:
            return parts[0] or None, None, parts[1] or None
        return parts[0] or None, parts[1] or None, parts[2] or None

    # ------------------------------------------------------------------
    def _build_card_branch_key(self, card: Card, sprint: Sprint) -> Optional[str]:
        source_key = self._effective_sprint_branch_key(sprint)
        group, project, _ = self._split_branch_key(source_key)
        if group is None and project is None and not (source_key or ""):
            return None
        branch = (card.branch or "").strip()
        if not branch:
            return None
        group = group or ""
        project = project or ""
        return f"{group}/{project}/{branch}".strip("/")

    # ------------------------------------------------------------------
    def _effective_sprint_branch_key(self, sprint: Optional[Sprint]) -> Optional[str]:
        if not sprint:
            return None
        primary = (sprint.qa_branch_key or sprint.branch_key or "").strip()
        return primary or None

    # ------------------------------------------------------------------
    def _restore_selection(self) -> None:
        if self._selected_card_ids:
            if self._select_cards_in_tree(self._selected_card_ids):
                return
            self._selected_card_ids = []
            self._selected_card_id = None

        if self._selected_card_id and self._selected_card_id in self._cards:
            card = self._cards[self._selected_card_id]
            if card.id is not None:
                self._select_tree_item("card", card.id)
                return
        else:
            self._selected_card_id = None

        if self._selected_sprint_id and self._selected_sprint_id in self._sprints:
            self._select_tree_item("sprint", self._selected_sprint_id)
            return
        else:
            self._selected_sprint_id = None

        self.tree.clearSelection()
        self._card_parent_id = None
        self._close_sprint_dialog()
        self._close_card_dialog()

    # ------------------------------------------------------------------
    def _select_tree_item(self, kind: str, ident: int) -> None:
        item = self._find_tree_item(kind, ident)
        if item is not None:
            self.tree.setCurrentItem(item)

    # ------------------------------------------------------------------
    def _find_tree_item(self, kind: str, ident: int) -> Optional[QTreeWidgetItem]:
        iters: Iterable[QTreeWidgetItem] = self.tree.findItems(
            "*", Qt.MatchWildcard | Qt.MatchRecursive, 0
        )
        for item in iters:
            data = item.data(0, Qt.UserRole) or (None, None)
            if data == (kind, ident):
                return item
        return None

    # ------------------------------------------------------------------
    def _select_cards_in_tree(self, card_ids: Iterable[int]) -> bool:
        items: List[QTreeWidgetItem] = []
        for card_id in card_ids:
            item = self._find_tree_item("card", card_id)
            if item is not None:
                items.append(item)
        if not items:
            return False
        self.tree.blockSignals(True)
        try:
            self.tree.clearSelection()
            last_item: Optional[QTreeWidgetItem] = None
            for item in items:
                item.setSelected(True)
                last_item = item
            if last_item is not None:
                self.tree.setCurrentItem(last_item)
        finally:
            self.tree.blockSignals(False)
        self._on_selection_changed()
        return True

    # ------------------------------------------------------------------
    def _populate_user_combo(
        self,
        combo: QComboBox,
        current: Optional[str],
        *,
        allow_empty: bool = False,
        required_role: Optional[str] = None,
    ) -> None:
        combo.blockSignals(True)
        combo.clear()
        names = filter_users_by_role(self._users, self._user_roles, required_role)
        added = set()
        if allow_empty:
            combo.addItem("", userData="")
        for name in names:
            if name in added:
                continue
            combo.addItem(name, userData=name)
            added.add(name)
        if current and current not in added:
            combo.addItem(current, userData=current)
            added.add(current)
        if current:
            index = combo.findText(current)
            if index >= 0:
                combo.setCurrentIndex(index)
            elif allow_empty:
                combo.setCurrentIndex(0)
            elif combo.count() > 0:
                combo.setCurrentIndex(0)
        else:
            if combo.count() > 0:
                combo.setCurrentIndex(0)
        combo.blockSignals(False)

    # ------------------------------------------------------------------
    def _populate_company_combo(self, selected: Optional[int], group_filter: Optional[str] = None) -> None:
        if not hasattr(self, "cboCompany"):
            return
        self.cboCompany.blockSignals(True)
        self.cboCompany.clear()
        self.cboCompany.addItem("Sin empresa", None)
        if group_filter:
            companies = list(self._companies_by_group.get(group_filter, []))
        else:
            companies = sorted(
                self._companies.values(), key=lambda comp: (comp.name or "").lower()
            )
        for company in companies:
            if company.id is None:
                continue
            self.cboCompany.addItem(company.name, company.id)
        if selected is not None:
            index = self.cboCompany.findData(selected)
            if index >= 0:
                self.cboCompany.setCurrentIndex(index)
            else:
                self.cboCompany.setCurrentIndex(0)
        else:
            self.cboCompany.setCurrentIndex(0)
        self.cboCompany.blockSignals(False)

    # ------------------------------------------------------------------
    def _populate_unassigned_cards(
        self, sprint: Optional[Sprint], company_id: Optional[int]
    ) -> None:
        if not hasattr(self, "lstUnassignedCards"):
            return
        self.lstUnassignedCards.clear()
        self.lstUnassignedCards.setEnabled(False)
        self.btnAssignCards.setEnabled(False)
        self._unassigned_cards = {}
        if not sprint or sprint.id is None:
            if hasattr(self, "pending_box"):
                self.pending_box.setVisible(False)
            return
        if company_id in (None, ""):
            if hasattr(self, "pending_box"):
                self.pending_box.setVisible(False)
            return
        try:
            company_key = int(company_id)
        except (TypeError, ValueError):
            return
        try:
            pending = list_cards(
                company_ids=[company_key],
                without_sprint=True,
                include_closed=False,
            )
        except Exception:
            pending = []
        group_key = sprint.group_name or None
        if group_key:
            pending = [card for card in pending if (card.group_name or None) == group_key]
        for card in pending:
            if card.id is None:
                continue
            title = card.title or ""
            ticket = card.ticket_id or ""
            if ticket and title:
                label = f"{ticket} — {title}"
            else:
                label = ticket or title or f"Tarjeta #{card.id}"
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, int(card.id))
            self.lstUnassignedCards.addItem(item)
            self._unassigned_cards[int(card.id)] = card
        is_open = (sprint.status or "open").lower() != "closed"
        has_items = bool(self._unassigned_cards)
        self.lstUnassignedCards.setEnabled(is_open and has_items)
        self.btnAssignCards.setEnabled(is_open and has_items)
        if hasattr(self, "pending_box"):
            self.pending_box.setVisible(bool(self._unassigned_cards))

    # ------------------------------------------------------------------
    def _on_assign_pending_cards(self) -> None:
        if not hasattr(self, "lstUnassignedCards") or not hasattr(self, "btnAssignCards"):
            return
        if self._selected_sprint_id is None:
            QMessageBox.information(self, "Sprint", "Guarda el sprint antes de asignar tarjetas.")
            return
        selected_items = self.lstUnassignedCards.selectedItems()
        if not selected_items:
            QMessageBox.information(self, "Sprint", "Selecciona al menos una tarjeta pendiente.")
            return
        card_ids: List[int] = []
        for item in selected_items:
            card_id = item.data(Qt.UserRole)
            if card_id in (None, ""):
                continue
            try:
                card_ids.append(int(card_id))
            except (TypeError, ValueError):
                continue
        if not card_ids:
            QMessageBox.warning(self, "Sprint", "Las tarjetas seleccionadas ya no son válidas.")
            return
        try:
            assign_cards_to_sprint(int(self._selected_sprint_id), card_ids)
        except Exception as exc:
            QMessageBox.critical(self, "Sprint", f"No se pudieron asignar las tarjetas: {exc}")
            return
        self.refresh()
        sprint = self._sprints.get(self._selected_sprint_id) if self._selected_sprint_id else None
        if sprint:
            self._show_sprint_form(sprint)
            self._select_tree_item("sprint", sprint.id)

    # ------------------------------------------------------------------
    def _apply_sprint_filters(self) -> None:
        if hasattr(self, "cboSprintFilterGroup"):
            self._sprint_filter_group = self.cboSprintFilterGroup.currentData()
            if self._sprint_filter_group in ("", None):
                self._sprint_filter_group = None
        else:
            self._sprint_filter_group = None
        if hasattr(self, "cboSprintFilterStatus"):
            self._sprint_filter_status = self.cboSprintFilterStatus.currentData()
            if self._sprint_filter_status in ("", None):
                self._sprint_filter_status = None
        else:
            self._sprint_filter_status = None
        self._populate_tree()

    # ------------------------------------------------------------------
    def _current_sprint_group(self) -> Optional[str]:
        if hasattr(self, "cboSprintGroup"):
            value = self.cboSprintGroup.currentData()
            return value or None
        return None

    # ------------------------------------------------------------------
    def _set_sprint_group(self, group_key: Optional[str]) -> None:
        if not hasattr(self, "cboSprintGroup"):
            return
        self.cboSprintGroup.blockSignals(True)
        target = group_key or None
        index = 0
        for idx in range(self.cboSprintGroup.count()):
            if self.cboSprintGroup.itemData(idx) == target:
                index = idx
                break
        self.cboSprintGroup.setCurrentIndex(index)
        self.cboSprintGroup.blockSignals(False)
        self._on_sprint_group_changed()

    # ------------------------------------------------------------------
    def _on_sprint_group_changed(self) -> None:
        group_key = self._current_sprint_group()
        selected_company = self.cboCompany.currentData() if hasattr(self, "cboCompany") else None
        self._populate_company_combo(selected_company, group_key)
        if self._sprint_form_sprint:
            self._sprint_form_sprint.group_name = group_key
        company_value = self.cboCompany.currentData() if hasattr(self, "cboCompany") else None
        self._update_sprint_sequence_label(None, company_value)
        self._populate_unassigned_cards(self._sprint_form_sprint, company_value)

    # ------------------------------------------------------------------
    def _on_company_changed(self) -> None:
        company_id = self.cboCompany.currentData() if hasattr(self, "cboCompany") else None
        if company_id:
            company = self._companies.get(int(company_id))
            if company and company.group_name and not self._current_sprint_group():
                self._set_sprint_group(company.group_name)
        if self._sprint_form_sprint:
            try:
                self._sprint_form_sprint.company_id = int(company_id) if company_id not in (None, "") else None
            except (TypeError, ValueError):
                self._sprint_form_sprint.company_id = None
        self._update_sprint_sequence_label(None, company_id)
        self._populate_unassigned_cards(self._sprint_form_sprint, company_id)

    # ------------------------------------------------------------------
    def _update_sprint_sequence_label(
        self,
        sequence: Optional[int],
        company_id: Optional[int],
    ) -> None:
        if not hasattr(self, "lblSprintSequence"):
            return
        if not company_id:
            self.lblSprintSequence.setText("Sin empresa")
            return
        company = self._companies.get(int(company_id))
        next_value = company.next_sprint_number if company else None
        if sequence:
            base = str(sequence)
        else:
            base = "-"
        if next_value:
            hint = next_value
            if sequence and sequence >= next_value:
                hint = sequence + 1
            if base == "-":
                text = f"Próximo: {hint}"
            else:
                text = f"{base} (siguiente: {hint})"
        else:
            text = base
        self.lblSprintSequence.setText(text)

    # ------------------------------------------------------------------
    def _populate_card_sprint_combo(
        self, selected: Optional[int], company_filter: Optional[int] = None
    ) -> None:
        if not hasattr(self, "cboCardSprint"):
            return
        self.cboCardSprint.blockSignals(True)
        self.cboCardSprint.clear()
        self.cboCardSprint.addItem("Sin sprint", None)
        filter_id: Optional[int] = None
        if company_filter not in (None, ""):
            try:
                filter_id = int(company_filter)
            except (TypeError, ValueError):
                filter_id = None
        for sprint in sorted(
            self._sprints.values(), key=lambda s: ((s.version or "").lower(), (s.name or "").lower())
        ):
            if filter_id is not None and sprint.company_id != filter_id:
                continue
            label = f"{sprint.version} — {sprint.name}"
            if sprint.status == "closed":
                label += " (cerrado)"
            self.cboCardSprint.addItem(label, sprint.id)
        normalized: Optional[int] = None
        try:
            if selected not in (None, ""):
                normalized = int(selected)
        except (TypeError, ValueError):
            normalized = None
        if normalized is not None:
            idx = self.cboCardSprint.findData(normalized)
            if idx >= 0:
                self.cboCardSprint.setCurrentIndex(idx)
            else:
                self.cboCardSprint.setCurrentIndex(0)
        else:
            self.cboCardSprint.setCurrentIndex(0)
        self.cboCardSprint.blockSignals(False)

    # ------------------------------------------------------------------
    def _populate_card_group_combo(self, selected: Optional[str]) -> None:
        if not hasattr(self, "cboCardGroup"):
            return
        group_keys = sorted({group.key for group in self._cfg.groups if getattr(group, "key", None)})
        self.cboCardGroup.blockSignals(True)
        self.cboCardGroup.clear()
        self.cboCardGroup.addItem("Sin grupo", None)
        for key in group_keys:
            self.cboCardGroup.addItem(key, key)
        if selected and selected in group_keys:
            idx = self.cboCardGroup.findData(selected)
            if idx >= 0:
                self.cboCardGroup.setCurrentIndex(idx)
            else:
                self.cboCardGroup.setCurrentIndex(0)
        else:
            self.cboCardGroup.setCurrentIndex(0)
        self.cboCardGroup.blockSignals(False)

    # ------------------------------------------------------------------
    def _populate_card_company_combo(
        self,
        selected: Optional[int],
        group_filter: Optional[str],
    ) -> None:
        if not hasattr(self, "cboCardCompany"):
            return
        self.cboCardCompany.blockSignals(True)
        self.cboCardCompany.clear()
        self.cboCardCompany.addItem("Sin empresa", None)
        companies: Iterable[Company]
        if group_filter:
            companies = self._companies_by_group.get(group_filter, [])
        else:
            companies = sorted(
                self._companies.values(), key=lambda comp: (comp.name or "").lower()
            )
        for company in companies:
            if company.id is None:
                continue
            self.cboCardCompany.addItem(company.name, company.id)
        if selected:
            idx = self.cboCardCompany.findData(selected)
            if idx >= 0:
                self.cboCardCompany.setCurrentIndex(idx)
            else:
                self.cboCardCompany.setCurrentIndex(0)
        else:
            self.cboCardCompany.setCurrentIndex(0)
        self.cboCardCompany.blockSignals(False)

    # ------------------------------------------------------------------
    def _populate_card_incidence_combo(self, selected: Optional[int]) -> None:
        if not hasattr(self, "cboCardIncidence"):
            return
        self.cboCardIncidence.blockSignals(True)
        self.cboCardIncidence.clear()
        self.cboCardIncidence.addItem("Sin tipo", None)
        for incidence in sorted(
            self._incidence_types.values(),
            key=lambda entry: (entry.name or "").lower(),
        ):
            if incidence.id is None:
                continue
            self.cboCardIncidence.addItem(incidence.name, incidence.id)
        target = selected if selected not in (None, "") else None
        index = 0
        if target is not None:
            for idx in range(self.cboCardIncidence.count()):
                if self.cboCardIncidence.itemData(idx) == target:
                    index = idx
                    break
        self.cboCardIncidence.setCurrentIndex(index)
        self.cboCardIncidence.blockSignals(False)

    # ------------------------------------------------------------------
    def _set_card_group(self, group_key: Optional[str]) -> None:
        if not hasattr(self, "cboCardGroup"):
            return
        self.cboCardGroup.blockSignals(True)
        target = group_key or None
        index = 0
        for idx in range(self.cboCardGroup.count()):
            if self.cboCardGroup.itemData(idx) == target:
                index = idx
                break
        self.cboCardGroup.setCurrentIndex(index)
        self.cboCardGroup.blockSignals(False)
        self._on_card_group_changed()

    # ------------------------------------------------------------------
    def _set_card_company(self, company_id: Optional[int]) -> None:
        if not hasattr(self, "cboCardCompany"):
            return
        self.cboCardCompany.blockSignals(True)
        target = company_id if company_id not in (None, "") else None
        index = 0
        for idx in range(self.cboCardCompany.count()):
            if self.cboCardCompany.itemData(idx) == target:
                index = idx
                break
        self.cboCardCompany.setCurrentIndex(index)
        self.cboCardCompany.blockSignals(False)

    # ------------------------------------------------------------------
    def _on_card_sprint_changed(self) -> None:
        if not hasattr(self, "cboCardSprint"):
            return
        raw_value = self.cboCardSprint.currentData()
        sprint_id: Optional[int] = None
        try:
            if raw_value not in (None, ""):
                sprint_id = int(raw_value)
        except (TypeError, ValueError):
            sprint_id = None

        sprint = self._sprints.get(sprint_id) if sprint_id is not None else None
        if sprint and sprint.status == "closed" and (
            self._card_form_card and (self._card_form_card.status or "").lower() != "terminated"
        ):
            QMessageBox.warning(self, "Tarjeta", "No puedes mover la tarjeta a un sprint cerrado.")
            previous = None
            if self._card_form_card:
                previous = getattr(self._card_form_card, "sprint_id", None)
            company_filter = None
            if hasattr(self, "cboCardCompany"):
                company_filter = self.cboCardCompany.currentData()
            self._populate_card_sprint_combo(previous, company_filter)
            return
        self._card_form_sprint = sprint
        self._card_parent_id = sprint.id if sprint else None
        self.lblCardSprint.setText(self._card_sprint_label(sprint, self._card_form_card))

        if sprint and sprint.group_name:
            self._set_card_group(sprint.group_name)
        elif not sprint and self._card_form_card:
            self._set_card_group(self._card_form_card.group_name)

        current_company = self._card_form_card.company_id if self._card_form_card else None
        filter_group = self.cboCardGroup.currentData() if hasattr(self, "cboCardGroup") else None
        self._populate_card_company_combo(current_company, filter_group)
        if sprint and sprint.company_id:
            self._set_card_company(sprint.company_id)
        elif not sprint and current_company:
            self._set_card_company(current_company)

        reference_card = self._card_form_card or Card(id=None, sprint_id=sprint_id)
        self._prepare_branch_inputs(reference_card, sprint)
        self.update_permissions()

    # ------------------------------------------------------------------
    def _on_card_group_changed(self) -> None:
        if not hasattr(self, "cboCardGroup"):
            return
        group_key = self.cboCardGroup.currentData()
        current_company = self.cboCardCompany.currentData() if hasattr(self, "cboCardCompany") else None
        self._populate_card_company_combo(current_company, group_key)

    # ------------------------------------------------------------------
    def _on_card_company_changed(self) -> None:
        if not hasattr(self, "cboCardCompany"):
            return
        data = self.cboCardCompany.currentData()
        company_id: Optional[int] = None
        if data not in (None, ""):
            try:
                company_id = int(data)
            except (TypeError, ValueError):
                company_id = None

        if company_id is not None:
            company = self._companies.get(company_id)
            if company and company.group_name:
                current_group = (
                    self.cboCardGroup.currentData() if hasattr(self, "cboCardGroup") else None
                )
                if current_group != company.group_name:
                    self._set_card_group(company.group_name)

        current_sprint = None
        if hasattr(self, "cboCardSprint"):
            current_sprint = self.cboCardSprint.currentData()
        self._populate_card_sprint_combo(current_sprint, company_id)
        self.update_permissions()

    # ------------------------------------------------------------------
    def _current_user(self) -> str:
        fallback = current_username("")
        if fallback:
            return fallback
        active = get_active_user()
        if active:
            return active.username
        return ""

    # ------------------------------------------------------------------
    def _update_new_card_button(self) -> None:
        sprint = self._sprints.get(self._selected_sprint_id or -1)
        allow = sprint is not None and sprint.status != "closed"
        self.btnNewCard.setEnabled(bool(allow))

    # ------------------------------------------------------------------
    def _on_selection_changed(self) -> None:
        selected_items = self.tree.selectedItems()
        selected_card_ids: List[int] = []
        for selected in selected_items:
            kind_value, ident_value = selected.data(0, Qt.UserRole) or (None, None)
            if kind_value == "card" and ident_value is not None:
                try:
                    selected_card_ids.append(int(ident_value))
                except (TypeError, ValueError):
                    continue
        self._selected_card_ids = selected_card_ids

        item = self.tree.currentItem()
        if not item:
            self._selected_card_id = None
            self._selected_sprint_id = None
            self._card_parent_id = None
            self._close_sprint_dialog()
            self._close_card_dialog()
            self.update_permissions()
            return

        kind, ident = item.data(0, Qt.UserRole) or (None, None)
        self._card_parent_id = None
        if kind == "sprint" and ident is not None:
            sprint_id: Optional[int]
            try:
                sprint_id = int(ident)
            except (TypeError, ValueError):
                sprint_id = None
            self._selected_sprint_id = sprint_id
            self._selected_card_id = None
            if self._active_form == "card":
                self._close_card_dialog()
            if self._active_form == "sprint" and (
                sprint_id is None or self._sprint_form_sprint is None or self._sprint_form_sprint.id != sprint_id
            ):
                self._close_sprint_dialog()
        elif kind == "card" and ident is not None:
            card_id: Optional[int]
            try:
                card_id = int(ident)
            except (TypeError, ValueError):
                card_id = None
            self._selected_card_id = card_id
            sprint_id: Optional[int] = None
            if card_id is not None:
                card = self._cards.get(card_id)
                if card:
                    try:
                        if getattr(card, "sprint_id", None) not in (None, ""):
                            sprint_id = int(card.sprint_id)
                    except (TypeError, ValueError):
                        sprint_id = None
            self._selected_sprint_id = sprint_id
            self._card_parent_id = sprint_id
            if self._active_form == "sprint" and (
                self._sprint_form_sprint is None or self._sprint_form_sprint.id != sprint_id
            ):
                self._close_sprint_dialog()
            if self._active_form == "card" and (
                card_id is None or self._card_form_card is None or self._card_form_card.id != card_id
            ):
                self._close_card_dialog()
        else:
            self._selected_sprint_id = None
            self._selected_card_id = None
            self._close_sprint_dialog()
            self._close_card_dialog()

        if len(self._selected_card_ids) != 1 and self._active_form == "card":
            self._close_card_dialog()

        self._update_new_card_button()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _on_planning_item_activated(self, item: QTreeWidgetItem, _: int) -> None:
        if not item:
            return
        kind, ident = item.data(0, Qt.UserRole) or (None, None)
        if kind == "sprint" and ident is not None:
            try:
                sprint_id = int(ident)
            except (TypeError, ValueError):
                return
            sprint = self._sprints.get(sprint_id)
            if not sprint:
                QMessageBox.warning(
                    self,
                    "Sprint",
                    "El sprint seleccionado ya no existe.",
                )
                self.refresh()
                return
            if (
                self._sprint_dialog
                and self._sprint_dialog.isVisible()
                and self._sprint_form_sprint
                and self._sprint_form_sprint.id == sprint.id
            ):
                try:
                    self._sprint_dialog.raise_()
                    self._sprint_dialog.activateWindow()
                except AttributeError:
                    pass
                return
            self._selected_sprint_id = sprint.id
            self._selected_card_id = None
            self._card_parent_id = None
            self._show_sprint_form(sprint)
            return

        if kind == "card" and ident is not None:
            try:
                card_id = int(ident)
            except (TypeError, ValueError):
                return
            card = self._cards.get(card_id)
            if not card:
                QMessageBox.warning(
                    self,
                    "Tarjeta",
                    "La tarjeta seleccionada ya no existe o fue movida.",
                )
                self.refresh()
                return
            sprint = self._sprints.get(card.sprint_id) if card.sprint_id else None
            if (
                self._card_dialog
                and self._card_dialog.isVisible()
                and self._card_form_card
                and self._card_form_card.id == card.id
            ):
                try:
                    self._card_dialog.raise_()
                    self._card_dialog.activateWindow()
                except AttributeError:
                    pass
                return
            self._selected_card_id = card.id
            self._card_parent_id = sprint.id if sprint and sprint.id is not None else None
            self._selected_sprint_id = sprint.id if sprint and sprint.id is not None else None
            self._show_card_form(card, sprint)

    # ------------------------------------------------------------------
    def _start_new_sprint(self) -> None:
        self._selected_sprint_id = None
        self._selected_card_id = None
        self._current_sprint_branch_key = None
        self._current_sprint_qa_branch_key = None
        sprint = Sprint(id=None, branch_key="", name="", version="")
        self._show_sprint_form(sprint, new=True)
        self.update_permissions()

    # ------------------------------------------------------------------
    def _start_new_card(self) -> None:
        sprint_id = self._selected_sprint_id or self._current_sprint_id()
        if sprint_id is None:
            QMessageBox.information(self, "Tarjeta", "Selecciona un sprint primero")
            return
        sprint = self._sprints.get(sprint_id)
        if not sprint:
            QMessageBox.warning(self, "Tarjeta", "El sprint seleccionado ya no existe.")
            return
        if sprint.status == "closed":
            QMessageBox.warning(self, "Tarjeta", "El sprint está finalizado, no se pueden agregar tarjetas.")
            return
        self._selected_card_id = None
        self._card_parent_id = sprint_id
        self._selected_sprint_id = sprint_id
        card = Card(id=None, sprint_id=sprint_id)
        self._show_card_form(card, sprint, new=True)
        self.update_permissions()

    # ------------------------------------------------------------------
    def _start_new_card_from_browser(
        self, group_key: Optional[str], company_id: Optional[int]
    ) -> None:
        self._selected_card_id = None
        self._card_parent_id = None
        self._selected_sprint_id = None
        card = Card(id=None, sprint_id=None)
        card.group_name = group_key or None
        card.company_id = company_id if company_id not in (None, "") else None
        card.status = card.status or "pending"

        self.tree.clearSelection()
        self._show_card_form(card, None, new=True)
        self.update_permissions()

    # ------------------------------------------------------------------
    def _open_card_from_browser(self, card_id: Optional[int]) -> None:
        if not card_id:
            return
        card = self._cards.get(int(card_id))
        if not card:
            QMessageBox.warning(
                self,
                "Tarjeta",
                "La tarjeta seleccionada ya no existe o fue movida.",
            )
            self.refresh()
            return
        sprint = self._sprints.get(card.sprint_id) if card.sprint_id else None
        self._selected_sprint_id = sprint.id if sprint else None
        self._selected_card_id = card.id
        self._card_parent_id = sprint.id if sprint else None
        self._show_card_form(card, sprint)
        if sprint and card.id is not None:
            self._select_tree_item("card", card.id)
        else:
            self.tree.clearSelection()
        self.tree.setFocus()

    # ------------------------------------------------------------------
    def _current_sprint_id(self) -> Optional[int]:
        if self._selected_sprint_id is not None:
            return self._selected_sprint_id
        item = self.tree.currentItem()
        if not item:
            return None
        kind, ident = item.data(0, Qt.UserRole) or (None, None)
        if kind == "sprint" and ident is not None:
            return int(ident)
        if kind == "card" and ident is not None:
            card = self._cards.get(int(ident))
            if card:
                return card.sprint_id
        return None

    # ------------------------------------------------------------------
    def _show_sprint_form(self, sprint: Sprint, new: bool = False) -> None:
        self._close_card_dialog()
        self._close_sprint_dialog()
        form = self._create_sprint_form()
        dialog = FormDialog(self, 'Sprint', form)
        dialog.destroyed.connect(lambda _=None, kind='sprint': self._on_dialog_closed(kind))
        self._sprint_dialog = dialog
        self._active_form = 'sprint'
        self._current_sprint_branch_key = sprint.branch_key
        self._current_sprint_qa_branch_key = sprint.qa_branch_key or None
        self._populate_group_combo(sprint.group_name)
        self._set_sprint_group(sprint.group_name)
        self.txtSprintBranch.setText(sprint.branch_key)
        self.txtSprintQABranch.setText(sprint.qa_branch_key or '')
        self.txtSprintName.setText(sprint.name)
        self.txtSprintVersion.setText(sprint.version)
        self._populate_company_combo(sprint.company_id, sprint.group_name)
        self._update_sprint_sequence_label(sprint.company_sequence, sprint.company_id)
        self._populate_user_combo(self.cboSprintLead, sprint.lead_user or None)
        self._populate_user_combo(
            self.cboSprintQA,
            sprint.qa_user or None,
            allow_empty=True,
            required_role='qa',
        )
        self.chkSprintClosed.setChecked(sprint.status == 'closed')
        meta_lines: list[str] = []
        if sprint.created_by:
            meta_lines.append(f'Creado por {sprint.created_by}')
        if sprint.status == 'closed' and sprint.closed_by:
            meta_lines.append(f'Finalizado por {sprint.closed_by}')
        self.lblSprintMeta.setText('\n'.join(meta_lines))
        self._selected_sprint_id = sprint.id
        self._selected_card_id = None
        self._sprint_form_sprint = sprint
        self._populate_unassigned_cards(sprint, sprint.company_id)
        if new:
            self.lblSprintMeta.clear()
            company_id = self.cboCompany.currentData() if hasattr(self, 'cboCompany') else None
            self._update_sprint_sequence_label(None, company_id)
        self._update_new_card_button()
        dialog.show()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _card_sprint_label(self, sprint: Optional[Sprint], card: Optional[Card]) -> str:
        if sprint:
            parts = []
            version = (sprint.version or "").strip()
            name = (sprint.name or "").strip()
            if version:
                parts.append(version)
            if name:
                parts.append(name)
            label = " — ".join(parts)
            if not label and sprint.id:
                label = f"Sprint #{sprint.id}"
            return label or "Sprint sin nombre"

        sprint_ref: Optional[int] = None
        if card and getattr(card, "sprint_id", None) not in (None, ""):
            try:
                sprint_ref = int(card.sprint_id)
            except (TypeError, ValueError):
                sprint_ref = None
        if sprint_ref is not None:
            return f"Sprint #{sprint_ref} (no disponible)"
        return "Sin sprint asignado"

    # ------------------------------------------------------------------
    def _show_card_form(
        self, card: Card, sprint: Optional[Sprint], new: bool = False
    ) -> None:
        self._close_sprint_dialog()
        self._close_card_dialog()
        form = self._create_card_form()
        dialog = FormDialog(self, 'Tarjeta', form)
        dialog.finished.connect(lambda _=None, kind='card': self._on_dialog_closed(kind))
        dialog.destroyed.connect(lambda _=None, kind='card': self._on_dialog_closed(kind))
        self._card_dialog = dialog
        self._active_form = 'card'
        self._card_form_card = card
        self._card_form_sprint = sprint
        self._load_card_script(card)
        self.lblCardSprint.setText(self._card_sprint_label(sprint, card))

        target_sprint_id: Optional[int] = None
        try:
            if getattr(card, 'sprint_id', None) not in (None, ''):
                target_sprint_id = int(card.sprint_id)
        except (TypeError, ValueError):
            target_sprint_id = None
        if target_sprint_id is None and sprint and sprint.id is not None:
            target_sprint_id = sprint.id

        group_value = card.group_name or (sprint.group_name if sprint else None)
        company_value = card.company_id or (sprint.company_id if sprint else None)

        self._populate_card_sprint_combo(target_sprint_id, company_value)
        if target_sprint_id is not None:
            idx = self.cboCardSprint.findData(target_sprint_id)
            if idx >= 0:
                self.cboCardSprint.setCurrentIndex(idx)
            else:
                self.cboCardSprint.setCurrentIndex(0)
        else:
            self.cboCardSprint.setCurrentIndex(0)

        self._populate_card_group_combo(group_value)
        if group_value:
            self._set_card_group(group_value)
        else:
            self._set_card_group(None)

        self._populate_card_company_combo(company_value, group_value)
        if company_value:
            self._set_card_company(company_value)
        else:
            self._set_card_company(None)
        self._populate_card_incidence_combo(card.incidence_type_id)
        self.lblCardStatus.setText((card.status or 'pendiente').capitalize())
        self.cboCardSprint.setEnabled((card.status or '').lower() != 'terminated')
        self.txtCardTicket.blockSignals(True)
        self.txtCardTicket.setText(card.ticket_id or '')
        self.txtCardTicket.blockSignals(False)
        self.txtCardTitle.setText(card.title or '')
        self._prepare_branch_inputs(card, sprint)
        self._populate_user_combo(
            self.cboCardAssignee,
            card.assignee or None,
            allow_empty=True,
            required_role='developer',
        )
        self._populate_user_combo(
            self.cboCardQA,
            card.qa_assignee or None,
            allow_empty=True,
            required_role='qa',
        )
        self.txtCardUnitUrl.setText(card.unit_tests_url or '')
        self.txtCardQAUrl.setText(card.qa_url or '')
        checks = []
        checks.append('Pruebas: ✔' if card.unit_tests_done else 'Pruebas: pendiente')
        checks.append('QA: ✔' if card.qa_done else 'QA: pendiente')
        self.lblCardChecks.setText(' | '.join(checks))
        record = self._branch_record_for_card(card, sprint)
        if record:
            self.lblCardLocal.setText('Local: Sí' if record.has_local_copy() else 'Local: No')
            self.lblCardOrigin.setText('Origen: Sí' if record.exists_origin else 'Origen: No')
        else:
            self.lblCardLocal.setText('Local: -')
            self.lblCardOrigin.setText('Origen: -')
        creator = card.branch_created_by or (record.last_updated_by if record else '')
        if not creator and record:
            creator = record.created_by
        self.lblCardCreator.setText(f"Creada por: {creator or '-'}")
        self._selected_card_id = card.id
        self._card_parent_id = target_sprint_id
        if new:
            self.lblCardCreator.setText('Creada por: -')
        self._update_branch_preview()
        dialog.show()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _load_card_script(self, card: Card) -> None:
        if not hasattr(self, "txtCardScript"):
            return
        script: Optional[CardScript] = None
        if card.id:
            try:
                script = load_card_script(int(card.id))
            except Exception as exc:  # pragma: no cover - errores de conexión
                logging.getLogger(__name__).warning(
                    "No se pudo cargar el script de la tarjeta %s: %s", card.id, exc
                )
                script = None
        self._card_form_script = script
        self._card_script_dirty = False
        self._card_script_deleted = False
        self._card_script_filename = script.file_name if script else None
        content = script.content if script and script.content is not None else ""
        self._card_script_original_text = content
        self._updating_script_text = True
        self.txtCardScript.blockSignals(True)
        try:
            self.txtCardScript.setPlainText(content)
        finally:
            self.txtCardScript.blockSignals(False)
            self._updating_script_text = False
        self._update_card_script_info(card)

    # ------------------------------------------------------------------
    def _update_card_script_info(self, card: Optional[Card]) -> None:
        if not hasattr(self, "lblCardScriptInfo"):
            return
        if self._card_script_deleted:
            text = "El script se eliminará al guardar."
        else:
            pieces: List[str] = []
            file_name = self._card_script_filename or (
                self._card_form_script.file_name if self._card_form_script else None
            )
            if file_name:
                pieces.append(file_name)
            script = self._card_form_script
            if script and script.updated_at:
                timestamp = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(script.updated_at)
                )
                if script.updated_by:
                    pieces.append(f"Actualizado {timestamp} por {script.updated_by}")
                else:
                    pieces.append(f"Actualizado {timestamp}")
            elif script and script.updated_by:
                pieces.append(f"Actualizado por {script.updated_by}")
            if not pieces and self.txtCardScript.toPlainText().strip():
                pieces.append("Script sin guardar")
            text = " · ".join(pieces) if pieces else "Sin script adjunto"
            if self._card_script_dirty and not self._card_script_deleted:
                text += " — cambios sin guardar"
        self.lblCardScriptInfo.setText(text)

    # ------------------------------------------------------------------
    def _on_card_script_changed(self) -> None:
        if getattr(self, "_updating_script_text", False):
            return
        if not hasattr(self, "txtCardScript"):
            return
        current = self.txtCardScript.toPlainText()
        original = self._card_script_original_text
        self._card_script_dirty = current != original
        if self._card_form_script and not current.strip():
            self._card_script_deleted = True
        elif current.strip():
            self._card_script_deleted = False
        self._update_card_script_info(self._card_form_card)

    # ------------------------------------------------------------------
    def _on_load_card_script_from_file(self) -> None:
        if not hasattr(self, "txtCardScript"):
            return
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Seleccionar script SQL",
            "",
            "Archivos SQL (*.sql);;Todos los archivos (*)",
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                content = handle.read()
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Script",
                f"No se pudo leer el archivo seleccionado:\n{exc}",
            )
            return
        self._updating_script_text = True
        self.txtCardScript.blockSignals(True)
        try:
            self.txtCardScript.setPlainText(content)
        finally:
            self.txtCardScript.blockSignals(False)
            self._updating_script_text = False
        self._card_script_filename = Path(path).name
        self._card_script_deleted = False
        self._on_card_script_changed()

    # ------------------------------------------------------------------
    def _on_delete_card_script(self) -> None:
        if not hasattr(self, "txtCardScript"):
            return
        has_existing = bool(self._card_form_script and self._card_form_script.id)
        has_text = bool(self.txtCardScript.toPlainText())
        if not has_existing and not has_text:
            return
        if has_existing:
            confirm = QMessageBox.question(
                self,
                "Script",
                "¿Eliminar el script asociado a esta tarjeta?",
            )
            if confirm != QMessageBox.Yes:
                return
        self._updating_script_text = True
        self.txtCardScript.blockSignals(True)
        try:
            self.txtCardScript.clear()
        finally:
            self.txtCardScript.blockSignals(False)
            self._updating_script_text = False
        self._card_script_filename = None
        if has_existing:
            self._card_script_deleted = True
            self._card_script_dirty = True
        else:
            self._card_script_deleted = False
            self._card_script_dirty = False
            self._card_script_original_text = ""
        self._update_card_script_info(self._card_form_card)

    # ------------------------------------------------------------------
    def _prepare_branch_inputs(self, card: Card, sprint: Optional[Sprint]) -> None:
        self._current_card_base = self._qa_branch_base(sprint)
        branch_value = (card.branch or "").strip()
        ticket_value = (card.ticket_id or "").strip()
        prefix = self._compose_branch_prefix(self._current_card_base, ticket_value)
        self._branch_override = False
        suffix = ""
        if branch_value:
            if prefix and (
                branch_value == prefix or branch_value.startswith(f"{prefix}_")
            ):
                suffix = branch_value[len(prefix) :]
                if suffix.startswith("_"):
                    suffix = suffix[1:]
            else:
                legacy = self._legacy_branch_prefix(sprint)
                if legacy and branch_value.startswith(legacy):
                    suffix = branch_value[len(legacy) :]
                    if suffix.startswith("_"):
                        suffix = suffix[1:]
                else:
                    self._branch_override = True
                    suffix = branch_value
        self.txtCardBranch.blockSignals(True)
        self.txtCardBranch.setText(suffix)
        self.txtCardBranch.blockSignals(False)

    # ------------------------------------------------------------------
    def _on_branch_text_changed(self) -> None:
        if self._active_form != "card":
            return
        text = self.txtCardBranch.text().strip()
        if self._branch_override and not text:
            self._branch_override = False
        self._update_branch_preview()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _on_ticket_changed(self) -> None:
        if self._active_form != "card":
            return
        self._update_branch_preview()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _current_branch_prefix(self) -> str:
        if self._branch_override:
            return ""
        ticket_value = self.txtCardTicket.text().strip()
        return self._compose_branch_prefix(self._current_card_base, ticket_value)

    # ------------------------------------------------------------------
    @staticmethod
    def _compose_branch_prefix(base: str, ticket: str) -> str:
        parts = []
        base_clean = base.strip()
        ticket_clean = ticket.strip()
        if base_clean:
            parts.append(base_clean)
        if ticket_clean:
            parts.append(ticket_clean)
        return "_".join(parts)

    # ------------------------------------------------------------------
    def _qa_branch_base(self, sprint: Optional[Sprint]) -> str:
        if not sprint:
            return ""
        key = (sprint.branch_key or sprint.qa_branch_key or "").strip()
        _, _, branch = self._split_branch_key(key)
        return branch or ""

    # ------------------------------------------------------------------
    @staticmethod
    def _legacy_branch_prefix(sprint: Optional[Sprint]) -> Optional[str]:
        if not sprint or not sprint.version:
            return None
        version = sprint.version.strip()
        if not version:
            return None
        return f"v{version}_"

    # ------------------------------------------------------------------
    def _update_branch_preview(self) -> None:
        prefix = self._current_branch_prefix()
        suffix = self.txtCardBranch.text().strip()
        if self._branch_override:
            self.lblCardPrefix.setText("")
        elif prefix:
            self.lblCardPrefix.setText(f"{prefix}_" if suffix else prefix)
        else:
            self.lblCardPrefix.setText("")
        full = self._full_branch_name()
        if full:
            self.lblCardBranchPreview.setText(f"→ {full}")
        else:
            self.lblCardBranchPreview.setText("")

    # ------------------------------------------------------------------
    def _on_pick_branch(self) -> None:
        key = self._select_branch_key(
            title="Nuevo sprint",
            prompt="Selecciona la rama base:",
        )
        if key:
            self._current_sprint_branch_key = key
            self.txtSprintBranch.setText(key)

    # ------------------------------------------------------------------
    def _on_pick_qa_branch(self) -> None:
        group_hint, _, _ = self._split_branch_key(self._current_sprint_branch_key)
        key = self._select_branch_key(
            title="Rama QA",
            prompt="Selecciona la rama QA:",
            group_hint=group_hint,
        )
        if key:
            self._current_sprint_qa_branch_key = key
            self.txtSprintQABranch.setText(key)

    # ------------------------------------------------------------------
    def _on_create_branch(self) -> None:
        if self._selected_card_id is None:
            QMessageBox.information(self, "Tarjeta", "Selecciona una tarjeta para crear su rama.")
            return

        card = self._cards.get(self._selected_card_id)
        sprint = self._sprints.get(card.sprint_id) if card else None
        if not card or not sprint:
            QMessageBox.warning(self, "Tarjeta", "La tarjeta seleccionada ya no existe.")
            return

        user = self._current_user()
        branch_name = self._full_branch_name().strip()
        if not branch_name:
            QMessageBox.warning(self, "Tarjeta", "La tarjeta no tiene un nombre de rama válido.")
            return

        if not (require_roles("leader") or (card.assignee and card.assignee == user)):
            QMessageBox.warning(
                self,
                "Tarjeta",
                "Solo el desarrollador asignado o un líder pueden crear la rama de la tarjeta.",
            )
            return

        if not sprint.qa_branch_key:
            QMessageBox.warning(
                self,
                "Tarjeta",
                "Configura la rama QA del sprint antes de crear ramas de tarjetas.",
            )
            return

        existing_record = self._branch_record_for_name(sprint, branch_name)
        if existing_record and (existing_record.has_local_copy() or existing_record.exists_origin):
            QMessageBox.information(
                self,
                "Tarjeta",
                "La rama ya existe. Elimina la rama local o sincroniza antes de recrearla.",
            )
            return

        effective_key = self._effective_sprint_branch_key(sprint)
        group_key, project_key, base_branch = self._split_branch_key(effective_key)
        if not base_branch:
            QMessageBox.warning(
                self,
                "Tarjeta",
                "La rama QA seleccionada no es válida para crear tarjetas.",
            )
            return

        logs: List[str] = []

        def emit(msg: str) -> None:
            logs.append(msg)

        try:
            ok = create_branches_local(
                self._cfg,
                group_key,
                project_key,
                branch_name,
                emit=emit,
                base_branch=base_branch,
            )
        except Exception as exc:  # pragma: no cover
            QMessageBox.critical(self, "Tarjeta", f"Error al crear la rama: {exc}")
            return

        message = "\n".join(logs) or "Operación completada."
        if ok:
            card.branch = branch_name
            card.branch_created_by = user
            card.branch_created_at = int(time.time())
            card.branch_created_flag = True
            card.updated_at = card.branch_created_at
            card.updated_by = card.branch_created_by
            updated = upsert_card(card)
            if updated.id:
                self._cards[updated.id] = updated
                self._selected_card_id = updated.id
            QMessageBox.information(self, "Tarjeta", message)
        else:
            QMessageBox.warning(self, "Tarjeta", message or "No se pudo crear la rama.")

        self.refresh()
        if card.id:
            self._select_tree_item("card", card.id)

    # ------------------------------------------------------------------
    def _select_branch_key(
        self,
        *,
        title: str,
        prompt: str,
        group_hint: Optional[str] = None,
    ) -> Optional[str]:
        grouped = branches_by_group()
        if not grouped:
            QMessageBox.information(
                self,
                title,
                "No hay ramas registradas en la NAS. Sincroniza el historial antes de crear un sprint.",
            )
            return None

        groups = sorted(grouped.keys())
        if not groups:
            QMessageBox.warning(self, title, "No hay grupos disponibles.")
            return None

        initial_idx = 0
        if group_hint and group_hint in groups:
            initial_idx = groups.index(group_hint)

        if len(groups) == 1:
            group = groups[0]
            ok = True
        else:
            group, ok = QInputDialog.getItem(
                self,
                title,
                "Selecciona el grupo:",
                groups,
                initial_idx,
                False,
            )
        if not ok or not group:
            return None

        options = []
        mapping: Dict[str, str] = {}
        for record in grouped.get(group, []):
            label = f"{record.project or '-'} / {record.branch}".strip()
            if not label:
                label = record.branch
            display = label
            if display in mapping:
                display = f"{label} ({record.key()})"
            mapping[display] = record.key()
            options.append(display)
        if not options:
            QMessageBox.warning(self, title, f"El grupo '{group}' no tiene ramas disponibles.")
            return None
        branch_label, ok = QInputDialog.getItem(
            self,
            title,
            prompt,
            options,
            0,
            False,
        )
        if not ok or not branch_label:
            return None
        return mapping.get(branch_label)

    # ------------------------------------------------------------------
    def _on_save_sprint(self) -> None:
        can_lead = require_roles("leader")
        if not can_lead:
            QMessageBox.warning(self, "Sprint", "No tienes permisos para guardar sprints.")
            return
        branch_key = (self._current_sprint_branch_key or "").strip()
        qa_branch_key = (self._current_sprint_qa_branch_key or self.txtSprintQABranch.text().strip() or "").strip()
        name = self.txtSprintName.text().strip()
        version = self.txtSprintVersion.text().strip()
        if not branch_key:
            QMessageBox.warning(self, "Sprint", "Selecciona la rama base del sprint.")
            return
        if not name or not version:
            QMessageBox.warning(self, "Sprint", "Nombre y versión son obligatorios.")
            return

        sprint_id = self._selected_sprint_id
        sprint = self._sprints.get(sprint_id) if sprint_id else Sprint(id=None, branch_key="", name="", version="")
        was_new = sprint.id is None
        now = int(time.time())
        user = self._current_user()
        sprint.branch_key = branch_key
        sprint.qa_branch_key = qa_branch_key or None
        sprint.name = name
        sprint.version = version
        sprint.group_name = self._current_sprint_group()
        company_data = self.cboCompany.currentData() if hasattr(self, "cboCompany") else None
        try:
            sprint.company_id = int(company_data) if company_data not in (None, "") else None
        except (TypeError, ValueError):
            sprint.company_id = None
        sprint.lead_user = self._combo_value(self.cboSprintLead)
        sprint.qa_user = self._combo_value(self.cboSprintQA)
        sprint.description = sprint.description or ""
        sprint.status = "closed" if self.chkSprintClosed.isChecked() else "open"
        if sprint.status == "closed":
            if not sprint.closed_at:
                sprint.closed_at = now
            sprint.closed_by = sprint.closed_by or user
        else:
            sprint.closed_at = None
            sprint.closed_by = None
        if sprint.id is None:
            sprint.created_at = now
            sprint.created_by = user
        sprint.updated_at = now
        sprint.updated_by = user

        try:
            saved = upsert_sprint(sprint)
        except sqlite3.IntegrityError:
            QMessageBox.warning(self, "Sprint", "No se pudo guardar: la rama seleccionada ya no existe.")
            return
        except Exception as exc:  # pragma: no cover
            QMessageBox.critical(self, "Sprint", f"Error al guardar el sprint: {exc}")
            return

        if saved.id is not None:
            self._sprints[saved.id] = saved
            self._selected_sprint_id = saved.id

        message = "Sprint creado correctamente." if was_new else "Sprint actualizado correctamente."

        self._close_sprint_dialog()
        self.refresh()
        if saved.id:
            self._select_tree_item("sprint", saved.id)
        QMessageBox.information(self, "Sprint", message)

    # ------------------------------------------------------------------
    def _combo_value(self, combo: QComboBox) -> Optional[str]:
        text = combo.currentText().strip()
        return text or None

    # ------------------------------------------------------------------
    def _on_delete_sprint(self) -> None:
        if self._selected_sprint_id is None:
            return
        confirm = QMessageBox.question(
            self,
            "Eliminar sprint",
            "¿Eliminar el sprint seleccionado y todas sus tarjetas?",
        )
        if confirm != QMessageBox.Yes:
            return
        delete_sprint(self._selected_sprint_id)
        self._selected_sprint_id = None
        self._selected_card_id = None
        self._close_sprint_dialog()
        self._close_card_dialog()
        self._card_parent_id = None
        self.refresh()
        QMessageBox.information(self, "Sprint", "Sprint eliminado correctamente.")

    # ------------------------------------------------------------------
    def _full_branch_name(self) -> str:
        suffix = self.txtCardBranch.text().strip()
        if self._branch_override:
            return suffix
        prefix = self._current_branch_prefix()
        if prefix and suffix:
            return f"{prefix}_{suffix}"
        return prefix or suffix

    # ------------------------------------------------------------------
    def _on_save_card(self) -> None:
        required_attrs = [
            "cboCardSprint",
            "txtCardTicket",
            "txtCardTitle",
            "txtCardBranch",
            "cboCardAssignee",
            "cboCardQA",
            "txtCardUnitUrl",
            "txtCardQAUrl",
            "cboCardGroup",
            "cboCardCompany",
            "cboCardIncidence",
        ]
        missing = [name for name in required_attrs if not hasattr(self, name)]
        if missing:
            logging.getLogger(__name__).warning(
                "Intento de guardar tarjeta sin formulario activo: faltan %s", ", ".join(missing)
            )
            QMessageBox.warning(
                self,
                "Tarjeta",
                "No hay un formulario de tarjeta activo para guardar los cambios.",
            )
            return

        target_data = self.cboCardSprint.currentData()
        target_sprint_id: Optional[int] = None
        sprint: Optional[Sprint] = None
        if target_data in (None, "", 0, "0"):
            target_sprint_id = None
        else:
            try:
                target_sprint_id = int(target_data)
            except (TypeError, ValueError):
                QMessageBox.warning(self, "Tarjeta", "Selecciona un sprint válido.")
                return
        if target_sprint_id is not None:
            sprint = self._sprints.get(target_sprint_id)
            if not sprint:
                QMessageBox.warning(self, "Tarjeta", "El sprint seleccionado ya no existe.")
                return
            if sprint.status == "closed":
                QMessageBox.warning(self, "Tarjeta", "No puedes mover la tarjeta a un sprint cerrado.")
                return

        ticket = self.txtCardTicket.text().strip()
        title = self.txtCardTitle.text().strip()
        branch_full = self._full_branch_name()
        if not ticket:
            QMessageBox.warning(self, "Tarjeta", "El identificador del ticket es obligatorio.")
            return
        if not title:
            QMessageBox.warning(self, "Tarjeta", "El título es obligatorio.")
            return
        if sprint and not branch_full:
            QMessageBox.warning(self, "Tarjeta", "Indica el nombre de la rama derivada.")
            return

        now = int(time.time())
        user = self._current_user()
        if self._selected_card_id and self._selected_card_id in self._cards:
            card = replace(self._cards[self._selected_card_id])
        else:
            card = Card(id=None, sprint_id=target_sprint_id)
            card.created_at = now
            card.created_by = user

        was_new = card.id is None
        previous_sprint_id = card.sprint_id
        card.sprint_id = target_sprint_id
        card.ticket_id = ticket
        card.title = title
        card.branch = branch_full
        card.assignee = self._combo_value(self.cboCardAssignee)
        card.qa_assignee = self._combo_value(self.cboCardQA)
        card.unit_tests_url = self.txtCardUnitUrl.text().strip() or None
        card.qa_url = self.txtCardQAUrl.text().strip() or None
        card.updated_at = now
        card.updated_by = user
        group_value = self.cboCardGroup.currentData() if hasattr(self, "cboCardGroup") else None
        card.group_name = group_value or (sprint.group_name if sprint else None)
        company_data = self.cboCardCompany.currentData() if hasattr(self, "cboCardCompany") else None
        try:
            if company_data not in (None, ""):
                card.company_id = int(company_data)
            else:
                card.company_id = sprint.company_id if sprint else None
        except (TypeError, ValueError):
            card.company_id = sprint.company_id if sprint else None

        if hasattr(self, "cboCardIncidence"):
            incidence_data = self.cboCardIncidence.currentData()
            try:
                if incidence_data not in (None, ""):
                    card.incidence_type_id = int(incidence_data)
                else:
                    card.incidence_type_id = None
            except (TypeError, ValueError):
                card.incidence_type_id = None

        if (
            (card.status or "").lower() == "terminated"
            and previous_sprint_id not in (None, card.sprint_id)
        ):
            QMessageBox.warning(
                self,
                "Tarjeta",
                "La tarjeta está marcada como terminada y no puede moverse a otro sprint.",
            )
            return

        try:
            saved = upsert_card(card)
        except sqlite3.IntegrityError:
            QMessageBox.warning(self, "Tarjeta", "No se pudo guardar: la rama indicada ya existe.")
            return
        except Exception as exc:  # pragma: no cover
            QMessageBox.critical(self, "Tarjeta", f"Error al guardar la tarjeta: {exc}")
            return

        if saved.id is not None:
            try:
                self._persist_card_script(saved)
            except Exception as exc:  # pragma: no cover - errores de conexión
                logging.getLogger(__name__).error(
                    "No se pudo guardar el script de la tarjeta %s: %s", saved.id, exc
                )
                QMessageBox.warning(
                    self,
                    "Tarjeta",
                    f"La tarjeta se guardó, pero el script no pudo actualizarse:\n{exc}",
                )

        if saved.id is not None:
            self._cards[saved.id] = saved
            self._selected_card_id = saved.id
        else:
            self._selected_card_id = None
        self._card_parent_id = saved.sprint_id if saved.sprint_id is not None else None
        if saved.sprint_id is not None:
            try:
                self._selected_sprint_id = int(saved.sprint_id)
            except (TypeError, ValueError):
                self._selected_sprint_id = None
        else:
            self._selected_sprint_id = None

        history = PipelineHistory()
        if saved.id:
            history.update_card_status(
                saved.id,
                unit_tests_status="done" if saved.unit_tests_done else "pending",
                qa_status="approved" if saved.qa_done else "pending",
            )

        message = "Tarjeta creada correctamente." if was_new else "Tarjeta actualizada correctamente."

        self._close_card_dialog()
        self.refresh()
        if saved.id:
            self._select_tree_item("card", saved.id)
        QMessageBox.information(self, "Tarjeta", message)

    # ------------------------------------------------------------------
    def _persist_card_script(self, card: Card) -> None:
        if not hasattr(self, "txtCardScript"):
            return
        if card.id is None:
            return
        content = self.txtCardScript.toPlainText()
        has_content = bool(content.strip())
        script = self._card_form_script
        filename_changed = False
        if script:
            filename_changed = (script.file_name or None) != (
                self._card_script_filename or script.file_name
            )
        elif self._card_script_filename:
            filename_changed = True

        should_delete = self._card_script_deleted or (
            script is not None and not has_content
        )

        if should_delete:
            delete_card_script(int(card.id))
            self._card_form_script = None
            self._card_script_dirty = False
            self._card_script_deleted = False
            self._card_script_filename = None
            self._card_script_original_text = ""
            card.script_id = None
            card.script_name = None
            card.script_updated_at = None
            card.script_updated_by = None
            self._update_card_script_info(card)
            return

        if not has_content:
            self._card_script_dirty = False
            self._card_script_filename = None
            self._card_script_original_text = ""
            self._update_card_script_info(card)
            return

        if not self._card_script_dirty and not filename_changed:
            return

        script = script or CardScript(id=None, card_id=int(card.id))
        script.card_id = int(card.id)
        script.content = content
        script.file_name = self._card_script_filename or script.file_name
        saved_script = save_card_script(script)
        self._card_form_script = saved_script
        self._card_script_dirty = False
        self._card_script_deleted = False
        self._card_script_filename = saved_script.file_name
        self._card_script_original_text = saved_script.content or ""
        card.script_id = saved_script.id
        card.script_name = saved_script.file_name
        card.script_updated_at = saved_script.updated_at
        card.script_updated_by = saved_script.updated_by
        self._update_card_script_info(card)

    # ------------------------------------------------------------------
    def _on_delete_card(self) -> None:
        if self._selected_card_id is None:
            return
        card = self._cards.get(self._selected_card_id)
        if not card:
            return
        confirm = QMessageBox.question(
            self,
            "Eliminar tarjeta",
            f"¿Eliminar la tarjeta '{card.ticket_id or card.title}'?",
        )
        if confirm != QMessageBox.Yes:
            return
        dialog = self._card_dialog
        if dialog and dialog.isVisible():
            try:
                dialog.hide()
            except RuntimeError:
                pass
        delete_card(card.id)
        self._selected_card_id = None
        self._card_parent_id = None
        self.tree.clearSelection()
        self._close_card_dialog()
        self.refresh()
        QMessageBox.information(self, "Tarjeta", "Tarjeta eliminada correctamente.")
        if dialog and dialog.isVisible():
            try:
                dialog.close()
            except RuntimeError:
                pass

    # ------------------------------------------------------------------
    def _on_cancel(self) -> None:
        self.tree.clearSelection()
        self._close_sprint_dialog()
        self._close_card_dialog()
        self._current_sprint_branch_key = None
        self._current_sprint_qa_branch_key = None
        self.update_permissions()

    # ------------------------------------------------------------------
    def _mark_card(self, kind: str) -> None:
        if self._selected_card_id is None:
            QMessageBox.information(self, "Tarjeta", "Selecciona una tarjeta primero")
            return
        card = self._cards.get(self._selected_card_id)
        sprint = self._sprints.get(card.sprint_id) if card else None
        if not card or not sprint:
            QMessageBox.warning(self, "Tarjeta", "La tarjeta seleccionada ya no existe.")
            return
        user = self._current_user()
        now = int(time.time())
        history = PipelineHistory()
        is_leader = require_roles("leader")
        is_card_assignee = bool(card.assignee and card.assignee == user)
        is_card_qa = bool(card.qa_assignee and card.qa_assignee == user)
        if kind == "unit":
            if not (is_leader or is_card_assignee):
                message = (
                    "Solo el desarrollador asignado o un líder pueden actualizar las pruebas unitarias."
                )
                if not card.assignee:
                    message = "Asigna un desarrollador antes de marcar las pruebas unitarias."
                QMessageBox.warning(self, "Tarjeta", message)
                return
            toggled_on = not card.unit_tests_done
            card.unit_tests_done = toggled_on
            card.unit_tests_by = user if toggled_on else None
            card.unit_tests_at = now if toggled_on else None
            history.update_card_status(
                card.id, unit_tests_status="done" if toggled_on else "pending"
            )
        elif kind == "qa":
            if not (is_leader or is_card_qa):
                message = "Solo la persona asignada en QA o un líder pueden aprobar QA."
                if not card.qa_assignee:
                    message = "Asigna un responsable de QA antes de marcar la revisión."
                QMessageBox.warning(self, "Tarjeta", message)
                return
            toggled_on = not card.qa_done
            card.qa_done = toggled_on
            card.qa_by = user if toggled_on else None
            card.qa_at = now if toggled_on else None
            history.update_card_status(
                card.id,
                qa_status="approved" if toggled_on else "pending",
                approved_by=user if toggled_on else "",
            )
        card.updated_at = now
        card.updated_by = user
        if card.qa_done and card.unit_tests_done:
            card.status = "qa"
        elif card.unit_tests_done:
            card.status = "unit"
        else:
            card.status = "pending"
        upsert_card(card)
        if card.id is not None:
            self._cards[card.id] = card
        checks = []
        checks.append("Pruebas: ✔" if card.unit_tests_done else "Pruebas: pendiente")
        checks.append("QA: ✔" if card.qa_done else "QA: pendiente")
        self.lblCardChecks.setText(" | ".join(checks))
        current_item = self.tree.currentItem()
        if current_item and current_item.data(0, Qt.UserRole) == ("card", card.id):
            card_checks = [
                "Unit ✔" if card.unit_tests_done else "Unit ✖",
                "QA ✔" if card.qa_done else "QA ✖",
                "Merge ✔" if is_card_ready_for_merge(card) else "Merge ✖",
            ]
            current_item.setText(6, " / ".join(card_checks))
        self.update_permissions()
        self._cards[card.id] = card
        self.refresh()
        if card.id:
            self._select_tree_item("card", card.id)


    # ------------------------------------------------------------------
    def _sanitize_slug(self, value: str) -> str:
        trimmed = (value or "").strip()
        return "".join(
            ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in trimmed
        )

    # ------------------------------------------------------------------
    def _default_script_filename(self, sprint: Sprint) -> str:
        parts: List[str] = ["scripts"]
        version_slug = self._sanitize_slug(sprint.version or "")
        name_slug = self._sanitize_slug(sprint.name or "")
        if version_slug:
            parts.append(version_slug)
        if name_slug:
            parts.append(name_slug)
        if len(parts) == 1 and sprint.id:
            parts.append(str(sprint.id))
        filename = "_".join(parts)
        return f"{filename}.sql"

    # ------------------------------------------------------------------
    def _compose_sprint_scripts(
        self, sprint: Sprint, bundle: List[Tuple[Card, CardScript]]
    ) -> str:
        lines: List[str] = []
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        sprint_parts = []
        if sprint.version:
            sprint_parts.append(sprint.version)
        if sprint.name:
            sprint_parts.append(sprint.name)
        sprint_label = " — ".join(sprint_parts)
        if not sprint_label:
            sprint_label = f"Sprint #{sprint.id}" if sprint.id else "Sprint"
        lines.append(f"-- Scripts del sprint {sprint_label}")
        lines.append(f"-- Generado el {timestamp}")
        lines.append("")
        for card, script in bundle:
            ticket = card.ticket_id or (f"Tarjeta #{card.id}" if card.id else "Tarjeta")
            title = card.title or "(sin título)"
            lines.append("--" + "=" * 70)
            lines.append(f"-- Tarjeta: {ticket} — {title}")
            if script.file_name:
                lines.append(f"-- Archivo original: {script.file_name}")
            if script.updated_at:
                updated_ts = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(script.updated_at)
                )
                if script.updated_by:
                    lines.append(
                        f"-- Última actualización: {updated_ts} por {script.updated_by}"
                    )
                else:
                    lines.append(f"-- Última actualización: {updated_ts}")
            elif script.updated_by:
                lines.append(
                    f"-- Última actualización registrada por {script.updated_by}"
                )
            lines.append("--" + "=" * 70)
            content = script.content or ""
            cleaned = content.rstrip("\n")
            if cleaned:
                lines.append(cleaned)
            else:
                lines.append("-- (Sin contenido)")
            lines.append("")
            lines.append("GO")
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    # ------------------------------------------------------------------
    def _on_bulk_assign_clicked(self) -> None:
        if not require_roles("leader"):
            QMessageBox.warning(
                self,
                "Asignar responsables",
                "No tienes permisos para asignar responsables masivamente.",
            )
            return

        card_ids = [card_id for card_id in self._selected_card_ids if card_id in self._cards]
        if not card_ids:
            QMessageBox.information(
                self,
                "Asignar responsables",
                "Selecciona al menos una tarjeta para actualizar.",
            )
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Asignar responsables")
        layout = QVBoxLayout(dialog)
        message = QLabel(
            f"Se actualizarán {len(card_ids)} tarjetas. Elige los cambios a aplicar."
        )
        message.setWordWrap(True)
        layout.addWidget(message)

        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        layout.addLayout(form)

        sentinel_no_change = "__no_change__"

        cbo_dev = QComboBox()
        cbo_dev.addItem("Sin cambios", sentinel_no_change)
        cbo_dev.addItem("Sin responsable", "")
        developer_names = sorted(
            filter_users_by_role(self._users, self._user_roles, "developer")
        )
        for name in developer_names:
            cbo_dev.addItem(name, name)
        form.addRow("Desarrollo:", cbo_dev)

        cbo_qa = QComboBox()
        cbo_qa.addItem("Sin cambios", sentinel_no_change)
        cbo_qa.addItem("Sin responsable", "")
        qa_names = sorted(filter_users_by_role(self._users, self._user_roles, "qa"))
        for name in qa_names:
            cbo_qa.addItem(name, name)
        form.addRow("QA:", cbo_qa)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec() != QDialog.Accepted:
            return

        dev_data = cbo_dev.currentData()
        qa_data = cbo_qa.currentData()
        apply_dev = dev_data != sentinel_no_change
        apply_qa = qa_data != sentinel_no_change
        if not apply_dev and not apply_qa:
            QMessageBox.information(
                self,
                "Asignar responsables",
                "No se seleccionó ningún cambio para aplicar.",
            )
            return

        dev_value = None
        if apply_dev and dev_data not in (None, ""):
            dev_value = str(dev_data)
        qa_value = None
        if apply_qa and qa_data not in (None, ""):
            qa_value = str(qa_data)

        now = int(time.time())
        user = self._current_user()
        updated_cards: List[int] = []
        failures: List[str] = []

        for card_id in card_ids:
            card = self._cards.get(card_id)
            if not card:
                continue
            updated = replace(card)
            if apply_dev:
                updated.assignee = dev_value
            if apply_qa:
                updated.qa_assignee = qa_value
            updated.updated_at = now
            updated.updated_by = user
            try:
                saved = upsert_card(updated)
            except sqlite3.IntegrityError:
                failures.append(
                    f"{card.ticket_id or card.title or card.id}: la rama indicada ya existe"
                )
                continue
            except Exception as exc:  # pragma: no cover - errores de conexión
                failures.append(f"{card.ticket_id or card.title or card.id}: {exc}")
                continue
            if saved.id is None:
                continue
            self._cards[saved.id] = saved
            updated_cards.append(saved.id)

        if not updated_cards:
            if failures:
                details = "\n".join(failures[:5])
                if len(failures) > 5:
                    details += f"\n... ({len(failures) - 5} más)"
                QMessageBox.warning(
                    self,
                    "Asignar responsables",
                    f"No se pudo actualizar ninguna tarjeta:\n{details}",
                )
            else:
                QMessageBox.information(
                    self,
                    "Asignar responsables",
                    "No hubo cambios que guardar en las tarjetas seleccionadas.",
                )
            return

        self._selected_card_ids = list(updated_cards)
        self._selected_card_id = updated_cards[-1]
        last_card = self._cards.get(updated_cards[-1])
        if last_card and getattr(last_card, "sprint_id", None) not in (None, ""):
            try:
                self._selected_sprint_id = int(last_card.sprint_id)
            except (TypeError, ValueError):
                self._selected_sprint_id = None
        else:
            self._selected_sprint_id = None

        self.refresh()

        summary = f"Se actualizaron {len(updated_cards)} tarjetas correctamente."
        if failures:
            details = "\n".join(failures[:5])
            if len(failures) > 5:
                details += f"\n... ({len(failures) - 5} más)"
            QMessageBox.warning(
                self,
                "Asignar responsables",
                f"{summary}\nSin embargo, {len(failures)} tarjetas no pudieron actualizarse:\n{details}",
            )
        else:
            QMessageBox.information(self, "Asignar responsables", summary)

    # ------------------------------------------------------------------
    def _on_export_sprint_scripts(self) -> None:
        sprint_id = self._selected_sprint_id
        if sprint_id is None:
            QMessageBox.information(
                self,
                "Scripts",
                "Selecciona un sprint para exportar sus scripts.",
            )
            return
        sprint = self._sprints.get(sprint_id)
        if not sprint:
            QMessageBox.warning(self, "Scripts", "El sprint seleccionado ya no existe.")
            return
        try:
            bundle = collect_sprint_scripts(int(sprint_id))
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.critical(
                self,
                "Scripts",
                f"No se pudieron obtener los scripts:\n{exc}",
            )
            return
        if not bundle:
            QMessageBox.information(
                self,
                "Scripts",
                "El sprint seleccionado no tiene tarjetas con script.",
            )
            return
        default_name = self._default_script_filename(sprint)
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Guardar scripts del sprint",
            default_name,
            "Archivos SQL (*.sql);;Todos los archivos (*)",
        )
        if not path:
            return
        try:
            payload = self._compose_sprint_scripts(sprint, bundle)
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(payload)
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Scripts",
                f"No se pudo guardar el archivo:\n{exc}",
            )
            return
        QMessageBox.information(
            self,
            "Scripts",
            f"Los scripts se guardaron en:\n{path}",
        )


class CardBrowser(QWidget):
    """Listado filtrable de tarjetas independientes del árbol de planeación."""

    cardActivated = Signal(int)
    newCardRequested = Signal(object, object)
    cardsImported = Signal()

    _ALL_VALUE = "__all__"
    _NONE_VALUE = "__none__"

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._cards: Dict[int, Card] = {}
        self._sprints: Dict[int, Sprint] = {}
        self._companies: Dict[int, Company] = {}
        self._incidence_types: Dict[int, IncidenceType] = {}
        self._incidence_icons: Dict[int, QIcon] = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        filter_row = QHBoxLayout()
        filter_row.setContentsMargins(0, 0, 0, 0)
        filter_row.setSpacing(6)

        lbl_group = QLabel("Grupo:")
        filter_row.addWidget(lbl_group)
        self.cboGroup = QComboBox()
        filter_row.addWidget(self.cboGroup, 1)

        lbl_company = QLabel("Empresa:")
        filter_row.addWidget(lbl_company)
        self.cboCompany = QComboBox()
        filter_row.addWidget(self.cboCompany, 1)

        lbl_sprint = QLabel("Sprint:")
        filter_row.addWidget(lbl_sprint)
        self.cboSprint = QComboBox()
        filter_row.addWidget(self.cboSprint, 1)

        lbl_status = QLabel("Estado:")
        filter_row.addWidget(lbl_status)
        self.cboStatus = QComboBox()
        filter_row.addWidget(self.cboStatus, 1)

        layout.addLayout(filter_row)

        search_row = QHBoxLayout()
        search_row.setContentsMargins(0, 0, 0, 0)
        search_row.setSpacing(6)
        lbl_search = QLabel("Buscar:")
        search_row.addWidget(lbl_search)
        self.txtSearch = QLineEdit()
        self.txtSearch.setPlaceholderText("Ticket, título, sprint o responsable")
        self.txtSearch.setClearButtonEnabled(True)
        search_row.addWidget(self.txtSearch, 1)
        layout.addLayout(search_row)

        button_row = QHBoxLayout()
        button_row.setContentsMargins(0, 0, 0, 0)
        button_row.setSpacing(6)
        self.btnImportCards = QPushButton("Importar tarjetas")
        self.btnImportCards.setIcon(get_icon("cloud-upload"))
        button_row.addWidget(self.btnImportCards)
        self.btnDownloadTemplate = QPushButton("Descargar plantilla")
        self.btnDownloadTemplate.setIcon(get_icon("cloud-download"))
        button_row.addWidget(self.btnDownloadTemplate)
        button_row.addStretch(1)
        self.btnNewCard = QPushButton("Nueva tarjeta")
        self.btnNewCard.setIcon(get_icon("build"))
        button_row.addWidget(self.btnNewCard)
        layout.addLayout(button_row)

        self.tree = QTreeWidget()
        self.tree.setColumnCount(10)
        self.tree.setHeaderLabels(
            [
                "Tarjeta",
                "Tipo incidencia",
                "Sprint",
                "Grupo",
                "Empresa",
                "Asignado",
                "QA",
                "Estado",
                "Script",
                "Checks",
            ]
        )
        self.tree.setRootIsDecorated(False)
        self.tree.setSelectionMode(QAbstractItemView.SingleSelection)
        layout.addWidget(self.tree, 1)

        self.cboGroup.currentIndexChanged.connect(self._on_group_filter_changed)
        self.cboCompany.currentIndexChanged.connect(self._on_company_filter_changed)
        self.cboSprint.currentIndexChanged.connect(self._apply_filters)
        self.cboStatus.currentIndexChanged.connect(self._apply_filters)
        self.txtSearch.textChanged.connect(self._apply_filters)
        self.tree.itemActivated.connect(self._on_item_activated)
        self.tree.itemDoubleClicked.connect(self._on_item_activated)
        self.btnNewCard.clicked.connect(self._on_new_card_clicked)
        self.btnImportCards.clicked.connect(self._on_import_cards_clicked)
        self.btnDownloadTemplate.clicked.connect(self._on_download_template_clicked)

        self._initialize_filters()

    # ------------------------------------------------------------------
    def _initialize_filters(self) -> None:
        self.cboGroup.addItem("Todos los grupos", self._ALL_VALUE)
        self.cboCompany.addItem("Todas las empresas", self._ALL_VALUE)
        self.cboSprint.addItem("Todos los sprints", self._ALL_VALUE)
        self.cboStatus.addItem("Todos los estados", self._ALL_VALUE)

    # ------------------------------------------------------------------
    def set_new_card_enabled(self, enabled: bool) -> None:
        self.btnNewCard.setEnabled(enabled)

    # ------------------------------------------------------------------
    def set_import_enabled(self, enabled: bool) -> None:
        self.btnImportCards.setEnabled(enabled)

    # ------------------------------------------------------------------
    def set_template_enabled(self, enabled: bool) -> None:
        self.btnDownloadTemplate.setEnabled(enabled)

    # ------------------------------------------------------------------
    def _on_new_card_clicked(self) -> None:
        group_filter = self._current_group_filter()
        if group_filter == self._NONE_VALUE:
            group_filter = None
        company_filter = self._normalize_company_id(self._current_company_filter())
        self.newCardRequested.emit(group_filter, company_filter)

    # ------------------------------------------------------------------
    def _on_import_cards_clicked(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Importar tarjetas",
            "",
            "Archivos de tarjetas (*.csv *.xlsx *.xlsm);;Todos los archivos (*)",
        )
        if not path:
            return

        try:
            summary = import_cards_from_file(path, username=self._active_username())
        except CardImportError as exc:
            QMessageBox.critical(self, "Importar tarjetas", str(exc))
            return

        message = self._format_import_summary(summary)
        if summary.errors:
            QMessageBox.warning(self, "Importar tarjetas", message)
        else:
            QMessageBox.information(self, "Importar tarjetas", message)

        if summary.created or summary.updated:
            self.cardsImported.emit()

    # ------------------------------------------------------------------
    def _on_download_template_clicked(self) -> None:
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Guardar plantilla de tarjetas",
            "tarjetas",
            "CSV (*.csv);;Excel (*.xlsx)",
        )
        if not path:
            return

        try:
            saved_path = write_cards_template(path)
        except CardImportError as exc:
            QMessageBox.critical(self, "Plantilla de tarjetas", str(exc))
            return

        QMessageBox.information(
            self,
            "Plantilla de tarjetas",
            f"La plantilla se guardó en:\n{saved_path}",
        )

    # ------------------------------------------------------------------
    def update_sources(
        self,
        cards: Dict[int, Card],
        sprints: Dict[int, Sprint],
        companies: Dict[int, Company],
        incidence_types: Dict[int, IncidenceType] | None = None,
        incidence_icons: Dict[int, QIcon] | None = None,
    ) -> None:
        prev_group = self._current_group_filter()
        prev_company = self._current_company_filter()
        prev_sprint = self._current_sprint_filter()
        prev_status = self._current_status_filter()
        prev_search = self.txtSearch.text()

        self._cards = dict(cards)
        self._sprints = dict(sprints)
        self._companies = dict(companies)
        self._incidence_types = dict(incidence_types or {})
        self._incidence_icons = dict(incidence_icons or {})

        self._update_group_filter_options(prev_group)
        self._update_status_filter_options(prev_status)
        self._update_company_filter_options(prev_company)
        self._update_sprint_filter_options(prev_sprint)

        self.txtSearch.blockSignals(True)
        self.txtSearch.setText(prev_search)
        self.txtSearch.blockSignals(False)

        self._apply_filters()

    # ------------------------------------------------------------------
    def _update_group_filter_options(self, previous: Optional[str]) -> None:
        include_blank = False
        groups: List[str] = []
        seen = set()
        for sprint in self._sprints.values():
            if sprint.group_name:
                if sprint.group_name not in seen:
                    groups.append(sprint.group_name)
                    seen.add(sprint.group_name)
            else:
                include_blank = True
        for card in self._cards.values():
            effective = card.group_name
            if not effective:
                sprint = self._sprints.get(card.sprint_id)
                effective = sprint.group_name if sprint else None
            if effective:
                if effective not in seen:
                    groups.append(effective)
                    seen.add(effective)
            else:
                include_blank = True
        groups.sort(key=lambda value: value.lower())

        self.cboGroup.blockSignals(True)
        self.cboGroup.clear()
        self.cboGroup.addItem("Todos los grupos", self._ALL_VALUE)
        if include_blank:
            self.cboGroup.addItem("Sin grupo", self._NONE_VALUE)
        for group in groups:
            self.cboGroup.addItem(group, group)
        target = self._ALL_VALUE if previous in (None, self._ALL_VALUE) else previous
        if previous == self._NONE_VALUE and include_blank:
            target = self._NONE_VALUE
        index = self.cboGroup.findData(target)
        if index < 0:
            index = 0
        self.cboGroup.setCurrentIndex(index)
        self.cboGroup.blockSignals(False)

    # ------------------------------------------------------------------
    def _update_company_filter_options(self, previous: Optional[object]) -> None:
        group_filter = self._current_group_filter()
        include_blank = False
        options: List[Tuple[str, object]] = [("Todas las empresas", self._ALL_VALUE)]
        added_ids: set = set()
        extra_companies: Dict[int, str] = {}

        for company in sorted(
            self._companies.values(), key=lambda comp: (comp.name or "").lower()
        ):
            if not self._matches_group_filter(company.group_name, group_filter):
                continue
            if company.id is None:
                include_blank = True
                continue
            options.append((company.name, company.id))
            added_ids.add(company.id)

        for card in self._cards.values():
            if not self._matches_group_filter(self._effective_group(card), group_filter):
                continue
            company_id = self._effective_company(card)
            if company_id is None:
                include_blank = True
                continue
            if company_id in added_ids:
                continue
            added_ids.add(company_id)
            extra_companies[company_id] = self._company_name(company_id)

        if include_blank:
            options.insert(1, ("Sin empresa", self._NONE_VALUE))

        for company_id, label in sorted(
            extra_companies.items(), key=lambda item: item[1].lower()
        ):
            options.append((label, company_id))

        self.cboCompany.blockSignals(True)
        self.cboCompany.clear()
        for label, value in options:
            self.cboCompany.addItem(label, value)
        target: object
        if previous in (None, self._ALL_VALUE):
            target = self._ALL_VALUE
        elif previous == self._NONE_VALUE and include_blank:
            target = self._NONE_VALUE
        else:
            target = previous
        index = self.cboCompany.findData(target)
        if index < 0:
            index = 0
        self.cboCompany.setCurrentIndex(index)
        self.cboCompany.blockSignals(False)

    # ------------------------------------------------------------------
    def _update_status_filter_options(self, previous: Optional[str]) -> None:
        statuses = sorted({(card.status or "pending").lower() for card in self._cards.values()})
        self.cboStatus.blockSignals(True)
        self.cboStatus.clear()
        self.cboStatus.addItem("Todos los estados", self._ALL_VALUE)
        for status in statuses:
            label = status.capitalize()
            self.cboStatus.addItem(label, status)
        target = self._ALL_VALUE if previous in (None, self._ALL_VALUE) else previous
        index = self.cboStatus.findData(target)
        if index < 0:
            index = 0
        self.cboStatus.setCurrentIndex(index)
        self.cboStatus.blockSignals(False)

    # ------------------------------------------------------------------
    def _update_sprint_filter_options(self, previous: Optional[int]) -> None:
        group_filter = self._current_group_filter()
        company_filter = self._current_company_filter()
        sprint_ids = set()
        for sprint in self._sprints.values():
            if self._matches_group_filter(sprint.group_name, group_filter) and self._matches_company_filter(
                sprint.company_id, company_filter
            ):
                if sprint.id is not None:
                    sprint_ids.add(sprint.id)
        for card in self._cards.values():
            if not self._matches_group_filter(self._effective_group(card), group_filter):
                continue
            if not self._matches_company_filter(self._effective_company(card), company_filter):
                continue
            if card.sprint_id:
                sprint_ids.add(card.sprint_id)

        sprints = [self._sprints[sid] for sid in sprint_ids if sid in self._sprints]
        sprints.sort(key=lambda sprint: ((sprint.version or "").lower(), (sprint.name or "").lower()))

        self.cboSprint.blockSignals(True)
        self.cboSprint.clear()
        self.cboSprint.addItem("Todos los sprints", self._ALL_VALUE)
        for sprint in sprints:
            if sprint.id is None:
                continue
            self.cboSprint.addItem(self._sprint_label(sprint), sprint.id)
        target = self._ALL_VALUE if previous in (None, self._ALL_VALUE) else previous
        index = self.cboSprint.findData(target)
        if index < 0:
            index = 0
        self.cboSprint.setCurrentIndex(index)
        self.cboSprint.blockSignals(False)

    # ------------------------------------------------------------------
    def _format_import_summary(self, summary: CardImportSummary) -> str:
        parts = [
            f"Tarjetas creadas: {summary.created}",
            f"Tarjetas actualizadas: {summary.updated}",
        ]
        if summary.skipped:
            parts.append(f"Filas omitidas: {summary.skipped}")
        if summary.errors:
            errors = [f"Fila {row}: {message}" for row, message in summary.errors[:5]]
            if len(summary.errors) > 5:
                remaining = len(summary.errors) - 5
                errors.append(f"... ({remaining} errores adicionales)")
            parts.append("Errores:\n" + "\n".join(errors))
        return "\n".join(parts)

    # ------------------------------------------------------------------
    def _active_username(self) -> str:
        username = current_username("")
        if username:
            return username
        active = get_active_user()
        return active.username if active else ""

    # ------------------------------------------------------------------
    def _current_group_filter(self) -> Optional[str]:
        value = self.cboGroup.currentData()
        if value == self._ALL_VALUE:
            return None
        return value

    # ------------------------------------------------------------------
    def _current_company_filter(self) -> Optional[object]:
        value = self.cboCompany.currentData()
        if value == self._ALL_VALUE:
            return None
        return value

    # ------------------------------------------------------------------
    def _current_status_filter(self) -> Optional[str]:
        value = self.cboStatus.currentData()
        if value == self._ALL_VALUE:
            return None
        return value

    # ------------------------------------------------------------------
    def _current_sprint_filter(self) -> Optional[int]:
        value = self.cboSprint.currentData()
        if value == self._ALL_VALUE:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    def _matches_group_filter(self, value: Optional[str], group_filter: Optional[str]) -> bool:
        normalized = (value or "").strip() or None
        if group_filter is None:
            return True
        if group_filter == self._NONE_VALUE:
            return normalized is None
        return normalized == group_filter

    # ------------------------------------------------------------------
    def _matches_company_filter(
        self,
        value: Optional[object],
        company_filter: Optional[object],
    ) -> bool:
        normalized = self._normalize_company_id(value)
        if company_filter is None:
            return True
        if company_filter == self._NONE_VALUE:
            return normalized is None
        try:
            return normalized == int(company_filter)
        except (TypeError, ValueError):
            return False

    # ------------------------------------------------------------------
    def _normalize_company_id(self, value: Optional[object]) -> Optional[int]:
        if value in (None, "", 0):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    def _incidence_for_card(self, card: Card) -> Optional[IncidenceType]:
        raw_value = getattr(card, "incidence_type_id", None)
        if raw_value in (None, "", 0):
            return None
        try:
            type_id = int(raw_value)
        except (TypeError, ValueError):
            return None
        return self._incidence_types.get(type_id)

    # ------------------------------------------------------------------
    def _incidence_label(self, card: Card) -> str:
        incidence = self._incidence_for_card(card)
        return incidence.name if incidence else ""

    # ------------------------------------------------------------------
    def _effective_group(self, card: Card) -> Optional[str]:
        if card.group_name:
            return card.group_name
        sprint = self._sprints.get(card.sprint_id)
        return sprint.group_name if sprint else None

    # ------------------------------------------------------------------
    def _effective_company(self, card: Card) -> Optional[int]:
        if card.company_id not in (None, ""):
            return self._normalize_company_id(card.company_id)
        sprint = self._sprints.get(card.sprint_id)
        if sprint:
            return self._normalize_company_id(sprint.company_id)
        return None

    # ------------------------------------------------------------------
    def _company_name(self, company_id: Optional[int]) -> str:
        normalized = self._normalize_company_id(company_id)
        if normalized is None:
            return ""
        company = self._companies.get(normalized)
        if company:
            return company.name
        return f"Empresa #{normalized}"

    # ------------------------------------------------------------------
    def _sprint_label(self, sprint: Optional[Sprint]) -> str:
        if not sprint:
            return ""
        label = sprint.version or ""
        if sprint.name:
            if label:
                label += " — "
            label += sprint.name
        return label or f"Sprint #{sprint.id}" if sprint.id else ""

    # ------------------------------------------------------------------
    def _apply_filters(self) -> None:
        cards = self._filtered_cards()
        self._populate_tree(cards)

    # ------------------------------------------------------------------
    def _filtered_cards(self) -> List[Card]:
        group_filter = self._current_group_filter()
        company_filter = self._current_company_filter()
        status_filter = self._current_status_filter()
        sprint_filter = self._current_sprint_filter()
        search = self.txtSearch.text().strip().lower()

        results: List[Card] = []
        for card in self._cards.values():
            effective_group = self._effective_group(card)
            if not self._matches_group_filter(effective_group, group_filter):
                continue
            effective_company = self._effective_company(card)
            if not self._matches_company_filter(effective_company, company_filter):
                continue
            if status_filter:
                status_value = (card.status or "pending").lower()
                if status_value != status_filter:
                    continue
            if sprint_filter and card.sprint_id != sprint_filter:
                continue
            if search:
                sprint = self._sprints.get(card.sprint_id)
                haystack = " ".join(
                    filter(
                        None,
                        [
                            card.ticket_id or "",
                            card.title or "",
                            effective_group or "",
                            self._company_name(effective_company),
                            sprint.version if sprint else "",
                            sprint.name if sprint else "",
                            card.assignee or "",
                            card.qa_assignee or "",
                            self._incidence_label(card),
                        ],
                    )
                ).lower()
                if search not in haystack:
                    continue
            results.append(card)

        results.sort(
            key=lambda card: (
                (self._sprint_label(self._sprints.get(card.sprint_id)) or "").lower(),
                (card.ticket_id or "").lower(),
                (card.title or "").lower(),
            )
        )
        return results

    # ------------------------------------------------------------------
    def _populate_tree(self, cards: List[Card]) -> None:
        self.tree.setUpdatesEnabled(False)
        self.tree.clear()
        for card in cards:
            sprint = self._sprints.get(card.sprint_id)
            group_value = self._effective_group(card) or ""
            company_name = self._company_name(self._effective_company(card))
            status_value = (card.status or "pendiente").capitalize()
            checks = []
            checks.append("Unit ✔" if card.unit_tests_done else "Unit ✖")
            checks.append("QA ✔" if card.qa_done else "QA ✖")
            if card.status and card.status.lower() == "terminated":
                checks.append("Terminado")

            item = QTreeWidgetItem()
            if card.ticket_id and card.title:
                item.setText(0, f"{card.ticket_id} — {card.title}")
            elif card.title:
                item.setText(0, card.title)
            else:
                item.setText(0, card.ticket_id or "(sin título)")
            incidence = self._incidence_for_card(card)
            item.setText(1, incidence.name if incidence else "")
            item.setText(2, self._sprint_label(sprint) if sprint else "")
            item.setText(3, group_value)
            item.setText(4, company_name)
            item.setText(5, card.assignee or "")
            item.setText(6, card.qa_assignee or "")
            item.setText(7, status_value)
            item.setText(8, "Sí" if card.script_id else "No")
            item.setText(9, " / ".join(checks))
            if card.id is not None:
                item.setData(0, Qt.UserRole, card.id)
            self._apply_card_style(item, card, incidence)
            self.tree.addTopLevelItem(item)
        self.tree.setUpdatesEnabled(True)
        self.tree.resizeColumnToContents(0)
        self.tree.resizeColumnToContents(1)

    # ------------------------------------------------------------------
    def _apply_card_style(
        self,
        item: QTreeWidgetItem,
        card: Card,
        incidence: Optional[IncidenceType],
    ) -> None:
        icon = QIcon()
        if incidence and incidence.id is not None:
            icon = self._incidence_icons.get(int(incidence.id), QIcon())
        if icon and not icon.isNull():
            item.setIcon(1, icon)
        else:
            item.setIcon(1, QIcon())

        background = None
        foreground = None
        status_value = getattr(card, "status", None)
        if isinstance(status_value, str) and _card_has_assigned_sprint(card):
            background, foreground = status_brushes(status_value)

        for column in range(item.columnCount()):
            if background:
                item.setBackground(column, background)
            else:
                item.setBackground(column, QBrush())
            if foreground:
                item.setForeground(column, foreground)
            else:
                item.setForeground(column, QBrush())

    # ------------------------------------------------------------------
    def _on_item_activated(self, item: QTreeWidgetItem, _: int) -> None:
        if not item:
            return
        card_id = item.data(0, Qt.UserRole)
        if card_id is None:
            return
        try:
            value = int(card_id)
        except (TypeError, ValueError):
            return
        self.cardActivated.emit(value)

    # ------------------------------------------------------------------
    def _on_group_filter_changed(self) -> None:
        prev_company = self._current_company_filter()
        prev_sprint = self._current_sprint_filter()
        self._update_company_filter_options(prev_company)
        self._update_sprint_filter_options(prev_sprint)
        self._apply_filters()

    # ------------------------------------------------------------------
    def _on_company_filter_changed(self) -> None:
        prev_sprint = self._current_sprint_filter()
        self._update_sprint_filter_options(prev_sprint)
        self._apply_filters()
