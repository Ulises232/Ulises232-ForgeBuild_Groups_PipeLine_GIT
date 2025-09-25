from __future__ import annotations

import sqlite3
import time
from dataclasses import replace
from typing import Dict, Iterable, List, Optional, Tuple

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QStackedWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..core.branch_store import (
    BranchRecord,
    Card,
    Sprint,
    delete_card,
    delete_sprint,
    list_cards,
    list_sprints,
    list_users,
    list_user_roles,
    load_index,
    upsert_card,
    upsert_sprint,
)
from ..core.config import load_config
from ..core.git_tasks_local import create_branches_local
from ..core.pipeline_history import PipelineHistory
from ..core.session import current_username, get_active_user, require_roles
from ..core.sprint_queries import branches_by_group, is_card_ready_for_merge
from .sprint_helpers import filter_users_by_role
from ..ui.icons import get_icon

class SprintView(QWidget):
    """Single window to manage sprints and cards."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._sprints: Dict[int, Sprint] = {}
        self._cards: Dict[int, Card] = {}
        self._branch_index: Dict[str, BranchRecord] = {}
        self._users: List[str] = []
        self._user_roles: Dict[str, List[str]] = {}
        self._cfg = load_config()

        self._selected_sprint_id: Optional[int] = None
        self._selected_card_id: Optional[int] = None
        self._card_parent_id: Optional[int] = None
        self._current_sprint_branch_key: Optional[str] = None
        self._current_sprint_qa_branch_key: Optional[str] = None
        self._current_card_prefix: str = ""

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

        splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(splitter, 1)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)

        action_row = QHBoxLayout()
        action_row.setContentsMargins(0, 0, 0, 0)
        action_row.setSpacing(6)

        self.btnNewSprint = QPushButton("Nuevo sprint")
        self.btnNewSprint.setIcon(get_icon("branch"))
        action_row.addWidget(self.btnNewSprint)

        self.btnNewCard = QPushButton("Nueva tarjeta")
        self.btnNewCard.setIcon(get_icon("build"))
        action_row.addWidget(self.btnNewCard)

        action_row.addStretch(1)
        left_layout.addLayout(action_row)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(
            [
                "Sprint/Tarjeta",
                "Asignado",
                "QA",
                "Checks",
                "Rama",
                "Rama QA",
                "Local",
                "Origen",
                "Creada por",
            ]
        )
        self.tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tree.setUniformRowHeights(True)
        left_layout.addWidget(self.tree, 1)

        splitter.addWidget(left_panel)

        right_panel = QWidget()
        right_panel.setMinimumWidth(360)
        right_panel.setMaximumWidth(860)
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(12)

        self.stack = QStackedWidget()
        right_layout.addWidget(self.stack, 1)

        self._build_empty_page()
        self._build_sprint_form()
        self._build_card_form()

        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 3)
        splitter.setSizes([860, 520])

        self.btnRefresh.clicked.connect(self.refresh)
        self.btnNewSprint.clicked.connect(self._start_new_sprint)
        self.btnNewCard.clicked.connect(self._start_new_card)
        self.tree.itemSelectionChanged.connect(self._on_selection_changed)

        self.update_permissions()

    # ------------------------------------------------------------------
    def _build_empty_page(self) -> None:
        container = QWidget()
        box = QVBoxLayout(container)
        box.setAlignment(Qt.AlignCenter)
        label = QLabel("Selecciona un sprint o tarjeta para editar sus detalles.")
        label.setAlignment(Qt.AlignCenter)
        label.setWordWrap(True)
        box.addWidget(label)
        self.stack.addWidget(container)

    # ------------------------------------------------------------------
    def _build_sprint_form(self) -> None:
        self.pageSprint = QGroupBox("Detalles del sprint")
        form = QFormLayout(self.pageSprint)
        form.setLabelAlignment(Qt.AlignRight)

        branch_row = QHBoxLayout()
        self.txtSprintBranch = QLineEdit()
        self.txtSprintBranch.setReadOnly(True)
        branch_row.addWidget(self.txtSprintBranch, 1)
        self.btnPickBranch = QPushButton("Seleccionar rama")
        self.btnPickBranch.setIcon(get_icon("branch"))
        branch_row.addWidget(self.btnPickBranch)
        form.addRow("Rama base", branch_row)

        qa_row = QHBoxLayout()
        self.txtSprintQABranch = QLineEdit()
        self.txtSprintQABranch.setReadOnly(True)
        qa_row.addWidget(self.txtSprintQABranch, 1)
        self.btnPickQABranch = QPushButton("Seleccionar rama QA")
        self.btnPickQABranch.setIcon(get_icon("branch"))
        qa_row.addWidget(self.btnPickQABranch)
        form.addRow("Rama QA", qa_row)

        self.txtSprintName = QLineEdit()
        form.addRow("Nombre", self.txtSprintName)

        self.txtSprintVersion = QLineEdit()
        form.addRow("Versión", self.txtSprintVersion)

        self.cboSprintLead = QComboBox()
        form.addRow("Responsable", self.cboSprintLead)

        self.cboSprintQA = QComboBox()
        form.addRow("Responsable QA", self.cboSprintQA)

        self.chkSprintClosed = QCheckBox("Sprint finalizado")
        form.addRow("Estado", self.chkSprintClosed)

        self.lblSprintMeta = QLabel("")
        self.lblSprintMeta.setWordWrap(True)
        form.addRow("", self.lblSprintMeta)

        button_row = QHBoxLayout()
        button_row.addStretch(1)
        self.btnSprintDelete = QPushButton("Eliminar")
        self.btnSprintDelete.setIcon(get_icon("delete"))
        button_row.addWidget(self.btnSprintDelete)
        self.btnSprintCancel = QPushButton("Cancelar")
        button_row.addWidget(self.btnSprintCancel)
        self.btnSprintSave = QPushButton("Guardar")
        self.btnSprintSave.setIcon(get_icon("save"))
        button_row.addWidget(self.btnSprintSave)
        form.addRow("", button_row)

        self.btnPickBranch.clicked.connect(self._on_pick_branch)
        self.btnPickQABranch.clicked.connect(self._on_pick_qa_branch)
        self.btnSprintSave.clicked.connect(self._on_save_sprint)
        self.btnSprintCancel.clicked.connect(self._on_cancel)
        self.btnSprintDelete.clicked.connect(self._on_delete_sprint)

        self.stack.addWidget(self.pageSprint)

    # ------------------------------------------------------------------
    def _build_card_form(self) -> None:
        self.pageCard = QGroupBox("Detalles de la tarjeta")
        form = QFormLayout(self.pageCard)
        form.setLabelAlignment(Qt.AlignRight)

        self.lblCardSprint = QLabel("-")
        form.addRow("Sprint", self.lblCardSprint)

        self.txtCardTicket = QLineEdit()
        form.addRow("Ticket", self.txtCardTicket)

        self.txtCardTitle = QLineEdit()
        form.addRow("Título", self.txtCardTitle)

        branch_row = QHBoxLayout()
        self.lblCardPrefix = QLabel("")
        branch_row.addWidget(self.lblCardPrefix)
        self.txtCardBranch = QLineEdit()
        branch_row.addWidget(self.txtCardBranch, 1)
        self.lblCardBranchPreview = QLabel("")
        branch_row.addWidget(self.lblCardBranchPreview)
        form.addRow("Rama", branch_row)
        self.txtCardBranch.textChanged.connect(self._update_branch_preview)

        self.cboCardAssignee = QComboBox()
        form.addRow("Desarrollador", self.cboCardAssignee)

        self.cboCardQA = QComboBox()
        form.addRow("QA", self.cboCardQA)

        self.txtCardUnitUrl = QLineEdit()
        self.txtCardUnitUrl.setPlaceholderText("https://...")
        form.addRow("Link pruebas unitarias", self.txtCardUnitUrl)

        self.txtCardQAUrl = QLineEdit()
        self.txtCardQAUrl.setPlaceholderText("https://...")
        form.addRow("Link QA", self.txtCardQAUrl)

        self.lblCardChecks = QLabel("Pruebas: pendiente | QA: pendiente")
        form.addRow("Checks", self.lblCardChecks)

        status_row = QHBoxLayout()
        self.lblCardLocal = QLabel("Local: -")
        status_row.addWidget(self.lblCardLocal)
        self.lblCardOrigin = QLabel("Origen: -")
        status_row.addWidget(self.lblCardOrigin)
        self.lblCardCreator = QLabel("Creada por: -")
        status_row.addWidget(self.lblCardCreator)
        status_row.addStretch(1)
        form.addRow("Estado de rama", status_row)

        button_row = QHBoxLayout()
        self.btnCardDelete = QPushButton("Eliminar")
        self.btnCardDelete.setIcon(get_icon("delete"))
        button_row.addWidget(self.btnCardDelete)
        self.btnCardMarkUnit = QPushButton("Marcar pruebas unitarias")
        self.btnCardMarkUnit.setIcon(get_icon("build"))
        button_row.addWidget(self.btnCardMarkUnit)
        self.btnCardMarkQA = QPushButton("Marcar QA")
        self.btnCardMarkQA.setIcon(get_icon("log"))
        button_row.addWidget(self.btnCardMarkQA)
        self.btnCardCreateBranch = QPushButton("Crear rama")
        self.btnCardCreateBranch.setIcon(get_icon("branch"))
        button_row.addWidget(self.btnCardCreateBranch)
        button_row.addStretch(1)
        self.btnCardCancel = QPushButton("Cancelar")
        button_row.addWidget(self.btnCardCancel)
        self.btnCardSave = QPushButton("Guardar")
        self.btnCardSave.setIcon(get_icon("save"))
        button_row.addWidget(self.btnCardSave)
        form.addRow("", button_row)

        self.btnCardSave.clicked.connect(self._on_save_card)
        self.btnCardCancel.clicked.connect(self._on_cancel)
        self.btnCardDelete.clicked.connect(self._on_delete_card)
        self.btnCardCreateBranch.clicked.connect(self._on_create_branch)
        self.btnCardMarkUnit.clicked.connect(lambda: self._mark_card("unit"))
        self.btnCardMarkQA.clicked.connect(lambda: self._mark_card("qa"))

        self.stack.addWidget(self.pageCard)

    # ------------------------------------------------------------------
    def update_permissions(self) -> None:
        can_lead = require_roles("leader")
        can_mark_unit = require_roles("developer", "leader")
        can_mark_qa = require_roles("qa", "leader")
        sprint_mode = self.stack.currentWidget() is self.pageSprint
        card_mode = self.stack.currentWidget() is self.pageCard
        card = self._cards.get(self._selected_card_id or -1)

        self.btnNewSprint.setEnabled(can_lead)

        self.btnPickBranch.setEnabled(can_lead and sprint_mode)
        self.btnPickQABranch.setEnabled(can_lead and sprint_mode)
        self.btnSprintSave.setEnabled(can_lead and sprint_mode)
        self.btnSprintDelete.setEnabled(can_lead and sprint_mode and self._selected_sprint_id is not None)
        self.chkSprintClosed.setEnabled(can_lead and sprint_mode)

        has_card = card is not None and card.id is not None
        allow_unit_toggle = card_mode and has_card and can_mark_unit
        allow_qa_toggle = card_mode and has_card and can_mark_qa

        if card and card.unit_tests_done:
            self.btnCardMarkUnit.setText("Desmarcar pruebas unitarias")
        else:
            self.btnCardMarkUnit.setText("Marcar pruebas unitarias")
        self.btnCardMarkUnit.setEnabled(allow_unit_toggle)
        if not allow_unit_toggle:
            if card_mode and not has_card and can_mark_unit:
                tooltip = "Guarda la tarjeta antes de actualizar las pruebas unitarias"
            elif not can_mark_unit:
                tooltip = (
                    "Solo desarrolladores o líderes pueden actualizar las pruebas unitarias"
                )
            else:
                tooltip = ""
            self.btnCardMarkUnit.setToolTip(tooltip)
        else:
            self.btnCardMarkUnit.setToolTip("")

        if card and card.qa_done:
            self.btnCardMarkQA.setText("Desmarcar QA")
        else:
            self.btnCardMarkQA.setText("Marcar QA")
        self.btnCardMarkQA.setEnabled(allow_qa_toggle)
        if not allow_qa_toggle:
            if card_mode and not has_card and can_mark_qa:
                tooltip = "Guarda la tarjeta antes de actualizar las pruebas QA"
            elif not can_mark_qa:
                tooltip = "Solo QA o líderes pueden aprobar QA"
            else:
                tooltip = ""
            self.btnCardMarkQA.setToolTip(tooltip)
        else:
            self.btnCardMarkQA.setToolTip("")

        can_edit_unit_url = card_mode and can_mark_unit
        can_edit_qa_url = card_mode and can_mark_qa
        self.txtCardUnitUrl.setReadOnly(not can_edit_unit_url)
        self.txtCardQAUrl.setReadOnly(not can_edit_qa_url)
        if not can_edit_unit_url:
            self.txtCardUnitUrl.setToolTip(
                "Solo desarrolladores o líderes pueden registrar el enlace de pruebas unitarias"
            )
        else:
            self.txtCardUnitUrl.setToolTip("")
        if not can_edit_qa_url:
            self.txtCardQAUrl.setToolTip(
                "Solo QA o líderes pueden registrar el enlace de pruebas QA"
            )
        else:
            self.txtCardQAUrl.setToolTip("")

    # ------------------------------------------------------------------
    def refresh(self) -> None:
        self._sprints.clear()
        self._cards.clear()
        self._branch_index = load_index()

        for sprint in list_sprints():
            if sprint.id is None:
                continue
            self._sprints[sprint.id] = sprint

        sprint_ids = list(self._sprints.keys())
        if sprint_ids:
            for card in list_cards(sprint_ids=sprint_ids):
                if card.id is None:
                    continue
                self._cards[card.id] = card

        users = list_users(include_inactive=False)
        self._users = sorted({user.username for user in users})
        self._user_roles = list_user_roles()

        self._populate_user_combo(self.cboSprintLead, None)
        self._populate_user_combo(
            self.cboSprintQA, None, allow_empty=True, required_role="qa"
        )
        self._populate_user_combo(
            self.cboCardAssignee, None, allow_empty=True, required_role="developer"
        )
        self._populate_user_combo(
            self.cboCardQA, None, allow_empty=True, required_role="qa"
        )

        self._populate_tree()
        self._restore_selection()
        self._update_new_card_button()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _populate_tree(self) -> None:
        self.tree.clear()
        for sprint in sorted(
            self._sprints.values(), key=lambda s: ((s.version or "").lower(), (s.name or "").lower())
        ):
            sprint_item = QTreeWidgetItem()
            sprint_label = f"{sprint.version} — {sprint.name}"
            if sprint.status == "closed":
                sprint_label += " (finalizado)"
            sprint_item.setText(0, sprint_label)
            sprint_item.setText(1, sprint.lead_user or "")
            sprint_item.setText(2, sprint.qa_user or "")
            sprint_item.setText(3, "Cerrado" if sprint.status == "closed" else "Abierto")
            sprint_item.setText(4, sprint.branch_key)
            sprint_item.setText(5, sprint.qa_branch_key or "")
            sprint_item.setText(6, "-")
            sprint_item.setText(7, "-")
            sprint_item.setText(8, sprint.created_by or "")
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

        item = QTreeWidgetItem()
        item.setText(0, display)
        item.setText(1, card.assignee or "")
        item.setText(2, card.qa_assignee or "")
        checks = []
        checks.append("Unit ✔" if card.unit_tests_done else "Unit ✖")
        checks.append("QA ✔" if card.qa_done else "QA ✖")
        checks.append("Merge ✔" if is_card_ready_for_merge(card) else "Merge ✖")
        item.setText(3, " / ".join(checks))
        item.setText(4, card.branch)
        item.setText(5, sprint.qa_branch_key or "")

        record = self._branch_record_for_card(card, sprint)
        has_branch = bool((card.branch or "").strip())
        if record:
            local_text = "Sí" if record.exists_local else "No"
            origin_text = "Sí" if record.exists_origin else "No"
            creator = card.branch_created_by or record.last_updated_by or record.created_by or ""
        else:
            local_text = "No" if has_branch else "-"
            origin_text = "No" if has_branch else "-"
            creator = card.branch_created_by or ""

        item.setText(6, local_text)
        item.setText(7, origin_text)
        item.setText(8, creator or "")
        item.setData(0, Qt.UserRole, ("card", card.id))
        parent.addChild(item)

    # ------------------------------------------------------------------
    def _branch_record_for_card(self, card: Card, sprint: Sprint) -> Optional[BranchRecord]:
        branch_key = card.branch_key or self._build_card_branch_key(card, sprint)
        if not branch_key:
            return None
        return self._branch_index.get(branch_key)

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
        if self._selected_card_id and self._selected_card_id in self._cards:
            self._select_tree_item("card", self._selected_card_id)
            return
        if self._selected_sprint_id and self._selected_sprint_id in self._sprints:
            self._select_tree_item("sprint", self._selected_sprint_id)
            return
        self.stack.setCurrentIndex(0)

    # ------------------------------------------------------------------
    def _select_tree_item(self, kind: str, ident: int) -> None:
        iters: Iterable[QTreeWidgetItem] = self.tree.findItems("*", Qt.MatchWildcard | Qt.MatchRecursive, 0)
        for item in iters:
            data = item.data(0, Qt.UserRole) or (None, None)
            if data == (kind, ident):
                self.tree.setCurrentItem(item)
                return

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
        item = self.tree.currentItem()
        if not item:
            self._selected_card_id = None
            self._selected_sprint_id = None
            self.stack.setCurrentIndex(0)
            self.update_permissions()
            return

        kind, ident = item.data(0, Qt.UserRole) or (None, None)
        if kind == "sprint" and ident is not None:
            sprint_id = int(ident)
            self._selected_sprint_id = sprint_id
            self._selected_card_id = None
            sprint = self._sprints.get(sprint_id)
            if sprint:
                self._show_sprint_form(sprint)
        elif kind == "card" and ident is not None:
            card_id = int(ident)
            self._selected_card_id = card_id
            card = self._cards.get(card_id)
            sprint = None
            if card:
                sprint = self._sprints.get(card.sprint_id)
            if sprint:
                self._selected_sprint_id = sprint.id
                self._show_card_form(card, sprint)
        self._update_new_card_button()
        self.update_permissions()

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
        self.stack.setCurrentWidget(self.pageSprint)
        self._current_sprint_branch_key = sprint.branch_key
        self._current_sprint_qa_branch_key = sprint.qa_branch_key or None
        self.txtSprintBranch.setText(sprint.branch_key)
        self.txtSprintQABranch.setText(sprint.qa_branch_key or "")
        self.txtSprintName.setText(sprint.name)
        self.txtSprintVersion.setText(sprint.version)
        self._populate_user_combo(self.cboSprintLead, sprint.lead_user or None)
        self._populate_user_combo(
            self.cboSprintQA,
            sprint.qa_user or None,
            allow_empty=True,
            required_role="qa",
        )
        self.chkSprintClosed.setChecked(sprint.status == "closed")
        meta_lines = []
        if sprint.created_by:
            meta_lines.append(f"Creado por {sprint.created_by}")
        if sprint.status == "closed" and sprint.closed_by:
            meta_lines.append(f"Finalizado por {sprint.closed_by}")
        self.lblSprintMeta.setText("\n".join(meta_lines))
        self._selected_sprint_id = sprint.id
        self._selected_card_id = None
        if new:
            self.lblSprintMeta.clear()
        self._update_new_card_button()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _show_card_form(self, card: Card, sprint: Sprint, new: bool = False) -> None:
        self.stack.setCurrentWidget(self.pageCard)
        self.lblCardSprint.setText(f"{sprint.version} — {sprint.name}")
        prefix = self._branch_prefix(sprint)
        self._current_card_prefix = prefix
        self.lblCardPrefix.setText(prefix)
        suffix = card.branch or ""
        if prefix and suffix.startswith(prefix):
            suffix = suffix[len(prefix) :]
        self.txtCardBranch.setText(suffix)
        self.txtCardTicket.setText(card.ticket_id or "")
        self.txtCardTitle.setText(card.title or "")
        self._populate_user_combo(
            self.cboCardAssignee,
            card.assignee or None,
            allow_empty=True,
            required_role="developer",
        )
        self._populate_user_combo(
            self.cboCardQA,
            card.qa_assignee or None,
            allow_empty=True,
            required_role="qa",
        )
        self.txtCardUnitUrl.setText(card.unit_tests_url or "")
        self.txtCardQAUrl.setText(card.qa_url or "")
        checks = []
        checks.append("Pruebas: ✔" if card.unit_tests_done else "Pruebas: pendiente")
        checks.append("QA: ✔" if card.qa_done else "QA: pendiente")
        self.lblCardChecks.setText(" | ".join(checks))
        record = self._branch_record_for_card(card, sprint)
        if record:
            self.lblCardLocal.setText("Local: Sí" if record.exists_local else "Local: No")
            self.lblCardOrigin.setText("Origen: Sí" if record.exists_origin else "Origen: No")
        else:
            self.lblCardLocal.setText("Local: -")
            self.lblCardOrigin.setText("Origen: -")
        creator = card.branch_created_by or (record.last_updated_by if record else "")
        if not creator and record:
            creator = record.created_by
        self.lblCardCreator.setText(f"Creada por: {creator or '-'}")
        self._selected_card_id = card.id
        self._card_parent_id = card.sprint_id
        if new:
            self.lblCardCreator.setText("Creada por: -")
        self._update_branch_preview()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _update_branch_preview(self) -> None:
        full = self._full_branch_name()
        if full:
            self.lblCardBranchPreview.setText(f"→ {full}")
        else:
            self.lblCardBranchPreview.setText("")

    # ------------------------------------------------------------------
    def _branch_prefix(self, sprint: Optional[Sprint]) -> str:
        if not sprint or not sprint.version:
            return ""
        version = sprint.version.strip()
        if not version:
            return ""
        return f"v{version}_"

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
        now = int(time.time())
        user = self._current_user()
        sprint.branch_key = branch_key
        sprint.qa_branch_key = qa_branch_key or None
        sprint.name = name
        sprint.version = version
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
        self.refresh()
        self._select_tree_item("sprint", saved.id)

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
        self.refresh()
        self.stack.setCurrentIndex(0)

    # ------------------------------------------------------------------
    def _full_branch_name(self) -> str:
        suffix = self.txtCardBranch.text().strip()
        if self._current_card_prefix:
            return f"{self._current_card_prefix}{suffix}" if suffix else ""
        return suffix

    # ------------------------------------------------------------------
    def _on_save_card(self) -> None:
        sprint_id = self._card_parent_id or self._current_sprint_id()
        if sprint_id is None:
            QMessageBox.warning(self, "Tarjeta", "Selecciona un sprint válido.")
            return
        sprint = self._sprints.get(sprint_id)
        if not sprint:
            QMessageBox.warning(self, "Tarjeta", "El sprint ya no existe.")
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
        if not branch_full:
            QMessageBox.warning(self, "Tarjeta", "Indica el nombre de la rama derivada.")
            return

        now = int(time.time())
        user = self._current_user()
        card: Optional[Card]
        if self._selected_card_id and self._selected_card_id in self._cards:
            card = replace(self._cards[self._selected_card_id])
        else:
            card = Card(id=None, sprint_id=sprint_id)
            card.created_at = now
            card.created_by = user

        card.sprint_id = sprint_id
        card.ticket_id = ticket
        card.title = title
        card.branch = branch_full
        card.assignee = self._combo_value(self.cboCardAssignee)
        card.qa_assignee = self._combo_value(self.cboCardQA)
        card.unit_tests_url = self.txtCardUnitUrl.text().strip() or None
        card.qa_url = self.txtCardQAUrl.text().strip() or None
        card.updated_at = now
        card.updated_by = user

        saved = upsert_card(card)
        if saved.id is not None:
            self._cards[saved.id] = saved
            self._selected_card_id = saved.id
        self._card_parent_id = saved.sprint_id

        history = PipelineHistory()
        if saved.id:
            history.update_card_status(
                saved.id,
                unit_tests_status="done" if saved.unit_tests_done else "pending",
                qa_status="approved" if saved.qa_done else "pending",
            )

        self.refresh()
        if saved.id:
            self._select_tree_item("card", saved.id)

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
        delete_card(card.id)
        self._selected_card_id = None
        self.refresh()
        self.stack.setCurrentIndex(0)

    # ------------------------------------------------------------------
    def _on_cancel(self) -> None:
        self.tree.clearSelection()
        self.stack.setCurrentIndex(0)
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
        if kind == "unit":
            if not require_roles("developer", "leader"):
                QMessageBox.warning(
                    self,
                    "Tarjeta",
                    "Solo desarrolladores o líderes pueden actualizar las pruebas unitarias.",
                )
                return
            toggled_on = not card.unit_tests_done
            card.unit_tests_done = toggled_on
            card.unit_tests_by = user if toggled_on else None
            card.unit_tests_at = now if toggled_on else None
            history.update_card_status(
                card.id, unit_tests_status="done" if toggled_on else "pending"
            )
        elif kind == "qa":
            if not require_roles("qa", "leader"):
                QMessageBox.warning(self, "Tarjeta", "No tienes permisos para aprobar QA")
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
            current_item.setText(3, " / ".join(card_checks))
        self.update_permissions()
        self._cards[card.id] = card
        self.refresh()
        if card.id:
            self._select_tree_item("card", card.id)

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
        branch_name = card.branch.strip()
        if not branch_name:
            QMessageBox.warning(self, "Tarjeta", "La tarjeta no tiene un nombre de rama válido.")
            return
        if not sprint.qa_branch_key:
            QMessageBox.warning(
                self,
                "Tarjeta",
                "Configura la rama QA del sprint antes de crear ramas de tarjetas.",
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
            card.branch_created_by = self._current_user()
            card.branch_created_at = int(time.time())
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
