from __future__ import annotations

import time
from typing import Dict, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QMessageBox,
    QPushButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..core.branch_store import (
    Sprint,
    Card,
    list_sprints,
    list_cards,
    upsert_sprint,
    upsert_card,
    delete_card,
    list_users,
)
from ..core.session import current_username, require_roles, get_active_user
from ..core.sprint_queries import is_card_ready_for_merge
from ..core.pipeline_history import PipelineHistory
from ..ui.icons import get_icon


class SprintView(QWidget):
    """Simple management UI for sprints and cards."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._sprints: Dict[int, Sprint] = {}
        self._cards: Dict[int, Card] = {}
        self._selected_card: Optional[int] = None
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

        actions = QHBoxLayout()
        actions.setSpacing(8)
        self.btnAddSprint = QPushButton("Nuevo sprint")
        self.btnAddSprint.setIcon(get_icon("branch"))
        actions.addWidget(self.btnAddSprint)
        self.btnAddCard = QPushButton("Agregar tarjeta")
        self.btnAddCard.setIcon(get_icon("build"))
        actions.addWidget(self.btnAddCard)
        self.btnMarkUnit = QPushButton("Marcar pruebas unitarias")
        self.btnMarkUnit.setIcon(get_icon("build"))
        actions.addWidget(self.btnMarkUnit)
        self.btnMarkQA = QPushButton("Marcar QA")
        self.btnMarkQA.setIcon(get_icon("log"))
        actions.addWidget(self.btnMarkQA)
        self.btnDelete = QPushButton("Eliminar tarjeta")
        self.btnDelete.setIcon(get_icon("delete"))
        actions.addWidget(self.btnDelete)
        actions.addStretch(1)
        layout.addLayout(actions)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Sprint/Tarjeta", "Asignado", "QA", "Estado"])
        self.tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tree.setUniformRowHeights(True)
        layout.addWidget(self.tree, 1)

        self.btnRefresh.clicked.connect(self.refresh)
        self.btnAddSprint.clicked.connect(self._create_sprint)
        self.btnAddCard.clicked.connect(self._create_card)
        self.btnMarkUnit.clicked.connect(lambda: self._mark_card("unit"))
        self.btnMarkQA.clicked.connect(lambda: self._mark_card("qa"))
        self.btnDelete.clicked.connect(self._delete_card)
        self.tree.itemSelectionChanged.connect(self._on_selection_changed)
        self.update_permissions()

    # ------------------------------------------------------------------
    def update_permissions(self) -> None:
        can_mark_qa = require_roles("qa", "leader")
        self.btnMarkQA.setEnabled(can_mark_qa)

    # ------------------------------------------------------------------
    def refresh(self) -> None:
        self._sprints.clear()
        self._cards.clear()
        items = list_sprints()
        for sprint in items:
            if sprint.id is not None:
                self._sprints[sprint.id] = sprint
        for sprint_id in list(self._sprints.keys()):
            cards = list_cards(sprint_ids=[sprint_id])
            for card in cards:
                if card.id is not None:
                    self._cards[card.id] = card
        self._populate_tree()
        self.update_permissions()

    # ------------------------------------------------------------------
    def _populate_tree(self) -> None:
        self.tree.clear()
        for sprint in sorted(self._sprints.values(), key=lambda s: (s.version, s.name)):
            item = QTreeWidgetItem(
                [
                    f"{sprint.version} — {sprint.name}",
                    sprint.lead_user or "",
                    sprint.qa_user or "",
                    "",
                ]
            )
            item.setData(0, Qt.UserRole, ("sprint", sprint.id))
            self.tree.addTopLevelItem(item)
            for card in sorted(
                [c for c in self._cards.values() if c.sprint_id == sprint.id],
                key=lambda c: c.title.lower(),
            ):
                status = "Aprobada" if is_card_ready_for_merge(card) else card.status
                child = QTreeWidgetItem(
                    [
                        card.title,
                        card.assignee or "",
                        card.qa_assignee or "",
                        status,
                    ]
                )
                child.setData(0, Qt.UserRole, ("card", card.id))
                item.addChild(child)
            item.setExpanded(True)

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
    def _on_selection_changed(self) -> None:
        item = self.tree.currentItem()
        if not item:
            self._selected_card = None
            return
        kind, ident = item.data(0, Qt.UserRole) or (None, None)
        if kind == "card" and ident is not None:
            self._selected_card = int(ident)
        else:
            self._selected_card = None

    # ------------------------------------------------------------------
    def _create_sprint(self) -> None:
        branch_key, ok = QInputDialog.getText(
            self, "Nuevo sprint", "Rama base (grupo/proyecto/rama):"
        )
        if not ok or not branch_key:
            return
        name, ok = QInputDialog.getText(self, "Nuevo sprint", "Nombre del sprint:")
        if not ok or not name:
            return
        version, ok = QInputDialog.getText(self, "Nuevo sprint", "Versión:")
        if not ok or not version:
            return
        leader = self._prompt_user("Responsable del sprint")
        qa_lead = self._prompt_user("Responsable QA", allow_empty=True)
        now = int(time.time())
        user = self._current_user()
        sprint = Sprint(
            id=None,
            branch_key=branch_key,
            name=name,
            version=version,
            lead_user=leader or None,
            qa_user=qa_lead or None,
            description="",
            created_at=now,
            created_by=user,
            updated_at=now,
            updated_by=user,
        )
        upsert_sprint(sprint)
        self.refresh()

    # ------------------------------------------------------------------
    def _create_card(self) -> None:
        sprint_id = self._current_sprint_id()
        if sprint_id is None:
            QMessageBox.information(self, "Tarjeta", "Selecciona un sprint primero")
            return
        title, ok = QInputDialog.getText(self, "Nueva tarjeta", "Título/Descripción")
        if not ok or not title:
            return
        branch_name, ok = QInputDialog.getText(
            self, "Nueva tarjeta", "Nombre de la rama (derivada):"
        )
        if not ok or not branch_name:
            return
        developer = self._prompt_user("Asignar a (desarrollador)", allow_empty=True)
        qa_user = self._prompt_user("Asignar QA", allow_empty=True)
        card = Card(
            id=None,
            sprint_id=sprint_id,
            title=title,
            branch=branch_name,
            assignee=developer or None,
            qa_assignee=qa_user or None,
            description="",
            unit_tests_done=False,
            qa_done=False,
            unit_tests_by=None,
            qa_by=None,
            unit_tests_at=None,
            qa_at=None,
            status="pendiente",
        )
        upsert_card(card)
        if card.id:
            PipelineHistory().update_card_status(
                card.id,
                unit_tests_status="pending",
                qa_status="pending",
            )
        self.refresh()

    # ------------------------------------------------------------------
    def _current_sprint_id(self) -> Optional[int]:
        item = self.tree.currentItem()
        if not item:
            return None
        kind, ident = item.data(0, Qt.UserRole) or (None, None)
        if kind == "sprint":
            return int(ident)
        if kind == "card" and ident is not None:
            card = self._cards.get(int(ident))
            return card.sprint_id if card else None
        return None

    # ------------------------------------------------------------------
    def _prompt_user(self, title: str, allow_empty: bool = False) -> Optional[str]:
        users = list_users(include_inactive=False)
        if not users:
            return None
        names = [u.username for u in users]
        names.insert(0, "")
        name, ok = QInputDialog.getItem(
            self,
            title,
            "Usuario",
            names,
            0,
            False,
        )
        if not ok:
            return None
        if not name and not allow_empty:
            return None
        return name or None

    # ------------------------------------------------------------------
    def _mark_card(self, kind: str) -> None:
        if self._selected_card is None:
            QMessageBox.information(self, "Tarjeta", "Selecciona una tarjeta")
            return
        card = self._cards.get(self._selected_card)
        if not card:
            return
        user = self._current_user()
        now = int(time.time())
        history = PipelineHistory()
        if kind == "unit":
            card.unit_tests_done = True
            card.unit_tests_by = user
            card.unit_tests_at = now
            if not card.status or card.status == "pendiente":
                card.status = "unit"
            history.update_card_status(card.id, unit_tests_status="done")
        elif kind == "qa":
            if not require_roles("qa", "leader"):
                QMessageBox.warning(self, "Tarjeta", "No tienes permisos para aprobar QA")
                return
            card.qa_done = True
            card.qa_by = user
            card.qa_at = now
            card.status = "qa"
            history.update_card_status(card.id, qa_status="approved", approved_by=user)
        upsert_card(card)
        self.refresh()

    # ------------------------------------------------------------------
    def _delete_card(self) -> None:
        if self._selected_card is None:
            QMessageBox.information(self, "Tarjeta", "Selecciona una tarjeta")
            return
        card = self._cards.get(self._selected_card)
        if not card:
            return
        confirm = QMessageBox.question(
            self,
            "Eliminar",
            f"¿Eliminar la tarjeta '{card.title}'?",
        )
        if confirm != QMessageBox.Yes:
            return
        delete_card(card.id)
        self.refresh()


__all__ = ["SprintView"]
