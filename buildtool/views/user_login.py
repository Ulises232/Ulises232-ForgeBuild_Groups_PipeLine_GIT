from __future__ import annotations

from typing import Dict, List, Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from ..core.branch_store import (
    User,
    Role,
    list_users,
    list_roles,
    upsert_user,
    upsert_role,
    list_user_roles,
    set_user_roles,
)
from ..core.session import set_active_user


_DEFAULT_ROLES = [
    Role(key="developer", name="Desarrollador"),
    Role(key="qa", name="QA"),
    Role(key="leader", name="Líder"),
]


class UserLoginDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Seleccionar usuario")
        self.resize(360, 220)
        self._users: Dict[str, User] = {}
        self._roles: List[Role] = []
        self._setup_ui()
        self._ensure_default_roles()
        self._load_users()
        self._load_roles()

    # ------------------------------------------------------------------
    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)
        self.cboUser = QComboBox()
        self.cboUser.currentIndexChanged.connect(self._on_user_changed)
        form.addRow(QLabel("Usuario:"), self.cboUser)

        new_user_box = QHBoxLayout()
        self.txtNewUser = QLineEdit()
        self.txtNewUser.setPlaceholderText("usuario")
        self.txtNewDisplay = QLineEdit()
        self.txtNewDisplay.setPlaceholderText("Nombre para mostrar")
        self.btnCreate = QPushButton("Crear")
        self.btnCreate.clicked.connect(self._create_user)
        new_user_box.addWidget(self.txtNewUser)
        new_user_box.addWidget(self.txtNewDisplay)
        new_user_box.addWidget(self.btnCreate)
        form.addRow(QLabel("Nuevo:"), new_user_box)

        layout.addLayout(form)

        layout.addWidget(QLabel("Roles"))
        self.role_checks: Dict[str, QCheckBox] = {}
        for role in _DEFAULT_ROLES:
            chk = QCheckBox(role.name)
            chk.setObjectName(role.key)
            layout.addWidget(chk)
            self.role_checks[role.key] = chk

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    # ------------------------------------------------------------------
    def _ensure_default_roles(self) -> None:
        existing = {role.key for role in list_roles()}
        for role in _DEFAULT_ROLES:
            if role.key not in existing:
                upsert_role(role)

    # ------------------------------------------------------------------
    def _load_users(self) -> None:
        self.cboUser.blockSignals(True)
        self.cboUser.clear()
        users = list_users()
        self._users = {user.username: user for user in users}
        for user in users:
            self.cboUser.addItem(f"{user.display_name} ({user.username})", user.username)
        self.cboUser.blockSignals(False)
        if users:
            self.cboUser.setCurrentIndex(0)
            self._on_user_changed(0)

    # ------------------------------------------------------------------
    def _load_roles(self) -> None:
        self._roles = list_roles()
        for role in self._roles:
            if role.key not in self.role_checks:
                chk = QCheckBox(role.name)
                chk.setObjectName(role.key)
                self.role_checks[role.key] = chk
                self.layout().insertWidget(self.layout().count() - 1, chk)

    # ------------------------------------------------------------------
    @Slot()
    def _create_user(self) -> None:
        username = self.txtNewUser.text().strip()
        display = self.txtNewDisplay.text().strip() or username
        if not username:
            QMessageBox.warning(self, "Usuario", "Captura un usuario válido")
            return
        user = User(username=username, display_name=display, active=True, email=None)
        upsert_user(user)
        self.txtNewUser.clear()
        self.txtNewDisplay.clear()
        self._load_users()
        index = self.cboUser.findData(user.username)
        if index >= 0:
            self.cboUser.setCurrentIndex(index)

    # ------------------------------------------------------------------
    @Slot(int)
    def _on_user_changed(self, index: int) -> None:
        username = self.cboUser.itemData(index)
        assigned = set()
        if username:
            mapping = list_user_roles(username)
            assigned = set(mapping.get(username, []))
        for key, chk in self.role_checks.items():
            chk.setChecked(key in assigned)

    # ------------------------------------------------------------------
    @Slot()
    def accept(self) -> None:
        index = self.cboUser.currentIndex()
        username = self.cboUser.itemData(index)
        if not username:
            QMessageBox.warning(self, "Usuario", "Selecciona un usuario")
            return
        user = self._users.get(username)
        if not user:
            QMessageBox.warning(self, "Usuario", "Usuario desconocido")
            return
        selected_roles = [key for key, chk in self.role_checks.items() if chk.isChecked()]
        set_user_roles(username, selected_roles)
        set_active_user(user, set(selected_roles))
        super().accept()


__all__ = ["UserLoginDialog"]
