from __future__ import annotations

import re
from typing import Dict, List, Optional

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QVBoxLayout,
)

from ..core.branch_store import (
    User,
    Role,
    list_users,
    list_roles,
    upsert_role,
    list_user_roles,
    authenticate_user,
    set_user_password,
    get_user,
)
from ..core.session import set_active_user


_DEFAULT_ROLES = [
    Role(key="developer", name="Desarrollador"),
    Role(key="qa", name="QA"),
    Role(key="leader", name="Líder"),
    Role(key="admin", name="Administrador"),
]


class PasswordDialog(QDialog):
    def __init__(
        self,
        username: str,
        parent=None,
        *,
        title: str,
        message: str,
        forbid_password: Optional[str] = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self._forbid = forbid_password or ""
        self.password: Optional[str] = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        header = QLabel(message)
        header.setWordWrap(True)
        layout.addWidget(header)

        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)
        self.txtPassword = QLineEdit()
        self.txtPassword.setEchoMode(QLineEdit.Password)
        self.txtPassword.setPlaceholderText("Nueva contraseña")
        form.addRow(QLabel("Contraseña:"), self.txtPassword)

        self.txtConfirm = QLineEdit()
        self.txtConfirm.setEchoMode(QLineEdit.Password)
        self.txtConfirm.setPlaceholderText("Confirmar contraseña")
        form.addRow(QLabel("Confirmar:"), self.txtConfirm)
        layout.addLayout(form)

        self.lblError = QLabel()
        self.lblError.setWordWrap(True)
        self.lblError.setStyleSheet("color: #c62828;")
        self.lblError.hide()
        layout.addWidget(self.lblError)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

        self.txtPassword.returnPressed.connect(self.accept)
        self.txtConfirm.returnPressed.connect(self.accept)
        self.txtPassword.setFocus()

    # ------------------------------------------------------------------
    def _validate(self) -> bool:
        password = self.txtPassword.text()
        confirm = self.txtConfirm.text()
        errors: List[str] = []
        if len(password) < 8:
            errors.append("Debe tener al menos 8 caracteres.")
        if not re.search(r"[A-Z]", password):
            errors.append("Incluye al menos una letra mayúscula.")
        if not re.search(r"[a-z]", password):
            errors.append("Incluye al menos una letra minúscula.")
        if not re.search(r"\d", password):
            errors.append("Incluye al menos un número.")
        if not re.search(r"[^A-Za-z0-9]", password):
            errors.append("Incluye al menos un carácter especial.")
        if self._forbid and password == self._forbid:
            errors.append("La nueva contraseña debe ser diferente a la anterior.")
        if password != confirm:
            errors.append("Las contraseñas no coinciden.")

        if errors:
            self.lblError.setText("\n".join(f"• {msg}" for msg in errors))
            self.lblError.show()
            return False

        self.lblError.hide()
        return True

    # ------------------------------------------------------------------
    def accept(self) -> None:
        if not self._validate():
            return
        self.password = self.txtPassword.text()
        super().accept()


class UserLoginDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Seleccionar usuario")
        self.resize(380, 240)
        self._users: Dict[str, User] = {}
        self._setup_ui()
        self._ensure_default_roles()
        self._load_users()

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

        self.txtPassword = QLineEdit()
        self.txtPassword.setEchoMode(QLineEdit.Password)
        self.txtPassword.returnPressed.connect(self.accept)
        form.addRow(QLabel("Contraseña:"), self.txtPassword)

        layout.addLayout(form)

        self.lblHint = QLabel()
        self.lblHint.setWordWrap(True)
        self.lblHint.setStyleSheet("color: #455a64;")
        layout.addWidget(self.lblHint)

        self.lblError = QLabel()
        self.lblError.setWordWrap(True)
        self.lblError.setStyleSheet("color: #c62828;")
        self.lblError.hide()
        layout.addWidget(self.lblError)

        layout.addStretch(1)

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
        users = list_users(include_inactive=False)
        self._users = {user.username: user for user in users}
        for user in users:
            self.cboUser.addItem(f"{user.display_name} ({user.username})", user.username)
        self.cboUser.blockSignals(False)
        if users:
            self.cboUser.setCurrentIndex(0)
            self._on_user_changed(0)
        else:
            self._update_user_hint(None)

    # ------------------------------------------------------------------
    def _update_user_hint(self, user: Optional[User]) -> None:
        if not user:
            self.lblHint.setText("Crea usuarios desde el módulo de administración.")
            return
        hints: List[str] = ["Introduce la contraseña para continuar."]
        if not user.has_password:
            hints.append(
                "Este usuario no tiene contraseña configurada. Se solicitará crear una antes de entrar."
            )
        if user.require_password_reset:
            hints.append("Este usuario debe restablecer su contraseña al iniciar sesión.")
        self.lblHint.setText("\n".join(hints))

    # ------------------------------------------------------------------
    @Slot(int)
    def _on_user_changed(self, index: int) -> None:
        username = self.cboUser.itemData(index)
        user = self._users.get(username)
        self.txtPassword.clear()
        self.lblError.hide()
        self._update_user_hint(user)

    # ------------------------------------------------------------------
    @Slot()
    def accept(self) -> None:
        self.lblError.hide()
        index = self.cboUser.currentIndex()
        username = self.cboUser.itemData(index)
        if not username:
            QMessageBox.warning(self, "Usuario", "Selecciona un usuario")
            return

        password = self.txtPassword.text()
        result = authenticate_user(username, password)

        if result.status in {"password_required", "reset_required"}:
            current_password = password if result.status == "reset_required" else ""
            dialog = PasswordDialog(
                username,
                self,
                title="Crear contraseña" if result.status == "password_required" else "Restablecer contraseña",
                message="Define una contraseña segura para continuar." if result.status == "password_required" else "Debes actualizar tu contraseña antes de entrar.",
                forbid_password=current_password or None,
            )
            if dialog.exec() != QDialog.Accepted or not dialog.password:
                self.lblError.setText("No se configuró la contraseña.")
                self.lblError.show()
                return
            set_user_password(username, dialog.password, require_reset=False)
            password = dialog.password
            refreshed = get_user(username)
            if refreshed:
                self._users[username] = refreshed
                self._update_user_hint(refreshed)
            result = authenticate_user(username, password)

        if not result.success:
            message = result.message or "No fue posible iniciar sesión."
            self.lblError.setText(message)
            self.lblError.show()
            if result.status == "invalid_credentials":
                self.txtPassword.selectAll()
                self.txtPassword.setFocus()
            return

        user = result.user or get_user(username)
        if not user:
            self.lblError.setText("El usuario no existe o fue deshabilitado.")
            self.lblError.show()
            return

        mapping = list_user_roles(username)
        roles = set(mapping.get(username, []))
        set_active_user(user, roles)
        self.txtPassword.clear()
        super().accept()


__all__ = ["UserLoginDialog", "PasswordDialog"]
