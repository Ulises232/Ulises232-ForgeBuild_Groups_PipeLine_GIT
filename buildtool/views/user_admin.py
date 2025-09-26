"""Vista de administración de usuarios y roles."""

from __future__ import annotations

from typing import Dict, List, Optional, Set

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from ..core.branch_store import (
    Role,
    User,
    create_user,
    get_user,
    list_roles,
    list_user_roles,
    list_users,
    mark_user_password_reset,
    update_user,
)
from .user_login import PasswordDialog


class UserAdminView(QWidget):
    """Panel de administración de usuarios disponible sólo para administradores."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._users: Dict[str, User] = {}
        self._user_roles: Dict[str, Set[str]] = {}
        self._roles: List[Role] = []
        self._pending_password: Optional[str] = None
        self._editing_username: Optional[str] = None
        self._setup_ui()
        self.reload()

    # ------------------------------------------------------------------
    def _setup_ui(self) -> None:
        root = QHBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(16)

        # Panel izquierdo con la lista de usuarios
        left = QVBoxLayout()
        left.setContentsMargins(0, 0, 0, 0)
        left.setSpacing(8)

        self.lstUsers = QListWidget()
        self.lstUsers.setSelectionMode(QListWidget.SingleSelection)
        self.lstUsers.currentItemChanged.connect(self._on_user_selected)
        left.addWidget(self.lstUsers, 1)

        btn_row = QHBoxLayout()
        btn_row.setContentsMargins(0, 0, 0, 0)
        btn_row.setSpacing(8)
        self.btnNew = QPushButton("Nuevo usuario")
        self.btnNew.clicked.connect(self._start_new_user)
        self.btnRefresh = QPushButton("Recargar")
        self.btnRefresh.clicked.connect(self.reload)
        btn_row.addWidget(self.btnNew)
        btn_row.addWidget(self.btnRefresh)
        left.addLayout(btn_row)

        root.addLayout(left, 1)

        # Panel derecho con el formulario
        right = QVBoxLayout()
        right.setContentsMargins(0, 0, 0, 0)
        right.setSpacing(8)

        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)
        form.setSpacing(6)

        self.txtUsername = QLineEdit()
        self.txtUsername.setPlaceholderText("usuario")
        form.addRow(QLabel("Usuario:"), self.txtUsername)

        self.txtDisplayName = QLineEdit()
        self.txtDisplayName.setPlaceholderText("Nombre para mostrar")
        form.addRow(QLabel("Nombre:"), self.txtDisplayName)

        self.txtEmail = QLineEdit()
        self.txtEmail.setPlaceholderText("correo@empresa.com")
        form.addRow(QLabel("Correo:"), self.txtEmail)

        right.addLayout(form)

        self.chkActive = QCheckBox("Usuario activo")
        right.addWidget(self.chkActive)

        self.lblPasswordStatus = QLabel()
        self.lblPasswordStatus.setWordWrap(True)
        right.addWidget(self.lblPasswordStatus)

        password_row = QHBoxLayout()
        password_row.setContentsMargins(0, 0, 0, 0)
        password_row.setSpacing(8)
        self.btnDefinePassword = QPushButton("Definir contraseña inicial")
        self.btnDefinePassword.clicked.connect(self._define_initial_password)
        self.btnResetPassword = QPushButton("Solicitar restablecimiento")
        self.btnResetPassword.clicked.connect(self._reset_password)
        password_row.addWidget(self.btnDefinePassword)
        password_row.addWidget(self.btnResetPassword)
        right.addLayout(password_row)

        roles_box = QGroupBox("Roles asignados")
        self.rolesLayout = QVBoxLayout()
        self.rolesLayout.setContentsMargins(8, 8, 8, 8)
        self.rolesLayout.setSpacing(4)
        roles_box.setLayout(self.rolesLayout)
        self.role_checks: Dict[str, QCheckBox] = {}
        right.addWidget(roles_box, 1)

        self.btnSave = QPushButton("Crear usuario")
        self.btnSave.clicked.connect(self._save_user)
        right.addWidget(self.btnSave)

        root.addLayout(right, 2)

    # ------------------------------------------------------------------
    def reload(self, *, selected: Optional[str] = None) -> None:
        try:
            self._roles = list_roles()
            mapping = list_user_roles()
            self._user_roles = {user: set(roles) for user, roles in mapping.items()}
            users = list_users(include_inactive=True)
        except Exception as exc:  # pragma: no cover - errores de conexión
            QMessageBox.critical(self, "Usuarios", f"No fue posible cargar los usuarios: {exc}")
            return

        self._users = {user.username: user for user in users}
        self._ensure_role_checks()
        self._populate_user_list(selected)

    # ------------------------------------------------------------------
    def _ensure_role_checks(self) -> None:
        existing_keys = {role.key for role in self._roles}
        for key, checkbox in list(self.role_checks.items()):
            if key not in existing_keys:
                self.rolesLayout.removeWidget(checkbox)
                checkbox.deleteLater()
                del self.role_checks[key]
        for role in self._roles:
            checkbox = self.role_checks.get(role.key)
            if checkbox is None:
                checkbox = QCheckBox(role.name)
                self.rolesLayout.addWidget(checkbox)
                self.role_checks[role.key] = checkbox
            checkbox.setText(role.name)
            tooltip = role.description or role.name
            checkbox.setToolTip(tooltip)

    # ------------------------------------------------------------------
    def _populate_user_list(self, selected: Optional[str]) -> None:
        username_to_select = selected or self._editing_username
        self.lstUsers.blockSignals(True)
        self.lstUsers.clear()
        for user in sorted(self._users.values(), key=lambda u: (u.display_name or u.username).lower()):
            label = f"{user.display_name} ({user.username})"
            if not user.active:
                label += " — deshabilitado"
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, user.username)
            self.lstUsers.addItem(item)
            if user.username == username_to_select:
                self.lstUsers.setCurrentItem(item)
        self.lstUsers.blockSignals(False)

        if username_to_select and username_to_select in self._users:
            self._load_user(username_to_select)
        elif self.lstUsers.count():
            self.lstUsers.setCurrentRow(0)
        else:
            self._start_new_user()

    # ------------------------------------------------------------------
    def _set_role_selection(self, roles: Set[str]) -> None:
        for key, checkbox in self.role_checks.items():
            checkbox.setChecked(key in roles)

    # ------------------------------------------------------------------
    def _collect_roles(self) -> List[str]:
        return [key for key, checkbox in self.role_checks.items() if checkbox.isChecked()]

    # ------------------------------------------------------------------
    def _start_new_user(self) -> None:
        self.lstUsers.clearSelection()
        self._editing_username = None
        self._pending_password = None
        self.txtUsername.setEnabled(True)
        self.txtUsername.clear()
        self.txtDisplayName.clear()
        self.txtEmail.clear()
        self.chkActive.setChecked(True)
        self.chkActive.setEnabled(True)
        self.btnDefinePassword.setEnabled(True)
        self.btnResetPassword.setEnabled(False)
        self.btnSave.setText("Crear usuario")
        self._set_role_selection(set())
        self._update_password_status()
        self.txtUsername.setFocus()

    # ------------------------------------------------------------------
    def _load_user(self, username: str) -> None:
        user = self._users.get(username)
        if not user:
            return
        self._editing_username = username
        self._pending_password = None
        self.txtUsername.setEnabled(False)
        self.txtUsername.setText(user.username)
        self.txtDisplayName.setText(user.display_name)
        self.txtEmail.setText(user.email or "")
        self.chkActive.setChecked(user.active)
        self.chkActive.setEnabled(True)
        self.btnDefinePassword.setEnabled(False)
        self.btnResetPassword.setEnabled(True)
        self.btnSave.setText("Guardar cambios")
        roles = self._user_roles.get(username, set())
        self._set_role_selection(roles)
        self._update_password_status(user)

    # ------------------------------------------------------------------
    def _update_password_status(self, user: Optional[User] = None) -> None:
        if self._editing_username is None:
            if self._pending_password:
                self.lblPasswordStatus.setText(
                    "Contraseña inicial definida. Compártela con el usuario o cámbiala antes de entregar la cuenta."
                )
            else:
                self.lblPasswordStatus.setText(
                    "Sin contraseña inicial: se solicitará al usuario crearla en su primer ingreso."
                )
            return
        if not user:
            self.lblPasswordStatus.setText("Estado de contraseña desconocido.")
            return
        if not user.has_password:
            self.lblPasswordStatus.setText("Sin contraseña establecida actualmente.")
        elif user.require_password_reset:
            self.lblPasswordStatus.setText("Contraseña vigente, pero con restablecimiento pendiente.")
        else:
            self.lblPasswordStatus.setText("Contraseña configurada y vigente.")

    # ------------------------------------------------------------------
    @Slot()
    def _define_initial_password(self) -> None:
        dialog = PasswordDialog(
            self.txtUsername.text().strip() or "nuevo",
            self,
            title="Contraseña inicial",
            message="Define una contraseña temporal con al menos 7 caracteres, una letra mayúscula y un número.",
        )
        if dialog.exec() == QDialog.Accepted and dialog.password:
            self._pending_password = dialog.password
            self._update_password_status()

    # ------------------------------------------------------------------
    @Slot()
    def _reset_password(self) -> None:
        if not self._editing_username:
            return
        username = self._editing_username
        try:
            mark_user_password_reset(username, require_reset=True)
        except Exception as exc:  # pragma: no cover - errores inesperados
            QMessageBox.critical(self, "Restablecer contraseña", f"No fue posible marcar el restablecimiento: {exc}")
            return
        refreshed = get_user(username)
        if refreshed:
            self._users[username] = refreshed
            self._update_password_status(refreshed)
        QMessageBox.information(
            self,
            "Restablecer contraseña",
            "Se solicitó restablecer la contraseña. Al iniciar sesión se le pedirá definir una nueva.",
        )

    # ------------------------------------------------------------------
    @Slot()
    def _save_user(self) -> None:
        username = self.txtUsername.text().strip()
        display = self.txtDisplayName.text().strip() or username
        email = self.txtEmail.text().strip() or None
        roles = self._collect_roles()
        active = self.chkActive.isChecked()

        if not username:
            QMessageBox.warning(self, "Usuario", "Captura un nombre de usuario válido.")
            self.txtUsername.setFocus()
            return

        if self._editing_username is None:
            if username in self._users:
                QMessageBox.warning(self, "Usuario", "Ya existe un usuario con ese nombre.")
                return
            try:
                created = create_user(
                    username,
                    display,
                    email=email,
                    roles=roles,
                    password=self._pending_password,
                    active=active,
                )
            except Exception as exc:  # pragma: no cover
                QMessageBox.critical(self, "Crear usuario", f"No fue posible crear el usuario: {exc}")
                return
            self._pending_password = None
            QMessageBox.information(
                self,
                "Usuarios",
                "Usuario creado correctamente. Al guardar se actualizaron los roles seleccionados.",
            )
            self.reload(selected=created.username)
        else:
            username = self._editing_username
            try:
                updated = update_user(
                    username,
                    display_name=display,
                    email=email,
                    roles=roles,
                    active=active,
                )
            except Exception as exc:  # pragma: no cover
                QMessageBox.critical(self, "Usuarios", f"No fue posible guardar los cambios: {exc}")
                return
            QMessageBox.information(self, "Usuarios", "Cambios guardados correctamente.")
            self.reload(selected=updated.username if updated else username)

    # ------------------------------------------------------------------
    @Slot()
    def _on_user_selected(self, current: Optional[QListWidgetItem], previous: Optional[QListWidgetItem]) -> None:
        if not current:
            self._start_new_user()
            return
        username = current.data(Qt.UserRole)
        if username:
            self._load_user(username)


__all__ = ["UserAdminView"]
