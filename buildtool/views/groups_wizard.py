from __future__ import annotations
from typing import List, Optional, Dict, Tuple
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QWidget, QDialog, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QListWidget, QListWidgetItem,
    QFileDialog, QMessageBox, QCheckBox, QSplitter, QTabWidget, QGroupBox
)
import yaml
from ..core.config import (
    Config, Group, Project, Module, DeployTarget, save_config, _create_config_store
)
from ..core.session import get_active_user, require_roles
from ..ui.widgets import combo_with_arrow

# ----------------------------- Helpers -----------------------------

def _confirm(parent, text: str) -> bool:
    return QMessageBox.question(parent, "Confirmar", text,
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes

def _unique_key(base: str, existing: list[str]) -> str:
    if base not in existing:
        return base
    i = 2
    while f"{base}{i}" in existing:
        i += 1
    return f"{base}{i}"

# ----------------------------- ModuleRow -----------------------------

class ModuleRow(QWidget):
    """
    Editor de un módulo:
    - Nombre, Path, Goals
    - Flags: optional, no_profile, run_once, serial_across_profiles
    - Salida: WAR / UI-JAR / Carpeta personalizada (+ A la raíz)
    - Selectivo: select_pattern + rename_jar_to
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QGridLayout(self)
        lay.setColumnStretch(1, 1)
        lay.setHorizontalSpacing(8)
        lay.setVerticalSpacing(6)
        # --- Archivos de versión ---
        from PySide6.QtWidgets import QGroupBox, QVBoxLayout, QHBoxLayout, QListWidget, QListWidgetItem, QLineEdit, QPushButton, QLabel
        self.gbVersion = QGroupBox('Archivos de versión (relativos al módulo)')
        _vlay = QVBoxLayout(self.gbVersion)
        self.lstVersionFiles = QListWidget()
        _row = QHBoxLayout(); self.txtVF = QLineEdit(); self.txtVF.setPlaceholderText('src/.../web.xml')
        self.btnAddVF = QPushButton('Agregar'); self.btnDelVF = QPushButton('Quitar')
        _row.addWidget(QLabel('Archivo:')); _row.addWidget(self.txtVF, 1); _row.addWidget(self.btnAddVF); _row.addWidget(self.btnDelVF)
        _vlay.addLayout(_row); _vlay.addWidget(self.lstVersionFiles)
        lay.addWidget(self.gbVersion, 99, 0, 1, 2)
        self.btnAddVF.clicked.connect(lambda: (self.lstVersionFiles.addItem(QListWidgetItem(self.txtVF.text().strip())) if self.txtVF.text().strip() else None, self.txtVF.clear()))
        self.btnDelVF.clicked.connect(lambda: [self.lstVersionFiles.takeItem(self.lstVersionFiles.row(it)) for it in self.lstVersionFiles.selectedItems()])

        # Básicos
        self.txtName = QLineEdit()
        self.txtPath = QLineEdit()
        self.txtGoals = QLineEdit("clean package")

        # Flags
        self.cmbOptional  = QComboBox(); self.cmbOptional.addItems(["No opcional", "Opcional"])
        self.cmbNoProfile = QComboBox(); self.cmbNoProfile.addItems(["Con perfil", "Sin perfil"])
        self.cmbRunOnce   = QComboBox(); self.cmbRunOnce.addItems(["Cada perfil", "Una vez por sesión"])
        self.cmbSerial    = QComboBox(); self.cmbSerial.addItems(["Paralelo entre perfiles", "Serial entre perfiles"])
        self.txtProfileOverride = QLineEdit(); self.txtProfileOverride.setPlaceholderText("Perfil override")
        self.txtProfileOverride.setMaximumWidth(160)
        self.txtOnlyIfProfile = QLineEdit(); self.txtOnlyIfProfile.setPlaceholderText("Solo si perfil = ...")
        self.txtOnlyIfProfile.setMaximumWidth(180)

        # Salida
        self.cboSalida = QComboBox()
        self.cboSalida.addItems(["WAR → /war", "UI-JAR → /ui-ellis", "Carpeta personalizada"])
        self.txtCustomOut = QLineEdit()
        self.txtCustomOut.setPlaceholderText("ej. fp-correos")
        self.chkToRoot = QCheckBox("A la raíz")

        # Selectivo
        self.txtSelectPattern = QLineEdit()
        self.txtSelectPattern.setPlaceholderText("ej. *-jar-with-dependencies.jar o fp-correos-*.jar")
        self.txtRenameTo = QLineEdit()
        self.txtRenameTo.setPlaceholderText("nombre final: ej. fp-correos.jar")

        # Layout
        # Fila 0
        lay.addWidget(QLabel("Nombre:"), 0, 0); lay.addWidget(self.txtName, 0, 1)
        lay.addWidget(QLabel("Path:"),   0, 2); lay.addWidget(self.txtPath, 0, 3)
        # Fila 1
        lay.addWidget(QLabel("Goals:"),  1, 0); lay.addWidget(self.txtGoals, 1, 1)
        lay.addWidget(QLabel("Flags:"),  1, 2)
        flags_w = QWidget(); flags_h = QHBoxLayout(flags_w); flags_h.setContentsMargins(0,0,0,0); flags_h.setSpacing(6)
        flags_h.addWidget(combo_with_arrow(self.cmbOptional))
        flags_h.addWidget(combo_with_arrow(self.cmbNoProfile))
        flags_h.addWidget(combo_with_arrow(self.cmbRunOnce))
        flags_h.addWidget(combo_with_arrow(self.cmbSerial))
        flags_h.addWidget(self.txtProfileOverride)
        flags_h.addWidget(self.txtOnlyIfProfile)
        lay.addWidget(flags_w, 1, 3)
        # Fila 2
        lay.addWidget(QLabel("Salida:"), 2, 0); lay.addWidget(combo_with_arrow(self.cboSalida), 2, 1)
        lay.addWidget(QLabel("Carpeta:"), 2, 2); lay.addWidget(self.txtCustomOut, 2, 3)
        lay.addWidget(self.chkToRoot, 2, 4)
        # Fila 3
        lay.addWidget(QLabel("Patrón (1 archivo):"), 3, 0); lay.addWidget(self.txtSelectPattern, 3, 1)
        lay.addWidget(QLabel("Renombrar a:"),        3, 2); lay.addWidget(self.txtRenameTo,     3, 3)

        # Overrides de ruta por usuario
        self.txtUserPath = QLineEdit()
        self.txtUserPath.setPlaceholderText("Ruta personalizada para este módulo")
        lay.addWidget(QLabel("Ruta por usuario:"), 4, 0)
        lay.addWidget(self.txtUserPath, 4, 1, 1, 3)

        self.cboSalida.currentIndexChanged.connect(self._toggle_custom_out)
        self._toggle_custom_out()

        self._global_controls = [
            self.txtName,
            self.txtPath,
            self.txtGoals,
            self.lstVersionFiles,
            self.btnAddVF,
            self.btnDelVF,
            self.cmbOptional,
            self.cmbNoProfile,
            self.cmbRunOnce,
            self.cmbSerial,
            self.txtProfileOverride,
            self.txtOnlyIfProfile,
            self.cboSalida,
            self.txtCustomOut,
            self.chkToRoot,
            self.txtSelectPattern,
            self.txtRenameTo,
        ]

    def _toggle_custom_out(self):
        custom = (self.cboSalida.currentIndex() == 2)
        self.txtCustomOut.setEnabled(custom)
        # "A la raíz" solo aplica a WAR/UI
        self.chkToRoot.setEnabled(self.cboSalida.currentIndex() in (0, 1))

    def set_from_module(self, m: Module, user_path: Optional[str] = None):
        self.txtName.setText(m.name or "")
        self.txtPath.setText(m.path or "")
        self.txtGoals.setText(" ".join(m.goals or ["clean", "package"]) or "clean package")
        # versión files UI
        self.lstVersionFiles.clear()
        for rel in (getattr(m, 'version_files', []) or []):
            self.lstVersionFiles.addItem(QListWidgetItem(rel))
        self.cmbOptional.setCurrentIndex(1 if getattr(m, "optional", False) else 0)
        self.cmbNoProfile.setCurrentIndex(1 if getattr(m, "no_profile", False) else 0)
        self.cmbRunOnce.setCurrentIndex(1 if getattr(m, "run_once", False) else 0)
        self.cmbSerial.setCurrentIndex(1 if getattr(m, "serial_across_profiles", False) else 0)
        self.txtProfileOverride.setText(getattr(m, "profile_override", "") or "")
        self.txtOnlyIfProfile.setText(getattr(m, "only_if_profile_equals", "") or "")

        if getattr(m, "copy_to_subfolder", None):
            self.cboSalida.setCurrentIndex(2)
            self.txtCustomOut.setText(m.copy_to_subfolder or "")
        elif getattr(m, "copy_to_profile_ui", False):
            self.cboSalida.setCurrentIndex(1)
            self.txtCustomOut.clear()
        else:
            self.cboSalida.setCurrentIndex(0)
            self.txtCustomOut.clear()

        self.chkToRoot.setChecked(getattr(m, "copy_to_root", False))
        self.txtSelectPattern.setText(getattr(m, "select_pattern", "") or "")
        self.txtRenameTo.setText(getattr(m, "rename_jar_to", "") or "")
        self.set_user_override(user_path)

    def to_module(self) -> Module:
        goals = [g for g in (self.txtGoals.text().strip() or "clean package").split() if g]
        m = Module(
            name=self.txtName.text().strip(),
            path=self.txtPath.text().strip(),
            goals=goals,
            optional=(self.cmbOptional.currentIndex() == 1),
            no_profile=(self.cmbNoProfile.currentIndex() == 1),
            run_once=(self.cmbRunOnce.currentIndex() == 1),
            serial_across_profiles=(self.cmbSerial.currentIndex() == 1),
        )
        m.profile_override = (self.txtProfileOverride.text().strip() or None)
        m.only_if_profile_equals = (self.txtOnlyIfProfile.text().strip() or None)
        idx = self.cboSalida.currentIndex()
        if idx == 0:  # WAR
            m.copy_to_profile_war = True
            m.copy_to_profile_ui = False
            m.copy_to_subfolder = None
        elif idx == 1:  # UI-JAR
            m.copy_to_profile_war = False
            m.copy_to_profile_ui = True
            m.copy_to_subfolder = None
        else:  # custom
            m.copy_to_profile_war = False
            m.copy_to_profile_ui = False
            m.copy_to_subfolder = self.txtCustomOut.text().strip() or None

        m.copy_to_root = self.chkToRoot.isChecked()
        m.select_pattern = (self.txtSelectPattern.text().strip() or None)
        m.rename_jar_to = (self.txtRenameTo.text().strip() or None)
        # persistir version_files
        m.version_files = []
        for i in range(self.lstVersionFiles.count()):
            s = self.lstVersionFiles.item(i).text().strip()
            if s:
                m.version_files.append(s)
        return m

    def set_user_override(self, path: Optional[str]) -> None:
        self.txtUserPath.setText(path or "")

    def user_override_path(self) -> Optional[str]:
        value = self.txtUserPath.text().strip()
        return value or None

    def set_global_edit_enabled(self, enabled: bool) -> None:
        for widget in [
            self.txtName,
            self.txtPath,
            self.txtGoals,
            self.txtCustomOut,
            self.txtProfileOverride,
            self.txtOnlyIfProfile,
            self.txtSelectPattern,
            self.txtRenameTo,
        ]:
            widget.setReadOnly(not enabled)
            widget.setEnabled(True)
        for widget in [
            self.cmbOptional,
            self.cmbNoProfile,
            self.cmbRunOnce,
            self.cmbSerial,
            self.cboSalida,
            self.chkToRoot,
        ]:
            widget.setEnabled(enabled)
        self.lstVersionFiles.setEnabled(enabled)
        self.btnAddVF.setEnabled(enabled)
        self.btnDelVF.setEnabled(enabled)

    def set_user_edit_enabled(self, enabled: bool) -> None:
        self.txtUserPath.setReadOnly(not enabled)
        self.txtUserPath.setEnabled(True)

# ----------------------------- TargetRow -----------------------------

class TargetRow(QWidget):
    """
    Editor de targets de deploy:
    - name, project_key, profiles (CSV)
    - path_template
    - hotfix_path_template (opcional)
    """
    def __init__(self, group: Optional[Group], cfg: Config, parent=None):
        super().__init__(parent)
        self._group = group
        self._cfg = cfg

        lay = QGridLayout(self)
        lay.setColumnStretch(1, 1)
        lay.setHorizontalSpacing(8)
        lay.setVerticalSpacing(6)

        self.txtName = QLineEdit()
        self.cboProject = QComboBox()
        self.txtProfiles = QLineEdit()
        self.txtProfiles.setPlaceholderText("Perfiles separados por coma, ej: Desarrollo, Produccion")
        self.txtPath = QLineEdit(); self.txtPath.setPlaceholderText(r"\\server\...\{version}\ ")
        self.txtHotfix = QLineEdit(); self.txtHotfix.setPlaceholderText(r"(opcional) \\server\...\hotfix\{version}\ ")

        # Proyectos válidos
        if group and group.projects:
            for p in group.projects:
                self.cboProject.addItem(p.key, p.key)
        else:
            added: set[str] = set()
            for g in (cfg.groups or []):
                for p in (g.projects or []):
                    if p.key in added:
                        continue
                    added.add(p.key)
                    self.cboProject.addItem(p.key, p.key)

        lay.addWidget(QLabel("Nombre:"), 0, 0); lay.addWidget(self.txtName, 0, 1, 1, 3)
        lay.addWidget(QLabel("Proyecto:"), 1, 0); lay.addWidget(combo_with_arrow(self.cboProject), 1, 1)
        lay.addWidget(QLabel("Perfiles:"), 1, 2); lay.addWidget(self.txtProfiles, 1, 3)
        lay.addWidget(QLabel("Path:"), 2, 0); lay.addWidget(self.txtPath, 2, 1, 1, 3)
        lay.addWidget(QLabel("Hotfix path:"), 3, 0); lay.addWidget(self.txtHotfix, 3, 1, 1, 3)

        self.txtUserPath = QLineEdit()
        self.txtUserPath.setPlaceholderText("Ruta personalizada para despliegue")
        self.txtUserHotfix = QLineEdit()
        self.txtUserHotfix.setPlaceholderText("Ruta personalizada para hotfix")
        user_box = QGroupBox("Rutas por usuario")
        user_grid = QGridLayout(user_box)
        user_grid.setHorizontalSpacing(6)
        user_grid.setVerticalSpacing(6)
        user_grid.addWidget(QLabel("Path:"), 0, 0)
        user_grid.addWidget(self.txtUserPath, 0, 1)
        user_grid.addWidget(QLabel("Hotfix:"), 1, 0)
        user_grid.addWidget(self.txtUserHotfix, 1, 1)
        lay.addWidget(user_box, 4, 0, 1, 4)

        self._global_controls = [
            self.txtName,
            self.cboProject,
            self.txtProfiles,
            self.txtPath,
            self.txtHotfix,
        ]

    def set_from_target(
        self,
        t: DeployTarget,
        user_paths: Optional[Tuple[Optional[str], Optional[str]]] = None,
    ):
        self.txtName.setText(t.name or "")
        idx = self.cboProject.findData(t.project_key)
        if idx < 0:
            idx = self.cboProject.findText(t.project_key or "")
        self.cboProject.setCurrentIndex(idx if idx >= 0 else 0)
        self.txtProfiles.setText(", ".join(t.profiles or []))
        self.txtPath.setText(t.path_template or "")
        self.txtHotfix.setText(getattr(t, "hotfix_path_template", "") or "")
        resolved = user_paths or (None, None)
        self.set_user_paths(resolved[0], resolved[1])

    def to_target(self) -> DeployTarget:
        profiles = [p.strip() for p in (self.txtProfiles.text().split(",") if self.txtProfiles.text().strip() else []) if p.strip()]
        return DeployTarget(
            name=self.txtName.text().strip(),
            project_key=self.cboProject.currentData() or self.cboProject.currentText().strip(),
            profiles=profiles,
            path_template=self.txtPath.text().strip(),
            hotfix_path_template=(self.txtHotfix.text().strip() or None),
        )

    def set_user_paths(
        self, path_template: Optional[str], hotfix_template: Optional[str]
    ) -> None:
        self.txtUserPath.setText(path_template or "")
        self.txtUserHotfix.setText(hotfix_template or "")

    def user_paths_override(self) -> Tuple[Optional[str], Optional[str]]:
        path_value = self.txtUserPath.text().strip() or None
        hotfix_value = self.txtUserHotfix.text().strip() or None
        return path_value, hotfix_value

    def set_global_edit_enabled(self, enabled: bool) -> None:
        self.txtName.setReadOnly(not enabled)
        self.txtName.setEnabled(True)
        self.txtProfiles.setReadOnly(not enabled)
        self.txtProfiles.setEnabled(True)
        self.txtPath.setReadOnly(not enabled)
        self.txtPath.setEnabled(True)
        self.txtHotfix.setReadOnly(not enabled)
        self.txtHotfix.setEnabled(True)
        self.cboProject.setEnabled(enabled)

    def set_user_edit_enabled(self, enabled: bool) -> None:
        self.txtUserPath.setReadOnly(not enabled)
        self.txtUserPath.setEnabled(True)
        self.txtUserHotfix.setReadOnly(not enabled)
        self.txtUserHotfix.setEnabled(True)

# ----------------------------- ProjectEditor -----------------------------

class ProjectEditor(QWidget):
    """Editor de un proyecto con módulos (lista + detalle)."""
    def __init__(self, group: Group, cfg: Config, parent=None):
        super().__init__(parent)
        self._group = group
        self._cfg = cfg

        lay = QVBoxLayout(self); lay.setSpacing(8); lay.setContentsMargins(0,0,0,0)

        # Cabecera
        header = QGridLayout()
        header.setHorizontalSpacing(8); header.setVerticalSpacing(6)
        self.txtKey = QLineEdit()
        self.cboRepo = QComboBox()
        self.cboExec = QComboBox(); self.cboExec.addItems(["integrated", "separate_windows"])

        for k in (group.repos or {}).keys():
            self.cboRepo.addItem(k, k)

        header.addWidget(QLabel("Proyecto:"), 0, 0); header.addWidget(self.txtKey, 0, 1)
        header.addWidget(QLabel("Repo:"),     0, 2); header.addWidget(combo_with_arrow(self.cboRepo), 0, 3)
        header.addWidget(QLabel("Ejecución:"), 0, 4); header.addWidget(combo_with_arrow(self.cboExec), 0, 5)
        lay.addLayout(header)

        # Split
        split = QSplitter(); lay.addWidget(split, 1)
        left = QWidget(); left_lay = QVBoxLayout(left); left_lay.setContentsMargins(0,0,0,0); left_lay.setSpacing(6)
        self.lstModules = QListWidget()
        btns_w = QWidget(); btns = QHBoxLayout(btns_w); btns.setContentsMargins(0,0,0,0); btns.setSpacing(6)
        self.btnAddMod = QPushButton("Agregar módulo")
        self.btnDelMod = QPushButton("Quitar módulo")
        btns.addWidget(self.btnAddMod); btns.addWidget(self.btnDelMod); btns.addStretch(1)
        left_lay.addWidget(self.lstModules, 1); left_lay.addWidget(btns_w)
        split.addWidget(left)

        right = QWidget(); right_lay = QVBoxLayout(right); right_lay.setContentsMargins(0,0,0,0); right_lay.setSpacing(6)
        self.moduleEditor = ModuleRow()
        right_lay.addWidget(self.moduleEditor)
        split.addWidget(right)
        split.setStretchFactor(0, 1); split.setStretchFactor(1, 2)

        # signals
        self.lstModules.currentRowChanged.connect(self._load_selected_module)
        self.btnAddMod.clicked.connect(self._add_module)
        self.btnDelMod.clicked.connect(self._del_module)

        self._modules: List[Module] = []
        self._current_module_row = -1
        self._module_overrides: Dict[str, Dict[str, str]] = {}
        self._current_project_key: str = ""
        self._global_edit_enabled: bool = True

    def set_from_project(self, p: Project):
        self.txtKey.setText(p.key or "")
        idx = self.cboRepo.findData(getattr(p, "repo", None))
        if idx < 0:
            idx = self.cboRepo.findText(getattr(p, "repo", "") or "")
        self.cboRepo.setCurrentIndex(idx if idx >= 0 else 0)
        exec_mode = getattr(p, "execution_mode", "integrated") or "integrated"
        self.cboExec.setCurrentText(exec_mode)

        self._current_project_key = p.key or ""
        self._module_overrides.setdefault(self._current_project_key, {})
        self._modules = list(p.modules or [])
        self._refresh_modules_list()

    def set_user_context(self, overrides: Dict[str, Dict[str, str]]) -> None:
        self._module_overrides = {project: dict(values) for project, values in overrides.items()}
        if self._current_project_key:
            self._module_overrides.setdefault(self._current_project_key, {})

    def to_project(self) -> Project:
        key = self.txtKey.text().strip()
        if key and key != self._current_project_key:
            existing = self._module_overrides.pop(self._current_project_key, {})
            self._module_overrides[key] = existing
            self._current_project_key = key
        return Project(
            key=self.txtKey.text().strip(),
            repo=self.cboRepo.currentData() or self.cboRepo.currentText().strip(),
            execution_mode=self.cboExec.currentText(),
            modules=self._modules
        )

    def _refresh_modules_list(self):
        prev_row = self._current_module_row if 0 <= self._current_module_row < len(self._modules) else 0
        self.lstModules.blockSignals(True)
        self.lstModules.clear()
        for m in self._modules:
            self.lstModules.addItem(QListWidgetItem(m.name or ""))
        if self._modules:
            new_row = prev_row if prev_row < len(self._modules) else len(self._modules) - 1
            self.lstModules.setCurrentRow(new_row)
        else:
            new_row = -1
            self.lstModules.clearSelection()
        self.lstModules.blockSignals(False)

        overrides = self._module_overrides.get(self._current_project_key, {})
        if new_row >= 0:
            self._current_module_row = new_row
            module = self._modules[new_row]
            self.moduleEditor.set_from_module(
                module,
                overrides.get(module.name or ""),
            )
        else:
            self._current_module_row = -1
            empty = Module(name="", path="", goals=["clean", "package"])
            self.moduleEditor.set_from_module(empty, None)

    def _update_module_override(
        self,
        old_name: Optional[str],
        new_name: Optional[str],
        value: Optional[str],
    ) -> None:
        overrides = self._module_overrides.setdefault(self._current_project_key, {})
        old_key = (old_name or "").strip()
        new_key = (new_name or "").strip()
        if old_key and old_key != new_key:
            overrides.pop(old_key, None)
        target_key = new_key or old_key
        if not target_key:
            return
        if value:
            overrides[target_key] = value
        else:
            overrides.pop(target_key, None)

    def _load_selected_module(self, row: int):
        if 0 <= self._current_module_row < len(self._modules):
            previous = self._modules[self._current_module_row]
            override_value = self.moduleEditor.user_override_path()
            updated = self.moduleEditor.to_module()
            self._modules[self._current_module_row] = updated
            self._update_module_override(previous.name, updated.name, override_value)
            if self._global_edit_enabled:
                save_config(self._cfg)
        self._current_module_row = row
        if row < 0 or row >= len(self._modules):
            return
        overrides = self._module_overrides.get(self._current_project_key, {})
        module = self._modules[row]
        self.moduleEditor.set_from_module(module, overrides.get(module.name or ""))

    def _add_module(self):
        if not self._global_edit_enabled:
            return
        m = Module(name="nuevo-modulo", path="", goals=["clean", "package"])
        self._modules.append(m)
        self._refresh_modules_list()
        self.lstModules.setCurrentRow(len(self._modules)-1)

    def _del_module(self):
        if not self._global_edit_enabled:
            return
        row = self.lstModules.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este módulo?"):
            return
        removed = self._modules.pop(row)
        overrides = self._module_overrides.get(self._current_project_key, {})
        overrides.pop(getattr(removed, "name", "") or "", None)
        self._refresh_modules_list()

    def capture_current_module_override(self) -> None:
        if 0 <= self._current_module_row < len(self._modules):
            module = self._modules[self._current_module_row]
            value = self.moduleEditor.user_override_path()
            self._update_module_override(module.name, module.name, value)

    def module_overrides(self) -> Dict[str, Dict[str, str]]:
        result: Dict[str, Dict[str, str]] = {}
        for project, overrides in self._module_overrides.items():
            cleaned = {name: path for name, path in overrides.items() if path}
            if cleaned:
                result[project] = cleaned
        return result

    def set_global_edit_enabled(self, enabled: bool) -> None:
        self._global_edit_enabled = enabled
        self.txtKey.setReadOnly(not enabled)
        self.txtKey.setEnabled(True)
        self.cboRepo.setEnabled(enabled)
        self.cboExec.setEnabled(enabled)
        self.btnAddMod.setEnabled(enabled)
        self.btnDelMod.setEnabled(enabled)
        self.moduleEditor.set_global_edit_enabled(enabled)

    def remove_project_overrides(self, project_key: str) -> None:
        self._module_overrides.pop(project_key, None)

    def apply_editor_to_current(self):
        row = self._current_module_row
        if 0 <= row < len(self._modules):
            previous = self._modules[row]
            override_value = self.moduleEditor.user_override_path()
            updated = self.moduleEditor.to_module()
            self._modules[row] = updated
            self._update_module_override(previous.name, updated.name, override_value)
            item = self.lstModules.item(row)
            if item is not None:
                item.setText(updated.name or "")

# ----------------------------- GroupEditor -----------------------------

class GroupEditor(QWidget):
    """Editor de un grupo: repos, output_base, perfiles, proyectos y deploy targets."""
    def __init__(self, cfg: Config, parent=None):
        super().__init__(parent)
        self.cfg = cfg
        self.group: Optional[Group] = None
        active_user = get_active_user()
        self._active_user = active_user
        self._active_username: Optional[str] = getattr(active_user, "username", None)
        self._can_edit_global = require_roles("admin", "leader")
        self._user_repo_inputs: Dict[str, QLineEdit] = {}
        self._current_user_output: Optional[str] = None
        self._user_deploy_overrides: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

        main = QVBoxLayout(self); main.setContentsMargins(6,6,6,6); main.setSpacing(6)

        # NAS path + import/export
        nas_row = QHBoxLayout()
        self.txtNasDir = QLineEdit(getattr(getattr(self.cfg, "paths", {}), "nas_dir", ""))
        self.btnNasBrowse = QPushButton("...")
        self.btnNasImport = QPushButton("Importar")
        self.btnNasExport = QPushButton("Exportar")
        nas_row.addWidget(QLabel("Carpeta NAS:"))
        nas_row.addWidget(self.txtNasDir, 1)
        nas_row.addWidget(self.btnNasBrowse)
        nas_row.addWidget(self.btnNasImport)
        nas_row.addWidget(self.btnNasExport)
        main.addLayout(nas_row)

        # Environment variables editor
        env_box = QGroupBox("Variables de entorno (globales)")
        env_grid = QGridLayout(env_box)
        env_grid.setHorizontalSpacing(8)
        env_grid.setVerticalSpacing(6)

        self.lstEnv = QListWidget()
        env_grid.addWidget(self.lstEnv, 0, 0, 4, 1)

        env_form = QGridLayout()
        env_form.setHorizontalSpacing(6)
        env_form.setVerticalSpacing(6)
        self.txtEnvKey = QLineEdit(); self.txtEnvKey.setPlaceholderText("HERR_REPO")
        self.txtEnvValue = QLineEdit(); self.txtEnvValue.setPlaceholderText(r"C:\\Proyectos\\...")
        env_form.addWidget(QLabel("Variable:"), 0, 0); env_form.addWidget(self.txtEnvKey, 0, 1)
        env_form.addWidget(QLabel("Valor:"), 1, 0); env_form.addWidget(self.txtEnvValue, 1, 1)

        env_btns = QHBoxLayout()
        self.btnEnvSave = QPushButton("Guardar variable")
        self.btnEnvDelete = QPushButton("Eliminar")
        self.btnEnvClear = QPushButton("Limpiar campos")
        env_btns.addWidget(self.btnEnvSave)
        env_btns.addWidget(self.btnEnvDelete)
        env_btns.addWidget(self.btnEnvClear)
        env_btns.addStretch(1)
        env_form.addLayout(env_btns, 2, 0, 1, 2)

        env_grid.addLayout(env_form, 0, 1, 4, 1)
        main.addWidget(env_box)

        # Top: selector de grupo
        top = QHBoxLayout()
        self.cboGroup = QComboBox()
        self.btnAddGroup = QPushButton("Nuevo grupo")
        self.btnRenGroup = QPushButton("Renombrar")
        self.btnDelGroup = QPushButton("Eliminar grupo")
        top.addWidget(QLabel("Grupo:"))
        top.addWidget(combo_with_arrow(self.cboGroup), 1)
        top.addWidget(self.btnAddGroup); top.addWidget(self.btnRenGroup); top.addWidget(self.btnDelGroup)
        main.addLayout(top)

        # Tabs
        self.tabs = QTabWidget()
        main.addWidget(self.tabs, 1)

        # --- Tab General (repos, output_base, perfiles)
        tab_gen = QWidget()
        gen_container = QVBoxLayout(tab_gen)
        gen_container.setContentsMargins(0, 0, 0, 0)
        gen_container.setSpacing(6)

        self.generalTabs = QTabWidget()
        gen_container.addWidget(self.generalTabs)

        tab_global = QWidget()
        lay_gen = QGridLayout(tab_global)
        lay_gen.setHorizontalSpacing(8)
        lay_gen.setVerticalSpacing(6)

        # Repos
        self.lstRepos = QListWidget()
        self.btnAddRepo = QPushButton("Agregar repo")
        self.btnDelRepo = QPushButton("Quitar repo")
        self.txtRepoKey = QLineEdit()
        self.txtRepoPath = QLineEdit()
        self.btnRepoPath = QPushButton("...")

        lay_gen.addWidget(QLabel("Repos:"), 0, 0)
        lay_gen.addWidget(self.lstRepos, 1, 0, 3, 1)

        repo_form = QGridLayout()
        repo_form.addWidget(QLabel("Nombre repo:"), 0, 0); repo_form.addWidget(self.txtRepoKey, 0, 1)
        # Ruta con botón
        repo_path_w = QWidget(); repo_path_h = QHBoxLayout(repo_path_w)
        repo_path_h.setContentsMargins(0,0,0,0); repo_path_h.setSpacing(6)
        repo_path_h.addWidget(self.txtRepoPath, 1); repo_path_h.addWidget(self.btnRepoPath)
        repo_form.addWidget(QLabel("Ruta:"), 1, 0); repo_form.addWidget(repo_path_w, 1, 1)
        # Acciones
        repo_act_w = QWidget(); repo_act_h = QHBoxLayout(repo_act_w)
        repo_act_h.setContentsMargins(0,0,0,0); repo_act_h.setSpacing(6)
        repo_act_h.addWidget(self.btnAddRepo); repo_act_h.addWidget(self.btnDelRepo); repo_act_h.addStretch(1)
        repo_form.addWidget(repo_act_w, 2, 0, 1, 2)
        lay_gen.addLayout(repo_form, 1, 1, 3, 1)

        # Output base
        self.txtOutputBase = QLineEdit()
        self.btnOutputBase = QPushButton("...")
        out_w = QWidget(); out_h = QHBoxLayout(out_w)
        out_h.setContentsMargins(0,0,0,0); out_h.setSpacing(6)
        out_h.addWidget(self.txtOutputBase, 1); out_h.addWidget(self.btnOutputBase)
        lay_gen.addWidget(QLabel("Salida base:"), 0, 2); lay_gen.addWidget(out_w, 0, 3)

        # Perfiles
        self.lstProfiles = QListWidget()
        self.btnAddProfile = QPushButton("Agregar perfil")
        self.btnDelProfile = QPushButton("Quitar perfil")
        self.txtProfile = QLineEdit()

        lay_gen.addWidget(QLabel("Perfiles:"), 4, 0)
        lay_gen.addWidget(self.lstProfiles, 5, 0, 3, 1)

        prof_form = QGridLayout()
        prof_form.addWidget(QLabel("Perfil:"), 0, 0); prof_form.addWidget(self.txtProfile, 0, 1)
        prof_btns_w = QWidget(); prof_btns = QHBoxLayout(prof_btns_w)
        prof_btns.setContentsMargins(0,0,0,0); prof_btns.setSpacing(6)
        prof_btns.addWidget(self.btnAddProfile); prof_btns.addWidget(self.btnDelProfile); prof_btns.addStretch(1)
        prof_form.addWidget(prof_btns_w, 1, 0, 1, 2)
        lay_gen.addLayout(prof_form, 5, 1, 3, 1)

        self.generalTabs.addTab(tab_global, "Definición global")

        tab_user = QWidget()
        user_layout = QVBoxLayout(tab_user)
        user_layout.setContentsMargins(8, 8, 8, 8)
        user_layout.setSpacing(6)

        user_help = QLabel(
            "Define rutas personalizadas para el usuario activo. Los campos vacíos usarán la configuración global."
        )
        user_help.setWordWrap(True)
        user_layout.addWidget(user_help)

        output_row = QHBoxLayout()
        output_row.setSpacing(6)
        output_row.addWidget(QLabel("Salida base:"))
        self.txtUserOutputBase = QLineEdit()
        self.txtUserOutputBase.setPlaceholderText("Usar salida global")
        self.btnUserOutputBrowse = QPushButton("...")
        self.btnUserOutputClear = QPushButton("Limpiar")
        output_row.addWidget(self.txtUserOutputBase, 1)
        output_row.addWidget(self.btnUserOutputBrowse)
        output_row.addWidget(self.btnUserOutputClear)
        user_layout.addLayout(output_row)

        self.userReposBox = QGroupBox("Repositorios por usuario")
        self.userReposLayout = QGridLayout(self.userReposBox)
        self.userReposLayout.setHorizontalSpacing(6)
        self.userReposLayout.setVerticalSpacing(6)
        user_layout.addWidget(self.userReposBox)
        user_layout.addStretch(1)

        self.generalTabs.addTab(tab_user, "Rutas por usuario")
        if not getattr(self, "_active_username", None):
            self.generalTabs.setTabEnabled(1, False)

        self.tabs.addTab(tab_gen, "General")

        # --- Tab Proyectos
        tab_proj = QWidget(); lay_proj = QVBoxLayout(tab_proj); lay_proj.setSpacing(6)
        proj_top = QHBoxLayout()
        self.lstProjects = QListWidget()
        self.btnAddProject = QPushButton("Agregar proyecto")
        self.btnDelProject = QPushButton("Quitar proyecto")
        proj_top.addWidget(self.lstProjects, 1)
        proj_btns_w = QWidget(); proj_btns = QHBoxLayout(proj_btns_w)
        proj_btns.setContentsMargins(0,0,0,0); proj_btns.setSpacing(6)
        proj_btns.addWidget(self.btnAddProject); proj_btns.addWidget(self.btnDelProject); proj_btns.addStretch(1)
        proj_top.addWidget(proj_btns_w)
        lay_proj.addLayout(proj_top, 2)

        self.projectEditor = ProjectEditor(Group(key="", repos={}, output_base="", profiles=[], projects=[]), self.cfg)
        lay_proj.addWidget(self.projectEditor, 3)
        self.tabs.addTab(tab_proj, "Proyectos")

        # --- Tab Deploy
        tab_dep = QWidget(); lay_dep = QVBoxLayout(tab_dep); lay_dep.setSpacing(6)
        dep_top = QHBoxLayout()
        self.lstTargets = QListWidget()
        self.btnAddTarget = QPushButton("Agregar target")
        self.btnDelTarget = QPushButton("Quitar target")
        dep_top.addWidget(self.lstTargets, 1)
        dep_btns_w = QWidget(); dep_btns = QHBoxLayout(dep_btns_w)
        dep_btns.setContentsMargins(0,0,0,0); dep_btns.setSpacing(6)
        dep_btns.addWidget(self.btnAddTarget); dep_btns.addWidget(self.btnDelTarget); dep_btns.addStretch(1)
        dep_top.addWidget(dep_btns_w)
        lay_dep.addLayout(dep_top, 0)

        self.targetEditor = TargetRow(None, self.cfg)
        lay_dep.addWidget(self.targetEditor, 1)
        self._deploy_layout = lay_dep  # para reemplazar el editor luego

        self.tabs.addTab(tab_dep, "Deploy")

        # Bottom actions
        bottom = QHBoxLayout()
        self.btnSave = QPushButton("Guardar"); self.btnClose = QPushButton("Cerrar")
        bottom.addStretch(1); bottom.addWidget(self.btnSave); bottom.addWidget(self.btnClose)
        main.addLayout(bottom)

        # state trackers
        self._current_project_row = -1
        self._current_target_row = -1

        # signals
        self.cboGroup.currentIndexChanged.connect(self._change_group)
        self.btnAddGroup.clicked.connect(self._add_group)
        self.btnRenGroup.clicked.connect(self._ren_group)
        self.btnDelGroup.clicked.connect(self._del_group)

        self.lstRepos.currentRowChanged.connect(self._load_repo_row)
        self.btnAddRepo.clicked.connect(self._add_repo)
        self.btnDelRepo.clicked.connect(self._del_repo)
        self.btnRepoPath.clicked.connect(self._browse_repo)
        self.btnOutputBase.clicked.connect(self._browse_output)

        self.lstProfiles.currentRowChanged.connect(self._load_profile_row)
        self.btnAddProfile.clicked.connect(self._add_profile)
        self.btnDelProfile.clicked.connect(self._del_profile)

        self.lstProjects.currentRowChanged.connect(self._load_project_row)
        self.btnAddProject.clicked.connect(self._add_project)
        self.btnDelProject.clicked.connect(self._del_project)

        self.lstTargets.currentRowChanged.connect(self._load_target_row)
        self.btnAddTarget.clicked.connect(self._add_target)
        self.btnDelTarget.clicked.connect(self._del_target)

        self.btnSave.clicked.connect(self._save)
        self.btnClose.clicked.connect(self._close)

        self.btnNasBrowse.clicked.connect(self._browse_nas)
        self.btnNasImport.clicked.connect(self._import_cfg)
        self.btnNasExport.clicked.connect(self._export_cfg)
        self.txtNasDir.editingFinished.connect(self._save_nas_dir)

        self.lstEnv.currentRowChanged.connect(self._load_env_row)
        self.btnEnvSave.clicked.connect(self._save_env_entry)
        self.btnEnvDelete.clicked.connect(self._del_env_entry)
        self.btnEnvClear.clicked.connect(self._clear_env_fields)
        self.txtEnvValue.editingFinished.connect(self._auto_apply_env_value)
        self.txtEnvKey.editingFinished.connect(self._auto_rename_env_key)

        self.tabs.currentChanged.connect(lambda _: self._save(silent=True))

        self.btnUserOutputBrowse.clicked.connect(self._browse_user_output_override)
        self.btnUserOutputClear.clicked.connect(lambda: self.txtUserOutputBase.clear())

        self._refresh_env_list()


        # init groups
        for g in (self.cfg.groups or []):
            self.cboGroup.addItem(g.key, g.key)
        if self.cfg.groups:
            self.cboGroup.setCurrentIndex(0)
            self._load_group()

        self._apply_global_permissions()
        self._update_user_tab_state()

    # --------------- Environment vars ---------------

    def _env_map(self) -> Dict[str, str]:
        env = getattr(self.cfg, "environment", None)
        if env is None:
            env = {}
            self.cfg.environment = env
        return env

    def _refresh_env_list(self, select_key: Optional[str] = None):
        env = self._env_map()
        items = sorted(env.items(), key=lambda kv: kv[0].lower())
        self.lstEnv.blockSignals(True)
        self.lstEnv.clear()
        for key, value in items:
            label = f"{key} = {value}"
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, key)
            self.lstEnv.addItem(item)
        self.lstEnv.blockSignals(False)
        if select_key:
            self._select_env_key(select_key)
        elif self.lstEnv.count():
            self.lstEnv.setCurrentRow(0)
        else:
            self._clear_env_fields()

    def _select_env_key(self, key: str):
        for row in range(self.lstEnv.count()):
            item = self.lstEnv.item(row)
            if item.data(Qt.UserRole) == key:
                self.lstEnv.setCurrentRow(row)
                return

    def _load_env_row(self, row: int):
        env = self._env_map()
        if row < 0 or row >= self.lstEnv.count():
            self.txtEnvKey.clear(); self.txtEnvValue.clear()
            return
        item = self.lstEnv.item(row)
        key = item.data(Qt.UserRole) or ""
        self.txtEnvKey.setText(key)
        self.txtEnvValue.setText(env.get(key, ""))

    def _save_env_entry(self):
        key = self.txtEnvKey.text().strip()
        if not key:
            QMessageBox.warning(self, "Variables", "Escribe el nombre de la variable.")
            return
        value = self.txtEnvValue.text()
        env = dict(self._env_map())
        row = self.lstEnv.currentRow()
        if row >= 0:
            item = self.lstEnv.item(row)
            old_key = item.data(Qt.UserRole)
            if old_key and old_key != key and old_key in env:
                del env[old_key]
        env[key] = value
        self.cfg.environment = env
        save_config(self.cfg)
        self._refresh_env_list(select_key=key)

    def _del_env_entry(self):
        row = self.lstEnv.currentRow()
        if row < 0:
            self.txtEnvKey.clear(); self.txtEnvValue.clear()
            return
        item = self.lstEnv.item(row)
        key = item.data(Qt.UserRole)
        if not key:
            return
        env = dict(self._env_map())
        if key in env:
            del env[key]
        self.cfg.environment = env
        save_config(self.cfg)
        self._refresh_env_list()

    def _clear_env_fields(self):
        self.lstEnv.clearSelection()
        self.txtEnvKey.clear()
        self.txtEnvValue.clear()

    def _auto_apply_env_value(self):
        row = self.lstEnv.currentRow()
        if row < 0:
            return
        item = self.lstEnv.item(row)
        key = item.data(Qt.UserRole)
        if not key:
            return
        env = dict(self._env_map())
        value = self.txtEnvValue.text()
        if env.get(key, "") == value:
            return
        env[key] = value
        self.cfg.environment = env
        save_config(self.cfg)
        self._refresh_env_list(select_key=key)

    def _auto_rename_env_key(self):
        row = self.lstEnv.currentRow()
        if row < 0:
            return
        item = self.lstEnv.item(row)
        old_key = item.data(Qt.UserRole)
        if not old_key:
            return
        new_key = self.txtEnvKey.text().strip()
        if not new_key or new_key == old_key:
            return
        env = dict(self._env_map())
        if new_key in env and new_key != old_key:
            QMessageBox.warning(self, "Variables", "Ya existe una variable con ese nombre.")
            self.txtEnvKey.setText(old_key)
            return
        value = self.txtEnvValue.text()
        env.pop(old_key, None)
        env[new_key] = value
        self.cfg.environment = env
        save_config(self.cfg)
        self._refresh_env_list(select_key=new_key)

    # --------------- Group handlers ---------------

    def _find_group(self, key: str) -> Optional[Group]:
        return next((g for g in (self.cfg.groups or []) if g.key == key), None)

    def _add_group(self):
        if not self._can_edit_global:
            return
        keys = [g.key for g in (self.cfg.groups or [])]
        new_key = _unique_key("NuevoGrupo", keys)
        g = Group(key=new_key, repos={}, output_base="", profiles=[], projects=[], deploy_targets=[])
        self.cfg.groups = (self.cfg.groups or []) + [g]
        self.cboGroup.addItem(new_key, new_key)
        self.cboGroup.setCurrentIndex(self.cboGroup.count()-1)
        self._load_group()

    def _del_group(self):
        if not self._can_edit_global:
            return
        if not self.cfg.groups:
            return
        idx = self.cboGroup.currentIndex()
        if idx < 0:
            return
        key = self.cboGroup.currentData()
        if not _confirm(self, f"¿Eliminar el grupo '{key}'?"):
            return
        self.cfg.groups = [g for g in self.cfg.groups if g.key != key]
        self.cboGroup.removeItem(idx)
        if self.cboGroup.count() > 0:
            self.cboGroup.setCurrentIndex(0)
            self._load_group()
        else:
            # limpiar UI si ya no hay grupos
            self.group = None
            self.lstRepos.clear(); self.txtRepoKey.clear(); self.txtRepoPath.clear()
            self.txtOutputBase.clear()
            self.lstProfiles.clear(); self.txtProfile.clear()
            self.lstProjects.clear()
            self.lstTargets.clear()
    def _ren_group(self):
        if not self._can_edit_global:
            return
        if not self.cfg.groups:
            return
        idx = self.cboGroup.currentIndex()
        if idx < 0:
            return
        key = self.cboGroup.currentData()
        from PySide6.QtWidgets import QInputDialog
        new_name, ok = QInputDialog.getText(self, "Renombrar grupo", "Nuevo nombre:", text=key)
        new_name = new_name.strip()
        if not ok or not new_name:
            return
        if any(g.key == new_name for g in (self.cfg.groups or [])):
            QMessageBox.warning(self, "Grupo", "Ya existe un grupo con ese nombre.")
            return
        g = self._find_group(key)
        if g:
            g.key = new_name
            self.cboGroup.setItemText(idx, new_name)
            self.cboGroup.setItemData(idx, new_name)

    def _change_group(self, idx: int):
        self._save(silent=True)
        self._load_group()

    def _browse_nas(self):
        d = QFileDialog.getExistingDirectory(self, "Selecciona carpeta NAS")
        if d:
            self.txtNasDir.setText(d.replace("/", "\\"))
            self._save_nas_dir()

    def _save_nas_dir(self):
        self.cfg.paths.nas_dir = self.txtNasDir.text().strip()
        try:
            save_config(self.cfg)
        except Exception:
            pass

    def _import_cfg(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar configuración", self.txtNasDir.text().strip() or "", "YAML Files (*.yaml)")
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            new_cfg = Config(**data)
        except Exception as e:
            QMessageBox.critical(self, "Importar", f"No se pudo cargar:\n{e}")
            return
        self.cfg = new_cfg
        if hasattr(self.window(), "_cfg"):
            self.window()._cfg = self.cfg
        self.txtNasDir.setText(getattr(getattr(self.cfg, "paths", {}), "nas_dir", ""))
        self.projectEditor._cfg = self.cfg
        self.cboGroup.clear()
        for g in (self.cfg.groups or []):
            self.cboGroup.addItem(g.key, g.key)
        if self.cfg.groups:
            self.cboGroup.setCurrentIndex(0)
        self._load_group()
        self._refresh_env_list()
        save_config(self.cfg)

    def _export_cfg(self):
        path, _ = QFileDialog.getSaveFileName(self, "Exportar configuración", self.txtNasDir.text().strip() or "", "YAML Files (*.yaml)")
        if not path:
            return
        try:
            data = self.cfg.dict() if hasattr(self.cfg, "dict") else self.cfg.model_dump()
            with open(path, "w", encoding="utf-8") as f:
                yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
            QMessageBox.information(self, "Exportar", "Configuración exportada.")
        except Exception as e:
            QMessageBox.critical(self, "Exportar", f"No se pudo exportar:\n{e}")

    def _close(self):
        self._save(silent=True)
        self.window().close()

    def _load_group(self):
        key = self.cboGroup.currentData()
        grp = self._find_group(key)
        prev_group = getattr(self, "group", None)
        prev_project_key: Optional[str] = None
        prev_target_name: Optional[str] = None
        if (
            prev_group
            and grp
            and prev_group.key == grp.key
            and 0 <= self._current_project_row < len(prev_group.projects or [])
        ):
            prev_project_key = prev_group.projects[self._current_project_row].key
        if (
            prev_group
            and grp
            and prev_group.key == grp.key
            and 0 <= self._current_target_row < len(prev_group.deploy_targets or [])
        ):
            prev_target_name = prev_group.deploy_targets[self._current_target_row].name

        self.group = grp
        self._current_project_row = -1
        self._current_target_row = -1

        self.projectEditor._cfg = self.cfg
        if not grp:
            self.lstRepos.clear(); self.txtRepoKey.clear(); self.txtRepoPath.clear()
            self.txtOutputBase.clear()
            self.lstProfiles.clear(); self.txtProfile.clear()
            self.lstProjects.clear()
            self.projectEditor.set_from_project(Project(key="", repo="", execution_mode="integrated", modules=[]))
            self.projectEditor.moduleEditor.set_user_edit_enabled(bool(self._active_username))
            self.lstTargets.clear()
            empty_editor = TargetRow(None, self.cfg)
            self._deploy_layout.replaceWidget(self.targetEditor, empty_editor)
            self.targetEditor.setParent(None)
            self.targetEditor.deleteLater()
            self.targetEditor = empty_editor
            self.targetEditor.set_global_edit_enabled(self._can_edit_global)
            self.targetEditor.set_user_edit_enabled(bool(self._active_username))
            self._user_deploy_overrides = {}
            self._current_user_output = None
            self._rebuild_user_repo_overrides({}, {})
            self.txtUserOutputBase.clear()
            self._update_user_tab_state()
            return

        repo_overrides: Dict[str, str] = {}
        module_overrides: Dict[str, Dict[str, str]] = {}
        self._user_deploy_overrides = {}
        self._current_user_output = None
        if self._active_username:
            try:
                store = _create_config_store()
                repo_overrides, output_override = store.get_group_user_paths(grp.key, self._active_username)
                module_overrides = store.get_module_user_paths(grp.key, self._active_username)
                self._user_deploy_overrides = store.get_deploy_user_paths(grp.key, self._active_username)
                self._current_user_output = output_override
            except Exception:
                module_overrides = {}
                self._user_deploy_overrides = {}
                self._current_user_output = None

        # General
        self.lstRepos.clear()
        for k, v in (grp.repos or {}).items():
            self.lstRepos.addItem(QListWidgetItem(f"{k} = {v}"))
        self.txtRepoKey.clear(); self.txtRepoPath.clear()
        self.txtOutputBase.setText(grp.output_base or "")
        self.txtUserOutputBase.setPlaceholderText(grp.output_base or "Usar salida global")
        self.txtUserOutputBase.setText(self._current_user_output or "")
        self._rebuild_user_repo_overrides(grp.repos or {}, repo_overrides)

        self.lstProfiles.clear()
        for p in (grp.profiles or []):
            self.lstProfiles.addItem(QListWidgetItem(p))
        self.txtProfile.clear()

        # actualizar combo de repo del projectEditor antes de cargar proyecto
        self.projectEditor._group = grp
        self.projectEditor.set_user_context(module_overrides)
        self.projectEditor.cboRepo.blockSignals(True)
        self.projectEditor.cboRepo.clear()
        for rk in (grp.repos or {}).keys():
            self.projectEditor.cboRepo.addItem(rk, rk)
        self.projectEditor.cboRepo.blockSignals(False)

        # Proyectos
        desired_project_idx = 0
        if prev_project_key:
            for idx, proj in enumerate(grp.projects or []):
                if proj.key == prev_project_key:
                    desired_project_idx = idx
                    break
        self.lstProjects.blockSignals(True)
        self.lstProjects.clear()
        for p in (grp.projects or []):
            self.lstProjects.addItem(QListWidgetItem(p.key))
        if grp.projects:
            self.lstProjects.setCurrentRow(desired_project_idx)
        else:
            self.lstProjects.clearSelection()
        self.lstProjects.blockSignals(False)

        if grp.projects:
            self._current_project_row = desired_project_idx
            self.projectEditor.set_from_project(grp.projects[desired_project_idx])
        else:
            self._current_project_row = -1
            self.projectEditor.set_from_project(Project(key="", repo="", execution_mode="integrated", modules=[]))
        self.projectEditor.moduleEditor.set_user_edit_enabled(bool(self._active_username))

        # Deploy
        desired_target_idx = 0
        if prev_target_name:
            for idx, target in enumerate(grp.deploy_targets or []):
                if target.name == prev_target_name:
                    desired_target_idx = idx
                    break
        self.lstTargets.blockSignals(True)
        self.lstTargets.clear()
        for t in (grp.deploy_targets or []):
            self.lstTargets.addItem(QListWidgetItem(t.name))
        if grp.deploy_targets:
            self.lstTargets.setCurrentRow(desired_target_idx)
        else:
            self.lstTargets.clearSelection()
        self.lstTargets.blockSignals(False)

        new_editor = TargetRow(grp, self.cfg)
        self._deploy_layout.replaceWidget(self.targetEditor, new_editor)
        self.targetEditor.setParent(None)
        self.targetEditor.deleteLater()
        self.targetEditor = new_editor
        self.targetEditor.set_global_edit_enabled(self._can_edit_global)
        self.targetEditor.set_user_edit_enabled(bool(self._active_username))

        if grp.deploy_targets:
            self._current_target_row = desired_target_idx
            current_target = grp.deploy_targets[desired_target_idx]
            override = self._user_deploy_overrides.get(current_target.name)
            self.targetEditor.set_from_target(current_target, override)
        else:
            self._current_target_row = -1
            self.targetEditor.set_user_paths(None, None)

        self._update_user_tab_state()

    # --------------- Repos ---------------

    def _rebuild_user_repo_overrides(
        self, global_repos: Dict[str, str], overrides: Dict[str, str]
    ) -> None:
        while self.userReposLayout.count():
            item = self.userReposLayout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._user_repo_inputs.clear()
        repos_items = sorted(global_repos.items(), key=lambda kv: kv[0].lower())
        if not repos_items:
            empty_label = QLabel("No hay repos configurados en este grupo.")
            empty_label.setStyleSheet("color: #666666;")
            self.userReposLayout.addWidget(empty_label, 0, 0, 1, 3)
            return
        self.userReposLayout.setColumnStretch(1, 1)
        enabled = bool(self._active_username)
        for row, (repo_key, repo_path) in enumerate(repos_items):
            label = QLabel(repo_key)
            label.setToolTip(repo_path or "")
            edit = QLineEdit()
            edit.setPlaceholderText(repo_path or "Ruta global no definida")
            edit.setText(overrides.get(repo_key, ""))
            edit.setReadOnly(not enabled)
            edit.setEnabled(True)
            browse = QPushButton("...")
            browse.setEnabled(enabled)
            browse.clicked.connect(lambda _, key=repo_key: self._browse_user_repo_override(key))
            self.userReposLayout.addWidget(label, row, 0)
            self.userReposLayout.addWidget(edit, row, 1)
            self.userReposLayout.addWidget(browse, row, 2)
            self._user_repo_inputs[repo_key] = edit

    def _browse_user_output_override(self) -> None:
        if not self._active_username:
            return
        d = QFileDialog.getExistingDirectory(self, "Selecciona carpeta personalizada")
        if d:
            self.txtUserOutputBase.setText(d.replace("/", "\\"))

    def _browse_user_repo_override(self, repo_key: str) -> None:
        if not self._active_username:
            return
        edit = self._user_repo_inputs.get(repo_key)
        if edit is None:
            return
        d = QFileDialog.getExistingDirectory(self, f"Selecciona carpeta para '{repo_key}'")
        if d:
            edit.setText(d.replace("/", "\\"))

    def _update_user_tab_state(self) -> None:
        enabled = bool(self._active_username)
        if hasattr(self, "generalTabs"):
            self.generalTabs.setTabEnabled(1, enabled)
        self.txtUserOutputBase.setReadOnly(not enabled)
        self.txtUserOutputBase.setEnabled(True)
        self.btnUserOutputBrowse.setEnabled(enabled)
        self.btnUserOutputClear.setEnabled(enabled)
        for edit in self._user_repo_inputs.values():
            edit.setReadOnly(not enabled)
            edit.setEnabled(True)
        for row in range(self.userReposLayout.rowCount()):
            item = self.userReposLayout.itemAtPosition(row, 2)
            widget = item.widget() if item else None
            if widget is not None:
                widget.setEnabled(enabled)
        self.projectEditor.moduleEditor.set_user_edit_enabled(enabled)
        self.targetEditor.set_user_edit_enabled(enabled)

    def _store_current_target_override(self) -> None:
        if (
            not self.group
            or self._current_target_row < 0
            or self._current_target_row >= len(self.group.deploy_targets or [])
        ):
            return
        target = self.group.deploy_targets[self._current_target_row]
        self._user_deploy_overrides[target.name] = self.targetEditor.user_paths_override()

    def _apply_global_permissions(self) -> None:
        editable = self._can_edit_global
        for edit in [self.txtRepoKey, self.txtRepoPath, self.txtOutputBase, self.txtProfile]:
            edit.setReadOnly(not editable)
            edit.setEnabled(True)
        for widget in [
            self.btnAddRepo,
            self.btnDelRepo,
            self.btnRepoPath,
            self.btnOutputBase,
            self.btnAddProfile,
            self.btnDelProfile,
            self.btnAddGroup,
            self.btnRenGroup,
            self.btnDelGroup,
            self.btnAddProject,
            self.btnDelProject,
            self.btnAddTarget,
            self.btnDelTarget,
        ]:
            widget.setEnabled(editable)
        self.projectEditor.set_global_edit_enabled(editable)
        self.targetEditor.set_global_edit_enabled(editable)

    def _load_repo_row(self, row: int):
        if not self.group or row < 0 or row >= len(self.group.repos or {}):
            self.txtRepoKey.clear(); self.txtRepoPath.clear(); return
        keys = list((self.group.repos or {}).keys())
        k = keys[row]
        self.txtRepoKey.setText(k)
        self.txtRepoPath.setText(self.group.repos[k])

    def _add_repo(self):
        if not self._can_edit_global:
            return
        if not self.group: return
        key = self.txtRepoKey.text().strip() or "repo"
        path = self.txtRepoPath.text().strip() or ""
        if not key:
            QMessageBox.warning(self, "Repo", "Escribe el nombre del repo."); return
        self.group.repos = self.group.repos or {}
        self.group.repos[key] = path
        self._load_group()

    def _del_repo(self):
        if not self._can_edit_global:
            return
        if not self.group: return
        row = self.lstRepos.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este repo?"): return
        keys = list((self.group.repos or {}).keys())
        del self.group.repos[keys[row]]
        self._load_group()

    def _browse_repo(self):
        d = QFileDialog.getExistingDirectory(self, "Selecciona carpeta del repo")
        if d:
            self.txtRepoPath.setText(d.replace("/", "\\"))

    def _browse_output(self):
        d = QFileDialog.getExistingDirectory(self, "Selecciona carpeta de salida base")
        if d:
            self.txtOutputBase.setText(d.replace("/", "\\"))

    # --------------- Perfiles ---------------

    def _load_profile_row(self, row: int):
        if not self.group or row < 0 or row >= len(self.group.profiles or []):
            self.txtProfile.clear(); return
        self.txtProfile.setText(self.group.profiles[row])

    def _add_profile(self):
        if not self._can_edit_global:
            return
        if not self.group: return
        p = self.txtProfile.text().strip()
        if not p:
            QMessageBox.warning(self, "Perfiles", "Escribe el nombre del perfil."); return
        self.group.profiles = (self.group.profiles or []) + [p]
        self._load_group()

    def _del_profile(self):
        if not self._can_edit_global:
            return
        if not self.group: return
        row = self.lstProfiles.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este perfil?"): return
        del self.group.profiles[row]
        self._load_group()

    # --------------- Proyectos ---------------

    def _load_project_row(self, row: int):
        if self.group and 0 <= self._current_project_row < len(self.group.projects or []):
            self.projectEditor.capture_current_module_override()
            if self._can_edit_global:
                self.projectEditor.apply_editor_to_current()
                self.group.projects[self._current_project_row] = self.projectEditor.to_project()
                save_config(self.cfg)
        self._current_project_row = row
        if not self.group or row < 0 or row >= len(self.group.projects or []):
            self.projectEditor.set_from_project(Project(key="", repo="", execution_mode="integrated", modules=[]))
            self.projectEditor.moduleEditor.set_user_edit_enabled(bool(self._active_username))
            return
        self.projectEditor._group = self.group
        self.projectEditor.set_from_project(self.group.projects[row])
        self.projectEditor.moduleEditor.set_user_edit_enabled(bool(self._active_username))

    def _add_project(self):
        if not self._can_edit_global:
            return
        if not self.group: return
        default_repo = next(iter((self.group.repos or {}).keys()), "")
        p = Project(key=_unique_key("NuevoProyecto", [x.key for x in (self.group.projects or [])]),
                    repo=default_repo, execution_mode="integrated", modules=[])
        self.group.projects = (self.group.projects or []) + [p]
        self._load_group()
        self.lstProjects.setCurrentRow(len(self.group.projects)-1)

    def _del_project(self):
        if not self._can_edit_global:
            return
        if not self.group: return
        row = self.lstProjects.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este proyecto?"): return
        project = self.group.projects[row]
        self.projectEditor.remove_project_overrides(project.key)
        del self.group.projects[row]
        self._load_group()

    # --------------- Targets ---------------

    def _load_target_row(self, row: int):
        if self.group and 0 <= self._current_target_row < len(self.group.deploy_targets or []):
            previous = self.group.deploy_targets[self._current_target_row]
            override_value = self.targetEditor.user_paths_override()
            self._user_deploy_overrides[previous.name] = override_value
            if self._can_edit_global:
                updated_target = self.targetEditor.to_target()
                self.group.deploy_targets[self._current_target_row] = updated_target
                if previous.name != updated_target.name:
                    self._user_deploy_overrides.pop(previous.name, None)
                    self._user_deploy_overrides[updated_target.name] = override_value
                save_config(self.cfg)
        self._current_target_row = row
        if not self.group:
            return
        new_editor = TargetRow(self.group, self.cfg)
        self._deploy_layout.replaceWidget(self.targetEditor, new_editor)
        self.targetEditor.setParent(None)
        self.targetEditor.deleteLater()
        self.targetEditor = new_editor
        self.targetEditor.set_global_edit_enabled(self._can_edit_global)
        self.targetEditor.set_user_edit_enabled(bool(self._active_username))

        if 0 <= row < len(self.group.deploy_targets or []):
            t = self.group.deploy_targets[row]
            override = self._user_deploy_overrides.get(t.name)
            self.targetEditor.set_from_target(t, override)
        else:
            self.targetEditor.set_user_paths(None, None)

    def _add_target(self):
        if not self._can_edit_global:
            return
        if not self.group: return
        project_key = self.group.projects[0].key if (self.group.projects) else ""
        t = DeployTarget(
            name=_unique_key("NuevoTarget", [x.name for x in (self.group.deploy_targets or [])]),
            project_key=project_key,
            profiles=self.group.profiles or [],
            path_template=r"\\server\share\{version}\ "
        )
        self.group.deploy_targets = (self.group.deploy_targets or []) + [t]
        self._load_group()
        self.lstTargets.setCurrentRow(len(self.group.deploy_targets)-1)

    def _del_target(self):
        if not self._can_edit_global:
            return
        if not self.group: return
        row = self.lstTargets.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este target?"): return
        target = self.group.deploy_targets[row]
        self._user_deploy_overrides.pop(target.name, None)
        del self.group.deploy_targets[row]
        self._load_group()

    # --------------- Guardar ---------------

    def _save(self, silent: bool = False):
        self.cfg.paths.nas_dir = self.txtNasDir.text().strip()
        override_error: Optional[str] = None

        if self.group:
            self.projectEditor.capture_current_module_override()
            self._store_current_target_override()
            if self._can_edit_global:
                self.group.output_base = self.txtOutputBase.text().strip()

                # Proyecto seleccionado: aplicar cambios del editor
                prow = self.lstProjects.currentRow()
                if 0 <= prow < len(self.group.projects or []):
                    self.projectEditor.apply_editor_to_current()
                    self.group.projects[prow] = self.projectEditor.to_project()

                # Target seleccionado: aplicar cambios
                trow = self.lstTargets.currentRow()
                if 0 <= trow < len(self.group.deploy_targets or []):
                    previous = self.group.deploy_targets[trow]
                    override_value = self.targetEditor.user_paths_override()
                    self._user_deploy_overrides[previous.name] = override_value
                    updated_target = self.targetEditor.to_target()
                    self.group.deploy_targets[trow] = updated_target
                    if previous.name != updated_target.name:
                        self._user_deploy_overrides.pop(previous.name, None)
                        self._user_deploy_overrides[updated_target.name] = override_value

        try:
            save_config(self.cfg)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo guardar:\n{e}")
            return

        if self.group and self._active_username:
            try:
                store = _create_config_store()
                repo_payload = {
                    key: edit.text().strip()
                    for key, edit in self._user_repo_inputs.items()
                    if edit.text().strip()
                }
                output_value = self.txtUserOutputBase.text().strip() or None
                store.set_group_user_paths(
                    self.group.key,
                    self._active_username,
                    repos=repo_payload,
                    output_base=output_value,
                )

                module_overrides = self.projectEditor.module_overrides()
                existing_projects: Dict[str, set[str]] = {
                    project.key: {m.name for m in (project.modules or [])}
                    for project in (self.group.projects or [])
                }
                for project_key, module_names in existing_projects.items():
                    overrides_for_project = module_overrides.get(project_key, {})
                    for module_name in module_names:
                        store.set_module_user_path(
                            self.group.key,
                            project_key,
                            module_name,
                            self._active_username,
                            overrides_for_project.get(module_name),
                        )
                    for extra_name in list(overrides_for_project.keys()):
                        if extra_name not in module_names:
                            store.set_module_user_path(
                                self.group.key,
                                project_key,
                                extra_name,
                                self._active_username,
                                None,
                            )
                for project_key in list(module_overrides.keys()):
                    if project_key not in existing_projects:
                        for module_name in module_overrides[project_key].keys():
                            store.set_module_user_path(
                                self.group.key,
                                project_key,
                                module_name,
                                self._active_username,
                                None,
                            )

                for target in self.group.deploy_targets or []:
                    path_value, hotfix_value = self._user_deploy_overrides.get(target.name, (None, None))
                    store.set_deploy_user_paths(
                        self.group.key,
                        target.name,
                        self._active_username,
                        path_template=path_value,
                        hotfix_path_template=hotfix_value,
                    )
                for target_name in list(self._user_deploy_overrides.keys()):
                    if not any(t.name == target_name for t in (self.group.deploy_targets or [])):
                        store.set_deploy_user_paths(
                            self.group.key,
                            target_name,
                            self._active_username,
                            path_template=None,
                            hotfix_path_template=None,
                        )
            except Exception as exc:
                override_error = str(exc)

        if not silent:
            if override_error:
                QMessageBox.warning(
                    self,
                    "Guardar",
                    "Se guardó la configuración pero algunas rutas personalizadas no pudieron almacenarse:\n"
                    f"{override_error}",
                )
            else:
                QMessageBox.information(self, "Guardar", "Configuración guardada.")

# ----------------------------- Wizard wrapper -----------------------------

class GroupsWizard(QDialog):
    def __init__(self, cfg: Config, on_saved_callback=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Grupos")
        self.setModal(False)
        self.resize(900, 680)
        self._cfg = cfg
        self._on_saved = on_saved_callback

        lay = QVBoxLayout(self)
        self.editor = GroupEditor(cfg)
        lay.addWidget(self.editor)

        # interceptar guardar para refrescar cfg padre
        orig_save = self.editor._save
        def _save_and_callback(*args, **kwargs):
            orig_save(*args, **kwargs)
            if callable(self._on_saved):
                self._on_saved()
        self.editor._save = _save_and_callback
