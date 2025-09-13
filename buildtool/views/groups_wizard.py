from __future__ import annotations
from typing import List, Optional
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QWidget, QDialog, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QListWidget, QListWidgetItem,
    QFileDialog, QMessageBox, QCheckBox, QSplitter, QTabWidget
)
from ..core.config import (
    Config, Group, Project, Module, DeployTarget, save_config
)

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
        flags_h.addWidget(self.cmbOptional)
        flags_h.addWidget(self.cmbNoProfile)
        flags_h.addWidget(self.cmbRunOnce)
        flags_h.addWidget(self.cmbSerial)
        lay.addWidget(flags_w, 1, 3)
        # Fila 2
        lay.addWidget(QLabel("Salida:"), 2, 0); lay.addWidget(self.cboSalida, 2, 1)
        lay.addWidget(QLabel("Carpeta:"), 2, 2); lay.addWidget(self.txtCustomOut, 2, 3)
        lay.addWidget(self.chkToRoot, 2, 4)
        # Fila 3
        lay.addWidget(QLabel("Patrón (1 archivo):"), 3, 0); lay.addWidget(self.txtSelectPattern, 3, 1)
        lay.addWidget(QLabel("Renombrar a:"),        3, 2); lay.addWidget(self.txtRenameTo,     3, 3)

        self.cboSalida.currentIndexChanged.connect(self._toggle_custom_out)
        self._toggle_custom_out()

    def _toggle_custom_out(self):
        custom = (self.cboSalida.currentIndex() == 2)
        self.txtCustomOut.setEnabled(custom)
        # "A la raíz" solo aplica a WAR/UI
        self.chkToRoot.setEnabled(self.cboSalida.currentIndex() in (0, 1))

    def set_from_module(self, m: Module):
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
            for p in cfg.projects:
                self.cboProject.addItem(p.key, p.key)

        lay.addWidget(QLabel("Nombre:"), 0, 0); lay.addWidget(self.txtName, 0, 1, 1, 3)
        lay.addWidget(QLabel("Proyecto:"), 1, 0); lay.addWidget(self.cboProject, 1, 1)
        lay.addWidget(QLabel("Perfiles:"), 1, 2); lay.addWidget(self.txtProfiles, 1, 3)
        lay.addWidget(QLabel("Path:"), 2, 0); lay.addWidget(self.txtPath, 2, 1, 1, 3)
        lay.addWidget(QLabel("Hotfix path:"), 3, 0); lay.addWidget(self.txtHotfix, 3, 1, 1, 3)

    def set_from_target(self, t: DeployTarget):
        self.txtName.setText(t.name or "")
        idx = self.cboProject.findData(t.project_key)
        if idx < 0:
            idx = self.cboProject.findText(t.project_key or "")
        self.cboProject.setCurrentIndex(idx if idx >= 0 else 0)
        self.txtProfiles.setText(", ".join(t.profiles or []))
        self.txtPath.setText(t.path_template or "")
        self.txtHotfix.setText(getattr(t, "hotfix_path_template", "") or "")

    def to_target(self) -> DeployTarget:
        profiles = [p.strip() for p in (self.txtProfiles.text().split(",") if self.txtProfiles.text().strip() else []) if p.strip()]
        return DeployTarget(
            name=self.txtName.text().strip(),
            project_key=self.cboProject.currentData() or self.cboProject.currentText().strip(),
            profiles=profiles,
            path_template=self.txtPath.text().strip(),
            hotfix_path_template=(self.txtHotfix.text().strip() or None),
        )

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
        header.addWidget(QLabel("Repo:"),     0, 2); header.addWidget(self.cboRepo, 0, 3)
        header.addWidget(QLabel("Ejecución:"), 0, 4); header.addWidget(self.cboExec, 0, 5)
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

    def set_from_project(self, p: Project):
        self.txtKey.setText(p.key or "")
        idx = self.cboRepo.findData(getattr(p, "repo", None))
        if idx < 0:
            idx = self.cboRepo.findText(getattr(p, "repo", "") or "")
        self.cboRepo.setCurrentIndex(idx if idx >= 0 else 0)
        exec_mode = getattr(p, "execution_mode", "integrated") or "integrated"
        self.cboExec.setCurrentText(exec_mode)

        self._modules = list(p.modules or [])
        self._refresh_modules_list()

    def to_project(self) -> Project:
        return Project(
            key=self.txtKey.text().strip(),
            repo=self.cboRepo.currentData() or self.cboRepo.currentText().strip(),
            execution_mode=self.cboExec.currentText(),
            modules=self._modules
        )

    def _refresh_modules_list(self):
        self.lstModules.clear()
        for m in self._modules:
            self.lstModules.addItem(QListWidgetItem(m.name))
        if self._modules:
            self.lstModules.setCurrentRow(0)

    def _load_selected_module(self, row: int):
        if row < 0 or row >= len(self._modules):
            return
        self.moduleEditor.set_from_module(self._modules[row])

    def _add_module(self):
        m = Module(name="nuevo-modulo", path="", goals=["clean", "package"])
        self._modules.append(m)
        self._refresh_modules_list()
        self.lstModules.setCurrentRow(len(self._modules)-1)

    def _del_module(self):
        row = self.lstModules.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este módulo?"):
            return
        self._modules.pop(row)
        self._refresh_modules_list()

    def apply_editor_to_current(self):
        row = self.lstModules.currentRow()
        if 0 <= row < len(self._modules):
            self._modules[row] = self.moduleEditor.to_module()

# ----------------------------- GroupEditor -----------------------------

class GroupEditor(QWidget):
    """Editor de un grupo: repos, output_base, perfiles, proyectos y deploy targets."""
    def __init__(self, cfg: Config, parent=None):
        super().__init__(parent)
        self.cfg = cfg
        self.group: Optional[Group] = None

        main = QVBoxLayout(self); main.setContentsMargins(6,6,6,6); main.setSpacing(6)

        # Top: selector de grupo
        top = QHBoxLayout()
        self.cboGroup = QComboBox()
        self.btnAddGroup = QPushButton("Nuevo grupo")
        self.btnDelGroup = QPushButton("Eliminar grupo")
        top.addWidget(QLabel("Grupo:")); top.addWidget(self.cboGroup, 1)
        top.addWidget(self.btnAddGroup); top.addWidget(self.btnDelGroup)
        main.addLayout(top)

        # Tabs
        self.tabs = QTabWidget()
        main.addWidget(self.tabs, 1)

        # --- Tab General (repos, output_base, perfiles)
        tab_gen = QWidget(); lay_gen = QGridLayout(tab_gen)
        lay_gen.setHorizontalSpacing(8); lay_gen.setVerticalSpacing(6)

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

        # signals
        self.cboGroup.currentIndexChanged.connect(self._load_group)
        self.btnAddGroup.clicked.connect(self._add_group)
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
        self.btnClose.clicked.connect(self.window().close)

        # init groups
        for g in (self.cfg.groups or []):
            self.cboGroup.addItem(g.key, g.key)
        if self.cfg.groups:
            self.cboGroup.setCurrentIndex(0)
            self._load_group()

    # --------------- Group handlers ---------------

    def _find_group(self, key: str) -> Optional[Group]:
        return next((g for g in (self.cfg.groups or []) if g.key == key), None)

    def _add_group(self):
        keys = [g.key for g in (self.cfg.groups or [])]
        new_key = _unique_key("NuevoGrupo", keys)
        g = Group(key=new_key, repos={}, output_base="", profiles=[], projects=[], deploy_targets=[])
        self.cfg.groups = (self.cfg.groups or []) + [g]
        self.cboGroup.addItem(new_key, new_key)
        self.cboGroup.setCurrentIndex(self.cboGroup.count()-1)
        self._load_group()

    def _del_group(self):
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

    def _load_group(self):
        key = self.cboGroup.currentData()
        grp = self._find_group(key)
        self.group = grp
        if not grp:
            return

        # General
        self.lstRepos.clear()
        for k, v in (grp.repos or {}).items():
            self.lstRepos.addItem(QListWidgetItem(f"{k} = {v}"))
        self.txtRepoKey.clear(); self.txtRepoPath.clear()
        self.txtOutputBase.setText(grp.output_base or "")

        self.lstProfiles.clear()
        for p in (grp.profiles or []):
            self.lstProfiles.addItem(QListWidgetItem(p))
        self.txtProfile.clear()

        # Proyectos
        self.lstProjects.clear()
        for p in (grp.projects or []):
            self.lstProjects.addItem(QListWidgetItem(p.key))
        self.projectEditor._group = grp
        if grp.projects:
            self.lstProjects.setCurrentRow(0)
            self.projectEditor.set_from_project(grp.projects[0])
        else:
            self.projectEditor.set_from_project(Project(key="", repo="", execution_mode="integrated", modules=[]))

        # Deploy
        self.lstTargets.clear()
        for t in (grp.deploy_targets or []):
            self.lstTargets.addItem(QListWidgetItem(t.name))
        # Asegurar que el editor corresponde al grupo actual
        new_editor = TargetRow(grp, self.cfg)
        self._deploy_layout.replaceWidget(self.targetEditor, new_editor)
        self.targetEditor.setParent(None)
        self.targetEditor.deleteLater()
        self.targetEditor = new_editor
        if grp.deploy_targets:
            self.lstTargets.setCurrentRow(0)
            self.targetEditor.set_from_target(grp.deploy_targets[0])

        # actualizar combo de repo del projectEditor
        self.projectEditor.cboRepo.clear()
        for rk in (grp.repos or {}).keys():
            self.projectEditor.cboRepo.addItem(rk, rk)

    # --------------- Repos ---------------

    def _load_repo_row(self, row: int):
        if not self.group or row < 0 or row >= len(self.group.repos or {}):
            self.txtRepoKey.clear(); self.txtRepoPath.clear(); return
        keys = list((self.group.repos or {}).keys())
        k = keys[row]
        self.txtRepoKey.setText(k)
        self.txtRepoPath.setText(self.group.repos[k])

    def _add_repo(self):
        if not self.group: return
        key = self.txtRepoKey.text().strip() or "repo"
        path = self.txtRepoPath.text().strip() or ""
        if not key:
            QMessageBox.warning(self, "Repo", "Escribe el nombre del repo."); return
        self.group.repos = self.group.repos or {}
        self.group.repos[key] = path
        self._load_group()

    def _del_repo(self):
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
        if not self.group: return
        p = self.txtProfile.text().strip()
        if not p:
            QMessageBox.warning(self, "Perfiles", "Escribe el nombre del perfil."); return
        self.group.profiles = (self.group.profiles or []) + [p]
        self._load_group()

    def _del_profile(self):
        if not self.group: return
        row = self.lstProfiles.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este perfil?"): return
        del self.group.profiles[row]
        self._load_group()

    # --------------- Proyectos ---------------

    def _load_project_row(self, row: int):
        if not self.group or row < 0 or row >= len(self.group.projects or []):
            self.projectEditor.set_from_project(Project(key="", repo="", execution_mode="integrated", modules=[]))
            return
        self.projectEditor._group = self.group
        self.projectEditor.set_from_project(self.group.projects[row])

    def _add_project(self):
        if not self.group: return
        default_repo = next(iter((self.group.repos or {}).keys()), "")
        p = Project(key=_unique_key("NuevoProyecto", [x.key for x in (self.group.projects or [])]),
                    repo=default_repo, execution_mode="integrated", modules=[])
        self.group.projects = (self.group.projects or []) + [p]
        self._load_group()
        self.lstProjects.setCurrentRow(len(self.group.projects)-1)

    def _del_project(self):
        if not self.group: return
        row = self.lstProjects.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este proyecto?"): return
        del self.group.projects[row]
        self._load_group()

    # --------------- Targets ---------------

    def _load_target_row(self, row: int):
        if not self.group:
            return
        new_editor = TargetRow(self.group, self.cfg)
        self._deploy_layout.replaceWidget(self.targetEditor, new_editor)
        self.targetEditor.setParent(None)
        self.targetEditor.deleteLater()
        self.targetEditor = new_editor

        if 0 <= row < len(self.group.deploy_targets or []):
            t = self.group.deploy_targets[row]
            self.targetEditor.set_from_target(t)

    def _add_target(self):
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
        if not self.group: return
        row = self.lstTargets.currentRow()
        if row < 0: return
        if not _confirm(self, "¿Eliminar este target?"): return
        del self.group.deploy_targets[row]
        self._load_group()

    # --------------- Guardar ---------------

    def _save(self):
        if not self.group:
            return

        # General
        self.group.output_base = self.txtOutputBase.text().strip()

        # Proyecto seleccionado: aplicar cambios del editor
        prow = self.lstProjects.currentRow()
        if 0 <= prow < len(self.group.projects or []):
            self.projectEditor.apply_editor_to_current()
            self.group.projects[prow] = self.projectEditor.to_project()

        # Target seleccionado: aplicar cambios
        trow = self.lstTargets.currentRow()
        if 0 <= trow < len(self.group.deploy_targets or []):
            self.group.deploy_targets[trow] = self.targetEditor.to_target()

        try:
            save_config(self.cfg)
            QMessageBox.information(self, "Guardar", "Configuración guardada.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo guardar:\n{e}")

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
        def _save_and_callback():
            orig_save()
            if callable(self._on_saved):
                self._on_saved()
        self.editor._save = _save_and_callback
