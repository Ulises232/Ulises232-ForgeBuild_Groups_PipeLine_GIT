from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QLineEdit, QTextEdit,
    QCheckBox
)
from PySide6.QtCore import QThread
from ..core.config import Config
from ..core.tasks import deploy_version

from ..ui.multi_select import Logger, MultiSelectComboBox

# ---------- worker ----------

def _run_deploy(cfg: Config, group_key: str|None, project_key: str, profile: str,
                version: str, target_name: str, hotfix: bool, logger: Logger):
    def emit(s: str): logger.line.emit(s)
    try:
        deploy_version(cfg, project_key, profile, version, target_name, log_cb=emit, group_key=group_key, hotfix=hotfix)
        emit(">> Copia completada.")
    except Exception as e:
        emit(f"<< ERROR: {e}")


# ---------- vista ----------

class DeployView(QWidget):
    """
    - Grupo (combo)
    - Proyecto (oculto si el grupo tiene uno solo)
    - Perfiles (multiselección)
    - Versión (input corto)
    - Botones: Copiar seleccionados / Copiar TODOS
    """
    def __init__(self, cfg: Config):
        super().__init__()
        self.cfg = cfg
        self._profile_to_target: dict[str, str] = {}  # perfil -> target_name

        lay = QVBoxLayout(self)
        row = QHBoxLayout(); row.setSpacing(12)

        row.addWidget(QLabel("Grupo:"))
        self.cboGroup = QComboBox()
        groups = [g.key for g in (cfg.groups or [])] or ["GLOBAL"]
        for g in groups: self.cboGroup.addItem(g, g)
        row.addWidget(self.cboGroup)

        self.lblProject = QLabel("Proyecto:")
        self.cboProject = QComboBox()
        row.addWidget(self.lblProject); row.addWidget(self.cboProject)

        row.addWidget(QLabel("Perfiles:"))
        self.cboProfiles = MultiSelectComboBox("Perfiles…", show_max=2)
        row.addWidget(self.cboProfiles)

        row.addWidget(QLabel("Versión:"))
        self.txtVersion = QLineEdit()
        self.txtVersion.setPlaceholderText("yyyy-mm-dd_nnn")
        self.txtVersion.setFixedWidth(200)  # más corto
        row.addWidget(self.txtVersion)

        self.chkHotfix = QCheckBox("Hotfix")
        row.addWidget(self.chkHotfix)

        self.btnDeploySel = QPushButton("Copiar seleccionados")
        self.btnDeployAll = QPushButton("Copiar TODOS")
        row.addWidget(self.btnDeploySel)
        row.addWidget(self.btnDeployAll)

        row.addStretch(1)
        lay.addLayout(row)

        self.log = QTextEdit(); self.log.setReadOnly(True)
        self.log.setStyleSheet("font-family:Consolas,monospace;")
        self.log.setMinimumHeight(280)
        lay.addWidget(self.log)

        # señales
        self.cboGroup.currentIndexChanged.connect(self.refresh_group)
        self.cboProject.currentIndexChanged.connect(self.refresh_project)
        self.btnDeploySel.clicked.connect(self.start_deploy_selected)
        self.btnDeployAll.clicked.connect(self.start_deploy_all)

        self.refresh_group()

    # ---- helpers de datos ----
    def _current_group(self):
        val = self.cboGroup.currentData()
        return None if val == "GLOBAL" else val

    def refresh_group(self):
        self.cboProject.clear()
        gkey = self._current_group()

        if gkey:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None)
            projects = [p.key for p in (grp.projects if grp else [])]
            for k in projects: self.cboProject.addItem(k, k)
            # Ocultar “Proyecto” si solo hay uno
            show_proj = len(projects) > 1
            self.lblProject.setVisible(show_proj)
            self.cboProject.setVisible(show_proj)
        else:
            # legacy
            projects = [p.key for p in self.cfg.projects]
            for k in projects: self.cboProject.addItem(k, k)
            self.lblProject.setVisible(True)
            self.cboProject.setVisible(True)

        self.refresh_project()

    def refresh_project(self):
        self._profile_to_target.clear()
        self.cboProfiles.set_items([])

        gkey = self._current_group()
        pkey = self.cboProject.currentData()

        # perfiles disponibles (como en Build)
        profiles = []
        proj = None
        if gkey:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None)
            if grp:
                if self.cboProject.isVisible():
                    proj = next((p for p in grp.projects if p.key==pkey), None)
                else:
                    proj = grp.projects[0] if grp.projects else None
                if proj and getattr(proj, "profiles", None):
                    profiles = proj.profiles
                elif grp and grp.profiles:
                    profiles = grp.profiles
                # targets: perfil -> target_name
                for t in (grp.deploy_targets or []):
                    if proj and t.project_key != proj.key:
                        continue
                    for prof in t.profiles:
                        # si hay duplicados para el mismo perfil, tomar el primero definido
                        self._profile_to_target.setdefault(prof, t.name)
        else:
            proj = next((p for p in self.cfg.projects if p.key==pkey), None)
            if proj and getattr(proj, "profiles", None):
                profiles = proj.profiles
            elif self.cfg.profiles:
                profiles = self.cfg.profiles
            for t in (self.cfg.deploy_targets or []):
                if proj and t.project_key != proj.key:
                    continue
                for prof in t.profiles:
                    self._profile_to_target.setdefault(prof, t.name)

        self.cboProfiles.set_items(profiles or [])

    # ---- despliegue ----
    def _deploy_profiles(self, profiles):
        gkey = self._current_group()
        # si el proyecto está oculto, tomar el único
        if self.cboProject.isVisible():
            pkey = self.cboProject.currentData()
        else:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None) if gkey else None
            pkey = grp.projects[0].key if (grp and grp.projects) else None

        if not pkey:
            self.log.append("<< No hay proyecto configurado."); return

        version = self.txtVersion.text().strip()
        if not version:
            self.log.append("<< Escribe la versión (ej. 2025-09-08_001)."); return

        for prof in profiles:
            tgt = self._profile_to_target.get(prof)
            if not tgt:
                self.log.append(f"<< No hay destino configurado para el perfil '{prof}'.")
                continue
            logger = Logger(); logger.line.connect(lambda s, p=prof: self.log.append(f"[{p}] {s}"))
            th = QThread(self)
            hotfix = self.chkHotfix.isChecked()
            # ...
            th.run = lambda p=prof, t=tgt, lg=logger, hf=hotfix: _run_deploy(self.cfg, gkey, pkey, p, version, t, hf, lg)

            th.start()

    def start_deploy_selected(self):
        profiles = self.cboProfiles.checked_items()
        if not profiles:
            self.log.append("<< Elige al menos un perfil."); return
        self._deploy_profiles(profiles)

    def start_deploy_all(self):
        profiles = self.cboProfiles.all_items()
        if not profiles:
            self.log.append("<< No hay perfiles configurados."); return
        self._deploy_profiles(profiles)
