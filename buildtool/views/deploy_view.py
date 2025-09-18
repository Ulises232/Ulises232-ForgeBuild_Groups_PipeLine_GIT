import threading

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QLineEdit, QTextEdit,
    QCheckBox
)
from PySide6.QtCore import Qt, QThread

from ..core.bg import run_in_thread
from ..core.config import Config
from ..core.tasks import deploy_profiles_scheduled
from ..core.thread_tracker import TRACKER
from ..core.workers import PipelineWorker

from ..ui.multi_select import MultiSelectComboBox
from ..ui.widgets import combo_with_arrow

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
        self.cboGroupContainer = combo_with_arrow(self.cboGroup)
        row.addWidget(self.cboGroupContainer)

        self.lblProject = QLabel("Proyecto:")
        self.cboProject = QComboBox()
        row.addWidget(self.lblProject)
        self.cboProjectContainer = combo_with_arrow(self.cboProject)
        row.addWidget(self.cboProjectContainer)

        row.addWidget(QLabel("Perfiles:"))
        self.cboProfiles = MultiSelectComboBox("Perfiles…", show_max=2)
        self.cboProfilesContainer = combo_with_arrow(self.cboProfiles, arrow_tooltip="Seleccionar perfiles")
        row.addWidget(self.cboProfilesContainer)

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

        footer = QHBoxLayout(); footer.setContentsMargins(0, 0, 0, 0); footer.setSpacing(12)
        footer.addStretch(1)
        self.btnClearLog = QPushButton("Limpiar consola")
        footer.addWidget(self.btnClearLog)
        self.btnCancel = QPushButton("Cancelar pipeline")
        self.btnCancel.setEnabled(False)
        footer.addWidget(self.btnCancel)
        lay.addLayout(footer)

        # señales
        self.cboGroup.currentIndexChanged.connect(self.refresh_group)
        self.cboProject.currentIndexChanged.connect(self.refresh_project)
        self.btnDeploySel.clicked.connect(self.start_deploy_selected)
        self.btnDeployAll.clicked.connect(self.start_deploy_all)
        self.btnCancel.clicked.connect(self.cancel_active_deploys)
        self.btnClearLog.clicked.connect(self.log.clear)

        self._worker_record: dict | None = None
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
            self.cboProjectContainer.setVisible(show_proj)
            self.cboProject.setVisible(show_proj)
        else:
            # legacy
            projects = [p.key for p in self.cfg.projects]
            for k in projects: self.cboProject.addItem(k, k)
            self.lblProject.setVisible(True)
            self.cboProjectContainer.setVisible(True)
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

        selected = []
        targets: dict[str, str] = {}
        for prof in profiles:
            tgt = self._profile_to_target.get(prof)
            if not tgt:
                self.log.append(f"<< No hay destino configurado para el perfil '{prof}'.")
                continue
            selected.append(prof)
            targets[prof] = tgt

        if not selected:
            self.btnDeploySel.setEnabled(True)
            self.btnDeployAll.setEnabled(True)
            return

        self.btnDeploySel.setEnabled(False)
        self.btnDeployAll.setEnabled(False)
        self.btnCancel.setEnabled(True)

        hotfix = self.chkHotfix.isChecked()
        cancel_event = threading.Event()
        worker = PipelineWorker(
            deploy_profiles_scheduled,
            success_message=">> Copia completada.",
            cfg=self.cfg,
            project_key=pkey,
            profiles=selected,
            profile_targets=targets,
            version=version,
            group_key=gkey,
            hotfix=hotfix,
            cancel_event=cancel_event,
        )
        thread, worker = run_in_thread(worker)
        worker.progress.connect(self.log.append, Qt.QueuedConnection)
        worker.finished.connect(lambda ok, wk=worker: self._on_deploy_finished(ok, wk), Qt.QueuedConnection)

        self._worker_record = {
            "thread": thread,
            "event": cancel_event,
            "user_cancelled": False,
            "worker": worker,
        }
        thread.start()

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

    def _on_deploy_finished(self, ok: bool, worker: PipelineWorker) -> None:
        record = self._worker_record
        if not record or record.get("worker") is not worker:
            return

        user_cancelled = record.get("user_cancelled")
        self._cleanup_worker()

        self.btnDeploySel.setEnabled(True)
        self.btnDeployAll.setEnabled(True)
        self.btnCancel.setEnabled(False)

        if not ok and user_cancelled:
            self.log.append("<< Ejecución cancelada por el usuario.")

    def _cleanup_worker(self) -> None:
        record = self._worker_record
        if not record:
            return
        thread: QThread = record.get("thread")
        worker: PipelineWorker = record.get("worker")
        self._worker_record = None
        try:
            if thread and thread.isRunning():
                thread.quit()
                thread.wait(5000)
        except Exception:
            pass
        try:
            if worker:
                worker.deleteLater()
        except Exception:
            pass
        try:
            if thread:
                thread.deleteLater()
        except Exception:
            pass
        if thread:
            TRACKER.remove(thread)

    def cancel_active_deploys(self) -> None:
        record = self._worker_record
        if not record:
            self.log.append("<< No hay ejecuciones en curso.")
            return

        self.log.append("<< Cancelando ejecuciones en curso…")
        record["user_cancelled"] = True
        event: threading.Event | None = record.get("event")
        if event:
            event.set()
        thread: QThread = record.get("thread")
        try:
            if thread:
                thread.requestInterruption()
        except Exception:
            pass
        self.btnCancel.setEnabled(False)

    def closeEvent(self, event):
        if self._worker_record:
            self.cancel_active_deploys()
        self._cleanup_worker()
        super().closeEvent(event)
