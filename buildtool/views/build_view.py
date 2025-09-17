from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QTextEdit, QComboBox
from PySide6.QtCore import Qt, QThread

from ..core.bg import run_in_thread
from ..core.config import Config
from ..core.tasks import build_project_scheduled
from ..core.thread_tracker import TRACKER
from ..core.workers import PipelineWorker, build_worker

from ..ui.multi_select import MultiSelectComboBox


class BuildView(QWidget):
    def __init__(self, cfg: Config, on_request_reload_config):
        super().__init__()
        self.cfg = cfg
        self.on_request_reload_config = on_request_reload_config
        lay = QVBoxLayout(self); lay.setContentsMargins(16,12,16,12); lay.setSpacing(10)

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
        self.cboProfiles = MultiSelectComboBox("Perfiles…", show_max=2); row.addWidget(self.cboProfiles)

        row.addWidget(QLabel("Módulos:"))
        self.cboModules = MultiSelectComboBox("Módulos…", show_max=2); row.addWidget(self.cboModules)

        self.btnBuildSel = QPushButton("Compilar seleccionados"); row.addWidget(self.btnBuildSel)
        self.btnBuildAll = QPushButton("Compilar TODOS"); row.addWidget(self.btnBuildAll)
        row.addStretch(1); lay.addLayout(row)

        self.log = QTextEdit(); self.log.setReadOnly(True)
        self.log.setStyleSheet("font-family: Consolas, monospace; font-size: 12px;")
        self.log.setMinimumHeight(320); lay.addWidget(self.log)

        self.btnBuildSel.clicked.connect(self.start_build_selected)
        self.btnBuildAll.clicked.connect(self.start_build_all)
        self.cboGroup.currentIndexChanged.connect(self.refresh_group)
        self.cboProject.currentIndexChanged.connect(self.refresh_project_data)

        self._live_workers: list[tuple[QThread, PipelineWorker]] = []
        self.refresh_group()

    def _current_group(self):
        val = self.cboGroup.currentData()
        return None if val=="GLOBAL" else val

    def _get_current_project(self):
        gkey = self._current_group()
        pkey = self.cboProject.currentData()
        proj = None
        if gkey:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None)
            if grp:
                proj = next((p for p in grp.projects if p.key==pkey), None)
        if not proj:
            proj = next((p for p in self.cfg.projects if p.key==pkey), None)
        return proj, gkey

    def refresh_group(self):
        self.cboProject.clear()
        gkey = self._current_group()

        if gkey:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None)
            projects = [p.key for p in (grp.projects if grp else [])]
            for k in projects: self.cboProject.addItem(k, k)
            show_proj = len(projects) > 1
            self.lblProject.setVisible(show_proj)
            self.cboProject.setVisible(show_proj)
        else:
            projects = [p.key for p in self.cfg.projects]
            for k in projects: self.cboProject.addItem(k, k)
            self.lblProject.setVisible(True)
            self.cboProject.setVisible(True)

        self.refresh_project_data()

    def refresh_project_data(self):
        gkey = self._current_group()
        pkey = self.cboProject.currentData()
        profiles = []
        modules = []

        if gkey:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None)
            if grp:
                proj = next((p for p in grp.projects if p.key==pkey), None) if self.cboProject.isVisible() else (grp.projects[0] if grp.projects else None)
                if proj:
                    modules = [m.name for m in proj.modules]
                    if getattr(proj, "profiles", None): profiles = proj.profiles
                if not profiles and grp.profiles: profiles = grp.profiles
        if not modules:
            proj = next((p for p in self.cfg.projects if p.key==pkey), None)
            if proj:
                modules = [m.name for m in proj.modules]
                if getattr(proj, "profiles", None): profiles = proj.profiles
        if not profiles and self.cfg.profiles:
            profiles = self.cfg.profiles

        self.cboProfiles.set_items(profiles or [])
        self.cboModules.set_items(modules or [], checked_all=True)

    def _start_schedule(self, profiles):
        proj, gkey = self._get_current_project()
        if not proj:
            self.log.append("<< No hay proyecto configurado en este grupo."); return
        selected_modules = self.cboModules.checked_items() or self.cboModules.all_items()

        self.btnBuildSel.setEnabled(False)
        self.btnBuildAll.setEnabled(False)

        modules_filter = set(selected_modules) if selected_modules else None
        worker = build_worker(
            build_project_scheduled,
            success_message=">> Listo.",
            cfg=self.cfg,
            project_key=proj.key,
            profiles=profiles,
            modules_filter=modules_filter,
            group_key=gkey,
        )
        thread, worker = run_in_thread(worker)
        worker.progress.connect(self.log.append, Qt.QueuedConnection)
        worker.finished.connect(lambda ok, th=thread, wk=worker: self._on_build_finished(th, wk), Qt.QueuedConnection)

        self._live_workers.append((thread, worker))
        thread.start()

    def start_build_selected(self):
        profiles = self.cboProfiles.checked_items()
        if not profiles: self.log.append("<< Elige al menos un perfil."); return
        self._start_schedule(profiles)

    def start_build_all(self):
        profiles = self.cboProfiles.all_items()
        if not profiles: self.log.append("<< No hay perfiles configurados."); return
        self._start_schedule(profiles)

    def _on_build_finished(self, thread: QThread, worker) -> None:
        self.btnBuildSel.setEnabled(True)
        self.btnBuildAll.setEnabled(True)
        self._cleanup_worker(thread, worker)

    def _cleanup_worker(self, thread: QThread, worker) -> None:
        if (thread, worker) not in self._live_workers:
            return
        self._live_workers.remove((thread, worker))
        try:
            if thread.isRunning():
                thread.quit()
                thread.wait(5000)
        except Exception:
            pass
        try:
            worker.deleteLater()
        except Exception:
            pass
        try:
            thread.deleteLater()
        except Exception:
            pass
        TRACKER.remove(thread)

    def closeEvent(self, event):
        for thread, worker in list(self._live_workers):
            self._cleanup_worker(thread, worker)
        super().closeEvent(event)
