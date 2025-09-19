import threading
from typing import Optional

from PySide6.QtCore import Qt, QThread, QSignalBlocker
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QComboBox,
    QInputDialog,
    QMessageBox,
    QCompleter,
)

from ..core.bg import run_in_thread
from ..core.config import Config, PipelinePreset, save_config
from ..core.tasks import build_project_scheduled
from ..core.thread_tracker import TRACKER
from ..core.workers import PipelineWorker, build_worker

from ..ui.multi_select import MultiSelectComboBox
from ..ui.widgets import combo_with_arrow
from .preset_manager import PresetManagerDialog


class BuildView(QWidget):
    def __init__(self, cfg: Config, on_request_reload_config, preset_notifier) -> None:
        super().__init__()
        self.cfg = cfg
        self.on_request_reload_config = on_request_reload_config
        self.preset_notifier = preset_notifier

        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 12, 16, 12)
        lay.setSpacing(10)

        row = QHBoxLayout()
        row.setSpacing(12)
        row.addWidget(QLabel("Grupo:"))
        self.cboGroup = QComboBox()
        groups = [g.key for g in (cfg.groups or [])] or ["GLOBAL"]
        for g in groups:
            self.cboGroup.addItem(g, g)
        self.cboGroupContainer = combo_with_arrow(self.cboGroup)
        row.addWidget(self.cboGroupContainer)

        self.lblProject = QLabel("Proyecto:")
        self.cboProject = QComboBox()
        row.addWidget(self.lblProject)
        self.cboProjectContainer = combo_with_arrow(self.cboProject)
        row.addWidget(self.cboProjectContainer)

        row.addWidget(QLabel("Perfiles:"))
        self.cboProfiles = MultiSelectComboBox("Perfiles…", show_max=2)
        self.cboProfiles.enable_filter("Filtra perfiles…")
        self.cboProfilesContainer = combo_with_arrow(
            self.cboProfiles, arrow_tooltip="Seleccionar perfiles"
        )
        row.addWidget(self.cboProfilesContainer)

        row.addWidget(QLabel("Módulos:"))
        self.cboModules = MultiSelectComboBox("Módulos…", show_max=2)
        self.cboModules.enable_filter("Filtra módulos…")
        self.cboModulesContainer = combo_with_arrow(
            self.cboModules, arrow_tooltip="Seleccionar módulos"
        )
        row.addWidget(self.cboModulesContainer)

        self.btnBuildSel = QPushButton("Compilar seleccionados")
        row.addWidget(self.btnBuildSel)
        self.btnBuildAll = QPushButton("Compilar TODOS")
        row.addWidget(self.btnBuildAll)
        row.addStretch(1)
        lay.addLayout(row)

        preset_row = QHBoxLayout()
        preset_row.setSpacing(8)
        preset_row.addWidget(QLabel("Presets:"))
        self.cboPresets = QComboBox()
        self.cboPresetsContainer = combo_with_arrow(self.cboPresets)
        preset_row.addWidget(self.cboPresetsContainer)
        self.btnApplyPreset = QPushButton("Aplicar")
        preset_row.addWidget(self.btnApplyPreset)
        self.btnSavePreset = QPushButton("Guardar preset…")
        preset_row.addWidget(self.btnSavePreset)
        self.btnManagePresets = QPushButton("Administrar…")
        preset_row.addWidget(self.btnManagePresets)
        preset_row.addStretch(1)
        lay.addLayout(preset_row)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setStyleSheet("font-family: Consolas, monospace; font-size: 12px;")
        self.log.setMinimumHeight(320)
        lay.addWidget(self.log)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(12)
        footer.addStretch(1)
        self.btnClearLog = QPushButton("Limpiar consola")
        footer.addWidget(self.btnClearLog)
        self.btnCancel = QPushButton("Cancelar pipeline")
        self.btnCancel.setEnabled(False)
        footer.addWidget(self.btnCancel)
        lay.addLayout(footer)

        self.btnBuildSel.clicked.connect(self.start_build_selected)
        self.btnBuildAll.clicked.connect(self.start_build_all)
        self.btnCancel.clicked.connect(self.cancel_active_builds)
        self.btnClearLog.clicked.connect(self.log.clear)
        self.cboGroup.currentIndexChanged.connect(self.refresh_group)
        self.cboProject.currentIndexChanged.connect(self.refresh_project_data)
        self.cboPresets.currentIndexChanged.connect(self._on_preset_selected)
        self.btnApplyPreset.clicked.connect(self.apply_selected_preset)
        self.btnSavePreset.clicked.connect(self.prompt_save_preset)
        self.btnManagePresets.clicked.connect(self.open_preset_manager)

        self._worker_records: dict[PipelineWorker, dict] = {}
        self._setup_quick_filter(self.cboGroup)
        self._setup_quick_filter(self.cboProject)
        self._refresh_presets()
        if hasattr(self.preset_notifier, "changed"):
            self.preset_notifier.changed.connect(self._refresh_presets)
        self.refresh_group()

    def _current_group(self) -> Optional[str]:
        val = self.cboGroup.currentData()
        return None if val == "GLOBAL" else val

    def _get_current_project(self):
        gkey = self._current_group()
        pkey = self.cboProject.currentData()
        proj = None
        if gkey:
            grp = next((g for g in self.cfg.groups if g.key == gkey), None)
            if grp:
                proj = next((p for p in grp.projects if p.key == pkey), None)
        if not proj:
            proj = next((p for p in self.cfg.projects if p.key == pkey), None)
        return proj, gkey

    def _setup_quick_filter(self, combo: QComboBox) -> None:
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.NoInsert)
        completer: QCompleter = combo.completer()
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.setFilterMode(Qt.MatchContains)
        combo.lineEdit().setReadOnly(False)

    def _refresh_presets(self) -> None:
        block = QSignalBlocker(self.cboPresets)
        _ = block
        self.cboPresets.clear()
        self.cboPresets.addItem("Selecciona un preset…", None)
        presets = [p for p in (self.cfg.pipeline_presets or []) if p.pipeline == "build"]
        presets.sort(key=lambda p: p.name.lower())
        for preset in presets:
            extra: list[str] = []
            if preset.group_key:
                extra.append(preset.group_key)
            if preset.project_key:
                extra.append(preset.project_key)
            label = preset.name
            if extra:
                label += f" ({' / '.join(extra)})"
            self.cboPresets.addItem(label, preset)

    def _on_preset_selected(self) -> None:
        preset: Optional[PipelinePreset] = self.cboPresets.currentData()
        if preset:
            self._apply_preset(preset)

    def apply_selected_preset(self) -> None:
        preset: Optional[PipelinePreset] = self.cboPresets.currentData()
        if preset:
            self._apply_preset(preset)

    def _apply_preset(self, preset: PipelinePreset) -> None:
        target_group = preset.group_key or "GLOBAL"
        idx = self.cboGroup.findData(target_group)
        if idx == -1:
            self.log.append(
                f"<< El grupo '{target_group}' no existe en la configuración actual."
            )
            return
        self.cboGroup.setCurrentIndex(idx)

        if preset.project_key:
            proj_idx = self.cboProject.findData(preset.project_key)
            if proj_idx != -1:
                self.cboProject.setCurrentIndex(proj_idx)

        self.cboProfiles.set_checked_items(preset.profiles or [])
        self.cboModules.set_checked_items(preset.modules or [])
        self.log.append(f"<< Preset '{preset.name}' aplicado.")

    def prompt_save_preset(self) -> None:
        name, ok = QInputDialog.getText(self, "Guardar preset", "Nombre del preset:")
        if not ok:
            return
        name = name.strip()
        if not name:
            QMessageBox.warning(self, "Preset", "Escribe un nombre para el preset.")
            return
        self._save_preset(name)

    def _save_preset(self, name: str) -> None:
        group = self._current_group()
        project = self.cboProject.currentData()
        profiles = self.cboProfiles.checked_items()
        modules = self.cboModules.checked_items()
        preset = PipelinePreset(
            name=name,
            pipeline="build",
            group_key=group,
            project_key=project,
            profiles=profiles,
            modules=modules,
        )

        existing = next(
            (p for p in self.cfg.pipeline_presets if p.name == name and p.pipeline == "build"),
            None,
        )
        if existing:
            reply = QMessageBox.question(
                self,
                "Reemplazar preset",
                f"Ya existe un preset llamado '{name}'. ¿Deseas reemplazarlo?",
            )
            if reply != QMessageBox.Yes:
                return
            self.cfg.pipeline_presets.remove(existing)

        self.cfg.pipeline_presets.append(preset)
        save_config(self.cfg)
        self._refresh_presets()
        if hasattr(self.preset_notifier, "changed"):
            self.preset_notifier.changed.emit()
        self.log.append(f"<< Preset '{name}' guardado.")

    def open_preset_manager(self) -> None:
        dlg = PresetManagerDialog(self.cfg, "build", self)
        dlg.exec()
        if getattr(dlg, "was_modified", False):
            save_config(self.cfg)
            self._refresh_presets()
            if hasattr(self.preset_notifier, "changed"):
                self.preset_notifier.changed.emit()

    def refresh_group(self) -> None:
        self.cboProject.clear()
        gkey = self._current_group()

        if gkey:
            grp = next((g for g in self.cfg.groups if g.key == gkey), None)
            projects = [p.key for p in (grp.projects if grp else [])]
            for k in projects:
                self.cboProject.addItem(k, k)
            show_proj = len(projects) > 1
            self.lblProject.setVisible(show_proj)
            self.cboProjectContainer.setVisible(show_proj)
            self.cboProject.setVisible(show_proj)
        else:
            projects = [p.key for p in self.cfg.projects]
            for k in projects:
                self.cboProject.addItem(k, k)
            self.lblProject.setVisible(True)
            self.cboProjectContainer.setVisible(True)
            self.cboProject.setVisible(True)

        self.refresh_project_data()

    def refresh_project_data(self) -> None:
        gkey = self._current_group()
        pkey = self.cboProject.currentData()
        profiles: list[str] = []
        modules: list[str] = []

        if gkey:
            grp = next((g for g in self.cfg.groups if g.key == gkey), None)
            if grp:
                if self.cboProject.isVisible():
                    proj = next((p for p in grp.projects if p.key == pkey), None)
                else:
                    proj = grp.projects[0] if grp.projects else None
                if proj:
                    modules = [m.name for m in proj.modules]
                    if getattr(proj, "profiles", None):
                        profiles = proj.profiles
                if not profiles and grp.profiles:
                    profiles = grp.profiles
        if not modules:
            proj = next((p for p in self.cfg.projects if p.key == pkey), None)
            if proj:
                modules = [m.name for m in proj.modules]
                if getattr(proj, "profiles", None):
                    profiles = proj.profiles
        if not profiles and self.cfg.profiles:
            profiles = self.cfg.profiles

        self.cboProfiles.set_items(profiles or [])
        self.cboModules.set_items(modules or [], checked_all=True)

    def _start_schedule(self, profiles):
        proj, gkey = self._get_current_project()
        if not proj:
            self.log.append("<< No hay proyecto configurado en este grupo.")
            return
        selected_modules = self.cboModules.checked_items() or self.cboModules.all_items()

        self.btnBuildSel.setEnabled(False)
        self.btnBuildAll.setEnabled(False)
        self.btnCancel.setEnabled(True)

        modules_filter = set(selected_modules) if selected_modules else None
        cancel_event = threading.Event()
        worker = build_worker(
            build_project_scheduled,
            success_message=">> Listo.",
            cfg=self.cfg,
            project_key=proj.key,
            profiles=profiles,
            modules_filter=modules_filter,
            group_key=gkey,
            cancel_event=cancel_event,
        )
        thread, worker = run_in_thread(worker)
        worker.progress.connect(self.log.append, Qt.QueuedConnection)
        worker.finished.connect(
            lambda ok, wk=worker: self._on_build_finished(ok, wk),
            Qt.QueuedConnection,
        )

        self._worker_records[worker] = {
            "thread": thread,
            "event": cancel_event,
            "user_cancelled": False,
        }
        thread.start()

    def start_build_selected(self) -> None:
        profiles = self.cboProfiles.checked_items()
        if not profiles:
            self.log.append("<< Elige al menos un perfil.")
            return
        self._start_schedule(profiles)

    def start_build_all(self) -> None:
        profiles = self.cboProfiles.all_items()
        if not profiles:
            self.log.append("<< No hay perfiles configurados.")
            return
        self._start_schedule(profiles)

    def _on_build_finished(self, ok: bool, worker: PipelineWorker) -> None:
        record = self._worker_records.get(worker)
        if not record:
            return

        self._cleanup_worker(worker)

        if not self._worker_records:
            self.btnBuildSel.setEnabled(True)
            self.btnBuildAll.setEnabled(True)
            self.btnCancel.setEnabled(False)

        if not ok and record.get("user_cancelled"):
            self.log.append("<< Ejecución cancelada por el usuario.")

    def _cleanup_worker(self, worker: PipelineWorker) -> None:
        record = self._worker_records.pop(worker, None)
        if not record:
            return
        thread: QThread = record["thread"]
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

    def cancel_active_builds(self) -> None:
        if not self._worker_records:
            self.log.append("<< No hay ejecuciones en curso.")
            return

        self.log.append("<< Cancelando ejecuciones en curso…")
        for record in self._worker_records.values():
            record["user_cancelled"] = True
            event = record.get("event")
            if event:
                event.set()
            thread: QThread = record["thread"]
            try:
                thread.requestInterruption()
            except Exception:
                pass
        self.btnCancel.setEnabled(False)

    def closeEvent(self, event):  # noqa: N802 (Qt override)
        for worker in list(self._worker_records):
            self._cleanup_worker(worker)
        super().closeEvent(event)
