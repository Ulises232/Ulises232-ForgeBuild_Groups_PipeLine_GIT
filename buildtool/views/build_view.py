import threading
from typing import Optional

from PySide6.QtCore import Qt, QThread, QSignalBlocker, Slot
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
    QSpinBox,
)

from ..core.bg import run_in_thread
from ..core.config import Config, PipelinePreset, save_config
from ..core.config_queries import (
    first_group_key,
    find_project,
    get_group,
    default_project_key,
    iter_group_projects,
    iter_groups,
    project_module_names,
    project_profiles,
)
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
        group_keys = [grp.key for grp in iter_groups(cfg)]
        if group_keys:
            for key in group_keys:
                self.cboGroup.addItem(key, key)
        else:
            self.cboGroup.addItem("Sin grupos", None)
            self.cboGroup.setEnabled(False)
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

        row.addWidget(QLabel("Hilos:"))
        self.spinMaxWorkers = QSpinBox()
        self.spinMaxWorkers.setRange(0, 32)
        self.spinMaxWorkers.setSpecialValueText("Auto")
        self.spinMaxWorkers.setToolTip(
            "Número máximo de hilos simultáneos para compilar perfiles."
        )
        default_workers = getattr(self.cfg, "max_build_workers", None) or 0
        self.spinMaxWorkers.setValue(default_workers)
        row.addWidget(self.spinMaxWorkers)

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
        self.spinMaxWorkers.valueChanged.connect(self._on_max_workers_changed)

        self._worker_records: dict[PipelineWorker, dict] = {}
        self._setup_quick_filter(self.cboGroup)
        self._setup_quick_filter(self.cboProject)
        self._refresh_presets()
        if hasattr(self.preset_notifier, "changed"):
            self.preset_notifier.changed.connect(self._refresh_presets)
        self.refresh_group()
        if not group_keys:
            self.cboProfiles.setEnabled(False)
            self.cboModules.setEnabled(False)
            self.btnBuildSel.setEnabled(False)
            self.btnBuildAll.setEnabled(False)

    def _current_group(self) -> Optional[str]:
        return self.cboGroup.currentData()

    def _get_current_project(self):
        gkey = self._current_group()
        if not gkey:
            return None, None
        grp = get_group(self.cfg, gkey)
        if not grp:
            return None, None
        if self.cboProject.isVisible():
            pkey = self.cboProject.currentData()
        else:
            pkey = default_project_key(self.cfg, gkey)
        _, proj = find_project(self.cfg, pkey, gkey)
        return proj, gkey

    def _setup_quick_filter(self, combo: QComboBox) -> None:
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.NoInsert)
        completer: QCompleter = combo.completer()
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.setFilterMode(Qt.MatchContains)
        combo.lineEdit().setReadOnly(False)

    @Slot()
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

    @Slot(int)
    def _on_preset_selected(self, _index: int = -1) -> None:
        preset: Optional[PipelinePreset] = self.cboPresets.currentData()
        if preset:
            self._apply_preset(preset)

    @Slot(bool)
    def apply_selected_preset(self, _checked: bool = False) -> None:
        preset: Optional[PipelinePreset] = self.cboPresets.currentData()
        if preset:
            self._apply_preset(preset)

    def _apply_preset(self, preset: PipelinePreset) -> None:
        target_group = preset.group_key or first_group_key(self.cfg)
        idx = self.cboGroup.findData(target_group)
        if idx == -1:
            self.log.append(
                f"<< El grupo '{target_group}' no existe en la configuración actual."
            )
            return
        if target_group is not None:
            self.cboGroup.setCurrentIndex(idx)

        if preset.project_key:
            proj_idx = self.cboProject.findData(preset.project_key)
            if proj_idx != -1:
                self.cboProject.setCurrentIndex(proj_idx)

        self.cboProfiles.set_checked_items(preset.profiles or [])
        self.cboModules.set_checked_items(preset.modules or [])
        self.log.append(f"<< Preset '{preset.name}' aplicado.")

    @Slot(bool)
    def prompt_save_preset(self, _checked: bool = False) -> None:
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

    @Slot(bool)
    def open_preset_manager(self, _checked: bool = False) -> None:
        dlg = PresetManagerDialog(self.cfg, "build", self)
        dlg.exec()
        if getattr(dlg, "was_modified", False):
            save_config(self.cfg)
            self._refresh_presets()
            if hasattr(self.preset_notifier, "changed"):
                self.preset_notifier.changed.emit()

    @Slot(int)
    def refresh_group(self, _index: Optional[int] = None) -> None:
        self.cboProject.clear()
        gkey = self._current_group()

        projects = [p.key for _, p in iter_group_projects(self.cfg, gkey)]
        for k in projects:
            self.cboProject.addItem(k, k)

        show_proj = len(projects) > 1
        self.lblProject.setVisible(show_proj)
        self.cboProjectContainer.setVisible(show_proj)
        self.cboProject.setVisible(show_proj)
        self.cboProject.setEnabled(bool(projects))

        self.refresh_project_data()

    @Slot(int)
    def refresh_project_data(self, _index: Optional[int] = None) -> None:
        gkey = self._current_group()
        if self.cboProject.isVisible():
            pkey = self.cboProject.currentData()
        else:
            pkey = default_project_key(self.cfg, gkey)

        profiles = project_profiles(self.cfg, gkey, pkey)
        modules = project_module_names(self.cfg, gkey, pkey)

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
        workers_value = self.spinMaxWorkers.value()
        max_workers = workers_value if workers_value > 0 else None
        worker = build_worker(
            build_project_scheduled,
            success_message=">> Listo.",
            cfg=self.cfg,
            project_key=proj.key,
            profiles=profiles,
            modules_filter=modules_filter,
            group_key=gkey,
            max_workers=max_workers,
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

    @Slot(bool)
    def start_build_selected(self, _checked: bool = False) -> None:
        profiles = self.cboProfiles.checked_items()
        if not profiles:
            self.log.append("<< Elige al menos un perfil.")
            return
        self._start_schedule(profiles)

    @Slot(bool)
    def start_build_all(self, _checked: bool = False) -> None:
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

    @Slot(bool)
    def cancel_active_builds(self, _checked: bool = False) -> None:
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

    @Slot(int)
    def _on_max_workers_changed(self, value: int) -> None:
        max_workers = value or None
        if getattr(self.cfg, "max_build_workers", None) == max_workers:
            return
        self.cfg.max_build_workers = max_workers
        save_config(self.cfg)
