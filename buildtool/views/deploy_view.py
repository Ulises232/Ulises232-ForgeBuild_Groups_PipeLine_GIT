import threading
from typing import Optional

from PySide6.QtCore import Qt, QThread, QSignalBlocker
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QComboBox,
    QLineEdit,
    QTextEdit,
    QCheckBox,
    QInputDialog,
    QMessageBox,
    QCompleter,
)

from ..core.bg import run_in_thread
from ..core.config import Config, PipelinePreset, save_config
from ..core.tasks import deploy_profiles_scheduled
from ..core.thread_tracker import TRACKER
from ..core.workers import PipelineWorker

from ..ui.multi_select import MultiSelectComboBox
from ..ui.widgets import combo_with_arrow
from .preset_manager import PresetManagerDialog


class DeployView(QWidget):
    """Vista para ejecutar despliegues con soporte de presets."""

    def __init__(self, cfg: Config, preset_notifier) -> None:
        super().__init__()
        self.cfg = cfg
        self.preset_notifier = preset_notifier
        self._profile_to_target: dict[str, str] = {}

        lay = QVBoxLayout(self)
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

        row.addWidget(QLabel("Versión:"))
        self.txtVersion = QLineEdit()
        self.txtVersion.setPlaceholderText("yyyy-mm-dd_nnn")
        self.txtVersion.setFixedWidth(200)
        row.addWidget(self.txtVersion)

        self.chkHotfix = QCheckBox("Hotfix")
        row.addWidget(self.chkHotfix)

        self.btnDeploySel = QPushButton("Copiar seleccionados")
        self.btnDeployAll = QPushButton("Copiar TODOS")
        row.addWidget(self.btnDeploySel)
        row.addWidget(self.btnDeployAll)

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
        self.log.setStyleSheet("font-family:Consolas,monospace;")
        self.log.setMinimumHeight(280)
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

        self.cboGroup.currentIndexChanged.connect(self.refresh_group)
        self.cboProject.currentIndexChanged.connect(self.refresh_project)
        self.btnDeploySel.clicked.connect(self.start_deploy_selected)
        self.btnDeployAll.clicked.connect(self.start_deploy_all)
        self.btnCancel.clicked.connect(self.cancel_active_deploys)
        self.btnClearLog.clicked.connect(self.log.clear)
        self.cboPresets.currentIndexChanged.connect(self._on_preset_selected)
        self.btnApplyPreset.clicked.connect(self.apply_selected_preset)
        self.btnSavePreset.clicked.connect(self.prompt_save_preset)
        self.btnManagePresets.clicked.connect(self.open_preset_manager)

        self._worker_record: Optional[dict] = None
        self._setup_quick_filter(self.cboGroup)
        self._setup_quick_filter(self.cboProject)
        self._refresh_presets()
        if hasattr(self.preset_notifier, "changed"):
            self.preset_notifier.changed.connect(self._refresh_presets)
        self.refresh_group()

    def _current_group(self) -> Optional[str]:
        val = self.cboGroup.currentData()
        return None if val == "GLOBAL" else val

    def _setup_quick_filter(self, combo: QComboBox) -> None:
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.NoInsert)
        completer: QCompleter = combo.completer()
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.setFilterMode(Qt.MatchContains)
        combo.lineEdit().setReadOnly(False)

    def _refresh_presets(self) -> None:
        blocker = QSignalBlocker(self.cboPresets)
        _ = blocker
        self.cboPresets.clear()
        self.cboPresets.addItem("Selecciona un preset…", None)
        presets = [p for p in (self.cfg.pipeline_presets or []) if p.pipeline == "deploy"]
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
        if preset.version:
            self.txtVersion.setText(preset.version)
        if preset.hotfix is not None:
            self.chkHotfix.setChecked(bool(preset.hotfix))
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
        preset = PipelinePreset(
            name=name,
            pipeline="deploy",
            group_key=group,
            project_key=project,
            profiles=profiles,
            version=self.txtVersion.text().strip() or None,
            hotfix=self.chkHotfix.isChecked(),
        )

        existing = next(
            (p for p in self.cfg.pipeline_presets if p.name == name and p.pipeline == "deploy"),
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
        dlg = PresetManagerDialog(self.cfg, "deploy", self)
        dlg.exec()
        if getattr(dlg, "was_modified", False):
            save_config(self.cfg)
            self._refresh_presets()
            if hasattr(self.preset_notifier, "changed"):
                self.preset_notifier.changed.emit()

    # ---- helpers de datos ----
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

        self.refresh_project()

    def refresh_project(self) -> None:
        self._profile_to_target.clear()
        self.cboProfiles.set_items([])

        gkey = self._current_group()
        pkey = self.cboProject.currentData()

        profiles: list[str] = []
        proj = None
        if gkey:
            grp = next((g for g in self.cfg.groups if g.key == gkey), None)
            if grp:
                if self.cboProject.isVisible():
                    proj = next((p for p in grp.projects if p.key == pkey), None)
                else:
                    proj = grp.projects[0] if grp.projects else None
                if proj and getattr(proj, "profiles", None):
                    profiles = proj.profiles
                elif grp and grp.profiles:
                    profiles = grp.profiles
                for t in (grp.deploy_targets or []):
                    if proj and t.project_key != proj.key:
                        continue
                    for prof in t.profiles:
                        self._profile_to_target.setdefault(prof, t.name)
        else:
            proj = next((p for p in self.cfg.projects if p.key == pkey), None)
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
    def start_deploy_selected(self) -> None:
        profiles = self.cboProfiles.checked_items()
        if not profiles:
            self.log.append("<< Elige al menos un perfil.")
            return
        self._deploy_profiles(profiles)

    def start_deploy_all(self) -> None:
        profiles = self.cboProfiles.all_items()
        if not profiles:
            self.log.append("<< No hay perfiles configurados.")
            return
        self._deploy_profiles(profiles)

    def _deploy_profiles(self, profiles) -> None:
        gkey = self._current_group()
        if self.cboProject.isVisible():
            pkey = self.cboProject.currentData()
        else:
            grp = next((g for g in self.cfg.groups if g.key == gkey), None) if gkey else None
            pkey = grp.projects[0].key if (grp and grp.projects) else None

        if not pkey:
            self.log.append("<< No hay proyecto configurado.")
            return

        version = self.txtVersion.text().strip()
        if not version:
            self.log.append("<< Escribe la versión (ej. 2025-09-08_001).")
            return

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
            hotfix=self.chkHotfix.isChecked(),
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
        if not self._worker_record:
            self.log.append("<< No hay ejecuciones en curso.")
            return
        self.log.append("<< Cancelando despliegue en curso…")
        self._worker_record["user_cancelled"] = True
        event = self._worker_record.get("event")
        if event:
            event.set()
        thread: QThread = self._worker_record.get("thread")
        try:
            if thread:
                thread.requestInterruption()
        except Exception:
            pass
        self.btnCancel.setEnabled(False)

    def closeEvent(self, event):  # noqa: N802 (Qt override)
        if self._worker_record:
            self.cancel_active_deploys()
        self._cleanup_worker()
        super().closeEvent(event)
