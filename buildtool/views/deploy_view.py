import threading
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, QThread, QSignalBlocker, Slot
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
)

from ..core.bg import run_in_thread
from ..core.config import Config, PipelinePreset, save_config, load_config
from ..core.config_queries import (
    default_project_key,
    first_group_key,
    get_group,
    iter_group_projects,
    iter_groups,
    profile_target_map,
    project_profiles,
)
from ..core.tasks import deploy_profiles_scheduled
from ..core.thread_tracker import TRACKER
from ..core.workers import PipelineWorker

from ..ui.multi_select import MultiSelectComboBox
from ..ui.widgets import combo_with_arrow, setup_quick_filter, set_combo_enabled
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
        group_keys = [grp.key for grp in iter_groups(cfg)]
        if group_keys:
            for key in group_keys:
                self.cboGroup.addItem(key, key)
            set_combo_enabled(self.cboGroup, True)
        else:
            self.cboGroup.addItem("Sin grupos", None)
            set_combo_enabled(self.cboGroup, False)
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
        setup_quick_filter(self.cboGroup)
        setup_quick_filter(self.cboProject)
        self._refresh_presets()
        if hasattr(self.preset_notifier, "changed"):
            self.preset_notifier.changed.connect(self._refresh_presets)
        self.refresh_group()
        if not group_keys:
            self.cboProfiles.setEnabled(False)
            self.btnDeploySel.setEnabled(False)
            self.btnDeployAll.setEnabled(False)
            self.chkHotfix.setEnabled(False)
            self.txtVersion.setEnabled(False)

    def _current_group(self) -> Optional[str]:
        return self.cboGroup.currentData()

    @Slot()
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

    def _reload_cfg_from_store(self) -> None:
        current_group = self._current_group()
        current_project = self.cboProject.currentData()
        selected_profiles = self.cboProfiles.checked_items()
        version_text = self.txtVersion.text()
        hotfix_checked = self.chkHotfix.isChecked()
        current_preset = self.cboPresets.currentData()
        selected_preset_name = getattr(current_preset, "name", None)

        try:
            refreshed = load_config()
        except Exception as exc:  # pragma: no cover - defensive UI logging
            self.log.append(f"<< No se pudo recargar la configuración: {exc}")
            return

        self.cfg = refreshed
        group_keys = [grp.key for grp in iter_groups(self.cfg)]

        blocker = QSignalBlocker(self.cboGroup)
        _ = blocker
        self.cboGroup.clear()
        if group_keys:
            for key in group_keys:
                self.cboGroup.addItem(key, key)
            set_combo_enabled(self.cboGroup, True)
        else:
            self.cboGroup.addItem("Sin grupos", None)
            set_combo_enabled(self.cboGroup, False)
        target_index = self.cboGroup.findData(current_group)
        if target_index != -1:
            self.cboGroup.setCurrentIndex(target_index)
        elif group_keys:
            self.cboGroup.setCurrentIndex(0)
        else:
            self.cboGroup.setCurrentIndex(-1)

        self.refresh_group()

        if current_project:
            project_blocker = QSignalBlocker(self.cboProject)
            _ = project_blocker
            proj_idx = self.cboProject.findData(current_project)
            if proj_idx != -1:
                self.cboProject.setCurrentIndex(proj_idx)
            self.refresh_project()

        self.cboProfiles.set_checked_items(selected_profiles)
        self.txtVersion.setText(version_text)
        self.chkHotfix.setChecked(hotfix_checked)

        has_groups = bool(group_keys)
        self.btnDeploySel.setEnabled(has_groups)
        self.btnDeployAll.setEnabled(has_groups)
        self.cboProfiles.setEnabled(has_groups)
        self.chkHotfix.setEnabled(has_groups)
        self.txtVersion.setEnabled(has_groups)

        previous_index = self.cboPresets.currentIndex()
        self._refresh_presets()
        if selected_preset_name:
            for idx in range(self.cboPresets.count()):
                preset = self.cboPresets.itemData(idx)
                if preset and getattr(preset, "name", None) == selected_preset_name:
                    self.cboPresets.setCurrentIndex(idx)
                    break
            else:
                self.cboPresets.setCurrentIndex(0)
        else:
            self.cboPresets.setCurrentIndex(previous_index if previous_index >= 0 else 0)

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
        if preset.version:
            self.txtVersion.setText(preset.version)
        if preset.hotfix is not None:
            self.chkHotfix.setChecked(bool(preset.hotfix))
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

    @Slot(bool)
    def open_preset_manager(self, _checked: bool = False) -> None:
        dlg = PresetManagerDialog(self.cfg, "deploy", self)
        dlg.exec()
        if getattr(dlg, "was_modified", False):
            save_config(self.cfg)
            self._refresh_presets()
            if hasattr(self.preset_notifier, "changed"):
                self.preset_notifier.changed.emit()

    # ---- helpers de datos ----
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
        set_combo_enabled(self.cboProject, bool(projects))

        self.refresh_project()

    @Slot(int)
    def refresh_project(self, _index: Optional[int] = None) -> None:
        gkey = self._current_group()
        if self.cboProject.isVisible():
            pkey = self.cboProject.currentData()
        else:
            pkey = default_project_key(self.cfg, gkey)

        self._profile_to_target = profile_target_map(self.cfg, gkey, pkey)
        profiles = project_profiles(self.cfg, gkey, pkey)

        self.cboProfiles.set_items(profiles or [])

    # ---- despliegue ----
    @Slot(bool)
    def start_deploy_selected(self, _checked: bool = False) -> None:
        self._reload_cfg_from_store()
        profiles = self.cboProfiles.checked_items()
        if not profiles:
            self.log.append("<< Elige al menos un perfil.")
            return
        self._deploy_profiles(profiles)

    @Slot(bool)
    def start_deploy_all(self, _checked: bool = False) -> None:
        self._reload_cfg_from_store()
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
            pkey = default_project_key(self.cfg, gkey)

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

        group = get_group(self.cfg, gkey) if gkey else None
        target_lookup = {t.name: t for t in (getattr(group, "deploy_targets", None) or [])}
        preview_lines: list[str] = []
        hotfix_mode = self.chkHotfix.isChecked()
        for prof in selected:
            target_name = targets.get(prof)
            tgt = target_lookup.get(target_name) if target_lookup else None
            if not tgt:
                preview_lines.append(
                    f"   [{prof}] {target_name}: target no encontrado en el grupo."
                )
                continue
            template = (
                tgt.hotfix_path_template if (hotfix_mode and tgt.hotfix_path_template) else tgt.path_template
            ) or ""
            if not template:
                preview_lines.append(f"   [{prof}] {target_name}: plantilla vacía.")
                continue
            try:
                if "{version}" in template:
                    resolved = template.format(version=version)
                else:
                    resolved = str(Path(template) / version)
            except Exception as exc:  # pragma: no cover - defensivo
                preview_lines.append(
                    f"   [{prof}] {target_name}: error al formatear destino ({exc})"
                )
            else:
                preview_lines.append(f"   [{prof}] {target_name} → {resolved}")

        if preview_lines:
            self.log.append("<< Destinos de despliegue (usuario actual):")
            for line in preview_lines:
                self.log.append(line)

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

    @Slot(bool)
    def cancel_active_deploys(self, _checked: bool = False) -> None:
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
