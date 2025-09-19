from __future__ import annotations

from typing import Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QListWidget,
    QListWidgetItem,
    QHBoxLayout,
    QPushButton,
    QMessageBox,
    QInputDialog,
    QLabel,
)

from ..core.config import Config, PipelinePreset


class PresetManagerDialog(QDialog):
    """Permite renombrar o eliminar presets almacenados en la configuración."""

    def __init__(self, cfg: Config, pipeline: str, parent=None) -> None:
        super().__init__(parent)
        self.cfg = cfg
        self.pipeline = pipeline
        self.was_modified = False

        self.setWindowTitle(f"Presets de {pipeline.title()}")
        self.resize(420, 320)

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Selecciona un preset para editar:"))
        self.list = QListWidget()
        layout.addWidget(self.list, 1)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(8)
        self.btnRename = QPushButton("Renombrar…")
        self.btnDelete = QPushButton("Eliminar")
        btn_close = QPushButton("Cerrar")
        btn_row.addWidget(self.btnRename)
        btn_row.addWidget(self.btnDelete)
        btn_row.addStretch(1)
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)

        self.btnRename.clicked.connect(self.rename_selected)
        self.btnDelete.clicked.connect(self.delete_selected)
        btn_close.clicked.connect(self.accept)
        self.list.itemDoubleClicked.connect(lambda _: self.rename_selected())

        self.refresh_list()

    # ------------------------------------------------------------------
    def _presets(self) -> list[PipelinePreset]:
        return [p for p in (self.cfg.pipeline_presets or []) if p.pipeline == self.pipeline]

    def refresh_list(self) -> None:
        self.list.clear()
        presets = self._presets()
        presets.sort(key=lambda p: p.name.lower())
        for preset in presets:
            extra = []
            if preset.group_key:
                extra.append(preset.group_key)
            if preset.project_key:
                extra.append(preset.project_key)
            label = preset.name
            if extra:
                label += f" — {' / '.join(extra)}"
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, preset)
            self.list.addItem(item)

    def _current_preset(self) -> Optional[PipelinePreset]:
        item = self.list.currentItem()
        if not item:
            return None
        return item.data(Qt.UserRole)

    def rename_selected(self) -> None:
        preset = self._current_preset()
        if not preset:
            QMessageBox.information(self, "Presets", "Selecciona un preset para renombrar.")
            return

        new_name, ok = QInputDialog.getText(
            self,
            "Renombrar preset",
            "Nuevo nombre:",
            text=preset.name,
        )
        if not ok:
            return
        new_name = new_name.strip()
        if not new_name:
            QMessageBox.warning(self, "Presets", "El nombre no puede quedar vacío.")
            return

        other = next(
            (
                p
                for p in self._presets()
                if p is not preset and p.name.lower() == new_name.lower()
            ),
            None,
        )
        if other:
            QMessageBox.warning(
                self,
                "Presets",
                "Ya existe un preset con ese nombre.",
            )
            return

        preset.name = new_name
        self.was_modified = True
        self.refresh_list()

    def delete_selected(self) -> None:
        preset = self._current_preset()
        if not preset:
            QMessageBox.information(self, "Presets", "Selecciona un preset para eliminar.")
            return

        reply = QMessageBox.question(
            self,
            "Eliminar preset",
            f"¿Eliminar definitivamente '{preset.name}'?",
        )
        if reply != QMessageBox.Yes:
            return

        try:
            self.cfg.pipeline_presets.remove(preset)
        except ValueError:
            return

        self.was_modified = True
        self.refresh_list()
