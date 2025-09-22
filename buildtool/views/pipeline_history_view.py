from __future__ import annotations

from datetime import datetime
from pathlib import Path

from PySide6.QtCore import Qt, QDate, QSignalBlocker, Slot
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QComboBox,
    QDateEdit,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QFileDialog,
    QMessageBox,
)

from ..core.config import Config
from ..core.config_queries import iter_group_projects, iter_groups
from ..core.pipeline_history import PipelineHistory


class PipelineHistoryView(QWidget):
    """Muestra el historial de builds y deploys con filtros básicos."""

    def __init__(self, cfg: Config, parent=None) -> None:
        super().__init__(parent)
        self.cfg = cfg
        self.history = PipelineHistory()
        self.current_runs: list[dict] = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(10)

        filter_row = QHBoxLayout()
        filter_row.setSpacing(8)

        filter_row.addWidget(QLabel("Tipo:"))
        self.cboPipeline = QComboBox()
        self.cboPipeline.addItem("Todos", None)
        self.cboPipeline.addItem("Build", "build")
        self.cboPipeline.addItem("Deploy", "deploy")
        filter_row.addWidget(self.cboPipeline)

        filter_row.addWidget(QLabel("Estado:"))
        self.cboStatus = QComboBox()
        self.cboStatus.addItem("Todos", None)
        self.cboStatus.addItem("Exitoso", "success")
        self.cboStatus.addItem("Falló", "error")
        self.cboStatus.addItem("Cancelado", "cancelled")
        filter_row.addWidget(self.cboStatus)

        filter_row.addWidget(QLabel("Grupo:"))
        self.cboGroup = QComboBox()
        self.cboGroup.addItem("Todos", None)
        for grp in iter_groups(cfg):
            self.cboGroup.addItem(grp.key, grp.key)
        filter_row.addWidget(self.cboGroup)

        filter_row.addWidget(QLabel("Proyecto:"))
        self.cboProject = QComboBox()
        filter_row.addWidget(self.cboProject)

        filter_row.addWidget(QLabel("Desde:"))
        self.dtStart = QDateEdit()
        self.dtStart.setCalendarPopup(True)
        self.dtStart.setDate(QDate.currentDate().addDays(-7))
        filter_row.addWidget(self.dtStart)

        filter_row.addWidget(QLabel("Hasta:"))
        self.dtEnd = QDateEdit()
        self.dtEnd.setCalendarPopup(True)
        self.dtEnd.setDate(QDate.currentDate())
        filter_row.addWidget(self.dtEnd)

        self.btnRefresh = QPushButton("Refrescar")
        filter_row.addWidget(self.btnRefresh)
        self.btnExport = QPushButton("Exportar CSV…")
        filter_row.addWidget(self.btnExport)
        self.btnClear = QPushButton("Limpiar historial")
        filter_row.addWidget(self.btnClear)
        filter_row.addStretch(1)

        layout.addLayout(filter_row)

        self.table = QTableWidget(0, 12)
        self.table.setHorizontalHeaderLabels(
            [
                "Inicio",
                "Fin",
                "Pipeline",
                "Estado",
                "Grupo",
                "Proyecto",
                "Usuario",
                "Perfiles",
                "Módulos",
                "Versión",
                "Hotfix",
                "Mensaje",
            ]
        )
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self.table, 3)

        layout.addWidget(QLabel("Log del pipeline:"))
        self.txtLogs = QTextEdit()
        self.txtLogs.setReadOnly(True)
        self.txtLogs.setMinimumHeight(160)
        layout.addWidget(self.txtLogs, 1)

        self._group_project_keys: dict[str, set[str]] = {}
        for grp in iter_groups(cfg):
            self._group_project_keys[grp.key] = {
                project.key for _, project in iter_group_projects(cfg, grp.key)
            }
        all_projects: set[str] = set()
        for keys in self._group_project_keys.values():
            all_projects.update(keys)
        self._all_project_keys = sorted(all_projects)

        self._populate_project_combo(None)

        self.cboGroup.currentIndexChanged.connect(self._on_group_changed)
        self.btnRefresh.clicked.connect(self.refresh)
        self.btnExport.clicked.connect(self.export_csv)
        self.btnClear.clicked.connect(self.clear_history)
        self.table.itemSelectionChanged.connect(self._load_selected_logs)

        self.refresh()

    def _populate_project_combo(self, group_key: str | None) -> None:
        previous = self.cboProject.currentData()
        with QSignalBlocker(self.cboProject):
            self.cboProject.clear()
            self.cboProject.addItem("Todos", None)

            if group_key:
                project_keys = sorted(self._group_project_keys.get(group_key, set()))
            else:
                project_keys = list(self._all_project_keys)

            for key in project_keys:
                self.cboProject.addItem(key, key)

            idx = self.cboProject.findData(previous)
            if idx != -1:
                self.cboProject.setCurrentIndex(idx)
            else:
                self.cboProject.setCurrentIndex(0)

    @Slot()
    def _on_group_changed(self) -> None:
        group_key = self.cboGroup.currentData()
        self._populate_project_combo(group_key)

    def _filters(self) -> dict:
        start = datetime.combine(self.dtStart.date().toPython(), datetime.min.time())
        end = datetime.combine(self.dtEnd.date().toPython(), datetime.max.time())
        offset = datetime.now() - datetime.utcnow()
        start = (start - offset).replace(microsecond=0)
        end = (end - offset).replace(microsecond=0)
        filters: dict = {
            "pipeline": self.cboPipeline.currentData(),
            "status": self.cboStatus.currentData(),
            "group_key": self.cboGroup.currentData(),
            "project_key": self.cboProject.currentData(),
            "start": start,
            "end": end,
        }
        return {k: v for k, v in filters.items() if v is not None}

    @Slot()
    def refresh(self) -> None:
        filters = self._filters()
        runs = self.history.list_runs(**filters)
        self.current_runs = [r.__dict__ for r in runs]
        self.table.setRowCount(len(runs))

        for row_idx, run in enumerate(runs):
            self.table.setItem(row_idx, 0, QTableWidgetItem(run.started_at or ""))
            self.table.setItem(row_idx, 1, QTableWidgetItem(run.finished_at or ""))
            self.table.setItem(row_idx, 2, QTableWidgetItem(run.pipeline))
            self.table.setItem(row_idx, 3, QTableWidgetItem(run.status or ""))
            self.table.setItem(row_idx, 4, QTableWidgetItem(run.group_key or ""))
            self.table.setItem(row_idx, 5, QTableWidgetItem(run.project_key or ""))
            self.table.setItem(row_idx, 6, QTableWidgetItem(run.user or ""))
            self.table.setItem(row_idx, 7, QTableWidgetItem(", ".join(run.profiles)))
            self.table.setItem(row_idx, 8, QTableWidgetItem(", ".join(run.modules)))
            self.table.setItem(row_idx, 9, QTableWidgetItem(run.version or ""))
            self.table.setItem(row_idx, 10, QTableWidgetItem("Sí" if run.hotfix else "No"))
            self.table.setItem(row_idx, 11, QTableWidgetItem(run.message or ""))
            self.table.setRowHeight(row_idx, 22)
            self.table.item(row_idx, 0).setData(Qt.UserRole, run.id)

        if runs:
            self.table.selectRow(0)
        else:
            self.txtLogs.clear()

    @Slot()
    def _load_selected_logs(self) -> None:
        items = self.table.selectedItems()
        if not items:
            self.txtLogs.clear()
            return
        run_id = items[0].data(Qt.UserRole)
        if not run_id:
            self.txtLogs.clear()
            return
        logs = self.history.get_logs(int(run_id))
        self.txtLogs.clear()
        for ts, message in logs:
            self.txtLogs.append(f"[{ts}] {message}")

    @Slot(bool)
    def export_csv(self, _checked: bool = False) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Exportar historial", "historial.csv", "CSV (*.csv)")
        if not path:
            return
        self.history.export_csv(Path(path), **self._filters())
        QMessageBox.information(self, "Historial", "Exportación completada.")

    @Slot(bool)
    def clear_history(self, _checked: bool = False) -> None:
        reply = QMessageBox.question(
            self,
            "Limpiar historial",
            "¿Eliminar todos los registros del historial?",
        )
        if reply != QMessageBox.Yes:
            return
        self.history.clear()
        self.refresh()
