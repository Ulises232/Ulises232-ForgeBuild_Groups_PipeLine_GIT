from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTabWidget
from PySide6.QtCore import Qt
from .core.config import load_config, Config
from .views.pipeline_view import PipelineView
from .views.git_view import GitView
from .views.groups_wizard import GroupsWizard
from typing import Optional
from buildtool.core.thread_tracker import TRACKER


TAB_PIPELINE = "Pipeline"
TAB_GIT = "Repos (Git)"

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ForgeBuild (Grupos)")
        self.resize(1200, 760)
        self.cfg: Config = load_config()
        self._groups_win: Optional[GroupsWizard] = None
        root = QVBoxLayout(self); root.setContentsMargins(8,8,8,8); root.setSpacing(8)
        header = QHBoxLayout(); header.setSpacing(8)
        self.btnGroups = QPushButton("Config/Wizard"); header.addWidget(self.btnGroups); header.addStretch(); root.addLayout(header)
        self.tabs = QTabWidget(); self.tabs.setTabPosition(QTabWidget.North); self.tabs.setMovable(False); root.addWidget(self.tabs, 1)
        self.pipeline = PipelineView(self.cfg, self.reload_config); self.tabs.addTab(self.pipeline, TAB_PIPELINE)
        self.git = GitView(self.cfg, self); self.tabs.addTab(self.git, TAB_GIT)
        self.btnGroups.clicked.connect(self.open_groups)

    def reload_config(self):
        self.cfg = load_config(); idx = self.tabs.currentIndex()
        self.tabs.removeTab(1); self.git.deleteLater()
        self.tabs.removeTab(0); self.pipeline.deleteLater()
        self.pipeline = PipelineView(self.cfg, self.reload_config); self.git = GitView(self.cfg, self)
        self.tabs.insertTab(0, self.pipeline, TAB_PIPELINE); self.tabs.insertTab(1, self.git, TAB_GIT)
        self.tabs.setCurrentIndex(idx if idx < self.tabs.count() else 0)

    def open_groups(self):
        if self._groups_win is None:
            self._groups_win = GroupsWizard(self.cfg, self.reload_config)
            self._groups_win.setAttribute(Qt.WA_DeleteOnClose, True)
            self._groups_win.setWindowModality(Qt.ApplicationModal)
            self._groups_win.resize(820, 680)
            self._groups_win.destroyed.connect(lambda: setattr(self, "_groups_win", None))
            self._groups_win.show()
        else:
            if not self._groups_win.isVisible(): self._groups_win.show()
        self._groups_win.raise_(); self._groups_win.activateWindow()
    
    def closeEvent(self, event):
        # apaga todos los hilos ANTES de cerrar la UI
        try:
            TRACKER.stop_all(timeout_ms=7000)
        except Exception:
            pass
        super().closeEvent(event)
