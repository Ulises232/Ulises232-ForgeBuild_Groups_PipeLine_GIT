from typing import Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QSplitter,
    QTabWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from buildtool import __version__
from buildtool.core.thread_tracker import TRACKER
from .core.config import load_config, Config
from .views.pipeline_view import PipelineView
from .views.git_view import GitView
from .views.groups_wizard import GroupsWizard
from .views.sprint_view import SprintView
from .ui.icons import get_icon
from .ui.theme import apply_theme, ThemeMode


TAB_PIPELINE = "Pipeline"
TAB_GIT = "Repos (Git)"
TAB_SPRINTS = "Sprints"

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setObjectName("MainWindow")
        self.setWindowTitle(f"ForgeBuild (Grupos) v{__version__}")
        self.resize(1280, 780)
        self.cfg: Config = load_config()
        self._groups_win: Optional[GroupsWizard] = None
        self._ensure_theme()

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        splitter = QSplitter(Qt.Vertical)
        splitter.setObjectName("mainSplitter")
        splitter.setHandleWidth(2)
        root.addWidget(splitter)

        header_widget = QWidget()
        header_widget.setObjectName("headerPanel")
        header_widget.setMaximumHeight(96)
        header_widget.setMinimumHeight(80)
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(16, 16, 16, 16)
        header_layout.setSpacing(12)

        icon_label = QLabel()
        icon_label.setPixmap(get_icon("app").pixmap(48, 48))
        header_layout.addWidget(icon_label)

        title_block = QVBoxLayout()
        title_block.setContentsMargins(0, 0, 0, 0)
        title_block.setSpacing(4)
        title = QLabel("ForgeBuild")
        title.setProperty("role", "title")
        subtitle = QLabel(f"Grupos — v{__version__}")
        subtitle.setProperty("role", "subtitle")
        title_block.addWidget(title)
        title_block.addWidget(subtitle)
        header_layout.addLayout(title_block, 1)

        self.btnGroups = QToolButton()
        self.btnGroups.setText("Config/Wizard")
        self.btnGroups.setIcon(get_icon("config"))
        self.btnGroups.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self.btnGroups.setAutoRaise(True)
        header_layout.addWidget(self.btnGroups)

        splitter.addWidget(header_widget)

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(12)

        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        self.tabs.setMovable(False)
        content_layout.addWidget(self.tabs)

        self.pipeline = PipelineView(self.cfg, self.reload_config)
        self.git = GitView(self.cfg, self)
        self.sprints = SprintView(self)
        self.tabs.addTab(self.pipeline, get_icon("pipeline"), TAB_PIPELINE)
        self.tabs.addTab(self.git, get_icon("git"), TAB_GIT)
        self.tabs.addTab(self.sprints, get_icon("history"), TAB_SPRINTS)

        splitter.addWidget(content)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([header_widget.sizeHint().height(), 640])
        self.btnGroups.clicked.connect(self.open_groups)

    def reload_config(self):
        self.cfg = load_config()
        idx = self.tabs.currentIndex()
        for widget in (getattr(self, "sprints", None), getattr(self, "git", None), getattr(self, "pipeline", None)):
            if widget is None:
                continue
            tab_index = self.tabs.indexOf(widget)
            if tab_index >= 0:
                self.tabs.removeTab(tab_index)
            widget.deleteLater()
        self.pipeline = PipelineView(self.cfg, self.reload_config)
        self.git = GitView(self.cfg, self)
        self.sprints = SprintView(self)
        self.tabs.insertTab(0, self.pipeline, get_icon("pipeline"), TAB_PIPELINE)
        self.tabs.insertTab(1, self.git, get_icon("git"), TAB_GIT)
        self.tabs.insertTab(2, self.sprints, get_icon("history"), TAB_SPRINTS)
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

    def _ensure_theme(self, mode: ThemeMode = "auto") -> None:
        apply_theme(mode)

    def set_theme_mode(self, mode: ThemeMode) -> str:
        """Expose theme switching for future UI toggles."""

        return apply_theme(mode)
