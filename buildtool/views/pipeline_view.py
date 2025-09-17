from __future__ import annotations
from PySide6.QtWidgets import (
    QLabel,
    QHBoxLayout,
    QScrollArea,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)
from ..core.config import Config
from .build_view import BuildView
from .deploy_view import DeployView
from ..ui.icons import get_icon

class PipelineView(QWidget):
    def __init__(self, cfg: Config, on_request_reload_config):
        super().__init__()
        self.cfg = cfg
        self.on_request_reload_config = on_request_reload_config
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(12)
        icon = QLabel()
        icon.setPixmap(get_icon("pipeline").pixmap(32, 32))
        header.addWidget(icon)
        title = QLabel("Pipeline de compilación y despliegue")
        title.setProperty("role", "title")
        header.addWidget(title)
        header.addStretch(1)
        root.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        self.tabs.setMovable(False)
        root.addWidget(self.tabs, 1)

        self.build_view = BuildView(self.cfg, self.on_request_reload_config)
        self.deploy_view = DeployView(self.cfg)
        self.tabs.addTab(self._wrap_in_scroll(self.build_view), get_icon("build"), "Build")
        self.tabs.addTab(self._wrap_in_scroll(self.deploy_view), get_icon("deploy"), "Deploy")

    def _wrap_in_scroll(self, widget: QWidget) -> QScrollArea:
        area = QScrollArea()
        area.setWidgetResizable(True)
        area.setFrameShape(QScrollArea.NoFrame)
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(widget)
        area.setWidget(container)
        return area

    def closeEvent(self, event):
        for child in (self.build_view, self.deploy_view):
            try:
                child.close()
            except Exception:
                pass
        super().closeEvent(event)
